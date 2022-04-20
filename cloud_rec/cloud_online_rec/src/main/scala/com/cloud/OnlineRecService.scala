package com.cloud

import com.mongodb.casbah.commons.MongoDBObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author haruluya
 * 实时推荐服务。
 */
object OnlineRecService {
  val DB_RATING_COLLECTION_NAME= "Cloud_Rating"
  val STREAM_RECS = "onlineRecs"
  val PRODUCT_RECS = "Cloud_Similarity_Product"
  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20
  val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://localhost:27017/recommender",
    "mongo.db" -> "recommender",
    "kafka.topic" -> "recommender"
  )
  //kafaka配置常数。
  val kafkaParam = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "recommender",
    "auto.offset.reset" -> "latest"
  )

  
  def main(args: Array[String]): Unit = {
    // 环境配置。
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext = spark.sparkContext
    val sparkStreamContext = new StreamingContext(sparkContext, Seconds(2))
    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    // 数据加载。获取商品相似度矩阵。
    val simProductsMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Cloud_Similarity_Product]
      .rdd
      .map{item =>
        ( item.productId, item.recs.map( x=>(x.productId, x.score) ).toMap )
      }
      .collectAsMap()
    
    // 广播变量
    val simProcutsMatrixBroadCast = sparkContext.broadcast(simProductsMatrix)


    // kafkaStream生成.
    val kafkaStream = KafkaUtils.createDirectStream[String, String]( sparkStreamContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String]( Array(config("kafka.topic")), kafkaParam )
    )

    // 产生评分流，userId|productId|score|timestamp
    val ratingStream = kafkaStream.map{msg=>
      var attr = msg.value().split("\\|")
      ( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    }

    // 定义评分流的处理流程。
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case ( userId, productId, score, timestamp ) =>
          println("rating data__________________")

          // 获取redis数据。
          val userRecentlyRatings = getNRecentRatings( MAX_USER_RATING_NUM, userId, Connection_Serializable.jedis )

          // 获取当前商品最相似的商品列表.
          val candidateProducts = getHighSimProducts( MAX_SIM_PRODUCTS_NUM, productId, userId, simProcutsMatrixBroadCast.value )

          // 计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表.
          val streamRecs = getProductRecScore( candidateProducts, userRecentlyRatings, simProcutsMatrixBroadCast.value )

          //存入数据库。
          saveDataToDB( userId, streamRecs )
      }
    }
    // 启动streaming
    sparkStreamContext.start()
    println("spark streaming begin:")
    sparkStreamContext.awaitTermination()

  }


  import scala.collection.JavaConversions._


  /**
   * 从redis中获取最近num次评分数据。
   * @param num 数据条数
   * @param userId  用户id
   * @param jedis redis配置
   * @return  评分数据列表
   */
  def getNRecentRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    // key: [uid:USERID]  value: [PRODUCTID:SCORE]
    jedis.lrange( "userId:" + userId.toString, 0, num )
      .map{ item =>
        val attr = item.split("\\:")
        ( attr(0).trim.toInt, attr(1).trim.toDouble )
      }
      .toArray
  }


  /**
   * 获取当前商品的相似列表，并过滤掉用户已经评分过的，作为备选列表
   * @param num 所需相似商品数量
   * @param productId 商品id
   * @param userId  用户id
   * @param simProducts 商品相似矩阵
   * @param mongoConfig mongodb配置对象
   * @return  相似商品列表
   */
  def getHighSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] ={
    // 获取相似度列表。
    val allSimProducts = simProducts(productId).toArray

    // 过滤已经评分商品。
    val ratingCollection = Connection_Serializable.mongoClient( mongoConfig.db )( DB_RATING_COLLECTION_NAME)
    val ratingExist = ratingCollection.find( MongoDBObject("userId"->userId) )
      .toArray
      .map{item=>
        item.get("productId").toString.toInt
      }
    allSimProducts.filter( x => ! ratingExist.contains(x._1) )
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x=>x._1)
  }



  /**
   * 计算商品的推荐指数。
   * @param candidateProducts 候选商品。
   * @param userRecentlyRatings 用户最近评分。
   * @param simProducts 相似商品。
   * @return  按得分排序的推荐列表。
   */
  def getProductRecScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] ={
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    // 遍历每个备选商品，计算和已评分商品的相似度
    for( candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings ){
      val simScore = getProductsSimScore( candidateProduct, userRecentlyRating._1, simProducts )
      if( simScore > 0.4 ){
        // 公式计算。
        scores += ( (candidateProduct, simScore * userRecentlyRating._2) )
        if( userRecentlyRating._2 > 3 ){
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct, 0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct, 0) + 1
        }
      }
    }
    scores.groupBy(_._1).map{
      case (productId, scoreList) =>
        ( productId, scoreList.map(_._2).sum/scoreList.length + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)) )
    }
      .toArray
      .sortWith(_._2>_._2)
  }


  /**
   *  获取两个商品之间的相似度。
   * @param userRatingProduct 用户已经评分商品。
   * @param candidateProduct 候选商品
   * @param simProducts 商品相似度矩阵。
   * @return
   */
  def getProductsSimScore(userRatingProduct: Int, candidateProduct: Int,
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double ={
    simProducts.get(userRatingProduct) match {
        case Some(sims) => sims.get(candidateProduct) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }



  /**
   * log 函数。
   * @param m 指数。
   * @return
   */
  def log(m: Int): Double = {
    val N = 10
    math.log(m)/math.log(N)
  }




  /**
   *  数据存储至数据库。
   * @param userId  用户id。
   * @param streamRecs  推荐流。
   * @param mongoConfig 配置文件。
   */
  def saveDataToDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    val streamRecsCollection = Connection_Serializable.mongoClient(mongoConfig.db)(STREAM_RECS)
    streamRecsCollection.findAndRemove( MongoDBObject( "userId" -> userId ) )
    streamRecsCollection.insert( MongoDBObject( "userId" -> userId,
      "recs" -> streamRecs.map(x=>MongoDBObject("productId"->x._1, "score"->x._2)) ) )
  }

}
