package com.cloud

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix








object ALSRecService {
  
  //表名配置。
  val DB_RATING_COLLECTION_NAME = "Cloud_Rating"
  val DB_CLOUD_REC_USER_COLLECTION_NAME = "Cloud_Rec_User"
  val DB_CLOUD_SIMILARITY_PRODUCT_COLLECTION_NAME = "Cloud_Similarity_Product"
  val CLOUD_REC_USER_MAX_LENGTH = 20
  val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://localhost:27017/cloud",
    "mongo.db" -> "cloud"
  )
  
  
  def main(args: Array[String]): Unit = {

    //环境配置。
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    // RDD创建。
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", DB_RATING_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Cloud_Rating]
      .rdd
      .map(
        rating => (rating.userId, rating.productId, rating.score)
      ).cache()

    
    // 提取数据集
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()

    

    // 训练隐语义模型
    val alsTrainedData = ratingRDD.map(x=>Rating(x._1,x._2,x._3))
    // rank   iterations    lambda
    // :隐特征个数，迭代次数，正则化系数
    val ( rank, iterations, lambda ) = ( 5, 10, 0.01 )
    val model = ALS.train( alsTrainedData, rank, iterations, lambda )

    // 预测评分矩阵
    val userProducts = userRDD.cartesian(productRDD)
    val preRating = model.predict(userProducts)

    // 提取用户推荐列表
    val userRecs = preRating.filter(_.rating>0)
      .map(
        rating => ( rating.user, ( rating.product, rating.rating ) )
      )
      .groupByKey()
      .map{
        case (userId, recs) =>
          Cloud_Rec_User( userId, recs.toList.sortWith(_._2>_._2).take(CLOUD_REC_USER_MAX_LENGTH).map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", DB_CLOUD_REC_USER_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    // 计算相似度列表
    val productFeatures = model.productFeatures.map{
      case (productId, features) => ( productId, new DoubleMatrix(features) )
    }


    //计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{
        case (a, b) => a._1 != b._1
      }
      .map{
        case (a, b) =>
          val simScore = (a._2).dot(b._2)/ ( (a._2).norm2() * (b._2).norm2() )
          ( a._1, ( b._1, simScore ) )
      }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map{
        case (productId, recs) =>
          Cloud_Similarity_Product( productId, recs.toList.sortWith(_._2>_._2).map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()



    //写入数据库。
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", DB_CLOUD_SIMILARITY_PRODUCT_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    spark.stop()
  }

}
