package com.cloud

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 * 基于Item-CF的离线商品相似推荐。
 * @author haruluya
 */
object ItemCFRecService{
  // 定义常量和表名
  val DB_RATING_COLLECTION_NAME = "Cloud_Rating"
  val ITEM_CF_PRODUCT_RECS = "Cloud_ItemCFProductRecs"
  val MAX_RECOMMENDATION = 10

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/cloud",
      "mongo.db" -> "cloud"
    )

    //环境搭建。
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ItemCFRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )


    // 数据处理。
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", DB_RATING_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Cloud_Rating]
      .map(
        x => ( x.userId, x.productId, x.score )
      )
      .toDF("userId", "productId", "score")
      .cache()
    val productRatingCountDF = ratingDF.groupBy("productId").count()
    val ratingWithCountDF = ratingDF.join(productRatingCountDF, "productId")


    // 评分两两配对，统计两个商品被同一个用户评分过的次数
    val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId","product1","score1","count1","product2","score2","count2")
      .select("userId","product1","count1","product2","count2")
    joinedDF.createOrReplaceTempView("joined")
    val cooccurrenceDF = spark.sql(
      """
        |select product1
        |, product2
        |, count(userId) as cocount
        |, first(count1) as count1
        |, first(count2) as count2
        |from joined
        |group by product1, product2
      """.stripMargin
    ).cache()

    // 数据包装。
    val simDF = cooccurrenceDF.map{
      row =>
        val coocSim = cooccurrenceSim( row.getAs[Long]("cocount"), row.getAs[Long]("count1"), row.getAs[Long]("count2") )
        ( row.getInt(0), ( row.getInt(1), coocSim ) )
    }
      .rdd
      .groupByKey()
      .map{
        case (productId, recs) =>
          Cloud_Similarity_Product( productId, recs.toList
            .filter(x=>x._1 != productId)
            .sortWith(_._2>_._2)
            .take(MAX_RECOMMENDATION)
            .map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()


    // 数据存储。
    simDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", ITEM_CF_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }
  def cooccurrenceSim(coCount: Long, count1: Long, count2: Long): Double ={
    coCount / math.sqrt( count1 * count2 )
  }
}
