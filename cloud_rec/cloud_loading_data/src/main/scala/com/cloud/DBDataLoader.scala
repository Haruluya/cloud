package com.cloud

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * 数据加载类，将数据源数据存储到数据库中。
 * @author haruluya 2022/4/14
 */
object DBDataLoader {
  // 数据文件路径
  val CLOUD_PRODUCT_DATA_PATH = "F:\\Note-Haruluya\\Project\\Cloud\\cloud_rec\\cloud_loading_data\\src\\main\\resources\\products.csv"
  val CLOUD_RATING_DATA_PATH = "F:\\Note-Haruluya\\Project\\Cloud\\cloud_rec\\cloud_loading_data\\src\\main\\resources\\ratings.csv"
  // 数据库表名
  val MONGODB_CLOUD_PRODUCT_COLLECTION = "Cloud_Product"
  val MONGODB_CLOUD_RATING_COLLECTION = "Cloud_Rating"


  def main(args: Array[String]): Unit = {
    //类配置储存。
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/cloud",
      "mongo.db" -> "cloud"
    )

    //spark环境。
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._


    // 加载数据
    val cloud_productRDD = spark.sparkContext.textFile(CLOUD_PRODUCT_DATA_PATH)
    val cloud_productDF = cloud_productRDD.map( item => {
      val attr = item.split("\\^")
      // 转换成实体类。
      Cloud_Product( attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim )
    } ).toDF()

    val cloud_ratingRDD = spark.sparkContext.textFile(CLOUD_RATING_DATA_PATH)
    val cloud_ratingDF = cloud_ratingRDD.map( item => {
      val attr = item.split(",")
      Cloud_Rating( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    } ).toDF()


    //存入数据库。
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )
    storeDataInMongoDB( cloud_productDF, cloud_ratingDF )


    spark.stop()
  }



  /**
   * 将格式化数据储存mongodb中。
   * @param cloud_productDF product dataformat
   * @param cloud_ratingDF  rating dataformat
   * @param mongoConfig mongodb configuration
   */
  def storeDataInMongoDB( cloud_productDF: DataFrame, cloud_ratingDF: DataFrame )(implicit mongoConfig: MongoConfig): Unit ={
    //环境配置。
    val mongoClient = MongoClient( MongoClientURI(mongoConfig.uri) )
    val productCollection = mongoClient( mongoConfig.db )( MONGODB_CLOUD_PRODUCT_COLLECTION )
    val ratingCollection = mongoClient( mongoConfig.db )( MONGODB_CLOUD_RATING_COLLECTION )
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //写入数据库。
    cloud_productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_CLOUD_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    cloud_ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_CLOUD_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 创建索引。
    productCollection.createIndex( MongoDBObject( "productId" -> 1 ) )
    ratingCollection.createIndex( MongoDBObject( "productId" -> 1 ) )
    ratingCollection.createIndex( MongoDBObject( "userId" -> 1 ) )

    mongoClient.close()
  }
}
