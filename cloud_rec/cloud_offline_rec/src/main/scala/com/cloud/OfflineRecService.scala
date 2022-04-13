package com.cloud

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author haruluya
 * 基于spark sql 的简单离线评分统计，包括：
 * 统计评分次数多-->热门。配合时间戳-->近期热门。平均-->平均评分。
 */
object OfflineRecService {

    //表名。
    val MONGODB_CLOUD_RATING_COLLECTION = "Cloud_Rating"
    //服务名。
    val HIGH_RATE_CLOUD_PRODUCTS = "HighRateCloudProducts"
    val HIGH_RATE_RECENTLY_CLOUD_PRODUCTS = "HighRateRecentyCloudProducts"
    val AVERAGE_RATE_CLOUD_PRODUCTS = "AverageRateCloudProducts"
    //全局配置。
    val config = Map(
        "spark.cores" -> "local[1]",
        "mongo.uri" -> "mongodb://localhost:27017/cloud",
        "mongo.db" -> "cloud"
    )

    def main(args: Array[String]): Unit = {

        //spark环境。
        val sparkConf = new SparkConf()
                            .setMaster(config("spark.cores"))
                            .setAppName("OfflineRecService")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        //mongodb配置。
        implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

        // 数据加载。
        val cloudRatingDF = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_CLOUD_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Cloud_Rating]
                .toDF()

        // 临时表。
        cloudRatingDF.createOrReplaceTempView("ratings")

        // 基于评分数量的历史热门商品统计。
        val highRateProductsDF = spark.sql(
            "select productId, count(productId) as count from ratings group by productId order by count desc")
        storeDateFormateInDB( highRateProductsDF, HIGH_RATE_CLOUD_PRODUCTS )

        //基于时间戳的近期热门商品统计。
        val simpleDateFormat = new SimpleDateFormat("yyyyMM")
        spark.udf.register("changeDate", (x: Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt)
        val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")
        ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
        val rateMoreRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearmonth from ratingOfMonth group by yearmonth, productId order by yearmonth desc, count desc")
        storeDateFormateInDB( rateMoreRecentlyProductsDF, HIGH_RATE_RECENTLY_CLOUD_PRODUCTS )

        // 商品的平均评分统计。
        val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
        storeDateFormateInDB( averageProductsDF, AVERAGE_RATE_CLOUD_PRODUCTS )

        spark.stop()
    }


    /**
     * 保存数据到数据库中。
     * @param df    dateformat
     * @param collection_name 集合名
     * @param mongoConfig   配置对象
     */
    def storeDateFormateInDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={
        df.write
                .option("uri", mongoConfig.uri)
                .option("collection", collection_name)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()
    }
}
