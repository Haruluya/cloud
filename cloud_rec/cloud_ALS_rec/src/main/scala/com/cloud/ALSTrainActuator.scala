package com.cloud

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object ALSTrainActuator {
    val DB_RATING_COLLECTION_NAME = "Cloud_Rating"
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

    // 数据加载。
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", DB_RATING_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Cloud_Rating]
      .rdd
      .map(
        rating => Rating(rating.userId, rating.productId, rating.score)
      ).cache()


    // 取训练集和测试集。
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingCollectionRDD = splits(0)
    val testingCollectionRDD = splits(1)

    // 输出最优参数
    getSuitedALSParams( trainingCollectionRDD, testingCollectionRDD )

    spark.stop()
  }

  /**
   *获取数据的RMSE值。
   * @param model 训练模型
   * @param data  测试集
   * @return  RMSE值
   */
  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // 预测评分矩阵
    val userProducts = data.map( item=> (item.user, item.product) )
    val predictRating = model.predict(userProducts)

    // 计算rmse
    val observed = data.map( item=> ( (item.user, item.product),  item.rating ) )
    val predict = predictRating.map( item=> ( (item.user, item.product),  item.rating ) )
    sqrt(
      observed.join(predict).map{
        case ( (userId, productId), (actual, pre) ) =>
          val err = actual - pre
          err * err
      }.mean()
    )
  }

  /**
   * 根据RMSE的值返回合适的参数
   * @param trainCollectionData 训练集
   * @param testCollectionData  测试集
   */
  def getSuitedALSParams(trainCollectionData: RDD[Rating], testCollectionData: RDD[Rating]): Unit ={
    val result = for( rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01) )
      yield {
        val model = ALS.train(trainCollectionData, rank, 10, lambda)
        val rmse = getRMSE( model, testCollectionData )
        ( rank, lambda, rmse )
      }
    println(result.minBy(_._3))
  }



}
