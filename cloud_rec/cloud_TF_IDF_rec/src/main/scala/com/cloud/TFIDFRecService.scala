package com.cloud

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
 * 基于TF-IDF算法的离线推荐。
 * @author haruluya
 */
object TFIDFRecService {
  val DB_PRODUCT_COLLECTION_NAME = "Cloud_Product"
  val CONTENT_PRODUCT_RECS = "Cloud_ContentBasedProductRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/cloud",
      "mongo.db" -> "cloud"
    )

    //环境构建。
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )


    // 数据处理。
    val productTagsDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", DB_PRODUCT_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Cloud_Product]
      .map(
        x => ( x.productId, x.name, x.tags.map(c=> if(c=='|') ' ' else c) )
      )
      .toDF("productId", "name", "tags")
      .cache()

    // TF-IDF提取商品特征向量
    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    val wordsDataDF = tokenizer.transform(productTagsDF)

    // 定义HashingTF，计算频次
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
    val featurizedDataDF = hashingTF.transform(wordsDataDF)

    // 定义IDF工具，计算TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练模型
    val idfModel = idf.fit(featurizedDataDF)
    val rescaledDataDF = idfModel.transform(featurizedDataDF)

    // 得到RDD形式的features
    val productFeatures = rescaledDataDF.map{
      row => ( row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray )
    }
      .rdd
      .map{
        case (productId, features) => ( productId, new DoubleMatrix(features) )
      }

    // 计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{
        case (a, b) => a._1 != b._1
      }
      .map{
        case (a, b) =>
          val simScore = consinSim( a._2, b._2 )
          ( a._1, ( b._1, simScore ) )
      }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map{
        case (productId, recs) =>
          Cloud_Similarity_Product( productId, recs.toList.sortWith(_._2>_._2).map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", CONTENT_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }


  /**
   * 计算两商品的余弦相似度。
   * @param product1 商品1
   * @param product2  商品2
   * @return
   */
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double ={
    product1.dot(product2)/ ( product1.norm2() * product2.norm2() )
  }
}
