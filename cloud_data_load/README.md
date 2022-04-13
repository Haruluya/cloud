## CLOUD_DATA_LOAD

**基于Spark sql的noSQL数据库存取。**

此模块负责将外源数据导入到mongoDB中。虽然实际应用时项目使用的数据库应该为电商项目长期维护的，但鉴于完整性和测试需求，外源载入数据模块也需要存在。

#### 主要步骤

依靠spark sql的基本语法可以简单的完成需求。

**mongoDB配置：**

```scala
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/cloud",
      "mongo.db" -> "cloud"
    )
```

**数据加载和转换：**

```scala
    val cloud_productRDD = spark.sparkContext.textFile(CLOUD_PRODUCT_DATA_PATH)
    val cloud_productDF = cloud_productRDD.map(item => {
      val attr = item.split("\\^")
      // 转换成实体类。
      Cloud_Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val cloud_ratingRDD = spark.sparkContext.textFile(CLOUD_RATING_DATA_PATH)
    val cloud_ratingDF = cloud_ratingRDD.map(item => {
      val attr = item.split(",")
      Cloud_Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()
```

**mongoDB连接：**

```scala
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_CLOUD_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_CLOUD_RATING_COLLECTION)
    productCollection.dropCollection()
    ratingCollection.dropCollection()
```

**数据存储：**

```scala
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
```

**创建索引：**

```scala
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))
```

