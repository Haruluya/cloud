## CLOUD OFFLINE REC

**基于spark sql的基本离线统计**

此模块完成基本的热门统计，复杂性低，只是简单的利用spark sql统计评分次数多的商品，近期评分次数多的商品，和商品的平均评分。

此功能并非必须在此系统中完成，但是借助spark sql会更为简单。

#### 主要步骤

基于spark sql可以独立完成。

**数据加载：**

```scala
val cloudRatingDF = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_CLOUD_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Cloud_Rating]
                .toDF()
```

**基于评分数量的历史热门商品统计：**

```scala
val highRateProductsDF = spark.sql(
            "select productId, count(productId) as count from ratings group by productId order by count desc")
        storeDateFormateInDB( highRateProductsDF, HIGH_RATE_CLOUD_PRODUCTS )
```

**基于时间戳的近期热门商品统计：**

```scala
  val simpleDateFormat = new SimpleDateFormat("yyyyMM")
        spark.udf.register("changeDate", (x: Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt)
        val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")
        ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
        val rateMoreRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearmonth from ratingOfMonth group by yearmonth, productId order by yearmonth desc, count desc")
        storeDateFormateInDB( rateMoreRecentlyProductsDF, HIGH_RATE_RECENTLY_CLOUD_PRODUCTS )
```

**商品的平均评分统计：**

```scala
 val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
        storeDateFormateInDB( averageProductsDF, AVERAGE_RATE_CLOUD_PRODUCTS )
```

