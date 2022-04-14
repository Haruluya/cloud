## CLOUD ITEM CF REC

**基于Item-CF的离线商品相似推荐**

如果两个商品有同样的受众，那么它们就是有内在相关性的。所以可以利用已有的行为数据，分析商品受众的相似程度，进而得出商品间的相似度。于是基于物品的协同过滤（Item-CF），只需收集用户的常规行为数据，比如点击、收藏、购买，就可以得到商品间的相似度。

#### Item-CF

ItemCollaborationFilter，基于物品的协同过滤。

算法的核心思想:给用户推荐那些和他们之前喜欢的物品相似的物品，Item-cf本身很简单，有很多优化方法，鉴于能力有限，此模块并未进行优化操作。

**相似度**

在item-cf中相似度定义如下：

![img](https://img-blog.csdnimg.cn/20190215135300829.png)

其中，|N(i)|是喜欢物品i的用户集，|N(j)|是喜欢物品j的用户集，|N(i)&N(j)|是同时喜欢物品i和物品j的用户集。于是在协同过滤中两个物品产生相似度是因为它们共同被很多用户喜欢，两个物品相似度越高，说明这两个物品共同被很多人喜欢。



#### 实现步骤

##### **数据准备**

```scala
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
```

##### 数据统计

```scala
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
```

##### **计算相似度**

```scala
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
```

##### 数据存储

```scala
    simDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", ITEM_CF_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
```

