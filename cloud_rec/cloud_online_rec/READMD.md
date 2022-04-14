## CLOUD ONLINE REC

**实施推荐服务**

实时推荐服务无法单纯用ALS算法完成，因为ALS算法计算速度缓慢，并且影响其结果为所有评分数据，而实时推荐服务需要更具用户上一次或多次的评价来完成动态的推荐。

#### 算法设计

##### 影响因素

实时推荐服务的影响因素包括：

①用户上K次的评分行为。
②实时推荐服务计算的上一次的结果。

##### **主要步骤**

- 当用户对商品a做出评价时，选取商品中和a最相似的n个商品。
- 对这n个商品计算推荐指数。
- 将计算结果和上一次计算结果进行合并。

##### 推荐指数

推荐指数计算公式：

![img](file:///C:\Users\24558\AppData\Local\Temp\ksohtml\wps1602.tmp.jpg)

其**Rr**表示用户u 对商品r 的评分

**sim(q,r**)表示商品q 与商品r 的相似度，设定最小相似度为0.6，当商品q和商品r 相似度低于0.6 的阈值，则视为两者不相关并忽略。

**sim_sum** 表示q 与RK 中商品相似度大于最小阈值的个数。

**incount** 表示RK 中与商品q 相似的、且本身评分较高（>=3）的商品个数。

**recount** 表示RK 中与商品q 相似的、且本身评分较低（<3）的商品个数。

**lgmax{incount,1}**

意义：

式表示相似商品高评分对推荐指数的影响。即商品q 与u用户的最近K 个评分中的n 个高评分(>=3)商品相似，则商品q 的优先级被增加lgmax{incount,1}。

计算方法：

将u 最近的K 个评分中与商品q 相似的、且本身评分较高（>=3）的商品个数记为 incount，从而计算出igmax的值。

**lgmax{recount,1}**

意义：

式表示相似商品低评分对推荐指数的影响。商品q 与u 的最近K 个评分中的n 个低评分(<3)商品相似，则商品q 的优先级被削减lgmax{incount,1}。

计算方法：

如上述。

##### 合并计算

计算完每一个商品的推荐指数后，需要和上一次计算的{id，Euq}列表进行合并，其合并方法为：

![img](file:///C:\Users\24558\AppData\Local\Temp\ksohtml\wps37AB.tmp.jpg)

（即取高推荐值）

其中，i表示updated_S 与Rec 的商品集合中的每个商品，topK 是一个函数，表示从 Rec U updated _ S中选择出最大的 K 个商品，cmp = Eui 表示topK 函数将推荐指数值最大的K 个商品选出来。最终，NewRec 即为经过用户u 对商品p 评分后触发的实时推荐得到的最新推荐结果。

#### 实现方式

##### 数据来源

①在电商项目中，Redis集群中已经存储了每一个用户最近对商品的K次评分。实时算法可以快速获取。

②已经利用ALS算法将商品的相似度矩阵计算到数据库中。实时推荐可以随时获取。

③利用kafka获取用户的实时评分数据。

#### 实现过程

算法过程如下：

实时推荐算法输入为一个评分<userId, productId, rate, timestamp>，而执行的核心内容包括：获取userId 最近K 次评分、获取productId 最相似K 个商品、计算候选商品的推荐优先级、更新对userId 的实时推荐结果。

##### 获取最近K次评分

```scala
  /**
   * 从redis里获取最近num次评分
   */
  import scala.collection.JavaConversions._
  def getNRecentRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    // key: [uid:USERID]  value: [PRODUCTID:SCORE]
    jedis.lrange( "userId:" + userId.toString, 0, num )
      .map{ item =>
        val attr = item.split("\\:")
        ( attr(0).trim.toInt, attr(1).trim.toDouble )
      }
      .toArray
  }
```

##### 获取评分商品的最相似K个商品

```scala
	def getHighSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] ={
    val allSimProducts = simProducts(productId).toArray
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
```

##### 计算商品的推荐指数

```scala
def getProductRecScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] ={
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()
    
    for( candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings ){

      val simScore = getProductsSimScore( candidateProduct, userRecentlyRating._1, simProducts )
      if( simScore > 0.4 ){
        scores += ( (candidateProduct, simScore * userRecentlyRating._2) )
        if( userRecentlyRating._2 > 3 ){
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct, 0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct, 0) + 1
        }
      }
    }
```

##### 存储数据

```scala
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
```

