## CLOUD TF-IDF REC

**基于TF-IDF算法的内容离线相似推荐**

此模块实现通过商品的标签等描述性信息，同时利用TF-IDF算法对标签的权重进行调整，从而尽可能地接近用户偏好，获取商品的相似推荐。

#### TF-IDF

  TF-IDF（term frequency–inverse document frequency，词频-逆向文件频率）是一种用于信息检索（information retrieval）与文本挖掘（text mining）的常用加权技术。

   TF-IDF是一种统计方法，用以评估一字词对于一个文件集或一个语料库中的其中一份文件的重要程度。字词的重要性随着它在文件中出现的次数成正比增加，但同时会随着它在语料库中出现的频率成反比下降。

   TF-IDF的主要思想是：如果某个单词在一篇文章中出现的频率TF高，并且在其他文章中很少出现，则认为此词或者短语具有很好的类别区分能力，适合用来分类。

**TF**

表示词条（关键字）在文本中出现的频率。

这个数字通常会被归一化(一般是词频除以文章总词数), 以防止它偏向长的文件。

​    公式：  ![img](https://img-blog.csdn.net/20180807190429613?watermark/2/text/aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2FzaWFsZWVfYmlyZA==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70)  即：![img](https://img-blog.csdn.net/20180807190512798?watermark/2/text/aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2FzaWFsZWVfYmlyZA==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70)

 其中 ni,j 是该词在文件 dj 中出现的次数，分母则是文件 dj 中所有词汇出现的次数总和。

**IDF**

逆向文件频率 (IDF) ：某一特定词语的IDF，可以由总文件数目除以包含该词语的文件的数目，再将得到的商取对数得到。

如果包含词条t的文档越少, IDF越大，则说明词条具有很好的类别区分能力。

  公式： ![img](https://img-blog.csdn.net/20180807190920906?watermark/2/text/aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2FzaWFsZWVfYmlyZA==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70)

​    其中，**|D| 是语料库中的文件总数。 |{j:ti∈dj}| 表示包含词语 ti 的文件数目**（即 ni,j≠0 的文件数目）。如果该词语不在语料库中，就会导致分母为零，因此**一般情况下使用 1+|{j:ti∈dj}|**

​    即：![img](https://img-blog.csdn.net/20180807191126207?watermark/2/text/aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L2FzaWFsZWVfYmlyZA==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70)

#### 实现步骤

##### 数据准备：

```scala
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
```

##### 提取商品特征向量

```scala
    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    val wordsDataDF = tokenizer.transform(productTagsDF)
```

##### 计算频次

```scala
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
    val featurizedDataDF = hashingTF.transform(wordsDataDF)
```

##### 计算TF-IDF

```scala
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
```

##### 数据处理

```scala
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
```

##### 数据存储

```scala
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", CONTENT_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
```

