# Product Recommendation System

>Spark learning work.
>
>Base spark and machine learning.

## Introduction 
![Haruluya](https://img.shields.io/badge/X-Haruluya-brightgreen)	![★: Spark (shields.io)](https://img.shields.io/badge/★-Spark-blue)		![★: MogoDB (shields.io)](https://img.shields.io/badge/★-MongoDB-red)	![★: Kafka (shields.io)](https://img.shields.io/badge/★-Kafka-green)	![★: Machine learning (shields.io)](https://img.shields.io/badge/★-MachineLearning-yellow)

#### Name:

![cloud](https://n.sinaimg.cn/sinacn20113/600/w1920h1080/20191130/7425-ikcaceq7047502.jpg)

克劳德·斯特莱夫（日文：クラウド・ストライフ；英文：Cloud Strife），是日本史克威尔公司出品的电子游戏《最终幻想VII》中的男主角。

## Development Environment

| <img width=50/>Modules <img width=50/> | <img width=50/>Version  <img width=50/>| <img width=50/>Directory<img width=50/>|
| -------| ------- | --------|
| ✅Spark |  2.1.1 | |
| ✅MongoDB    | 2.0.0  |       |
| ✅kafka   |   0.10      |  |
| ✅scala   |   2.11.8      |    |
| ✅spark-mllib | 2.11 | |

## Deployment method

## Meaning of project

## Function Introduction

#### Cloud

所有模块的父项目。

#### Cloud_data_load

**数据导入模块**

此模块负责将外源数据导入到mongoDB中。虽然实际应用时项目使用的数据库应该为电商项目长期维护的，但鉴于完整性和测试需求，外源载入数据模块也需要存在。

#### Cloud_kafka_app

**kafka配置模块**

此模块为kafka stream的启动模块。

#### Cloud_rec

推荐模块的父模块。

#### Cloud_ALS_res

**基于隐语义模型的协同过滤推荐模块**

此模块基于隐语义模型的协同过滤算法，根据数据库中的用户对各产品的评分，计算非实时的商品推荐列表和各商品相似度矩阵。

#### Cloud_Item_CF_res

**基于Item-CF的离线商品相似推荐**

此模块基于基于物品的协同过滤（Item-CF），只需收集用户的常规行为数据（比如点击、收藏、购买）就可以得到商品间的相似度，从而实现离线商品推荐。

#### Cloud_offline_rec

**基于spark sql的基本离线统计**

此模块完成基本的热门统计，复杂性低，只是简单的利用spark sql统计评分次数多的商品，近期评分次数多的商品，和商品的平均评分。此功能并非必须在此系统中完成，但是借助spark sql会更为简单。

#### Cloud_online_rec

**基于实时推荐模型的实时推荐**

此模块基于自定义的实时推荐模型完成商品的实施推荐服务。



## Presentation

## Contribution

## Contact Author 

| ![image-20220327170927104](https://i.postimg.cc/MGB5hN3S/image-20220327170927104.png) |
| ------------------------------------------------------------ |
| <a href="https://github.com/Haruluya">Follow</a>             |



## License

MIT

![MIT](https://img.shields.io/badge/License-MIT-red)
