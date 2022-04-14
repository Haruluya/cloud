package com.cloud

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.Jedis

/**
 * 数据库连接序列化配置。
 */
object Connection_Serializable extends Serializable {
    //redis和mongoDB配置。
    lazy val jedis = new Jedis("localhost")
    lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/cloud"))
}
