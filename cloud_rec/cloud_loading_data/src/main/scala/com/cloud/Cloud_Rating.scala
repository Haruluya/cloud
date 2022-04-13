package com.cloud


/**
 * @author haruluya 2022/4/13
 * @param userId 用户id
 * @param productId 产品id
 * @param score 评分分数
 * @param timestamp 时间戳
 */

case class Cloud_Rating( userId: Int, productId: Int, score: Double, timestamp: Int )
