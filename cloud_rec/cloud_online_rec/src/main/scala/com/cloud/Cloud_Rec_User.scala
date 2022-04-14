package com.cloud

/**
 * 用户推荐列表。
 * @author haruluya
 * @param userId
 * @param recs
 */
case class Cloud_Rec_User(userId: Int, recs: Seq[Recommendation] )