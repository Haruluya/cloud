package com.cloud

// 定义用户的推荐列表
case class Cloud_Rec_User(userId: Int, recs: Seq[Recommendation] )