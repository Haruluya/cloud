package com.cloud

// 定义商品相似度列表
case class Cloud_Similarity_Product( productId: Int, recs: Seq[Recommendation] )

