package com.cloud

/**
 * 商品相似度列表。
 * @author haruluya
 * @param productId
 * @param recs
 */
case class Cloud_Similarity_Product(productId: Int, recs: Seq[Recommendation] )

