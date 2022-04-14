package com.cloud

/**
 * @author haruluya 2022/4/13.
 *         product数据集样例类。
 * @param productId  商品id
 * @param name       商品名称
 * @param imageUrl   图片url
 * @param categories 商品分类
 * @param tags       商品标签
 */
case class Cloud_Product(productId: Int, name: String, imageUrl: String, categories: String, tags: String)
