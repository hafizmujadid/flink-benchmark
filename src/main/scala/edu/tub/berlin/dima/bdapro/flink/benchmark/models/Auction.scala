package edu.tub.berlin.dima.bdapro.flink.benchmark.models

/**
  * Auction DTO object
  * @param eventTime
  * @param auctionId
  * @param itemId
  * @param sellerId
  * @param initialPrice
  * @param categoryId
  * @param expiryDate
  * @param processTime
  */
case class Auction(eventTime: Long, auctionId: Long, itemId: Long,
                   sellerId: Long, initialPrice: Double, categoryId: Long,
                   expiryDate: Long, processTime: Long)
