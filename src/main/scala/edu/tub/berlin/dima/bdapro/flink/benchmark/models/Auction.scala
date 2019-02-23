package edu.tub.berlin.dima.bdapro.flink.benchmark.models

case class Auction(eventTime: Long, auctionId: Long, itemId: Long,
                   sellerId: Long, initialPrice: Double, categoryId: Long,
                   expiryDate: Long, processTime: Long)
