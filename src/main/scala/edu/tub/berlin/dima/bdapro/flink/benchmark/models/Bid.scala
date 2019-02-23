package edu.tub.berlin.dima.bdapro.flink.benchmark.models

case class Bid(auctionId:Long, price:Double, bidderId:Long, eventTime:Long,processingTime:Long)
