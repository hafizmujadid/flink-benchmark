package edu.tub.berlin.dima.bdapro.flink.benchmark.models

/**
  * Bid DTO
  * @author Hafiz Mujadid Khalid
  * @param auctionId
  * @param price
  * @param bidderId
  * @param eventTime
  * @param processingTime
  */
case class Bid(auctionId:Long, price:Double, bidderId:Long, eventTime:Long,processingTime:Long)
