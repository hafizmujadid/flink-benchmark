package edu.tub.berlin.dima.bdapro.flink.benchmark
import java.util.Properties

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.Auction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * @author Hafiz Mujadid Khalid
  */
class AvgSellingBySeller {
  /**
    *  to run average selling price per seller query.
    * @param env execution environment
    */
  def run(env:StreamExecutionEnvironment): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", JobConfig.BOOTSTRAP_SERVER)
    // only required for Kafka 0.8
    properties.setProperty("group.id", "test")

    val auctions = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.AUCTION_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest())
      .setParallelism(2).name("auction_source").uid("auction_source")
      .map(value=>{
        val tokens = value.split(",")
        Auction(tokens(0).toLong, tokens(1).toLong, tokens(2).toLong, tokens(3).toLong,
          tokens(4).toDouble, tokens(5).toLong, tokens(6).toLong, System.currentTimeMillis())
      }).name("map_auction").uid("map_auction").setParallelism(12)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Auction] {
        override def extractAscendingTimestamp(t: Auction): Long = t.eventTime
      })

    val result: DataStream[(Auction, Double)] = auctions.keyBy(_.sellerId)
      .window(TumblingEventTimeWindows.of(Time.minutes(30)))
      //.window(SlidingEventTimeWindows.of(Time.minutes(30),Time.minutes(10)))
      .aggregate(new AggregateFunction[Auction, (Double, Long, Auction), (Auction, Double)] {
      override def createAccumulator(): (Double, Long, Auction) = (0.toDouble, 0L, null)

      //sum, count, sellerId
      override def add(value: Auction, accumulator: (Double, Long, Auction)): (Double, Long, Auction) = {
        (accumulator._1 + value.initialPrice, accumulator._2 + 1L, value)
      }
      override def getResult(accumulator: (Double, Long, Auction)): (Auction, Double) = (accumulator._3, accumulator._1 / accumulator._2)

      override def merge(a: (Double, Long, Auction), b: (Double, Long, Auction)): (Double, Long, Auction) = {
        (a._1 + b._1, a._2 + b._2, a._3)
      }
    }).name("avg_bid_price").uid("avg_bid_price").setParallelism(12)


    result.map(x =>{
      val currentTime= System.currentTimeMillis()
      (currentTime,x._1.eventTime,x._1.processTime)
    }).name("metrics").uid("metrics").setParallelism(12)
      .writeAsText("hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/output").setParallelism(1).name("sink").uid("sink")
    env.execute("q6")
  }
}
