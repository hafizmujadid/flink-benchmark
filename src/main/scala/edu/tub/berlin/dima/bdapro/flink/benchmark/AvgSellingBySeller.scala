package edu.tub.berlin.dima.bdapro.flink.benchmark
import java.util.Properties

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.Auction
import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.Meter
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
class AvgSellingBySeller {
  def run(env:StreamExecutionEnvironment): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", JobConfig.BOOTSTRAP_SERVER)
    // only required for Kafka 0.8
    properties.setProperty("group.id", "test")

    /*val auctions = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.AUCTION_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest()).map(new RichMapFunction[String, Auction] {

      override def map(value: String): Auction = {
        val tokens = value.split(",")
        Auction(tokens(0).toLong, tokens(1).toLong, tokens(2).toLong, tokens(3).toLong,
          tokens(4).toDouble, tokens(5).toLong, tokens(6).toLong, System.currentTimeMillis())
      }
    }).assignAscendingTimestamps(_.processTime).name("auction_source").uid("auction_source")*/

    val auctions = env.addSource(new AuctionSource).assignAscendingTimestamps(_.processTime).name("auction_source").uid("auction_source")
    val result: DataStream[(Auction, Double)] = auctions.keyBy(_.sellerId)
      .window(TumblingEventTimeWindows.of(Time.minutes(3)))
      //.window(SlidingEventTimeWindows.of(Time.minutes(1),Time.milliseconds(30*1000)))
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
    })
    /*result.map(new RichMapFunction[(Auction, Double), (Auction, Double)] {

      @transient private var meter: Meter = _
      @transient private var processTimeLatency: Long = 0L
      @transient private var eventTimeLatency: Long = 0L

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val dropWizardMeter: com.codahale.metrics.Meter = new com.codahale.metrics.Meter()
        meter = getRuntimeContext
          .getMetricGroup.meter("Throughput", new DropwizardMeterWrapper(dropWizardMeter))
        getRuntimeContext
          .getMetricGroup
          .gauge[Long, ScalaGauge[Long]]("pLatency", ScalaGauge[Long](() => processTimeLatency))
        getRuntimeContext
          .getMetricGroup
          .gauge[Long, ScalaGauge[Long]]("eLatency", ScalaGauge[Long](() => processTimeLatency))
      }

      override def map(value: (Auction, Double)): (Auction, Double) = {
        processTimeLatency = System.currentTimeMillis() - value._1.processTime
        eventTimeLatency = System.currentTimeMillis() - value._1.eventTime
        meter.markEvent()
        value
      }
    }).addSink(x=> println(x._2))*/

    result.map(x =>{
      val currentTime= System.currentTimeMillis()
      (currentTime,x._1.eventTime,currentTime,x._1.eventTime)
    }).addSink(x=>println(x))//.writeAsText("hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/output").setParallelism(1)
    env.execute("q6")
  }
}
