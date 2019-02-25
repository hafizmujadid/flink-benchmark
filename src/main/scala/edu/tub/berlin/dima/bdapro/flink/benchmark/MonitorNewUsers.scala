package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.Properties

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.{Auction, Person}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.Meter
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

class MonitorNewUsers {
  def run(env:StreamExecutionEnvironment): Unit = {


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", JobConfig.BOOTSTRAP_SERVER)
    // only required for Kafka 0.8
    //properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    val personStream = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.PERSON_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest())

    //Auction Stream

    val auctions = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.AUCTION_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest()).map(new RichMapFunction[String, Auction] {
      @transient private var meter: Meter = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val dropWizardMeter: com.codahale.metrics.Meter = new com.codahale.metrics.Meter()
        meter = getRuntimeContext.getMetricGroup.meter("auction.event", new DropwizardMeterWrapper(dropWizardMeter))
      }

      override def map(value: String): Auction = {
        val tokens = value.split(",")
        meter.markEvent()
        //1549228675915,2,89673,4,3238.5446711336963,13,1549228716419
        Auction(tokens(0).toLong, tokens(1).toLong, tokens(2).toLong, tokens(3).toLong,
          tokens(4).toDouble, tokens(5).toLong, tokens(6).toLong, System.currentTimeMillis())
      }
    }).assignAscendingTimestamps(_.processTime).name("auction_stream").uid("auction_stream")
    //Person object

    val persons = personStream.map(new RichMapFunction[String, Person] {
      @transient private var meter: Meter = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val dropWizardMeter: com.codahale.metrics.Meter = new com.codahale.metrics.Meter()
        meter = getRuntimeContext.getMetricGroup.meter("person.event", new DropwizardMeterWrapper(dropWizardMeter))
      }

      override def map(value: String): Person = {
        val tokens = value.split(",")
        meter.markEvent()
        Person(tokens(0).toLong, tokens(1), tokens(2),
          tokens(3), tokens(4), tokens(5), tokens(6).toLong, System.currentTimeMillis())
      }
    }).assignAscendingTimestamps(_.processTime).name("person_stream").uid("person_stream")

    val result: DataStream[(Long, String, Long, Long, Long)] = persons.join(auctions)
      .where(p => p.personId)
      .equalTo(auction => auction.sellerId)
      //.window(TumblingEventTimeWindows.of(Time.minutes(30)))
      .window(SlidingEventTimeWindows.of(Time.minutes(30),Time.minutes(15)))
      .apply { (person, auction) => {
      val processTime = if (person.processTime > auction.processTime) person.processTime else auction.processTime
      val eventTime = if (person.eventTime > auction.eventTime) person.eventTime else auction.eventTime
      (person.personId, person.name, processTime, 2L, eventTime)
    }
    }.name("result").uid("result")

    result.map(new RichMapFunction[(Long, String, Long, Long, Long), (Long, String, Long, Long, Long)] {

      @transient private var processTimeLatency: Long = 0
      @transient private var eventTimeLatency: Long = 0
      @transient private var meter: Meter = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val dropWizardMeter: com.codahale.metrics.Meter = new com.codahale.metrics.Meter()
        meter = getRuntimeContext
          .getMetricGroup.meter("Throughput", new DropwizardMeterWrapper(dropWizardMeter))
        getRuntimeContext
          .getMetricGroup
          .gauge[Long, ScalaGauge[Long]]("eLatency", ScalaGauge[Long](() => eventTimeLatency))
        getRuntimeContext
          .getMetricGroup
          .gauge[Long, ScalaGauge[Long]]("pLatency", ScalaGauge[Long](() => processTimeLatency))
      }

      override def map(value: (Long, String, Long, Long, Long)): (Long, String, Long, Long, Long) = {
        processTimeLatency = System.currentTimeMillis() - value._3
        eventTimeLatency = System.currentTimeMillis() - value._5
        this.meter.markEvent()
        value
      }
    })
    env.execute("q8")
  }

}
