package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.Properties

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.{Bid, Person}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.Meter
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

class TopSellersByCity {
  def run(env:StreamExecutionEnvironment): Unit ={


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", JobConfig.BOOTSTRAP_SERVER)
    // only required for Kafka 0.8
    //properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    val personStream = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.PERSON_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest())

    /*
        val persons = env.addSource(new PersonSource).assignAscendingTimestamps(_.eventTime).name("person_stream").uid("person_stream")
    */
    //Bid Stream

    val bids: DataStream[Bid] = env.addSource(new
        FlinkKafkaConsumer011[String](JobConfig.BID_TOPIC, new SimpleStringSchema(), properties)
      .setStartFromEarliest()).map(new RichMapFunction[String, Bid] {
      @transient private var meter: Meter = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val dropWizardMeter: com.codahale.metrics.Meter = new com.codahale.metrics.Meter()
        meter = getRuntimeContext.getMetricGroup.meter("bid.event", new DropwizardMeterWrapper(dropWizardMeter))
      }

      override def map(value: String): Bid = {
        val tokens = value.split(",")
        meter.markEvent()
        Bid(tokens(0).toLong,tokens(1).toDouble,tokens(2).toLong,tokens(3).toLong,System.currentTimeMillis())
      }
    }).assignAscendingTimestamps(_.processingTime).name("bid_source").uid("bid_source")

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
          tokens(3), tokens(4), tokens(5), tokens(6).toLong,System.currentTimeMillis())
      }
    }).assignAscendingTimestamps(_.processTime).name("person_stream").uid("person_stream")

    val result: DataStream[(String, Int, Long, Long)] =bids.join(persons)
      .where(b=>b.bidderId)
      .equalTo(p=>p.personId)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .apply((bid,person) =>{
        val processingTime= if(person.processTime>bid.processingTime) person.processTime else bid.processingTime
        val eventTime = if(person.eventTime>bid.eventTime) person.eventTime else bid.eventTime
        (bid.bidderId,person.city,eventTime,processingTime,bid.price,1)
      })
      .keyBy(_._2)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .apply( (key, _, in, out: Collector[(String, Int, Long, Long)]) => {
        val countByCity: Int = in.iterator.length
        val processingTime = in.iterator.minBy(x=>x._4)._4
        val eventTime = in.iterator.minBy(x=>x._3)._3
        out.collect((key,countByCity,eventTime,processingTime))
      })

    result.map(new RichMapFunction[(String, Int,Long,Long),(String, Int, Long, Long)] {
      @transient private var processTimeLatency: Long = 0
      @transient private var eventTimeLatency: Long = 0
      @transient private var meter:Meter = _


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
          .gauge[Long, ScalaGauge[Long]]("eLatency", ScalaGauge[Long](() => eventTimeLatency))
      }

      override def map(value: (String, Int, Long, Long)): (String, Int, Long, Long) = {
        processTimeLatency = System.currentTimeMillis() - value._4
        eventTimeLatency = System.currentTimeMillis() - value._3
        this.meter.markEvent()
        value
      }
    }).addSink(x=>println(x._1,x._2))

    env.execute()

  }

}
