package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.Properties

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.{Bid, Person}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.Meter
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class TopSellersByCity {
  def run(env:StreamExecutionEnvironment): Unit ={


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", JobConfig.BOOTSTRAP_SERVER)
    // only required for Kafka 0.8
    //properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    /*
        val persons = env.addSource(new PersonSource).assignAscendingTimestamps(_.eventTime).name("person_stream").uid("person_stream")
    */
    //Bid Stream

    val bids: DataStream[Bid] = env.addSource(new
        FlinkKafkaConsumer011[String](JobConfig.BID_TOPIC, new SimpleStringSchema(), properties)
      .setStartFromEarliest()).setParallelism(2).map(value =>{
        val tokens = value.split(",")
        Bid(tokens(0).toLong,tokens(1).toDouble,tokens(2).toLong,tokens(3).toLong,System.currentTimeMillis())

    }).name("bid_source").uid("bid_source").setParallelism(22)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Bid] {
        override def extractAscendingTimestamp(b: Bid): Long = b.eventTime
      })

    //Person object

    val persons = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.PERSON_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest()).setParallelism(2).map(value => {
        val tokens = value.split(",")
        Person(tokens(0).toLong, tokens(1), tokens(2),
          tokens(3), tokens(4), tokens(5), tokens(6).toLong,System.currentTimeMillis())
    }).name("person_stream").uid("person_stream").setParallelism(22)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Person] {
        override def extractAscendingTimestamp(p: Person): Long = p.eventTime
      })


    val result: DataStream[(String, Int, Long, Long)] =bids.join(persons)
      .where(b=>b.bidderId)
      .equalTo(p=>p.personId)
      .window(TumblingEventTimeWindows.of(Time.minutes(30)))
      //.window(SlidingEventTimeWindows.of(Time.seconds(60),Time.seconds(20)))
      .apply((bid,person) =>{
        val processingTime= if(person.processTime>bid.processingTime) person.processTime else bid.processingTime
        val eventTime = if(person.eventTime>bid.eventTime) person.eventTime else bid.eventTime
        (bid.bidderId,person.city,eventTime,processingTime,bid.price,1)
      })
      .keyBy(_._2)
      .window(TumblingEventTimeWindows.of(Time.minutes(30)))
      //.window(SlidingEventTimeWindows.of(Time.seconds(60),Time.seconds(20)))
      .apply( (key, _, in, out: Collector[(String, Int, Long, Long)]) => {
        val countByCity: Int = in.iterator.length
        val processingTime = in.iterator.minBy(x=>x._4)._4
        val eventTime = in.iterator.minBy(x=>x._3)._3
        out.collect((key,countByCity,eventTime,processingTime))
      }).name("avg").uid("avg").setParallelism(22)

    /*result.map(new RichMapFunction[(String, Int,Long,Long),(String, Int, Long, Long)] {
      @transient private var processTimeLatency: Long = 0
      @transient private var eventTimeLatency: Long = 0
      @transient private var meter:Meter = _
      val logger: Logger = LoggerFactory.getLogger("throughput")
      private var totalReceived = 0
      private var lastTotalReceived:Long = 0
      private var lastLogTimeMs:Long = -1
      private val logfreq = 1000L
      private var processLatencySum:Double=0f
      private var eventLatencySum:Double=0f

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val dropWizardMeter: com.codahale.metrics.Meter = new com.codahale.metrics.Meter()
        meter = getRuntimeContext
          .getMetricGroup.meter("throughput", new DropwizardMeterWrapper(dropWizardMeter))

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
        processLatencySum+=processTimeLatency
        eventLatencySum+= eventLatencySum
        this.meter.markEvent()
        totalReceived +=1
        if (totalReceived % logfreq == 0) {
          val now = System.currentTimeMillis
          // throughput for the last "logfreq" elements
          if (lastLogTimeMs == -1) {
            lastLogTimeMs = now
            lastTotalReceived = totalReceived
            eventLatencySum =eventLatencySum
            processLatencySum = processLatencySum
          } else {

            val timeDiff = now - lastLogTimeMs
            val elementDiff = totalReceived - lastTotalReceived
            val ex = 1000 / timeDiff.toDouble
            val avrgPL = processLatencySum /elementDiff
            val avrgEL = eventLatencySum /elementDiff
            processLatencySum = 0
            eventLatencySum = 0
            val temp =elementDiff * ex
            val str=  "During the last "+timeDiff+" ms, we received "+elementDiff+" elements. That's "+temp+" elements/second/core."
            logger.info(str)
            println(str)
            val output ="During the last"+timeDiff+" ms, Event Time latency is "+avrgEL+" ms , Process Time Latency is "+avrgPL+" ms."
            logger.info(output)
            println(output)
            lastLogTimeMs = now
            lastTotalReceived = totalReceived
          }
        }
        value
      }
    }).name("metrics-mapper").uid("metrics-mapper")
      .addSink(x=>println(x._1,x._2)).name("console-sink").uid("console-sink")*/

    result.map(value =>{
      (System.currentTimeMillis(),value._3,value._4)
    }).name("metrics").uid("metrics").setParallelism(22)
      .writeAsCsv("hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/tumbling_qnew_inc").setParallelism(1).name("sink").uid("sink")

    env.execute("qnew")

  }

}
