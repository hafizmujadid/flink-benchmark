package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.Properties

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.{Auction, Person}
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * implements query 8 monitor new user from nexmark benchmark
  * @author Hafiz Mujadid Khalid
  */
class MonitorNewUsers {
  /**
    * run method to run the query
    * @param env
    */
  def run(env: StreamExecutionEnvironment): Unit = {


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", JobConfig.BOOTSTRAP_SERVER)
    // only required for Kafka 0.8
    //properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    //Auction Stream

    val auctions = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.AUCTION_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest()).setParallelism(2).map(value => {
      val tokens = value.split(",")
      //1549228675915,2,89673,4,3238.5446711336963,13,1549228716419
      Auction(tokens(0).toLong, tokens(1).toLong, tokens(2).toLong, tokens(3).toLong,
        tokens(4).toDouble, tokens(5).toLong, tokens(6).toLong, System.currentTimeMillis())
    }).name("auction_stream").uid("auction_stream").setParallelism(22)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Auction] {
        override def extractAscendingTimestamp(t: Auction): Long = t.eventTime
      })

    //Person object
    val persons = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.PERSON_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest()).setParallelism(2).map(value => {
      val tokens = value.split(",")
      Person(tokens(0).toLong, tokens(1), tokens(2),
        tokens(3), tokens(4), tokens(5), tokens(6).toLong, System.currentTimeMillis())
    }).name("person_stream").uid("person_stream").setParallelism(22)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Person] {
        override def extractAscendingTimestamp(p: Person): Long = p.eventTime
      })

    val result: DataStream[(Long, String, Long, Long)] = persons.join(auctions)
      .where(p => p.personId)
      .equalTo(auction => auction.sellerId)
      //.window(TumblingEventTimeWindows.of(Time.minutes(30)))
      .window(SlidingEventTimeWindows.of(Time.minutes(30),Time.minutes(10)))
      .apply { (person, auction) => {
      val processTime = if (person.processTime > auction.processTime) person.processTime else auction.processTime
      val eventTime = if (person.eventTime > auction.eventTime) person.eventTime else auction.eventTime
      (person.personId, person.name, eventTime,processTime)
     }
    }.name("joined-stream").uid("joined-stream").setParallelism(22)

    result.map(value =>(System.currentTimeMillis(), value._3,value._4))
      .name("metrics").uid("metrics").setParallelism(22)
      .writeAsText("hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/sliding_q8_full").setParallelism(1).name("sink").uid("sink")
    env.execute("q8")
  }

}
