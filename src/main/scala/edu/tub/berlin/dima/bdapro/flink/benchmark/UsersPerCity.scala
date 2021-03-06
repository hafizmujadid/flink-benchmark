package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.Properties

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.Person
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

/**
  * @author Hafiz Mujadid Khalid
  *  implements query to count persons per city
  */
class UsersPerCity {
  /**
    * run method to initiate execution
    * @param env
    */
  def run(env:StreamExecutionEnvironment): Unit ={

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", JobConfig.BOOTSTRAP_SERVER)
    properties.setProperty("group.id", "test")

    val persons = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.PERSON_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest()).setParallelism(2).map(value => {
        val tokens = value.split(",")
        Person(tokens(0).toLong, tokens(1), tokens(2),
          tokens(3), tokens(4), tokens(5), tokens(6).toLong,System.currentTimeMillis())
    }).name("person_stream").uid("person_stream")//.setParallelism(22)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Person] {
        override def extractAscendingTimestamp(p: Person): Long = p.eventTime
      })


    val result: DataStream[(String, Int, Long, Long)] = persons
      .keyBy(_.city)
      .window(TumblingEventTimeWindows.of(Time.minutes(30)))
      .apply( (key, _, in, out: Collector[(String, Int, Long, Long)]) => {
        val countByCity: Int = in.iterator.length
        val processingTime = in.iterator.maxBy(x=>x.processTime).processTime
        val eventTime = in.iterator.maxBy(x=>x.eventTime).eventTime
        out.collect((key,countByCity,eventTime,processingTime))
      }).name("users_per_city").uid("users_per_city").setParallelism(22)

    result.map(value =>{
      System.currentTimeMillis()+","+value._3+","+value._4
    }).name("metrics").uid("metrics").setParallelism(22)
      .writeAsCsv("hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/tumbling_qnew_inc")//.setParallelism(1).name("sink").uid("sink")

    env.execute("qnew")

  }

}
