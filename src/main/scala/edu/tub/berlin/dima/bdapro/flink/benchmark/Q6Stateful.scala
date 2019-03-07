/*
package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.Properties

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.Auction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

class Q6Stateful {
  def run(env:StreamExecutionEnvironment): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", JobConfig.BOOTSTRAP_SERVER)
    // only required for Kafka 0.8
    properties.setProperty("group.id", "test")

    val auctions:DataStream[Auction] = env
      .addSource(new
          FlinkKafkaConsumer011[String](JobConfig.AUCTION_TOPIC, new SimpleStringSchema(), properties)
        .setStartFromEarliest())
      .setParallelism(2).name("auction_source").uid("auction_source")
      .map(value=>{
        val tokens = value.split(",")
        Auction(tokens(0).toLong, tokens(1).toLong, tokens(2).toLong, tokens(3).toLong,
          tokens(4).toDouble, tokens(5).toLong, tokens(6).toLong, System.currentTimeMillis())
      }).
      name("map_auction").uid("map_auction").setParallelism(22)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Auction] {
        override def extractAscendingTimestamp(t: Auction): Long = t.eventTime
      })


/*
    result.map(x =>{
      val currentTime= System.currentTimeMillis()
      (currentTime,x._1.eventTime,x._1.processTime)
    }).name("metrics").uid("metrics").setParallelism(22)
      //.addSink(x=>println(x)).setParallelism(1).name("sink").uid("sink")
      .writeAsText("hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/output").setParallelism(1).name("sink").uid("sink")
    env.execute("q6")*/
  }

  class mapper extends RichMapFunction[Auction,(Long,Long)]{
    private var sum: ValueState[(Long, Long)] = _

    override def open(parameters: Configuration): Unit = {

    }

    override def map(in: Auction): (Long, Long) = {
      // access the state value
      val tmpCurrentSum = sum.value

      // If it hasn't been used before, it will be null
      val currentSum = if (tmpCurrentSum != null) {
        tmpCurrentSum
      } else {
        (0L, 0L)
      }

      // update the count
      val newSum = (in.sellerId,currentSum._1 + 1)

      // update the state
      sum.update(newSum)

      // if the count reaches 2, emit the average and clear the state
      if (newSum._1 >= 2) {
        out.collect((input._1, newSum._2 / newSum._1))
        sum.clear()
      }

      (in.sellerId,1L)/**/
    }
  }
}

*/
