package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.Properties

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

class WordCountStream {
  def run(env: StreamExecutionEnvironment): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", JobConfig.BOOTSTRAP_SERVER)
    // only required for Kafka 0.8
    //properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    env.addSource(new
          FlinkKafkaConsumer011[String](JobConfig.AUCTION_TOPIC, new
              SimpleStringSchema(), properties)
        .setStartFromEarliest()).setParallelism(2).map(value => {
      val tokens = value.split(",")
      //1549228675915,2,89673,4,3238.5446711336963,13,1549228716419
      (tokens(0).toLong, tokens(1),System.currentTimeMillis())
    }).name("word_count_source").uid("word_count_source").setParallelism(22)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[(Long,String,Long)] {
        override def extractAscendingTimestamp(t: (Long,String,Long)): Long = t._1
      }).keyBy(_._2).window(TumblingEventTimeWindows.of(Time.minutes(30)))
      .process(new WordCountProcessFunction)

  }

  class WordCountProcessFunction extends ProcessWindowFunction[(Long,String,Long),
    (String,Long,Long,Long), String, TimeWindow]  {

    @transient var state:ValueState[(String,Long)] = _

    override def open(parameters: Configuration): Unit = {

      val descriptor = new ValueStateDescriptor[(String,Long)]("wordCountState",
        TypeInformation.of(new TypeHint[(String, Long)]() {}),
        ("", 0L))
      state = getRuntimeContext.getState(descriptor)
    }

    def process(key: String, context: Context, input: Iterable[(Long,String,Long)],
                out: Collector[(String,Long,Long,Long)]):Unit  = {
      var count = 0L
      var eventTime=0L
      var processTime = 0L
      for (in <- input) {
        count = count + 1
        if(in._1>eventTime)
          eventTime =in._1
        if(in._3>processTime)
          processTime =in._3
      }
      state.update((key,count))
      //out.collect(s"Window ${context.window} count: $count")
      out.collect(key,count,eventTime,processTime)
    }
  }
}
