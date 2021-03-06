package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.Properties

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.Auction
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Word count job in flink
  * @author Hafiz Mujadid Khalid
  */
class WordCountStream {
  def run(env: StreamExecutionEnvironment): Unit ={
    env.addSource(new WordCountSource)
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[(String,Long)] {
        override def extractAscendingTimestamp(t: (String,Long)): Long = System.currentTimeMillis()
      }).flatMap(_._1.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.minutes(30)))
      .sum(1)
      .print()

    env.execute("word count")

  }
}
