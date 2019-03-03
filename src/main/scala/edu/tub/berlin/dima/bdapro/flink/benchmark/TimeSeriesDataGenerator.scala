package edu.tub.berlin.dima.bdapro.flink.benchmark

import java.util.concurrent.TimeUnit

import org.apache.commons.lang3.time.DateUtils


object TimeSeriesDataGenerator {
  def main(args: Array[String]): Unit = {
    val prefix ="/home/mujadid/Documents/sliding_full/"
    val throughput= scala.io.Source.fromFile(prefix+"processing_latency_sorted.csv").getLines().filter(_.nonEmpty)
    throughput.map(x=>{
      val t = x.split(",")
      val temp = t(1).toDouble.toLong
      val seconds = TimeUnit.MILLISECONDS.toSeconds(temp)
      (t(0).toInt,seconds)
    }).foreach(x=>println(x._1+","+x._2))
  }
}
