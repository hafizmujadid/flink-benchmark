package edu.tub.berlin.dima.bdapro.flink.benchmark
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object Aggregator {

  case class Result(timestamp: Long, eventTime: Long, processingTime: Long)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile("file:///home/hadoop/output.csv").filter(!_.isEmpty).map(x => {
      val tokens = x.drop(1).dropRight(1).split(",").map(_.toLong)
      Result(tokens(0), tokens(1), tokens(2))
    }).map(x => {
      val bucket = x.timestamp - (x.timestamp % 60)
      Result(bucket, math.abs(x.timestamp - x.eventTime), math.abs(x.timestamp - x.processingTime))
    })
    val throughput = data.map(x => (x.timestamp, 1)).groupBy(0)
      .reduce((a, b) => (a._1, a._2 + b._2))

    val eventTimeLatency = data.map(x => (x.timestamp, x.eventTime, 1)).groupBy(0)
      .reduce((a, b) => {
        (a._1, a._2 + b._2, a._3 + b._3)
      }).map(x => (x._1, x._2.toFloat / x._3))

    val processTimeLatency = data.map(x => (x.timestamp, x.processingTime, 1)).groupBy(0)
      .reduce((a, b) => {
        (a._1, a._2 + b._2, a._3 + b._3)
      }).map(x => (x._1, x._2.toFloat / x._3))

    throughput.writeAsCsv("file:///home/hadoop/mujadid_throughput.csv").setParallelism(1)
    eventTimeLatency.writeAsCsv("file:///home/hadoop/mujadid_event.csv").setParallelism(1)
    processTimeLatency.writeAsCsv("file:///home/hadoop/mujadid_process.csv").setParallelism(1)

    env.execute("metrices")

  }
}
