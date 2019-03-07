package edu.tub.berlin.dima.bdapro.flink.benchmark

object Sorter {
  def main(args: Array[String]): Unit = {
    val prefix ="/home/mujadid/Documents/q8/"
    val throughput= scala.io.Source.fromFile(prefix+"sliding_q8_inc_throughput.csv").getLines().filter(_.nonEmpty)
    val eventTime = scala.io.Source.fromFile(prefix+"sliding_q8_inc_event.csv").getLines().filter(_.nonEmpty)
    val processTime: Iterator[String] =scala.io.Source.fromFile(prefix+"sliding_q8_inc_process.csv").getLines().filter(_.nonEmpty)

    processLatency(eventTime,"EventTime")
    processLatency(processTime,"processTime")

    val tuples = throughput.map(x=>{
      val t =x.split(",")
      val ts = t(0).toLong
      (ts,t(1).toInt)
    }).toList.sortBy(_._1)
    val avg_throughput = tuples.map(_._2).sum / tuples.size
    print(tuples,avg_throughput,"throughput")
  }

  private def print[A](tuples:List[A], avrg:Double, metric:String): Unit ={
    println("Printing "+metric+"  \n")
    println("Average "+metric +" value is "+avrg)
    tuples.take(30).foreach(x=>println(x))
  }

  private def processLatency(iter:Iterator[String], metric:String): Unit ={
    val eventLatency = iter.map(x=>{
      val t =x.split(",")
      (t(0).toLong,t(1).toDouble)
    }).toList.sortBy(_._1)
    val avg_event_latency= eventLatency.map(_._2).sum / eventLatency.size
    print(eventLatency,avg_event_latency,metric)
  }
}
