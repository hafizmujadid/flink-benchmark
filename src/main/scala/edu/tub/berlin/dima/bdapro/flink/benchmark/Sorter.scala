package edu.tub.berlin.dima.bdapro.flink.benchmark

object Sorter {
  def main(args: Array[String]): Unit = {
    val prefix ="/home/mujadid/Documents/tumbling_full/"
    val throughput= scala.io.Source.fromFile(prefix+"tumbling_full_throughput_short.csv").getLines().filter(_.nonEmpty)
    val eventTime = scala.io.Source.fromFile(prefix+"tumbling_full_event_short.csv").getLines().filter(_.nonEmpty)
    val processTime: Iterator[String] =scala.io.Source.fromFile(prefix+"tumbling_full_process_short.csv").getLines().filter(_.nonEmpty)

    processLatency(eventTime,"EventTime")
    processLatency(processTime,"processTime")

    val tuples = throughput.map(x=>{
      val t =x.split(",")
      (t(0).toLong,t(1).toInt)
    }).toList.sortBy(_._1)
    val avg_throughput = tuples.map(_._2).sum / tuples.size
    print(tuples,avg_throughput,"throughput")
  }

  private def print[A](tuples:List[A], avrg:Double, metric:String): Unit ={
    println("Printing "+metric+"  \n")
    println("Average "+metric +" value is "+avrg)
    tuples.foreach(x=>println(x))
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
