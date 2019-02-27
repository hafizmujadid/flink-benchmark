package edu.tub.berlin.dima.bdapro.flink.benchmark

object Sorter {
  def main(args: Array[String]): Unit = {
    val prefix ="/home/mujadid/Documents/"
    val throughput= scala.io.Source.fromFile(prefix+"throughput_inc_uniq_q6_tumbling.csv").getLines().filter(_.nonEmpty)
    val eventTime = scala.io.Source.fromFile(prefix+"event_letency_inc_uniq_q6_tumbling.csv").getLines().filter(_.nonEmpty)
    val processTime =scala.io.Source.fromFile(prefix+"process_letency_inc_uniq_q6_tumbling.csv").getLines().filter(_.nonEmpty)

    val tuples = throughput.map(x=>{
      val t =x.split(",")
      (t(0).toLong,t(1).toInt)
    }).toList.sortBy(_._1)
    val avg_throughput = tuples.map(_._2).sum.toFloat / tuples.size
    print(tuples,avg_throughput,"throughput")
  }

  def print[A](tuples:List[A],avrg:Double, metric:String)={
    println("Printing "+metric+"  \n")
    println("Average "+metric +" value is "+avrg)
    tuples.foreach(println(_))
  }
}
