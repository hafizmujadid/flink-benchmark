package edu.tub.berlin.dima.bdapro.flink.benchmark

object Temp {
  def main(args: Array[String]): Unit = {
    val data = scala.io.Source.fromFile("/home/mujadid/Desktop/q6_chk_full_time.txt")
      .getLines().filter(_.nonEmpty)

    val ints = data.map(x=>x.split("=")(1).split(",")(0).trim.toInt).toList.take(27)
    println(ints.sum)
    val data2 = scala.io.Source.fromFile("/home/mujadid/Desktop/q6_chk_inc_time.txt")
      .getLines().filter(_.nonEmpty)
    val ints2 = data2.map(x=>x.split("=")(1).split(",")(0).trim.toInt).toList.take(27)
    println(ints2.sum)

  }
}
