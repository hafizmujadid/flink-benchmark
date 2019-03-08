package edu.tub.berlin.dima.bdapro.flink.benchmark

object Temp {
  def main(args: Array[String]): Unit = {
    val data = scala.io.Source.fromFile("/home/mujadid/Desktop/chk1.txt").getLines().filter(_.nonEmpty)

    val ints = data.map(x=>x.split("=")(1).split(",")(0).trim.toInt).toList

    val l1 = ints.length
    println(ints.length)
    println(ints.sum.toDouble / ints.length)

    val data2 = scala.io.Source.fromFile("/home/mujadid/Desktop/full-chk.txt").getLines().filter(_.nonEmpty)
    val ints2 = data2.map(x=>x.split("=")(1).split(",")(0).trim.toInt).toList
    println(ints2.length)
    println(ints2.sum.toDouble / ints2.length)

  }
}
