package edu.tub.berlin.dima.bdapro.flink.benchmark

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.Bid
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.util.Random

class BidSource extends ParallelSourceFunction[Bid]{
  var running = true
  override def run(ctx: SourceFunction.SourceContext[Bid]): Unit = {
    val random= new Random()
    for(i<- 0 until 100 if running){
      val price =random.nextDouble()%100 + 50
      val event= Bid(i+1,price,i+1,System.currentTimeMillis(),0)
      ctx.collect(event)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
