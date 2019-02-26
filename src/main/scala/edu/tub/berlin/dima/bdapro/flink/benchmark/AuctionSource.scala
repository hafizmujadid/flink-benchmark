package edu.tub.berlin.dima.bdapro.flink.benchmark

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.Auction
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.util.Random

class AuctionSource extends ParallelSourceFunction[Auction]{
  var running = true
  override def run(ctx: SourceFunction.SourceContext[Auction]): Unit = {
    val random= new Random()
    for(i<- 0 until 1000000 if running){
      val price =random.nextDouble()%100 + 50
      val event= Auction(System.currentTimeMillis(),i,1,i%2,price,1,System.currentTimeMillis()+10000,0)
      ctx.collect(event)
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
