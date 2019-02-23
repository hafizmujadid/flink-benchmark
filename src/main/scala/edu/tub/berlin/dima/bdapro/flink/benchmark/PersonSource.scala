package edu.tub.berlin.dima.bdapro.flink.benchmark

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.Person
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

class PersonSource extends ParallelSourceFunction[Person]{
  var running = true
  override def run(ctx: SourceFunction.SourceContext[Person]): Unit = {
    val cities =Array("Berling","Frankfurt","Hamburg")
    for(i<- 0 until 100 if running){
      val event= Person(i+1,"abc","abc@d.com","12344",cities(i%3),"state",System.currentTimeMillis(),0)
      ctx.collect(event)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
