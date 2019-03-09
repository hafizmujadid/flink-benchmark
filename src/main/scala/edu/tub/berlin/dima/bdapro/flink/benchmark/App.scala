package edu.tub.berlin.dima.bdapro.flink.benchmark
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
object App {
  def main(args: Array[String]): Unit = {
    JobConfig.CHECKPOINT_DIR="hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/checkpointing"
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(JobConfig.CHECKPOINT_INTERVAL)
    //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(60, TimeUnit.SECONDS) ))
    /*val rocksDBStateBackend: RocksDBStateBackend = new RocksDBStateBackend(JobConfig.CHECKPOINT_DIR,
      false)*/
    //rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)
    //env.setStateBackend(rocksDBStateBackend)
    env.setStateBackend(new FsStateBackend(JobConfig.CHECKPOINT_DIR))

    val query = new WordCountStream
    query.run(env)
  }
}
