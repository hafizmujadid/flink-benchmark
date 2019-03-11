package edu.tub.berlin.dima.bdapro.flink.benchmark

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
object App {
  def main(args: Array[String]): Unit = {
    JobConfig.CHECKPOINT_DIR="hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/checkpointing"
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(JobConfig.CHECKPOINT_INTERVAL)
    val rocksDBStateBackend: RocksDBStateBackend = new RocksDBStateBackend(JobConfig.CHECKPOINT_DIR,
      true)
    env.setStateBackend(rocksDBStateBackend)
    //env.setStateBackend(new FsStateBackend(JobConfig.CHECKPOINT_DIR))

    val query = new WordCountStream
    query.run(env)
  }
}
