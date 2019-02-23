package edu.tub.berlin.dima.bdapro.flink.benchmark
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
object App {
  def main(args: Array[String]): Unit = {
    JobConfig.CHECKPOINT_DIR="hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/checkpointing"

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.enableCheckpointing(JobConfig.CHECKPOINT_INTERVAL)
    //env.getConfig.setLatencyTrackingInterval(JobConfig.CHECKPOINT_INTERVAL)
    env.setParallelism(4*20)

    //val rocksDBStateBackend: RocksDBStateBackend = new RocksDBStateBackend(JobConfig.CHECKPOINT_DIR, true)
    //rocksDBStateBackend.setOptions(new RocksDbStateBackendOptions)
    //env.setStateBackend(rocksDBStateBackend)
    //env.setStateBackend(new FsStateBackend(JobConfig.CHECKPOINT_DIR))

    val query = new TopSellersByCity
    query.run(env)
  }

  /*class RocksDbStateBackendOptions extends OptionsFactory {
    override def createDBOptions(currentOptions: DBOptions): DBOptions = {
      currentOptions.setIncreaseParallelism(4).setUseFsync(false)
    }

    override def createColumnOptions(currentOptions: ColumnFamilyOptions): ColumnFamilyOptions = {
      currentOptions.setTableFormatConfig(
        new BlockBasedTableConfig()
          .setBlockCacheSize(256 * 1024 * 1024)  // 256 MB
          .setBlockSize(128 * 1024))  //128 MB
    }
  }*/
}
