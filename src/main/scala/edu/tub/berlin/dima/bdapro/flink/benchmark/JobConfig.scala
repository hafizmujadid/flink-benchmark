package edu.tub.berlin.dima.bdapro.flink.benchmark

object JobConfig {
  var PERSON_TOPIC="person"
  var AUCTION_TOPIC="auction"
  var BID_TOPIC="bid"
  var BOOTSTRAP_SERVER="ibm-power-6.dima.tu-berlin.de:9092"
  var CHECKPOINT_INTERVAL=1*60*1000
  var CHECKPOINT_DIR="file:///home/ubuntu/checkpoints"
}
