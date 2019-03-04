package edu.tub.berlin.dima.bdapro.flink.benchmark;

import edu.tub.berlin.dima.bdapro.flink.benchmark.models.Bid;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HighestBid {

    public void run(StreamExecutionEnvironment env){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",JobConfig.BOOTSTRAP_SERVER());
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer011<>(JobConfig.BID_TOPIC(),
                        new SimpleStringSchema(), properties).setStartFromEarliest()).setParallelism(2);
        DataStream<Bid> bid = stream.map(value -> {
            String[] tokens = value.split(",");
            return new Bid(Long.parseLong(tokens[0]),
                    Double.parseDouble(tokens[1]),
                    Long.parseLong(tokens[2]),
                    Long.parseLong(tokens[3]), System.currentTimeMillis());
        }).name("bid_source").uid("bid_source").setParallelism(22)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Bid>() {
                    @Override
                    public long extractAscendingTimestamp(Bid bid) {
                        return bid.eventTime();
                    }
                });

        DataStream<Tuple4<Long, Double,Long,Long>> tem = bid.keyBy(Bid::bidderId)
                .process(new ProcessFunction<Bid, Tuple4<Long,Double, Long, Long>>() {
                    private transient ValueState<Tuple2<Long,Double>> auc_price;
                    @Override
                    public void processElement(Bid bid_event, Context context, Collector<Tuple4<Long,Double, Long, Long>> collector) throws Exception {
                        Tuple2<Long,Double> auc_price_current = auc_price.value();
                        if (auc_price_current != null) {
                            auc_price_current.f0 = bid_event.bidderId();
                            auc_price_current.f1 = bid_event.price();
                            auc_price.update(auc_price_current);
                            collector.collect(new Tuple4<>(auc_price_current.f0, auc_price_current.f1, bid_event.eventTime(), bid_event.processingTime()));
                        }
                        else{
                            auc_price.update(new Tuple2<>(bid_event.bidderId(),bid_event.price()));
                            collector.collect(new Tuple4<>(auc_price.value().f0, auc_price.value().f1 ,bid_event.eventTime(),bid_event.processingTime()));
                        }
                    }


                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Tuple2<Long, Double>> descriptor =
                                new ValueStateDescriptor<>(
                                        "Query7.aucprice", // the state name
                                        TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {}),
                                        Tuple2.of(0L, 0.0));

                        auc_price = getRuntimeContext().getState(descriptor);
                    }
                }).name("query7.process").uid("query7.process").setParallelism(22);

        DataStream<Tuple4<Long, Double,Long,Long>> temp= tem.keyBy(1)
                .timeWindow(Time.seconds(10))
                .apply(
                        (WindowFunction<Tuple4<Long, Double, Long, Long>, Tuple4<Long, Double, Long, Long>, Tuple, TimeWindow>) (tuple, timeWindow, iterable, collector) -> {
                            double max=0;
                            long throughput = 0;
                            List<Tuple4<Long, Double,Long,Long>> result = new ArrayList<>();
                            for(Tuple4<Long, Double,Long,Long> in : iterable){
                                throughput = throughput + in.f3;
                                if(max<in.f1) {
                                    max=in.f1;
                                    result.clear();
                                    result.add(new Tuple4<>(in.f0, in.f1,in.f2,throughput));
                                    throughput = 0;
                                }

                                else if(max==in.f1) {
                                    result.add(new Tuple4<>(in.f0, in.f1, in.f2, throughput));
                                    throughput = 0;
                                }


                            }

                            for (Tuple4<Long, Double, Long, Long> longDoubleLongLongTuple4 : result) {
                                collector.collect(longDoubleLongLongTuple4);
                            }
                        }
                ).name("highest_bid").uid("highest_bid").setParallelism(22);

        temp.map(tuple -> new Tuple3<>(System.currentTimeMillis(), tuple.f2, tuple.f3))
                .setParallelism(22).writeAsCsv("hdfs://ibm-power-1.dima.tu-berlin.de:44000/issue13/tumbling_q8_inc").setParallelism(1);
    }
}