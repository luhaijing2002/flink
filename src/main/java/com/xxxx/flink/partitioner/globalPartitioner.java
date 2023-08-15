package com.xxxx.flink.partitioner;


import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class globalPartitioner {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStreamSource<String> source = environment.readTextFile("data/partition.txt").setParallelism(1);

        SingleOutputStreamOperator<String> upperStream = source.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "task[" + (getRuntimeContext().getIndexOfThisSubtask() + 1) + "]value[" + value + "]";
            }
        }).setParallelism(2);

        //输出数据
//        upperStream.global().print("GlobalPartitioner").setParallelism(4);
//        upperStream.rebalance().print("RebalancePartitioner").setParallelism(3);
//        upperStream.rescale().print("RescalePartitioner").setParallelism(4);
//        upperStream.shuffle().print("ShufflePartitioner").setParallelism(4);
//        upperStream.broadcast().print("BroadcastPartitioner").setParallelism(2);
//        upperStream.forward().print("forwardPartitioner").setParallelism(2);
//        upperStream.keyBy(word -> word).print("KeyGroupPartitioner").setParallelism(4);
//        upperStream.partitionCustom(new Partitioner<String>() {
//            @Override
//            public int partition(String value, int numPartitions) {
////                return value.hashCode() % numPartitions;
//                //上游的全部数据全部由下游的最后一个分区接收
//                return numPartitions - 1;
//            }
//        }, new KeySelector<String, String>() {
//            @Override
//            public String getKey(String value) throws Exception {
//                return value;
//            }
//        }).print("partitionCustom").setParallelism(4);

        upperStream.partitionCustom((v, n) -> n - 1, v -> v).print().setParallelism(4);


        //运行环境
        environment.execute();
    }
}
