package com.xxxx.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.Collector;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello03WordCountUseDataStreamByJava {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.Sourced
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        // 3.Transformation
        //完整版
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, 1);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                .sum(1);

        //
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator2 = dataStreamSource.flatMap((String value, Collector<String> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(word);
                    }
                }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .sum(1);

        // 4.Sink
        streamOperator2.print();
        //5.运行环境(并不是要执行代码，而是要执行环境，等待下一条数据的到来，然后执行，只有环境运行着，这个实时处理才能一起跑)
        env.execute();



    }

}
