package com.xxxx.flink.transformationcommon;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello01TransformationCommon {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        //数据源
        DataStreamSource<String> source = environment.fromElements("Hello Hadoop", "Hello Hive", "Hello HBase Phoenix   ", "Hello ClickHouse     ");


        //转移算子
//        source.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out) throws Exception {
//                String[] words = value.split(" ");
//                for (String word : words) {
//                    out.collect(word);
//                }
//            }
//        }).print();


        source.flatMap((line, collector) -> {
//            collector.collect(Arrays.stream(line.split(" ")));
                    Arrays.stream(line.split(" ")).forEach(collector::collect);
                }, Types.STRING)
                .filter(word -> word != null && word.length() > 0)
                .map(word -> Tuple2.of(word, 1),Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .print();


        //sink
//        flatMap.print();
        //执行环境
        environment.execute();

    }

}

