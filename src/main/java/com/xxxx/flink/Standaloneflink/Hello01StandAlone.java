package com.xxxx.flink.Standaloneflink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello01StandAlone {
//Arrays.stream(line.split(" ")).forEach(word -> collector.collect(word))
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Source[Word word word]
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        //Transformation
        source.flatMap((line, collector) -> Arrays.stream(line.split(" ")).forEach(collector::collect), Types.STRING)
                .map(word -> Tuple2.of(word, 1), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .print();
        //执行环境
        env.execute("Hello01StandAloneName" + System.currentTimeMillis());

    }
}
