package com.xxxx.flink.ProcessFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 鲁海晶
 * @version 1.0
 * 底层代码编写
 */
public class Hello01ProcessFunction {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<String> source = env.readTextFile("data/wordcount.txt");

        //进行处理
        source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String line, ProcessFunction<String,String>.Context ctx, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String word, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(word, 1));
            }
        }).keyBy(tuple2 -> tuple2.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("Hello01ProcessFunction.processElement[" + ctx.getCurrentKey() + "]");
                out.collect(value);
            }
        }).print();


        //运行环境
        env.execute();

    }

}
