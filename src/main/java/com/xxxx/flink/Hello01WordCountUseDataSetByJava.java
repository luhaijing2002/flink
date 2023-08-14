package com.xxxx.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello01WordCountUseDataSetByJava {
    public static void main(String[] args) throws Exception {
//        // 1. 创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 2. 读取数据
//        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);
//        // 3. 转换
//        DataStream<Tuple2<String, Integer>> wordToCount = inputStream.flatMap(new WordToFlatMapFunction())
//                .keyBy(0)
//                .sum(1);
//        // 4. 打印
//        wordToCount.print();
//        // 5. 执行任务
//        env.execute();


        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.Source
        DataSource<String> dataSource = env.readTextFile("data/wordcount.txt");
        //3.Transformation
        //扁平化操作
        FlatMapOperator<String, String> flatMapOperator = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //映射操作
        MapOperator<String, Tuple2<String, Integer>> mapOperator = flatMapOperator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        //分组并聚合
        AggregateOperator<Tuple2<String, Integer>> sum = mapOperator.groupBy(0).sum(1);
        //4.sink
        sum.print();


    }
}
