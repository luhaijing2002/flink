package com.xxxx.flink.metrics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.collection.generic.MapFactory;

import java.util.Arrays;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello08MetricsCounter {
    public static void main(String[] args) throws Exception {
        //写一个从9999端口的wordCount的案例
        //1.创建一个流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);
        //设置source
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        //处理source
         source.flatMap((line, out) -> Arrays.stream(line.split(" ")).forEach(out::collect), Types.STRING)
//                .map(word -> Tuple2.of(word, 1), Types.TUPLE(Types.STRING, Types.INT))
                .map(new lhjMapFactory())
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .print();


        //4.执行任务
        env.execute("Hello08MetricsCounter" + System.currentTimeMillis());

    }
}


class lhjMapFactory extends RichMapFunction<String, Tuple2<String,Integer>> {
    private transient Counter counter;


    @Override
    public Tuple2<String,Integer> map(String word) throws Exception {
        //判断长度是否为0
        if(word != null && word.length() == 0){
            this.counter.inc();
        }
        return Tuple2.of(word,1);
    }

    @Override
    public void open(Configuration conf) throws Exception {
        this.counter = getRuntimeContext().getMetricGroup().addGroup("lhjBD").counter("lhjCustomCounter");
    }
}