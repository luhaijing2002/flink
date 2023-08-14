package com.xxxx.flink.transformationcommon;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello02TansformationAgg {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("math2", 200));
        list.add(new Tuple2<>("chinese2", 20));
        list.add(new Tuple2<>("math1", 100));
        list.add(new Tuple2<>("chinese1", 10));
        list.add(new Tuple2<>("math4", 400));
        list.add(new Tuple2<>("chinese4", 40));
        list.add(new Tuple2<>("math3", 300));
        list.add(new Tuple2<>("chinese3", 30));
        DataStreamSource<Tuple2<String, Integer>> aggregationSource = environment.fromCollection(list);
        KeyedStream<Tuple2<String, Integer>, Integer> keyedStream = aggregationSource.keyBy(new KeySelector<Tuple2<String, Integer>, Integer>()
                {
                    @Override
                    public Integer getKey(Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple2.f0.length();
                    }
                });
        keyedStream.sum(1).print("sum-");
        keyedStream.max(1).print("max-");
        keyedStream.maxBy(1).print("maxBy-");
        keyedStream.min(1).print("min-");
        keyedStream.minBy(1).print("minBy-");

        environment.execute();

    }
}
