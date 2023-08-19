package com.xxxx.flink.reduce;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello10TimeWindowByWindow {
    public static void main(String[] args) throws Exception {
        //首先进行环境配置  518484417
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源配置,定义接收数据的格式为 admin:123456
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);


        //数据处理没有分区，滑动窗口
        source.map(line -> Tuple2.of(line.split(":")[0], Integer.parseInt(line.split(":")[1]))
                        , Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple3<String,Integer,String>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple3<String,Integer,String>> out) throws Exception {
                        System.out.println("Hello10TimeWindowByWindow.apply[" +key+ "]");
                        //计算总和
                        int sum = 0;
                        for (Tuple2<String, Integer> tuple2 : input) {
                            sum += tuple2.f1;
                        }
                        //输出数据
                        out.collect(Tuple3.of(key,sum,window.toString()));;
                    }
                }).print("TimeWindows--TumblingWindow:").setParallelism(1);


        //启动任务
        env.execute();

        //操作数据
        //输出数据
        //执行任务


    }
}
