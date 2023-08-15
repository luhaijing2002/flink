package com.xxxx.flink.time;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello07GlobalWindowAll {
    public static void main(String[] args) throws Exception {
        //首先进行环境配置  518484417
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源配置,定义接收数据的格式为 admin:123456
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
//        //TimeWindow--tumbling
////        全局窗口，滚动窗口
//        source.map(line -> Tuple2.of(line.split(":")[0], Integer.parseInt(line.split(":")[1]))
//                        , Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(word -> word.f0)
//                .window(GlobalWindows.create())
//                .trigger(PurgingTrigger.of(CountTrigger.of(5)))
//                .reduce((t1, t2) -> {
//                    t1.f0 = t1.f0 + "_" + t2.f0;
//                    t1.f1 = t1.f1 + t2.f1;
//                    return t1;
//                }).map(tuple2 -> {
//                    tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy年MM月dd日 HH时mm分ss秒SSS毫秒")) + tuple2.f0;
//                    return tuple2;
//                },Types.TUPLE(Types.STRING,Types.INT)).print("TimeWindows--window:").setParallelism(1);


        //        全局窗口，滚动窗口
        source.map(line -> Tuple2.of(line.split(":")[0], Integer.parseInt(line.split(":")[1]))
                        , Types.TUPLE(Types.STRING, Types.INT))
                .windowAll(GlobalWindows.create())
                .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                .reduce((t1, t2) -> {
                    t1.f0 = t1.f0 + "_" + t2.f0;
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                }).map(tuple2 -> {
                    tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy年MM月dd日 HH时mm分ss秒SSS毫秒")) + tuple2.f0;
                    return tuple2;
                },Types.TUPLE(Types.STRING,Types.INT)).print("TimeWindows--windowAll:").setParallelism(1);




        //启动任务
        env.execute();

        //操作数据
        //输出数据
        //执行任务


    }
}
