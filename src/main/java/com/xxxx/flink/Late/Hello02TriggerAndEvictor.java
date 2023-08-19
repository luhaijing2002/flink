package com.xxxx.flink.Late;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello02TriggerAndEvictor {

    public static void main(String[] args) throws Exception {
        //首先进行环境配置  518484417
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源配置,定义接收数据的格式为 admin:123456
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        //TimeWindow--tumbling


        //数据处理没有分区，滑动窗口
        source.map(line -> Tuple2.of(line.split(":")[0], Integer.parseInt(line.split(":")[1]))
                        , Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(10))
                .evictor(CountEvictor.of(10))
                .reduce((t1, t2) -> {
                    t1.f0 = t1.f0 + "_" + t2.f0;
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                }).print("TimeWindows--TumblingWindow:").setParallelism(1);



        //启动任务
        env.execute();

        //操作数据
        //输出数据
        //执行任务


    }

}
