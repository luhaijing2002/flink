package com.xxxx.flink.time;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello02CountWindow {
    public static void main(String[] args) throws Exception {
        //首先进行环境配置  518484417
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源配置,定义接收数据的格式为 admin:123456
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        //数据处理，滚动窗口
//        source.map(line -> Tuple2.of(line.split(":")[0], Integer.parseInt(line.split(":")[1]))
//                        , Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(tuple2 -> tuple2.f0)
//                .countWindow(2)
//                .reduce((t1, t2) -> {
//                    t1.f1 = t1.f1 + t2.f1;
//                    return t1;
//                })
//                .print("CountWindows--TublingWindow:").setParallelism(1);


        //滑动窗口
        source.map(line -> Tuple2.of(line.split(":")[0], Integer.parseInt(line.split(":")[1]))
                        , Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .countWindow(3,2)
                .reduce((t1, t2) -> {
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                })
                .print("CountWindows--SlidinaWindow:").setParallelism(1);


        //启动任务
        env.execute();

        //操作数据
        //输出数据
        //执行任务


    }
}
