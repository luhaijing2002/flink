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

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello10TimeWindowByProcessWindowFunction {
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
                .process(new ProcessWindowFunction<Tuple2<String, Integer>,Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println("ProcessWindowFunction--SlidingProcessingTimeWindows: ["+key+"]");
                        int sum = 0;
                        StringBuffer buffer = new StringBuffer();
                        for (Tuple2<String, Integer> tuple2 : elements) {
                            buffer.append(tuple2.f0+"_");
                            sum += tuple2.f1;
                        }
                        buffer.append("{水位线:"+context.currentWatermark()+",窗口信息:"+context.window()+",窗口状态"+context.windowState()+"}");
                        out.collect(Tuple2.of(buffer.toString(), sum ));
                    }
                })
                .print("ProcessWindowFunction--SlidingProcessingTimeWindows:").setParallelism(1);


        //启动任务
        env.execute();

        //操作数据
        //输出数据
        //执行任务


    }
}
