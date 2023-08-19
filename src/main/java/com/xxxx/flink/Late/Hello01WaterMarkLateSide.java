package com.xxxx.flink.Late;

import com.xxxx.flink.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Locale;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello01WaterMarkLateSide {
    public static void main(String[] args) throws Exception {

        //开启一个线程来进行向kafka进行写数据
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 100; i < 200; i++) {
                if (i % 10 == 0) {//设定真正迟到的数据，走侧输出流
                    KafkaUtil.sendMsg("test", uname + ":" + i + ":" + (System.currentTimeMillis() - 15000L));
                } else if (i % 5 == 0) {//设定迟到的数据被救回来
                    KafkaUtil.sendMsg("test", uname + ":" + i + ":" + (System.currentTimeMillis() - 5000L));
                }else{
                    KafkaUtil.sendMsg("test", uname + ":" + i + ":" + System.currentTimeMillis());
                }
                //设定时间间隔
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        //运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //源数据
        DataStreamSource<String> source = env.fromSource(KafkaUtil.getKafka("test", "test_lhj"), WatermarkStrategy.noWatermarks(), "kafka_source");

        //定义侧输出流
        OutputTag<Tuple3<String, String, Long>> output = new OutputTag<Tuple3<String, String, Long>>("side") {};


        //处理数据
        SingleOutputStreamOperator<String> streamOperator = source.filter(line -> line.split(":").length == 3)
                .map(line -> {
                    String[] split = line.split(":");
                    if (split.length >= 3) {
                        return Tuple3.of(split[0], split[1], Long.parseLong(split[2]));
                    } else {
                        return Tuple3.of("null", "null", 0L);
                    }
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((tuple3, recordTs) -> tuple3.f2))
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))//5秒一个窗口
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(output)
                .apply(new WindowFunction<Tuple3<String, String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<String> out) throws Exception {
                        StringBuffer buffer = new StringBuffer();
                        buffer.append("key");
                        for (Tuple3<String, String, Long> tuple3 : input) {
                            buffer.append("[" + tuple3.f1 + "_" + tuple3.f2 + "],");
                        }
                        buffer.append("[" + window + "]");
                        //返回结果
                        out.collect(buffer.toString());
                    }
                });

        //主流数据
        streamOperator.print("Main");
        //侧输出流
        streamOperator.getSideOutput(output).print("output");

        //执行
        env.execute("kafka_test");


    }

}
