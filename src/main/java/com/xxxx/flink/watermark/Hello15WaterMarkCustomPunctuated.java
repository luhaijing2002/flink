package com.xxxx.flink.watermark;

import com.xxxx.flink.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Locale;

//

/**
 * 自定义水位线生成器,定点生成水位线
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello15WaterMarkCustomPunctuated {
    public static void main(String[] args) throws Exception {

        //开启一个线程来进行向kafka进行写数据
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 100; i < 200; i++) {
                if(i%5 != 0){
                    KafkaUtil.sendMsg("test", uname + ":" + i + ":" + System.currentTimeMillis());
                }else {
                    KafkaUtil.sendMsg("test", uname + ":" + i + ":" + (System.currentTimeMillis() - (long)(Math.random() * 10000)));
                }
                //设定时间间隔
                try {
                    Thread.sleep(1500);
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

        //处理数据
        DataStreamSink<Tuple3<String,String, Long>> tuple3DataStreamSink = source.map(line -> {
                    return Tuple3.of(line.split(":")[0],line.split(":")[1],Long.parseLong(line.split(":")[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(new myWatermarkStrategy03())
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((t1, t2) -> {
                    t1.f1 = t1.f1 + "_" + t2.f1;
                    return t1;
                }).print().setParallelism(1);

        //执行
        env.execute("kafka_test");


    }
}

//自定义水位线生成策略
class  myWatermarkStrategy03 implements WatermarkStrategy<Tuple3<String,String,Long>>{

    //定义水位线的生成器
    @Override
    public WatermarkGenerator<Tuple3<String, String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyPeriodicWatermarkGenerator();

    }

    /**
     *
     * 自定义水位线生成器
     */
    private class MyPeriodicWatermarkGenerator implements WatermarkGenerator<Tuple3<String,String,Long>>{

        //声明一个变量，指定它作为 当前窗口最大的值
        private Long maxTs = Long.MIN_VALUE;
        //每个事件进行调用，(一个是来一个调用一次)
        @Override
        public void onEvent(Tuple3<String, String, Long> tuple3, long eventTimestamp, WatermarkOutput output) {
            maxTs = tuple3.f2;
            System.out.println("当前[" + tuple3 + "][" + maxTs + "]");
        }

        //周期性的进行调用(一个是周期性的调用)flink自动的调用,默认200ms就发送一次。
        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            System.out.println("WatermarkOutput=【" + watermarkOutput+"】");
            //为什么要减-1，因为是左闭右开，如果
            watermarkOutput.emitWatermark(new Watermark(maxTs -1L));

        }
    }

    //定点水位线
    private class MyPunctuatedWatermarkGenerator implements WatermarkGenerator<Tuple3<String,String,Long>>{

        @Override
        public void onEvent(Tuple3<String,String,Long> event, long eventTimestamp, WatermarkOutput output) {
            if(event.f2 % 100 == 0){
                output.emitWatermark(new Watermark(event.f2 - 1L));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }

}

