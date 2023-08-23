package com.xxxx.flink.Late.join;

import com.xxxx.flink.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello07interval {
    public static void main(String[] args) throws Exception {
        //创建一个线程生成数据
        new Thread(() -> {
            for (int i = 100; i < 200; i++) {
//                //生成一个商品ID
//                String goodId = RandomStringUtils.randomAlphabetic(16).toLowerCase();
                //发送goodInfo数据 [id:info:ts]
                KafkaUtil.sendMsg("t_goodinfo", i + ":info" + i + ":" + System.currentTimeMillis());

                //让线程休眠一下
                try {
                    Thread.sleep((int)(Math.random() * 3000) );
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();




        //创建一个线程生成数据
        new Thread(() -> {
            for (int i = 100; i < 200; i++) {
//                //生成一个商品ID
//                String goodId = RandomStringUtils.randomAlphabetic(16).toLowerCase();
                //创建goodPrice数据[id:price:ts]
                    KafkaUtil.sendMsg("t_goodprice", i + ":info" + i + ":" + System.currentTimeMillis());
                //让线程休眠一下
                try {
                    Thread.sleep((int)(Math.random() * 3000));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();


        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //数据源
        DataStreamSource<String> goodInfoSource = env.fromSource(KafkaUtil.getKafka("t_goodinfo", "lhj"), WatermarkStrategy.noWatermarks(), "goodInfoSource");
        DataStreamSource<String> goodPriceSource = env.fromSource(KafkaUtil.getKafka("t_goodprice", "lhj"), WatermarkStrategy.noWatermarks(), "goodpriceSource");

        //转换操作
        SingleOutputStreamOperator<Tuple3<String, String, Long>> infoStream = goodInfoSource.map(line -> {
                    String[] split = line.split(":");
                    return Tuple3.of(split[0], split[1], Long.parseLong(split[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> priceStream = goodPriceSource.map(line -> {
                    String[] split = line.split(":");
                    return Tuple3.of(split[0], split[1], Long.parseLong(split[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, recordTime) -> {
                            return element.f2;
                        }));

        infoStream.keyBy(tuple3 ->tuple3.f0)
                .intervalJoin(priceStream.keyBy(tuple3 ->tuple3.f0))
                .between(Time.seconds(-10), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> info, Tuple3<String, String, Long> price, ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("info:" + info + " price:" + price +", ctx:" + ctx);
                    }
                }).print("interval").setParallelism(1);

//        infoStream.join(priceStream)
//                .where(i -> i.f0)
//                .equalTo(j -> j.f0)
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
//                    @Override
//                    public String join(Tuple3<String, String, Long> info, Tuple3<String, String, Long> price) throws Exception {
//                        return "[" + info + "]" + "," + "[" + price + "]";
//                    }
//                }).print();


        //执行环境
        env.execute();
    }
}
