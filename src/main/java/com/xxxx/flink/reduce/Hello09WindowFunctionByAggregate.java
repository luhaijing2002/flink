package com.xxxx.flink.reduce;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello09WindowFunctionByAggregate {
    public static void main(String[] args) throws Exception {
        //首先进行环境配置  518484417
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源配置,定义接收数据的格式为 admin:123456
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        //TimeWindow--tumbling
        //数据处理没有分区，滚动窗口
        source.map(line -> Tuple2.of(line.split(":")[0], Integer.parseInt(line.split(":")[1]))
                        , Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .countWindow(2)
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0,0);
                    }

                    //acc是中间的状态(sum,count)
                    @Override
                    public Tuple2<Integer, Integer> add(Tuple2<String, Integer> in, Tuple2<Integer, Integer> acc) {
                        //sum
                        acc.f0 += in.f1;
                        //count
                        acc.f1 +=1;
                        return acc;
                    }

                    //有地方来计算平均值，不需要进行使用中间状态来保存平均值数据
                    //在Reduce中没有可以保存中间状态的结果的东西，所以只能自己去定义一个变量在tupe4里面来接收avg的中间结果
                    //在这里有累加器
                    @Override
                    public Double getResult(Tuple2<Integer, Integer> acc) {
                        if(acc.f1 != 0){
                            return acc.f0 * 1.0 /acc.f1;
                        }
                        return 0.0;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return null;
                    }
                }).print("TimeWindows--TumblingWindow:").setParallelism(1);


        //启动任务
        env.execute();

        //操作数据
        //输出数据
        //执行任务


    }
}
