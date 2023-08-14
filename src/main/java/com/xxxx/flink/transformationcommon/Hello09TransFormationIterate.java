package com.xxxx.flink.transformationcommon;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ListIterator;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello09TransFormationIterate {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置环境并行度
        environment.setParallelism(1);
        //source
        //1. 设置初始值
        DataStreamSource<String> source = environment.fromElements("香蕉，51", "苹果，101", "桃子，202");

        //Transformation(首先对数据进行处理一下)
        DataStream<Tuple2<String, Integer>> mapStream = source.map((item) -> {
//            String[] s = item.split(" ");
//            return Tuple2.of(s[0],s[1]);
            return Tuple2.of(item.split(" ")[0], Integer.parseInt(item.split(" ")[1]));
        }, Types.TUPLE(Types.STRING, Types.INT));

        //将数据转化成可迭代的
        IterativeStream<Tuple2<String, Integer>> iterate = mapStream.iterate();

        //针对于迭代就是fro循环的定义 1.设置初始值,2.设置条件,3.自增 4.是循环体与迭代体


        //3. 迭代 --假设每天水果销售10斤
        iterate.map(tuple2 -> {
            tuple2.f1 -= 10;
            return tuple2;
        },Types.TUPLE(Types.STRING, Types.INT));

        //2. 设置判断条件
        SingleOutputStreamOperator<Tuple2<String, Integer>> filter = iterate.filter((tuple2) -> tuple2.f1 > 10);
        //3.设置结束条件
        iterate.closeWith(filter);
        
        //运行
        environment.execute();


    }

}
