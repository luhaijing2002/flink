package com.xxxx.flink.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.security.KeyRep;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello03StateKeyed {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path" ,"D:\\idea_java_projects\\dsj\\flume\\flink\\ckpt\\18f2451af6094560ff5f5b5429d06eee\\chk-20");
        //运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        //开启检查点，5秒保存一次
        env.enableCheckpointing(5000);
        //设置保存路径为file:///D:\idea_java_projects\dsj\flume\flink\ckpt
        env.getCheckpointConfig().setCheckpointStorage("file:///" + System.getProperty("user.dir") + File.separator + "ckpt"+ File.separator +  System.currentTimeMillis());

        //定义数据源[水果:重量]
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        //数据转换
        source.map( line -> {
            String[] split = line.split(",");
            return Tuple2.of(split[0], Integer.parseInt(split[1]));
        }, Types.TUPLE(Types.STRING,Types.INT) ).keyBy(tuple -> tuple.f0)
                .reduce(new MyKeyedStateFunction())
                .print();

        //执行
        env.execute();
    }
}

class MyKeyedStateFunction extends RichReduceFunction<Tuple2<String,Integer>>{

    //首先声明一个状态对象,这个值对象怎么初始化，原来是调用的initializeState方法，而这边初始化要重写open方法
    private ValueState<Tuple2<String, Integer>>  valueState;


    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
        t1.f1 += t2.f1;
        //保存状态
        valueState.update(t1);
        return t1;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始状态对象,首先生成对应状态的描述对象
        ValueStateDescriptor<Tuple2<String, Integer>> reduceValueState = new ValueStateDescriptor<>("reduceValueState", Types.TUPLE(Types.STRING,Types.INT));
        this.valueState = getRuntimeContext().getState(reduceValueState);
    }

}
