package com.xxxx.flink.Environment;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello01ExecutionEvnironment {

    public static void main(String[] args) throws Exception {

//        //批处理
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        //实时处理
//        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        //执行代码时创建一个本地环境，可以更好的观察细节创建了执行环境，包含了一套页面。)
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);
        //transform
        source.map(word -> word.toUpperCase()).print();
        //运行环境
        environment.execute();

    }
}