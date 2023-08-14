package com.xxxx.flink.transformationcommon;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello07TransformationReduce {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置环境并行度
        environment.setParallelism(1);

        //从文件中读取数据,其中是v1就是合并的值，而v2就是当前的值
        environment.fromElements("a","bb","cc","c","dd","e","f","g","h","i","z","ss","bb","zz")
                .keyBy(String::length)
                .reduce((v1, v2) -> v1 + "," + v2)
                .print();
        //运行
        environment.execute();


    }
}
