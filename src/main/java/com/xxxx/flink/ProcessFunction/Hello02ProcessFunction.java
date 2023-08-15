package com.xxxx.flink.ProcessFunction;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.lang.model.element.ExecutableElement;
import java.util.stream.Stream;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello02ProcessFunction {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //源
        DataStreamSource<String> source = env.fromElements("aa", "bb", "cc", "a", "b", "c", "ddd", "aaa");


        //实现长度过滤
//        FilterOperator<String> filter1 = source.filter(w -> w.length() == 1);
//        filter1.print();
//        FilterOperator<String> filter2 = source.filter(w -> w.length() == 2);


        //使用侧输出，读取一次，将符合要求的数据全部取出
        OutputTag<String> outputTag1 = new OutputTag<String>("output1") {};
        OutputTag<String> outputTag2 = new OutputTag<String>("output2") {};
        //
        SingleOutputStreamOperator<String> process = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                if (value.length() == 1) {
                    ctx.output(outputTag1, value);
                } else if (value.length() == 2) {
                    ctx.output(outputTag2, value);
                }
                out.collect(value);
            }
        });

        //执行
        process.print();

//        System.out.println("===============");
        process.getSideOutput(outputTag1).print("outputTag1").setParallelism(1);
//        System.out.println("===============");
        process.getSideOutput(outputTag2).print("outputTag2").setParallelism(1);


        //执行
        env.execute();
    }
}
