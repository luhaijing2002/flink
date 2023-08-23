package com.xxxx.flink.flinkcep;



import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxxx.flink.pojo.Emp;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello05CEPCondition {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //souce
        DataStreamSource<String> source = env.readTextFile("data/emp.txt");
        DataStream<Emp> stream = source.map(line -> new ObjectMapper().readValue(line, Emp.class));

        //声明模式
        Pattern<Emp, Emp> pattern = Pattern.<Emp>begin("start").where(new SimpleCondition<Emp>() {
            @Override
            public boolean filter(Emp emp) throws Exception {
                return emp.getDeptno().equals(10);
            }
        });

        //开始匹配流和模式(1.10后使用了的默认使用的是事件时间，而之前是处理时间)
        PatternStream<Emp> patternStream = CEP.pattern(stream, pattern).inProcessingTime();
        //处理匹配的结果
        SingleOutputStreamOperator<Emp> process = patternStream.process(new PatternProcessFunction<Emp, Emp>() {
            @Override
            public void processMatch(Map<String, List<Emp>> map, Context context, Collector<Emp> collector) throws Exception {
                System.out.println("Hello02CEPByExample.processMatch[" + map + "]");
            }
        });


//        stream.print();

        //执行环境
        env.execute();



    }
}
