package com.xxxx.flink.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello01SinkCommon {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置环境并行度
        environment.setParallelism(1);
        //source
        DataStreamSource<Integer> dataStreamSource = environment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //sink
        //方式一:直接输出到控制台
//        dataStreamSource.print();
        //方式二：直接输出到文本文件
//         dataStreamSource.writeAsText("data/hello"+System.currentTimeMillis()+".txt");
        //方式三：输出到csv文件
         dataStreamSource.map(line -> Tuple2.of(line,666), Types.TUPLE(Types.INT,Types.INT))
                 .writeAsCsv("data/hello"+System.currentTimeMillis()+".csv");
         //方式四，写出到sokect,端口+地址+序列化的方式(以什么样的方式进行写出)
        dataStreamSource.writeToSocket("localhost", 9999, new SerializationSchema<Integer>() {
            @Override
            public byte[] serialize(Integer element) {
                return element.toString().getBytes(StandardCharsets.UTF_8);
            }
        });


        //打印输出)


        //运行
        environment.execute();


    }
}
