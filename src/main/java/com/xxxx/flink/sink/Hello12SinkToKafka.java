package com.xxxx.flink.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello12SinkToKafka {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置环境并行度
        environment.setParallelism(1);
        //source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 9999);

        //kafka的sink配置信息
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink_sink")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)//交货保证，设置为至少一次
                .build();

        source.sinkTo(sink);
        //运行
        environment.execute();


    }
}
