package com.xxxx.flink.source;

import com.xxxx.flink.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello03SourceFromKafka {
    public static void main(String[] args) throws Exception {

        //创建一个线程持续向kafka添加数据,相当于生产者生产数据，向kafka中添加数据
        new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                KafkaUtil.sendMsg("test", "hello flink" + i +","+  System.currentTimeMillis());
                //不让输出太快
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();


        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.source
        KafkaSource<String> sourceSetting = KafkaSource.<String>builder()//得到一个kafka配置对象
                .setBootstrapServers("node01:9092,node02:node03:9092")
                .setTopics("test")
                .setGroupId("test_lhj")
                .setStartingOffsets(OffsetsInitializer.earliest())//从最早开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //使用环境对象来获取数据源
        DataStreamSource<String> source = environment.fromSource(sourceSetting, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //2.transform
        source.map(word -> word.toUpperCase()).print();
        //3.运行环境
        environment.execute();
    }
}
