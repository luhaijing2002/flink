package com.xxxx.flink.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;

import java.util.Properties;

/**
 * Kafka工具类
 */
public class KafkaUtil {
    //获取生产者对象
    private static KafkaProducer<String, String> kafkaProducer = getKafka();

    /**
     * 创建生产者
     *
     * @return
     */
    private static KafkaProducer<String, String> getKafka() {
        //创建配置文件列表
        Properties properties = new Properties();
        // kafka地址，多个地址用逗号分割
        properties.put("bootstrap.servers", "node01:9092,node02:node03:9092");
        //设置写出数据的格式
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //写出的应答方式
        properties.put("acks", "all");
        //错误重试
        properties.put("retries", 1);
        //批量写出
        properties.put("batch.size", 16384);
        //创建生产者对象
        return new KafkaProducer<String, String>(properties);
    }

    /**
     * 向Kafka发送消息
     *
     * @param topicName
     * @param msg
     */
    public static void sendMsg(String topicName, String msg) {
        //封装消息对象
        ProducerRecord<String, String> banRecordBlue = new ProducerRecord<>(topicName, null, msg);
        //发送消息
        kafkaProducer.send(banRecordBlue);
        //刷出消息
        kafkaProducer.flush();
    }

    /**
     * 获取flink的kafka source
     * @param topic
     * @param groupId
     * @return
     */
    public static KafkaSource<String> getKafka(String topic,String groupId){
        //1.source
        KafkaSource<String> source = KafkaSource.<String>builder()//得到一个kafka配置对象
                .setBootstrapServers("node01:9092,node02:node03:9092")
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())//从最早开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return source;
    }






}
