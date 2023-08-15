package com.xxxx.flink.sink;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;
import java.util.RandomAccess;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello13sinkToJDBC {
    public static void main(String[] args) throws Exception {
        //运行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //操作数据
        List<Tuple3<String,String,String>> list = new LinkedList<>();
        list.add(Tuple3.of("张三1","good1",String.valueOf(System.currentTimeMillis())));
        list.add(Tuple3.of("张三2","good2",String.valueOf(System.currentTimeMillis())));
        list.add(Tuple3.of("张三3","good3",String.valueOf(System.currentTimeMillis())));
        list.add(Tuple3.of("张三4","good4",String.valueOf(System.currentTimeMillis())));

        DataStreamSource<Tuple3<String, String, String>> source = env.fromCollection(list);

        source.addSink(JdbcSink.sink(
                "insert into t_bullet_chat (id, username, msg, ts) values (?,?,?,?)",
                (preparedStatement, tuple3) -> {
                    preparedStatement.setString(1, RandomStringUtils.randomAlphabetic(8));//id
                    preparedStatement.setString(2, tuple3.f0);//username
                    preparedStatement.setString(3, tuple3.f1);//msg
                    preparedStatement.setString(4, tuple3.f2);//ts
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUrl("jdbc:mysql://node01:3306/scott?useUnicode=true&characterEncoding=utf-8&serverTimezone=GMT%2B8&useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));

        //运行
        env.execute();


    }

}
