package com.xxxx.flink.sink;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello13sinkToJDBCPlus {
    public static void main(String[] args) throws Exception {
        //运行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //操作数据,可以是从外部来的，比如格式为admin:123456
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        //对数据的格式进行转换
        SingleOutputStreamOperator<Tuple3<String, String, String>> mapStream = source.map(line -> {
            String[] split = line.split(":");
            return Tuple3.of(split[0], split[1], String.valueOf(System.currentTimeMillis()));
        }, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));

        mapStream.addSink(JdbcSink.sink(
                "insert into t_bullet_chat (id, username, msg, ts) values (?,?,?,?)",
                (preparedStatement, tuple3) -> {
                    preparedStatement.setString(1, RandomStringUtils.randomAlphabetic(8));//id
                    preparedStatement.setString(2, tuple3.f0);//username
                    preparedStatement.setString(3, tuple3.f1);//msg
                    preparedStatement.setString(4, tuple3.f2);//ts
                },
                new JdbcExecutionOptions.Builder().withBatchSize(2).build(),
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
