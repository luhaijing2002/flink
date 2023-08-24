package com.xxxx.flink.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 * flink sql 连接其他的组件
 */
public class Hello05ConnectorUseKafka {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //创建表：Source,相当于使用kafka的topic充当数据源[kafka_source_topic]，在kafka的基础上使用flink套了一层表[flink_kafka_topic]
        String flink_source_table  = "CREATE TABLE flink_source_table (\n" +
                "  `deptno` INT,\n" +
                "  `dname` STRING,\n" +
                "  `loc` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'kafka_source_topic',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'yjxxtliyi',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";
        //执行sql
        tableEnvironment.executeSql(flink_source_table);

        //查询结果
//        tableEnvironment.sqlQuery("select * from flink_kafka_topic").execute().print();

        //创建表：SINK 使用flink定义对kafka的topic使用flink操作时，对两个表进行一个查询添加的操作，将一个kafka中topic的数据添加到另一个topic中去。
        tableEnvironment.executeSql("CREATE TABLE flink_sink_table (\n" +
                "  `deptno` INT,\n" +
                "  `dname` STRING,\n" +
                "  `loc` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'kafka_sink_topic',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'yjxxtliyi',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        //运行代码--查询数据
//         tableEnvironment.sqlQuery("select * from flink_source_table").execute();

        //将一个表查询的数据输出到另外一张表
        //方式一：使用Table API,最后要使用执行的方法调用。
//        tableEnvironment.sqlQuery("select * from flink_source_table").insertInto("flink_sink_table").execute();
        //方式二:使用的是sql的操作方法
        tableEnvironment.executeSql("insert into flink_sink_table select * from flink_source_table");

//        env.execute();

    }
}
