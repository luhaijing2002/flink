package com.xxxx.flink.flinksql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello13ProcessTime {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //创建流环境
        DataStreamSource<String> dataStream = env.readTextFile("data/dept.txt");




        //sql创建表
//        tableEnvironment.executeSql("CREATE TABLE t_dept (\n" +
//                "  `deptno` INT,\n" + //物理字段
//                "  `dname` STRING,\n" +
//                "  `loc` STRING,\n" +
//                "  `pt` AS PROCTIME()\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'test_1day',\n" +
//                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
//                "  'properties.group.id' = 'yjxxtliyi',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'csv'\n" +
//                ")");
//
//        tableEnvironment.sqlQuery("select * from t_dept").execute().print();



        //dataStream方式一
        Table t_dept2 = tableEnvironment.fromDataStream(dataStream, $("str"), $("ts").proctime());

        tableEnvironment.sqlQuery("select * from " + t_dept2.toString()).execute().print();

        //dataStream方式二
        Table t_dept3 = tableEnvironment.fromDataStream(dataStream, Schema.newBuilder()
                .column("f0", DataTypes.STRING())//使用占位符在表示列
                .columnByExpression("ts", "now()")
                .build());
        tableEnvironment.sqlQuery("select * from " + t_dept3.toString()).execute().print();




    }

}