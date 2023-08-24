package com.xxxx.flink.flinksql;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello14EventTime {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);


        //创建表：Source,相当于使用kafka的topic充当数据源[kafka_source_topic]，在kafka的基础上使用flink套了一层表[flink_kafka_topic]
        //sql
//            tableEnvironment.executeSql("CREATE TABLE t_event_dept (\n" +
//                    "  `deptno` INT,\n" +
//                    "  `dname` STRING,\n" +
//                    "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
//                    "  `loc` STRING,\n" +
//                    //没有时间使用kafka的元数据字段,从源数据进行采集, event_time字段定义类型为TIMESTAMP(3),并且说明是元数据中的timestamp中获取
//                    "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n" + //把元数据时间戳字段设置为水位线，并且设置了间隔时间5秒
//                    ") WITH (\n" +
//                    "  'connector' = 'kafka',\n" +
//                    "  'topic' = 'kafka_source_topic',\n" +
//                    "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
//                    "  'properties.group.id' = 'yjxxtliyi',\n" +
//                    "  'scan.startup.mode' = 'earliest-offset',\n" +
//                    "  'format' = 'csv'\n" +
//                    ")");
//
//            tableEnvironment.sqlQuery("select * from t_event_dept").execute().print();
//
//        //tableApi
//        //创建流环境
//        DataStreamSource<String> dataStream = env.readTextFile("data/dept.txt");






        //api
        //首先对数据生成水位线，并返回一个流对象，要从数据中获取自身的时间
//        DataStreamSource<String> source = env.fromElements("zhangsan," + (System.currentTimeMillis() - 100));
//        DataStream<String> stream = source.assignTimestampsAndWatermarks(WatermarkStrategy
//                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))//方法前必须要一个泛型来指定元素的类型
//                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
//                    //给当前的数据加上生成,水位线,【打上时间戳】
//                    @Override
//                    public long extractTimestamp(String element, long timestamp) {//element当前的元素，timestamp：当前的系统时间
//                        return Long.parseLong(element.split(",")[1]);
//                    }
//                }));
//
//        //方法一：过时api
//        Table table = tableEnvironment.fromDataStream(stream, $("str"), $("ts").rowtime());
//
//        tableEnvironment.sqlQuery("select * from " + table.toString()).execute().print();



        //方法二:api
        DataStreamSource<String> source = env.fromElements("zhangsan," + (System.currentTimeMillis() - 100));

        SingleOutputStreamOperator<Tuple2<String, Long>> map = source.map(line -> {
            String[] split = line.split(",");
            return Tuple2.of(split[0], Long.parseLong(split[1]));
        }, Types.TUPLE(Types.STRING, Types.LONG));


        Table eventTable2 = tableEnvironment.fromDataStream(map, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.BIGINT())
                .columnByExpression("processTime", "now()")
                .columnByExpression("eventTime", "TO_TIMESTAMP(FROM_UNIXTIME(f1/1000))")
                .watermark("eventTime", "eventTime - INTERVAL '5' SECOND")
                .build());


        tableEnvironment.sqlQuery("select * from " + eventTable2.toString()).execute().print();



    }
}
