package com.xxxx.flink.flinksql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 将数据导入到临时数据后，再进行读取topic表数据
 *
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello11Schema {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

////        //使用sql进行连接kafka，并执行sql,生成表并将这个表注册为flink中
//        tableEnvironment.executeSql( "CREATE TABLE flink_source_table (\n" +
//                "  `deptno` INT,\n" +
//                "  `dname` STRING,\n" +
//                "  `loc` STRING\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'kafka_source_topic',\n" +
//                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
//                "  'properties.group.id' = 'yjxxtliyi',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'csv'\n" +
//                ")");
//
//        tableEnvironment.executeSql( "CREATE TABLE test (\n" +
//                "  `deptno` INT,\n" +
//                "  `dname` STRING,\n" +
//                "  `loc` STRING\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'test_1day',\n" +
//                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
//                "  'properties.group.id' = 'yjxxtliyi',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'csv'\n" +
//                ")");
////
////
//////        tableEnvironment.sqlQuery("select * from flink_source_table");
//        tableEnvironment.executeSql("insert into test select * from flink_source_table");
//        tableEnvironment.sqlQuery("select * from test").execute().print();//会卡在这里

        //sql方式
        tableEnvironment.executeSql("CREATE TABLE test_sql (\n" +
                "  `deptno` INT,\n" + //物理字段
                " `deptno_new` AS deptno * 10 ,\n" + //计算字段
                "`event_time` TIMESTAMP(3) METADATA FROM 'timestamp'," + //元数据字段
                "  `dname` STRING,\n" +
                "  `loc` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'test_1day',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'yjxxtliyi',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");

//        tableEnvironment.sqlQuery("select * from test_sql").execute().print();

        //table_api(path：表名，descriptor:描述表的信息)
        tableEnvironment.createTable("test_table_api", TableDescriptor.forConnector("kafka")
                //表或视图的架构{物理字段+计算字段+元数据字段}
                .schema(Schema.newBuilder()
                        .column("deptno", DataTypes.INT())
                        .columnByExpression("deptno_new", "deptno * 10")
                        .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "timestamp", true)
                        .column("dname", DataTypes.STRING())
                        .column("loc", DataTypes.STRING())
                        .build())
                .option("topic", "test_1day")
                .option("properties.bootstrap.servers", "node01:9092,node02:9092,node03:9092")
                .option("properties.group.id", "yjxxtliyi")
                .option("scan.startup.mode", "earliest-offset")
                .format("csv")
                .build());

        tableEnvironment.sqlQuery("select * from test_table_api").execute().print();


//        env.execute();

    }


}
