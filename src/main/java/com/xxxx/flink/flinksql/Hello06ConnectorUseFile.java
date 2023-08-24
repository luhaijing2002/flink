package com.xxxx.flink.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 * flink sql 连接其他的组件
 */
public class Hello06ConnectorUseFile {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //创建表：Source,相当于使用kafka的topic充当数据源[kafka_source_topic]，在kafka的基础上使用flink套了一层表[flink_kafka_topic]
        //-- 在 Flink SQL 中注册一张 MySQL 表 'users'
        String s = "CREATE TABLE t_dept (\n" +
                "  deptno INT,\n" +
                "  dname STRING,\n" +
                "  loc STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',           -- 必选：指定连接器类型\n" +
                "  'path' = 'file:///D:\\idea_java_projects\\dsj\\flume\\flink\\data\\dept.txt',  -- 必选：指定路径\n" +
                "  'format' = 'csv'                    -- 必选：文件系统连接器指定 format\n" +
                ")";
        //执行sql,要执行后才可以使用注册到flink中的dept_copy1表,执行sql
        tableEnvironment.executeSql(s);

        //打印flink表的数据，查询结果，并且mysql中的扫描的数据是有界的，不会一直等着。
        tableEnvironment.sqlQuery("select * from t_dept").execute().print();




    }
}
