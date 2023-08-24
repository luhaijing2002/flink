package com.xxxx.flink.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello14FormatJson {
    public static void main(String[] args) {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //sql创建表
        tableEnvironment.executeSql("CREATE TABLE t_emp (\n" +
                "  empno INT,\n" +
                "  ename STRING,\n" +
                "  job STRING,\n" +
                "  mgr INT,\n" +
                "  hiredate BIGINT,\n" +
                "  sal DECIMAL(10,2),\n" +
                "  comm DECIMAL(10,2),\n" +
                "  deptno INT,\n" +
                "  ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                " 'connector' = 'filesystem',\n" +
                " 'path' = 'file:///D:\\idea_java_projects\\dsj\\flume\\flink\\data\\emp.txt',\n" +
                " 'format' = 'json'\n" +
                ")");



        tableEnvironment.sqlQuery("select * from t_emp").execute().print();







    }
}
