package com.xxxx.flink.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class test {

    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("create table t_dept (\n" +
                " deptno int,\n" +
                " salenum int,\n" +
                " ts as localtimestamp,\n" +
                " watermark for ts as ts\n" +
                ") with (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.deptno.min'='88',\n" +
                " 'fields.deptno.max'='99',\n" +
                " 'fields.salenum.min'='1',\n" +
                " 'fields.salenum.max'='9'\n" +
                ")");
        //查询部门销售详情
        // tableEnvironment.sqlQuery("select deptno,sum(salenum) as sumsale from t_dept group by deptno").execute().print();

        //插入到一张Kafka的表中
        tableEnvironment.executeSql("create table flink_dept_sale_sum (\n" +
                "  deptno int,\n" +
                "  sumsale int,\n" +
                "  primary key (deptno) not enforced\n" +
                ") with (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'topic_dept_sum',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'key.format' = 'csv',\n" +
                "  'value.format' = 'json'\n" +
                ");");
        //插入数据
        tableEnvironment.executeSql("insert into flink_dept_sale_sum select deptno,sum(salenum) as sumsale from t_dept group by deptno");

    }
}
