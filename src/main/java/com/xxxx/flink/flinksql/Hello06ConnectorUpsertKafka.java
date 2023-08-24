package com.xxxx.flink.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 * flink sql 连接其他的组件
 */
public class Hello06ConnectorUpsertKafka {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //设置并行度
        env.setParallelism(1);

        //使用DataGen 生成数据

        tableEnvironment.executeSql("CREATE TABLE t_dept (\n" +
                " deptno INT,\n" +
                " salenum INT,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" + //一次5条数据与并行度有点小问题+
                " 'fields.deptno.min'='88',\n" +
                " 'fields.deptno.max'='99',\n" +
                " 'fields.salenum.min'='1',\n" +
                " 'fields.salenum.max'='9'\n" +
                ")");

        //执行并输出,查询部门销售详情
//        tableEnvironment.sqlQuery("select deptno,sum(salenum) as sumsale from t_dept group by deptno").execute().print();


        //把这样的信息添加到kafka的表中，插入到一张kafka的表中
        tableEnvironment.executeSql("CREATE TABLE flink_dept_sum (\n" +
                "  `deptno` INT,\n" +
                "  `sumsale` INT,\n" +
                "  PRIMARY KEY (deptno) NOT ENFORCED\n" +//多设置了一个主键，用于判断这个数据以哪个字段做为判断的标准。
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'topic_dept_sum',\n" + //	topic_dept_sum
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'key.format' = 'csv',\n" +
                "  'value.format' = 'json'\n" +
                ")");

        //插入数据
        tableEnvironment.executeSql("insert into flink_dept_sum select deptno,sum(salenum) as sumsale from t_dept group by deptno");


    }
}
