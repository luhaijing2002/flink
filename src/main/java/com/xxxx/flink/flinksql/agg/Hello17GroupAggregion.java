package com.xxxx.flink.flinksql.agg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello17GroupAggregion {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //设置并行度
        env.setParallelism(1);


        /**
         * gid:商品id
         * sales:投票数
         */

        //使用DataGen 生成数据
        tableEnvironment.executeSql("CREATE TABLE t_access (\n" +
                " uid INT,\n" +
                " url STRING,\n" +
                " ts AS localtimestamp,\n" + //使用本地的时间戳
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" + //设置水位线，并结束时间-5
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='10',\n" + //一次10条数据与并行度有点小问题，n/s条数据
                " 'fields.uid.min'='1000',\n" +
                " 'fields.uid.max'='2000',\n" +
                " 'fields.url.length'='10'\n" +//随机生成字符串，并且长度为10
                ")");

//        //查询表的数据

        //使用group by 聚合函数
//        tableEnvironment.sqlQuery("select * from t_access").execute().print();

        //统计网站的pv，uv数
        tableEnvironment.sqlQuery("select count(url) as pv,count(distinct url) as uv from t_access;").execute().print();

        //去重

    }
}
