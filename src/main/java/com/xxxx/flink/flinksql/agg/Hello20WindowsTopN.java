package com.xxxx.flink.flinksql.agg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello20WindowsTopN {
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
        tableEnvironment.executeSql("CREATE TABLE t_goods (\n" +
                " gid STRING,\n" +
                " type INT,\n" +
                " price INT,\n" +
                " ts AS localtimestamp,\n" + //使用本地的时间戳
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" + //设置水位线，并结束时间-5
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='10',\n" + //一次5条数据与并行度有点小问题，n/s条数据
                " 'fields.type.min'='1',\n" +
                " 'fields.type.max'='1',\n" +
                " 'fields.price.min'='100',\n" +
                " 'fields.price.max'='999',\n" +
                " 'fields.gid.length'='10'\n" +//随机生成字符串，并且长度为10
                ")");



        //排序窗口函数，所有窗口的排序

        tableEnvironment.sqlQuery("select * from (" +
                "   select *,ROW_NUMBER() \n" +
                "   OVER(partition by type " +
                "   order by price desc)" +
                "   AS rownum from t_goods "+
                ") WHERE rownum <= 3").execute().print();






    }



}
