package com.xxxx.flink.flinksql.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 滚动窗口
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello15TVFTumble {
    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //设置并行度
        env.setParallelism(1);

        //使用DataGen 生成数据

        tableEnvironment.executeSql("CREATE TABLE t_goods (\n" +
                " gid INT,\n" +
                " sales INT,\n" +
                " ts AS localtimestamp,\n" + //使用本地的时间戳
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" + //设置水位线，并结束时间-5
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='5',\n" + //一次5条数据与并行度有点小问题，n/s条数据
                " 'fields.gid.min'='100',\n" +
                " 'fields.gid.max'='200',\n" +
                " 'fields.sales.min'='1',\n" +
                " 'fields.sales.max'='9'\n" +
                ")");

        //执行并输出
//        tableEnvironment.sqlQuery("SELECT * FROM TABLE(TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECOND));").execute().print();


        //分组查询
//        tableEnvironment.sqlQuery("SELECT window_start, window_end, SUM(sales)\n" +
//                "  FROM TABLE(\n" +
//                "    TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECOND))\n" +
//                "  GROUP BY window_start, window_end;").execute().print();

        //
        tableEnvironment.sqlQuery("SELECT window_start,window_end,gid,SUM(sales)\n" +
                "  FROM TABLE(\n" +
                "  TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECOND))\n" +
                "  GROUP BY window_start, window_end,gid;").execute().print();




    }
}
