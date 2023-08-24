package com.xxxx.flink.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.rocksdb.util.Environment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello01Environment {

    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //方法一：表环境
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);


        //方法二:表环境
        StreamTableEnvironment tableEnvironment02 = StreamTableEnvironment.create(env);

    }
}
