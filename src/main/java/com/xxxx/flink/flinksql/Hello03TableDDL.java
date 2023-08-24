package com.xxxx.flink.flinksql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxxx.flink.pojo.Emp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello03TableDDL {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表的环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //执行环境加载source,生成一个DataStream对象，通过表的环境对这个对象进行转换生成一个table
        //方式一:
        Table table = tableEnvironment.fromDataStream(env.readTextFile("data/dept.txt"));
        table.select(Expressions.$("*")).execute().print();


        //方式二
        DataStreamSource<String> empSource = env.readTextFile("data/emp.txt");
        DataStream<Emp> empStream = empSource.map(line -> new ObjectMapper().readValue(line, Emp.class));

        Table empTable = tableEnvironment.fromDataStream(empStream);
        empTable.where(Expressions.$("job").isEqual("SALESMAN"))
                .select(Expressions.$("empno"), Expressions.$("ename").as("job"))
                .execute()
                .print();

        //创建视图:方法一
        tableEnvironment.createTemporaryView("emp", empTable);
        tableEnvironment.executeSql("select deptno,sum(sal) as sumsal from emp where job = 'CLERK' group by deptno").print();

        //创建视图:方法二
        tableEnvironment.sqlQuery("select * from emp" ).execute().print();
        tableEnvironment.sqlQuery("SELECT * FROM " + empTable).execute().print();
//        tableEnvironment.sqlQuery("SELECT * FROM " + empTable.toString()).execute().print();


        //执行环境
        env.execute();
    }

}
