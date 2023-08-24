package com.xxxx.flink.flinksql.agg;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello19GroupSet {
    public static void main(String[] args) {
        //构建一个批处理环境
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        //执行sql
        //1、使用正常使用
        tableEnvironment.sqlQuery("SELECT pid, sum(num) AS total\n" +
                "FROM (VALUES\n" +
                " ('省1','市1','县1',100),\n" +
                " ('省1','市2','县2',101),\n" +
                " ('省1','市2','县1',102),\n" +
                " ('省2','市1','县4',103),\n" +
                " ('省2','市2','县1',104),\n" +
                " ('省2','市2','县1',105),\n" +
                " ('省3','市1','县1',106),\n" +
                " ('省3','市2','县1',107),\n" +
                " ('省3','市2','县2',108),\n" +
                " ('省4','市1','县1',109),\n" +
                " ('省4','市2','县1',110))\n" +
                "AS t_person_num(pid, cid, xid,num)\n" +
                "GROUP BY pid;").execute().print();


        //使用GroupSet
        tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
                "FROM (VALUES\n" +
                " ('省1','市1','县1',100),\n" +
                " ('省1','市2','县2',101),\n" +
                " ('省1','市2','县1',102),\n" +
                " ('省2','市1','县4',103),\n" +
                " ('省2','市2','县1',104),\n" +
                " ('省2','市2','县1',105),\n" +
                " ('省3','市1','县1',106),\n" +
                " ('省3','市2','县1',107),\n" +
                " ('省3','市2','县2',108),\n" +
                " ('省4','市1','县1',109),\n" +
                " ('省4','市2','县1',110))\n" +
                "AS t_person_num(pid, cid, xid,num)\n" +
                "GROUP BY GROUPING SETS ((pid, cid, xid), (pid, cid), (pid),());").execute().print();

        //使用ROLLUP
        tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
                "FROM (VALUES\n" +
                " ('省1','市1','县1',100),\n" +
                " ('省1','市2','县2',101),\n" +
                " ('省1','市2','县1',102),\n" +
                " ('省2','市1','县4',103),\n" +
                " ('省2','市2','县1',104),\n" +
                " ('省2','市2','县1',105),\n" +
                " ('省3','市1','县1',106),\n" +
                " ('省3','市2','县1',107),\n" +
                " ('省3','市2','县2',108),\n" +
                " ('省4','市1','县1',109),\n" +
                " ('省4','市2','县1',110))\n" +
                "AS t_person_num(pid, cid, xid,num)\n" +
                "GROUP BY ROLLUP (pid, cid, xid);").execute().print();

        //使用CUBE
        tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
                "FROM (VALUES\n" +
                " ('省1','市1','县1',100),\n" +
                " ('省1','市2','县2',101),\n" +
                " ('省1','市2','县1',102),\n" +
                " ('省2','市1','县4',103),\n" +
                " ('省2','市2','县1',104),\n" +
                " ('省2','市2','县1',105),\n" +
                " ('省3','市1','县1',106),\n" +
                " ('省3','市2','县1',107),\n" +
                " ('省3','市2','县2',108),\n" +
                " ('省4','市1','县1',109),\n" +
                " ('省4','市2','县1',110))\n" +
                "AS t_person_num(pid, cid, xid,num)\n" +
                "GROUP BY CUBE (pid, cid, xid);").execute().print();



        //使用CUBE
//        tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
//                "FROM (VALUES\n" +
//                " ('省1','市1','县1',100),\n" +
//                " ('省1','市2','县2',101),\n" +
//                " ('省1','市2','县1',102),\n" +
//                " ('省2','市1','县4',103),\n" +
//                " ('省2','市2','县1',104),\n" +
//                " ('省2','市2','县1',105),\n" +
//                " ('省3','市1','县1',106),\n" +
//                " ('省3','市2','县1',107),\n" +
//                " ('省3','市2','县2',108),\n" +
//                " ('省4','市1','县1',109),\n" +
//                " ('省4','市2','县1',110))\n" +
//                "AS t_person_num(pid, cid, xid,num)\n" +
//                "GROUP BY GROUPING SET (\n" +
//                "    ( pid, cid, xid ),\n" +
//                "    ( pid, cid         ),\n" +
//                "    ( pid,             xid ),\n" +
//                "    ( pid                     ),\n" +
//                "    (              cid, xid ),\n" +
//                "    (              cid         ),\n" +
//                "    (                          xid),\n" +
//                "    (                              );").execute().print();




    }
}
