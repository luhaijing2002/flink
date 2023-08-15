package com.xxxx.flink.sink;

import com.xxxx.flink.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.File;
import java.util.ArrayList;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class HeLlo15SinkCustom {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置环境并行度
        environment.setParallelism(2);
        //操作数据
        ArrayList<String> list = new ArrayList<>();
        list.add("君子周而不比，小人比而不周");
        list.add("君子喻于义，小人喻于利");
        list.add("君子怀德，小人怀土，君子怀刑，小人怀惠");
        list.add("君子欲讷于言而敏于行");
        list.add("君子坦荡荡，小人长戚戚");
        DataStreamSource<String> source = environment.fromCollection(list);

        //写出数据
        source.addSink(new mySink("data/sink"+System.currentTimeMillis()+".txt")).setParallelism(1);
        //运行环境
        environment.execute();


    }
}

class mySink implements SinkFunction<String>{
    private String filePath;

    public mySink(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void invoke(String line, Context context) throws Exception {
        //加密数据
        String encode = DESUtil.encrypt("yjxxt0523",line) + "\r\n";
        //写出数据(到文件中)
        FileUtils.writeStringToFile(new File(filePath),encode,"utf-8",true);
    }
}

