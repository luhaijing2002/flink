package com.xxxx.flink.source;

import com.xxxx.flink.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.util.List;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello04SourceCustom {
    public static void main(String[] args) throws Exception {

        // 创建一个流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 一个数据源,并行度设置为1可以保证，数据有序
        DataStreamSource<String> source = env.addSource(new MySource("data/secret.txt")).setParallelism(2);
        //直接打印
        source.print().setParallelism(1);
        // 执行
        env.execute();

    }
}
//自定义数据源

class MySource implements ParallelSourceFunction<String> { //使用ParallelSourceFunction可以设置并行度
    private String filePath;

    public MySource(String filePath) {
        this.filePath = filePath;
    }

    public MySource() {
    }


    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //运行方法，实现逻辑的方法
        //读取数据文件
        List<String> lines = FileUtils.readLines(new File(filePath),"utf-8");
        //遍历数据添加到SourceContext
        for (String line : lines) {
            //DESUtil是一个对称加密算法，decrypt进行解密。
            ctx.collect(DESUtil.decrypt("yjxxt0523",line));
        }
    }

    @Override
    public void cancel() {
        //取消方法

    }
}

