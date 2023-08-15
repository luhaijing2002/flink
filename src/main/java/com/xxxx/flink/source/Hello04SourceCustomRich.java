package com.xxxx.flink.source;

import com.xxxx.flink.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.File;
import java.util.List;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello04SourceCustomRich {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
//        DataStreamSource<String> source = environment.fromElements("aa","bb","cc");
        DataStreamSource<String> source = environment.addSource(new MySource02("data/secret.txt")).setParallelism(2);
        //转换数据+输出数据
//        source.map(new MapFunction<String,String>(){
//
//
//            @Override
//            public String map(String s) throws Exception {
//                return null;
//            }
//        });

        source.map(new RichMapFunction<String,String>(){
            @Override
            public void setRuntimeContext(RuntimeContext t) {
//                System.out.println("Hello04SourceCustomRich.setRuntimeContext" + System.currentTimeMillis());
                super.setRuntimeContext(t);
            }

            @Override
            public RuntimeContext getRuntimeContext() {
//                System.out.println("Hello04SourceCustomRich.getRuntimeContext" + System.currentTimeMillis());
                return super.getRuntimeContext();
            }

            @Override
            public IterationRuntimeContext getIterationRuntimeContext() {
//                System.out.println("Hello04SourceCustomRich.getIterationRuntimeContext" + System.currentTimeMillis());
                return super.getIterationRuntimeContext();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
//                System.out.println("Hello04SourceCustomRich.open" + System.currentTimeMillis()) ;
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
//                System.out.println("Hello04SourceCustomRich.close" + System.currentTimeMillis());

                super.close();
            }

            @Override
            public String map(String s) throws Exception {

                return s.toUpperCase() +":"+ getRuntimeContext().getIndexOfThisSubtask();

            }
        }).setParallelism(2).print();

        //执行环境
        environment.execute();

    }
}
//自定义数据源
class MySource02 extends RichParallelSourceFunction<String> {


    private String filePath;
    private int numberOfParallelSubtasks;//并行子任务数
    private int indexOfThisSubtask;//并行子任务数并行子任务数

    @Override
    public void open(Configuration parameters) throws Exception {
        this.numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        System.out.println("MySource02.open任务的并行度为["+numberOfParallelSubtasks+"],当前子任务索引编号为["+indexOfThisSubtask+"]" );
    }

    public MySource02(String filePath) {
        this.filePath = filePath;
    }

    public MySource02() {
    }




    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //运行方法，实现逻辑的方法
        //读取数据文件
        List<String> lines = FileUtils.readLines(new File(filePath),"utf-8");
        //遍历数据添加到SourceContext
        for (String line : lines) {
            //相当于这里是数据收集的地方，在上面的源的并行度为2，则这个源被加载了两次，因为会执行两次run方法，Context并收集两次，可以从收集入手。
            //相同的行只收集一次，通过对行进行hash对总并行度进行取模得到一个唯一结果，如果已经收集过，则不进行收集。
            //DESUtil是一个对称加密算法，decrypt进行解密。
            if(Math.abs(line.hashCode()) % numberOfParallelSubtasks == indexOfThisSubtask) {
                ctx.collect(DESUtil.decrypt("yjxxt0523", line) +"[index" + indexOfThisSubtask + "]");
            }
        }
    }

    @Override
    public void cancel() {
        //取消方法

    }

}



