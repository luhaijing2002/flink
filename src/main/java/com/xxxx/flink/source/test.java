package com.xxxx.flink.source;


import com.xxxx.flink.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class test {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //source
        DataStreamSource<String> source = environment.addSource(new YjxxtCustomSourceRich("data/secret.txt")).setParallelism(2);
        //Transformation+sink
        SingleOutputStreamOperator<String> map = source.map(new RichMapFunction<String, String>() {

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                //运行代码时 自动执行设置运行环境
                System.out.println("Hello04SourceCustomRich.setRuntimeContext" + System.currentTimeMillis());
                super.setRuntimeContext(t);
            }

            @Override
            public RuntimeContext getRuntimeContext() {
                //这个就是为获取系统的上下文环境 对应函数就是我们操作的主场景
                System.out.println("Hello04SourceCustomRich.getRuntimeContext" + System.currentTimeMillis());
                return super.getRuntimeContext();
            }

            @Override
            public String map(String s) throws Exception {
//                System.out.println("Hello04SourceCustomRich.map"+System.currentTimeMillis());
                //获取并行度
                return s.toUpperCase() + ":" + getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public IterationRuntimeContext getIterationRuntimeContext() {
                System.out.println("Hello04SourceCustomRich.getIterationRuntimeContext" + System.currentTimeMillis());
                return super.getIterationRuntimeContext();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                //在执行代码之前首先执行此代码
                System.out.println("Hello04SourceCustomRich.open" + System.currentTimeMillis());
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                //在执行代码之后首先执行此代码
                System.out.println("Hello04SourceCustomRich.close" + System.currentTimeMillis());
                super.close();
            }
        });
        map.setParallelism(2).print();
        //运行环境
        environment.execute();
    }
}
class YjxxtCustomSourceRich extends RichParallelSourceFunction<String> {
    private String filePath;
    private int numberOfParallelSubtasks;
    private int indexOfThisSubtask;
    public YjxxtCustomSourceRich(String filePath){
        this.filePath=filePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.numberOfParallelSubtasks= getRuntimeContext().getMaxNumberOfParallelSubtasks();
        this.indexOfThisSubtask=getRuntimeContext().getIndexOfThisSubtask();
        System.out.println("YjxxtCustomSourceRich.open任务的并行度为["+numberOfParallelSubtasks+"]当前任务的并行度为["+indexOfThisSubtask+"]");
    }

    @Override
    public void run(SourceFunction.SourceContext<String> context) throws Exception {
        List<String> lines = FileUtils.readLines(new File(filePath), "utf-8");
        for (String line:lines) {
            if (Math.abs(line.hashCode())%numberOfParallelSubtasks == indexOfThisSubtask){
                context.collect(DESUtil.decrypt("yjxxt0523",line)+ "[rghgmhfhrghdindex"+indexOfThisSubtask+"]");
            }

        }
    }

    @Override
    public void cancel() {

    }

}
