package com.xxxx.flink.sink;

import com.xxxx.flink.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.File;
import java.util.ArrayList;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello15SinkCustomRich {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置环境并行度
        environment.setParallelism(5);
        //操作数据
        ArrayList<String> list = new ArrayList<>();
        list.add("君子周而不比，小人比而不周");
        list.add("君子喻于义，小人喻于利");
        list.add("君子怀德，小人怀土，君子怀刑，小人怀惠");
        list.add("君子欲讷于言而敏于行");
        list.add("君子坦荡荡，小人长戚戚");
        DataStreamSource<String> source = environment.fromCollection(list).setParallelism(1);

        //写出数据
        source.addSink(new mySink2("data/sink"+System.currentTimeMillis()));
        //运行环境
        environment.execute();
    }
}

class mySink2 extends RichSinkFunction<String> {
    private File file;
    private String filePath;
    private int indexOfThisSubtask;
    private int numberOfSubtasks;

    public mySink2(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void invoke(String line, Context context) throws Exception {
        //加密数据
        String encode = DESUtil.encrypt("yjxxt0523",line) + "\r\n";
        //写出数据(到文件中)
        FileUtils.writeStringToFile(file,encode+"\r\n","utf-8",true);
    }
    //
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取当前子任务的线程
        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        //获取当前子任务的并发数
        this.numberOfSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        //获取当前的写出路径，这里面的做法时每当一个子任务进来后，会有一个任务编号索引,会为它这个子任务读取的文件进行一个文件创建，
        //就是将每个任务或者每个线程对应的数据进行一个写成一个任务,并统一放到用户传入的文件目录下
        //以用户传入的文件目录为根目录，加上File.separator分隔符/，就相当于一个目录下级的文件，类似于   sink1692026749934/0.txt
        //为什么不会数据不会重复呢？没有对数据源设置并行度，如果对数据源设置并行度，说明对这个数据源会被读取多次，就会出现数据重复的问题,出现重复问题时
        //可以调度hash取模来解决。
        this.file = new File(this.filePath + File.separator + "Hello"+indexOfThisSubtask+".txt");
    }
    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return super.getIterationRuntimeContext();
    }



    @Override
    public void close() throws Exception {
        super.close();
    }
}

