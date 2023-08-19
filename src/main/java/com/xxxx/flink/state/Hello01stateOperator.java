package com.xxxx.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.io.File;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello01stateOperator {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path" ,"D:\\idea_java_projects\\dsj\\flume\\flink\\ckpt\\989975e220f435e8fc664cc0187c6ef5\\chk-31");
        //运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        //开启检查点，5秒保存一次
        env.enableCheckpointing(5000);
        //设置保存路径为file:///D:\idea_java_projects\dsj\flume\flink\ckpt
        env.getCheckpointConfig().setCheckpointStorage("file:///" + System.getProperty("user.dir") + File.separator + "ckpt");

        //定义数据源
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        //数据转换
//        source.map(word -> word.toUpperCase()).print();

        //转换并输出
        //转换需要添加当前SubTask的处理这个单词的序号并输出
        source.map(new MyOperatorStateFunction()).print();

        //执行
        env.execute();
    }
}


class MyOperatorStateFunction implements MapFunction<String, String>, CheckpointedFunction {
    //声明一个变量记数
    private int count;
    //创建一个状态对象
    private ListState<Integer> countListState;
    @Override
    public String map(String value) throws Exception {
        //更新计数器
        count++;
        return "[" + value.toUpperCase() + "][" + count + "]";
    }
    //拍摄快照
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //消除上次的历史数据,将上一次的状态对象清除掉
        countListState.clear();
        //保存数据
        countListState.add(count);
        System.out.println("MyOperatorStateFunction.snapshotState[" +  countListState +"][" + System.currentTimeMillis() + "]");

    }
    //初始化
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //支持的数据比较有限，listState,BroadcastState，创建时要借助状态描述器来做
        ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("count", Types.INT);
        //创建对象
        this.countListState = context.getOperatorStateStore().getListState(descriptor);
        for (Integer integer : countListState.get()) {
            this.count = integer;
        }
        //判断是否恢复成功
        if(context.isRestored()){
            System.out.println("MyOperatorStateFunction.initializeState【历史状态恢复成功】");
            //把
        }else {
            System.out.println("MyOperatorStateFunction.initializeState【历史状态恢复失败】");
        }

    }
}