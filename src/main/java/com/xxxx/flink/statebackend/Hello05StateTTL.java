package com.xxxx.flink.statebackend;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author 鲁海晶
 * @version 1.0
 */
public class Hello05StateTTL {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path" ,"D:\\idea_java_projects\\dsj\\flume\\flink\\ckpt\\989975e220f435e8fc664cc0187c6ef5\\chk-31");
        //运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        //开启检查点，5秒保存一次
        env.enableCheckpointing(5000);
        //开启本地状态维护
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //远程状态备份
        env.getCheckpointConfig().setCheckpointStorage("hdfs://node01:8020/flink/checkpoints");
        //定义数据源
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        //数据转换
        source.map(new MyOperatorStateFunction02()).print();

        //执行
        env.execute();
    }
}
class MyOperatorStateFunction02 implements MapFunction<String,String>, CheckpointedFunction {

    private ListState<Integer> listState;
    private int count = 0;//计算器，保存的值

    @Override
    public String map(String value) throws Exception {
        //计算器累加
        return "["+value.toUpperCase() +"]["+  count++ +"]";
    }

    //拍照不能要上次的记录
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //清除上次
        listState.clear();
        //保存当前
        listState.add(count);
    }

    //初始化状态
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(60))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                .cleanupFullSnapshot()
                .build();

        //创建描述器并创建对象
        ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("ListState", Types.INT());
        listStateDescriptor.enableTimeToLive(stateTtlConfig);
        //获取状态
        this.listState = context.getOperatorStateStore().getListState(listStateDescriptor);
    }
}