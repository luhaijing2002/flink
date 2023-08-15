package com.xxxx.flink.transformationcommon;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author 鲁海晶
 * @version 1.0
 */
public class HeLlo10TransformationUnionConnet {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

//        //数据源,针对多个相同的数据类型的合并数据源
//        DataStreamSource<Integer> dataStreamSource1 = environment.fromElements(1, 2, 3, 4, 5);
//        DataStreamSource<Integer> dataStreamSource2 = environment.fromElements(6,7,8,9,10);
//
//        DataStream<Integer> union = dataStreamSource1.union(dataStreamSource2);
//        union.print();






        //数据源,针对多个不相同的数据类型的合并数据源
        DataStreamSource<String> dataStreamSource1 = environment.fromElements("yes", "yes", "no", "yes", "no");
        DataStreamSource<Double> dataStreamSource2 = environment.fromElements(28.5, 38.3, 34.2, 50.2, 28.3);
        //首先将两个不同类型的流进行连接
        ConnectedStreams<String, Double> connect = dataStreamSource1.connect(dataStreamSource2);
        connect.map(new CoMapFunction<String, Double, String>() {
            @Override
            public String map1(String value) throws Exception {
                if("yes".equals(value)){
                    return "["+ value + "]火警传感器[安全]";
                }
                return "["+ value + "]火警传感器[危险]";
            }

            @Override
            public String map2(Double value) throws Exception {
                if(value <= 50){
                    return "["+ value + "]温度传感器[安全]";
                }
                return "["+ value + "]温度传感器[危险]";
            }
        }).print();

        //运行
        environment.execute();

    }


}
