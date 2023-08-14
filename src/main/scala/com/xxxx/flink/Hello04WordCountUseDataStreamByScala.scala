package com.xxxx.flink

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
 * @author 鲁海晶
 * @version 1.0
 */
object Hello04WordCountUseDataStreamByScala {
  def main(args: Array[String]): Unit = {
    //导入隐式转换(相当于自动开始了类型推断)
    import org.apache.flink.api.scala._

    // 创建一个流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1.Source
    val dataStream: DataStream[String] = env.socketTextStream("localhost", 9999)
    //2.Transformation
    val value: DataStream[(String, Int)] = dataStream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(tuple => tuple._1)
      .sum(1)
    //3.Sink
    value.print()
    // 执行
    env.execute("hello")
  }

}
