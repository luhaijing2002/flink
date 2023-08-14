package com.xxxx.flink

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author 鲁海晶
 * @version 1.0
 */
object Hello02WordCountUseDataSetByScala {
  def main(args: Array[String]): Unit = {
    //添加隐式转换
    import org.apache.flink.api.scala._

  //获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取数据
    //1.Source
    val dataSource: DataSet[String] = env.readTextFile("data/wordcount.txt")
    //2.Transformation
    val sum: AggregateDataSet[(String, Int)] = dataSource.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //3.Sink
    sum.print();

  }


}
