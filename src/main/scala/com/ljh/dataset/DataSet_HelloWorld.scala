package com.ljh.dataset

import org.apache.flink.api.scala._

/**
  * 离线计算程序：
  * DataSet    ExecutionEnvironment
  * DataStream StreamExecutionEnvironment
  *
  * 如何运行结果中报错含有 关键词TypeInformation 或者 $  那么一般要求导入隐式转换
  * 解决方法：
  * import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}改为：
  * import org.apache.flink.api.scala._ 即可
  *
  *
  */

object DataSet_HelloWorld {
  def main(args: Array[String]): Unit = {
    /** 获取运行环境对象 */

    val dataSetEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    /** 加载数据源得到数据对象 */

    /*    val dataset: DataSet[String] = dataSetEnv.fromElements("a", "b", "c", "a", "a", "b")

        val dataset1: DataSet[(String, Int)] = dataset.map((_, 1))*/

    val dataset: DataSet[String] = dataSetEnv.fromElements("a b", "c a", "a b")

    /** x 代表一组 "a b" ，再将这一组按照空格分割 ； 27+29 与 31+34 得到的结果是一样的*/
    val dataset1: DataSet[(String, Int)] = dataset.flatMap(x => x.split(" ")).map((_, 1))

    /**
      * DataSet  为 groupby
      * DataStream 为 keyby
      *
      *         Flink                           Spark
      * dataset1.groupBy(0).sum(1)   =   dataset1.reduceByKey(_+_)
      * 按第1位分组，按第2位求和
      **/
    val dataset2: GroupedDataSet[(String, Int)] = dataset1.groupBy(0)

    val dataset3: AggregateDataSet[(String, Int)] = dataset2.sum(1)

    dataset3.print()

  }


}
