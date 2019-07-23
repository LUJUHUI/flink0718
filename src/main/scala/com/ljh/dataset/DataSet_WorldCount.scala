package com.ljh.dataset

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * @author LUJUHUI
  * @date 2019/7/19 14:55
  */
object DataSet_WorldCount {
  def main(args: Array[String]): Unit = {
    val dataSetEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataSrc: DataSet[String] = dataSetEnv.fromElements("a b", "a d b", "c a b")

    case class WordCount(word: String, count: Long)

    val dataFinal: AggregateDataSet[WordCount] = dataSrc
      .flatMap(x => x.split(" "))
      .map(word => WordCount(word, 1))
      .groupBy(0)
      .sum(1)

//    dataFinal.print()

    val counter: Long = dataFinal.count()
    print(counter)

    /**
      * 如果并行度setParallelism(1)则output就是文件名；
      * 如果并行度setParallelism( >1 )则output就是文件夹的名。
      * */
    dataFinal.writeAsText("F:/Auro_BigData/flink0718/output", WriteMode.OVERWRITE).setParallelism(3)

    /** 启动执行 */
    dataSetEnv.execute("myflinkwordcount")


  }

}
