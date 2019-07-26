package table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import sink.MySink


/**
  * @author LUJUHUI
  * @date 2019/7/26 11:28
  */
object TableAPI_WC {
  def main(args: Array[String]): Unit = {

    //测试数据
    val testData: Seq[String] = Seq("hello", "hello", "hello", "flink", "spark", "scala")

    //stream 运行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(streamEnv)

    //最简单的获取source方式
    val source: Table = streamEnv.fromCollection(testData).toTable(tableEnv, 'word)

    // 单词计数的最核心逻辑

    /**
      * --------
      * word
      * --------
      * hello
      * hello
      * hello
      * flink
      * spark
      * scala
      * --------
      **/
    val result: Table = source.groupBy("word") //单词分组  “word”是自定义的列名
      .select('word, 'word.count) //单词统计

    val sink = new MySink
    result.toRetractStream[(String, Long)].addSink(sink)

    streamEnv.execute("tableN")

    /**
      * result:
      * (hello,3)
      * (spark,1)
      * (scala,1)
      * (flink,1)
      *
      **/


  }

}
