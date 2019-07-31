package scala_test

import org.apache.flink.api.scala._

/**
  * @author LUJUHUI
  * @date 2019/7/26 16:39
  */
object DataSet_1 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataset: DataSet[String] = env.fromElements("a b", "a d b", "c a b")

    case class WordCount(word: String, count: Int)

    val resultSet: AggregateDataSet[WordCount] = dataset
      .flatMap(x => x.split(" "))
      .map(x => WordCount(x, 1))
      .groupBy(0)
      .sum(1)

    /**
      * result:
      * WordCount(d,1)
      * WordCount(a,3)
      * WordCount(b,3)
      * WordCount(c,1)
      **/

    resultSet.print()

  }

}
