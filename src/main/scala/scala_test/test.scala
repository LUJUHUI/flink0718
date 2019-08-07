package scala_test

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.TableEnvironment

/**
  * @author LUJUHUI
  * @date 2019/8/1 11:28
  */
object test {

  /** 注意：hdfs://hadoop01 会自动补齐默认端口8020，
    * 如果配置了其他端口要加上，如：hdfs://hadoop01:9000/flink
    * */
  val hdfsPath = "hdfs://hadoop01/flink/users.csv"
  val outputPath = "hdfs://hadoop01/flink/output/"

  case class Users(id: String, userId: String, userIP: String, time: String, user_nm: String)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet: DataSet[Nothing] = env.readCsvFile(hdfsPath,
      "\n",
      ",",
      includedFields = Array[Int](0, 1, 2, 3, 4),
      pojoFields = Array[String]("id", "userId", "userIP", "time", "user_nm"))

    tableEnv.registerDataSet("users", dataSet)
    val resultTable = tableEnv.sqlQuery("select * from users")
    val lastTable: DataSet[Users] = tableEnv.toDataSet[Users](resultTable)

    lastTable.writeAsText(outputPath, WriteMode.OVERWRITE).setParallelism(2)

    env.execute("testJob")
  }

}
