package scala_test

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment

/**
  * @author LUJUHUI
  * @date 2019/7/31 8:50
  *
  * users.dat    数据格式为：  2::M::56::16::70072
  *       对应字段为：UserID BigInt, Gender String, Age Int, Occupation String, Zipcode String
  *       对应字段中文解释：用户id，性别，年龄，职业，邮政编码
  *
  */
object ask {
  val localPath: String = "F:/Auro_BigData/flink1907/users.csv"
  val outputPath: String = "F:/Auro_BigData/flink1907/output/"
  val hdfsPath: String = "hdfs://hadoop01/user/flink/input/users.csv"

  case class Users(userId: Int, gender: String, age: Int, occupation: String, zipcode: String)

  def main(args: Array[String]): Unit = {
    //1 获取环境
    val datasetEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(datasetEnv)

    //2 获取数据源
    //    val datasetFE: DataSet[String] = datasetEnv.fromElements("a d", "x v", "z h", "a a")
    val dateSetUsers: DataSet[Users] = datasetEnv.readCsvFile[Users](localPath,
      "\n",
      ",",
      includedFields = Array[Int](0, 1, 2, 3, 4),
      pojoFields = Array[String]("userId", "gender", "age", "occupation", "zipcode"))

    //3 数据处理 + 4 处理结果集
    tableEnv.registerDataSet("users", dateSetUsers)
    val scanTable: Table = tableEnv.scan("users")

    /** 第一种方式
      * result:
      *
      * (11,129)
      * (12,388)
      * (16,241)
      * (17,502)
      * (18,70)
      *
      * */
    val lastResultTable: Table = scanTable.groupBy("occupation")
      .select("occupation,count(userId) as total")
    val finalResult: DataSet[(String, Long)] = tableEnv.toDataSet[(String, Long)](lastResultTable)
    finalResult.writeAsCsv(outputPath,
      "\n",
      "--",
      WriteMode.OVERWRITE).setParallelism(2)
    finalResult.print()

    /** 第二种方式
      * result:
      * (3969,M,56,7)
      * (948,M,56,12)
      * (952,M,56,16)
      * (2058,F,56,1)
      * (5431,M,56,1)
      * (986,F,56,0)
      *
      * */

    val resultTable: Table = tableEnv.sqlQuery(
      """
                select userId,gender,age,occupation
                from users
                where gender in('M','F')
                group by userId,gender,age,occupation
                order by age desc
              """)
    val resultData: DataSet[(Int, String, Int, String)] = tableEnv
      .toDataSet[(Int, String, Int, String)](resultTable)
    resultData.writeAsCsv(outputPath,
      "\n",
      "  ",
      WriteMode.OVERWRITE).setParallelism(3)
    resultData.print()


    //5 调用执行
    datasetEnv.execute("usersExe")
  }

}
