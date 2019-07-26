package table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import source.Student

/**
  * @author LUJUHUI
  * @date 2019/7/25 10:24
  */
object TableAPI_0001 {
  def main(args: Array[String]): Unit = {
    val datasetEnv = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(datasetEnv)

    val hdfsDataPath="hdfs://hadoop01:9000/user/flink/testData/student.csv"
    val dataset1: DataSet[Student] = datasetEnv.readCsvFile[Student](hdfsDataPath,
      includedFields = Array[Int](0,1,2,3,4),
      pojoFields = Array[String]("id","name","sex","age","department"))

    //将dataset1转换成studentTable表
    tableEnv.registerDataSet("studentTable",dataset1)

    /** 所有表数据*/
    val allDataTable: Table = tableEnv.scan("studentTable")
    allDataTable.printSchema()

    val resultTable: Table = tableEnv.sqlQuery("select id,name,sex,age from studentTable where age >=20 order by age")
    resultTable.printSchema()

    //将resultTable表转换为dataset
    val lastResultDataset: DataSet[(Int, String, String, Int)] = tableEnv.toDataSet[(Int,String,String,Int)](resultTable)
    lastResultDataset.print()

    //datasetEnv.execute("studentTest")







  }

}
