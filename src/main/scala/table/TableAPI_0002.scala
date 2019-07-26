package table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.CsvTableSource
import source.Student

/**
  * @author LUJUHUI
  * @date 2019/7/25 15:02
  *
  *       flink的table api 有两种操作方式:
  *
  *       1 DSL  表直接操作
  *
  *    table.groupBy(".....").select(".....,......").orderBy("......")
  *
  *       关于 1 的传参有两种方式：
  *       1）字符串
  *       2）Expression
  *       注意：在使用Expression时，需要导入以下两个jar包：（不然会有隐式转换错误）
  *       import org.apache.flink.table.api._
  *       import org.apache.flink.table.api.scala._
  *
  *       2 SQL风格
  *
  *       val table:Table = tableEnv.sqlQuery("sql语句")
  *
  *       关于如何创建表对象有三种方式：
  *       1 fromDataSet
  *       2 registerDataSet
  *       3 registerTableSource
  *
  */
object TableAPI_0002 {
  def main(args: Array[String]): Unit = {

    //1 获取批处理运行环境
    val datasetEnv = ExecutionEnvironment.getExecutionEnvironment
    //  获取表环境
    val tableEnv = TableEnvironment.getTableEnvironment(datasetEnv)

    //2 获取资源
    val hdfsDataPath = "hdfs://hadoop01:9000/user/flink/testData/student.csv"
    val dataset1: DataSet[Student] = datasetEnv.readCsvFile[Student](hdfsDataPath,
      includedFields = Array[Int](0, 1, 2, 3, 4),
      pojoFields = Array[String]("id", "name", "sex", "age", "department"))

    //3 处理数据，将数据转化成表
    tableEnv.registerDataSet("tableStudent", dataset1)
    val allDataSet: Table = tableEnv.scan("tableStudent")
    // 计算各部门各有多少人    按部门分组 然后进行统计
    val allTable: Table = allDataSet.groupBy("department")
      .select("department,count(id) as total")

    // 另一种写法
    val otherAllTable: Table = allDataSet.groupBy("department")
      .select('department, 'age.count)

    //registerTableSource的方式：

    val studentCSVSource: CsvTableSource = new CsvTableSource(hdfsDataPath,
      Array[String]("id", "name", "sex", "age", "department"),
      Array[TypeInformation[_]](Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING),
      fieldDelim = ",",
      ignoreFirstLine = false)
    tableEnv.registerTableSource("studentTable1", studentCSVSource)
    val resultTable1: Table = tableEnv.scan("studentTable1")
    val lastResultTable1 = tableEnv.toDataSet[Student](resultTable1)
    lastResultTable1.print()  //全表打印对象
    println("------------------------------------")


    /**
      * val orders: Table = tEnv.scan("Orders") // schema (a, b, c, rowtime)
      * val result: Table = orders
      * .filter('a.isNotNull && 'b.isNotNull && 'c.isNotNull)
      * .select('a.lowerCase() as 'a, 'b, 'rowtime)
      * .window(Tumble over 1.hour on 'rowtime as 'hourlyWindow)
      * .groupBy('hourlyWindow, 'a)
      * .select('a, 'hourlyWindow.end as 'hour, 'b.avg as 'avgBillingAmount)
      *
      **/

    //4 再将表转换为数据，打印结果
    val lastResultData = tableEnv.toDataSet[(String, Long)](allTable)
    lastResultData.print()

    val hdfsDataOutputPath = "hdfs://hadoop01:9000/user/flink/testData/output"
    lastResultData.writeAsCsv(hdfsDataOutputPath,
      rowDelimiter = "\n",
      fieldDelimiter = "--",
      WriteMode.OVERWRITE)

    datasetEnv.execute("writetohdfs")

  }

}
