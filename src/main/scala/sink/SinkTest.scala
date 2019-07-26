package sink

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * @author LUJUHUI
  * @date 2019/7/24 16:08
  */
object SinkTest {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataSet[String] = env.fromElements("101,lju,111", "102,gfh,222", "103,dhg,333")

    val resultDataSet: DataSet[(String, String, String)] = dataSet.map(x => {
      val splits: Array[String] = x.split(",")
      (splits(0), splits(1), splits(2))
    })

    /** 第一种方式 直接打印 */
    /*    resultDataSet.print()  //正常打印
        resultDataSet.printToErr()  //打印异常
        resultDataSet.collect()*/

    /** 第二种方法 保存到文件 */
    val localPath = "F:/Auro_BigData/flink0718/output"

    //(1) WriteMode.OVERWRITE  ---如果存在目标文件夹则覆盖
    // resultDataSet.writeAsText(localPath,WriteMode.OVERWRITE)

    //（2）保存成CSV文件
    resultDataSet.writeAsCsv(localPath,
      rowDelimiter = "\n",   //行分隔符
      fieldDelimiter = "§", //列分隔符
      WriteMode.OVERWRITE)

    env.execute("sinktest")


  }

}
