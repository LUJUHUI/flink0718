package source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.StringValue

/**
  * @author LUJUHUI
  * @date 2019/7/19 17:17
  */
object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val streamEvn = StreamExecutionEnvironment.getExecutionEnvironment

    /** 第一种方式： 基于集合
      *
      * 1 fromCollection(Collection) - 从Java Java.util.Collection创建数据集。集合中的所有数据元必须属于同一类型。
      *
      * 2 fromCollection(Iterator, Class) - 从迭代器创建数据集。该类指定迭代器返回的数据元的数据类型。
      *
      * 3 fromElements(T ...) - 根据给定的对象序列创建数据集。所有对象必须属于同一类型。
      *
      * 4 fromParallelCollection(SplittableIterator, Class)- 并行地从迭代器创建数据集。该类指定迭代器返回的数据元的数据类型。
      *
      * 5 generateSequence(from, to) - 并行生成给定间隔中的数字序列。
      *
      * */

    val datasource1: DataSet[String] = env.fromElements("a b", "c d")
    val datasource2: DataSet[Int] = env.fromCollection(Seq(1, 2, 3, 4, 5))
    val datasource3: DataSet[Int] = env.fromCollection(List(1, 2, 3, 45, 5))
    val datasource4: DataSet[(String, Int)] = env.fromCollection(Map("a" -> 1, "b" -> 2))
    val datasource8: DataSet[Long] = env.generateSequence(1, 100000)

    /** 第二种方式： 基于文件的
      *
      *
      * 1 readTextFile(path)/ TextInputFormat- 按行读取文件并将其作为字符串返回。
      *
      * 2 readTextFileWithValue(path)/ TextValueInputFormat- 按行读取文件并将它们作为StringValues返回。StringValues是可变字符串。
      *
      * 3 readCsvFile(path)/ CsvInputFormat- 解析逗号（或其他字符）分隔字段的文件。返回元组或POJO的DataSet。支持基本java类型及其Value对应作为字段类型。
      *
      * 4 readFileOfPrimitives(path, Class)/ PrimitiveInputFormat- 解析新行（或其他字符序列）分隔的原始数据类型（如String或）的文件Integer。
      *
      * 5 readFileOfPrimitives(path, delimiter, Class)/ PrimitiveInputFormat- 解析新行（或其他字符序列）分隔的原始数据类型的文件，例如String或Integer使用给定的分隔符。
      *
      * 6 readSequenceFile(Key, Value, path)/ SequenceFileInputFormat- 创建一个JobConf并从类型为SequenceFileInputFormat，Key class和Value类的指定路径中读取文件，并将它们作为Tuple2 <Key，Value>返回。
      *
      *
      * */


    /**
      * 通用方法
      * readFile(inputFormat, path)/ FileInputFormat- 接受文件输入格式。
      *
      * createInput(inputFormat)/ InputFormat- 接受通用输入格式。
      *
      **/

    val localDataPath: String = "c:/filnktest/input"
    val hdfsDataPath: String = "hdfs://128.196.235.130/filnktest/input"
    val dataSource5: DataSet[String] = env.readTextFile(localDataPath, "UTF-8")
    val dataSource6: DataSet[Int] = env.readFileOfPrimitives[Int](localDataPath)
    val datasource7: DataSet[StringValue] = env.readTextFileWithValue(localDataPath)

    /** TSV与CSV的区别：
      * TSV为用制表符tab分隔的文本文件；
      * CSV为用逗号,分隔的文本文件。
      * */
    val datasouurce_csv: DataSet[Student] = env.readCsvFile[Student](hdfsDataPath, //文件输入路径
      lineDelimiter = "\n", //文本行分隔符
      fieldDelimiter = ",", //CSV每一行的文本内容都是以"，"分隔的
      quoteCharacter = null, // 用于引用字符串解析的字符，默认情况下禁用
      ignoreFirstLine = true, //是否应忽略文件中的第一行.因为csv文件第一行可能是 字段名，第二行开始才是真实数据
      ignoreComments = null, //以给定字符串开头的行将被忽略，默认情况下禁用 ---忽略注释。
      lenient = false, //解析器是否应该悄悄地忽略格式不正确的行。
      includedFields = Array[Int](0, 1, 2, 3, 4), //例：一行文本内容共有5列（0,1,2,3,4）代表文本的位置标识，需要哪一列就取那一列的下标
      pojoFields = Array[String]("id", "name", "sex", "age", "department") //获取的字段名，要与上面获取的文件内容一一对应
    )
    datasouurce_csv.print()

   /** 第三种方式   从网络端口 */

    streamEvn.socketTextStream("hadoop01",7658,'\n',3)

    /** 第四种方式  自定义
      *
      * 自己编写一个类去继承 RichSourceFunction
      * */

  }
}


case class Student(id: Int, name: String, sex: String, age: Int, department: String)