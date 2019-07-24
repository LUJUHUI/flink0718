package source

import java.sql.{Connection, DriverManager, Statement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction, _}

/**
  * @author LUJUHUI
  * @date 2019/7/24 10:19
  *
  *       自定义一个数据源：读取mysql
  *       重写四个方法
  */
class MySource extends RichSourceFunction[Student] {
  private var statement: Statement = _
  private var connection: Connection = _

  /** 在整个source初始化之后，就会执行一次 */
  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://hadoop01:3306/databaseName"
    connection = DriverManager.getConnection(url, "root", "root") //mysql的url、用户名、密码
    statement = connection.createStatement()
  }

  /** 不停的执行，然后发送数据到下一个组件 */
  override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
    val sql = "select id,name,sex,age,department from Student;"
    val resultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      val id = resultSet.getInt("id")
      val name = resultSet.getString("name")
      val sex = resultSet.getString("sex")
      val age = resultSet.getInt("age")
      val deparment = resultSet.getString("department")

      sourceContext.collect(Student(id, name, sex, age, deparment))

    }
  }

  /** 取消 */
  override def cancel(): Unit = {


  }

  /** 关闭 */
  override def close(): Unit = {
    //后打开的先关，先打开的后关
    statement.close()
    connection.close()
  }
}
