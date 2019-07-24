package source

import org.apache.flink.streaming.api.scala._

/**
  * @author LUJUHUI
  * @date 2019/7/24 11:12
  */
object MySourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val datastream: DataStream[Student] = env.addSource(new MySource)

    datastream.print()
    env.execute()

  }

}
