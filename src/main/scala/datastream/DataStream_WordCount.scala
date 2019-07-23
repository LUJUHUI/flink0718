package datastream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author LUJUHUI
  * @date 2019/7/19 15:56
  */
object DataStream_WordCount {
  def main(args: Array[String]): Unit = {

    /** Parameter Tool 参数工具*/
    val parameterTool = ParameterTool.fromArgs(args)

    /** edit configuation --> program arguments -->  --port 1234 */
    val port: Int = parameterTool.getInt("port")
    //    val name: String = parameterTool.get("name")

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /** 启动flink集群，并在hadoop集群hadoop01上运行 nc -lk 1234  ;   如果 nc 不存在的话，就yum install nc */
    val dataStream: DataStream[String] = environment.socketTextStream("hadoop01", port)

    val resultDS: DataStream[(String, Int)] = dataStream
      .flatMap(x => x.split(" "))
      .map((_, 1))
      .keyBy(0)   //等价于 groupBy
      .timeWindowAll(Time.seconds(5), Time.seconds(2)) //每2s钟计算一次，每个窗口间隔5s
      .sum(1)

    resultDS.print()

    /** 必须要调用execute函数来触发执行任务*/
    environment.execute("mywordcountstreaming")
  }
}
