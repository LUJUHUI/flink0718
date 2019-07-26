package sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import scala.collection.mutable

/**
  * @author LUJUHUI
  * @date 2019/7/25 10:00
  */
class MySink extends RichSinkFunction[(Boolean, (String, Long))] {

  private var resultSet: mutable.Set[(String, Long)] = _

  override def open(parameters: Configuration): Unit = {
    //初始化内存存储结构
    resultSet = new mutable.HashSet[(String, Long)]
  }

  override def invoke(v: (Boolean, (String, Long)), context: SinkFunction.Context[_]): Unit = {
    if (v._1) {
      //计算数据
      resultSet.add(v._2)
    } else {
      //撤回数据
      resultSet.remove(v._2)
    }
  }

  override def close(): Unit = {
    //打印写入sink的数据
    resultSet.foreach(println)
  }

}
