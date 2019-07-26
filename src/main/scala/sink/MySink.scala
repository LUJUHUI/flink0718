package sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * @author LUJUHUI
  * @date 2019/7/25 10:00
  */
class MySink extends RichSinkFunction[(String,Int)]{
  override def open(parameters: Configuration): Unit = {

  }

  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {

  }

  override def close(): Unit = {
     
  }

}
