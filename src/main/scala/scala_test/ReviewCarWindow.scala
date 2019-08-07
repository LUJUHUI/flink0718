package scala_test

/**
  * @author LUJUHUI
  * @date 2019/8/1 14:39
  */

import com.alibaba.fastjson.JSON
import com.entity.CarEntity
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.util.Collector
import javax.annotation.Nullable
import java.text.SimpleDateFormat
import java.util.Date
import java.util
import java.util.Properties


object ReviewCarWindow {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(2000)
    env.setParallelism(1)
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")
    val consumer = new Nothing("carwindow", new SimpleStringSchema, properties)
    val raw = env.addSource(consumer).map(new MapFunction[String, Tuple3[String, Long, Integer]]() {
      @throws[Exception]
      override def map(result: String): Tuple3[String, Long, Integer] = {
        val carEntity = JSON.parseObject(result, classOf[Nothing])
        new Tuple3[String, Long, Integer](carEntity.getCarKind, carEntity.getTimeStamp, carEntity.getCarSum)
      }
    }).assignTimestampsAndWatermarks(new ReviewCarWindow.CarTimestampExtractor)
    val window = raw.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(3))).allowedLateness(Time.seconds(5)).apply(new WindowFunction[Tuple3[String, Long, Integer], String, Tuple, TimeWindow]() {
      @throws[Exception]
      override def apply(tuple: Tuple, window: TimeWindow, input: Iterable[Tuple3[String, Long, Integer]], out: Collector[String]): Unit = {
        val data = new util.LinkedList[Tuple3[String, Long, Integer]]
        import scala.collection.JavaConversions._
        for (item <- input) {
          data.add(item)
        }
        var carSum = 0
        import scala.collection.JavaConversions._
        for (item <- input) {
          carSum = carSum + item.f2
        }
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val msg = String.format("key:%s,  window:[ %s  ,  %s ), elements count:%d, elements time range:[ %s  ,  %s ]", tuple.getField(0), format.format(new Date(window.getStart)), format.format(new Date(window.getEnd)), data.size, format.format(new Date(data.getFirst.f1)), format.format(new Date(data.getLast.f1))) + "|||" + carSum
        out.collect(msg)
      }
    })
    window.print
    env.execute
  }

  class CarTimestampExtractor extends AssignerWithPeriodicWatermarks[Tuple3[String, Long, Integer]] {
    val maxOutOfOrderness = 3500L
    var currentMaxTimeStamp = 0L

    @Nullable override def getCurrentWatermark: Watermark = {
      System.out.println()
      new Watermark(currentMaxTimeStamp - maxOutOfOrderness)
    }

    override def extractTimestamp(element: Tuple3[String, Long, Integer], l: Long): Long = {
      val timeStamp = element.f1
      currentMaxTimeStamp = Math.max(timeStamp, currentMaxTimeStamp)
      timeStamp
    }
  }

}
