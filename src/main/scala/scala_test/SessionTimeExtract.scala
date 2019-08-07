package scala_test

/**
  * @author LUJUHUI
  * @date 2019/8/1 14:41
  */

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import java.text.SimpleDateFormat

import scala.tools.jline_embedded.internal.Nullable

class SessionTimeExtract extends AssignerWithPeriodicWatermarks[Nothing] {
  final private val maxOutOfOrderness = 3500L
  private var currentMaxTimestamp = 0L
  private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  @Nullable override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

  override def extractTimestamp(order: Nothing, l: Long): Long = {
    val timestamp = order.rowtime
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }
}
