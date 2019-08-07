package scala_test

/**
  * @author LUJUHUI
  * @date 2019/8/1 14:37
  */

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.api.java.Tumble
import javax.annotation.Nullable
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.runtime.state.SharedStateRegistry.Result
import org.apache.flink.table.api.scala.map


object KafkaTable {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    properties.setProperty("zookeeper.connect", "127.0.0.1:2181")
    properties.setProperty("group.id", "test")
    val consumer08 = new Nothing("sqltime", new SimpleStringSchema, properties)
    val raw = env.addSource(consumer08).map(new MapFunction[String, KafkaTable.Order]() {
      @throws[Exception]
      override def map(s: String): KafkaTable.Order = if (s.contains("@")) {
        val split = s.split("@")
        val p1 = split(0).toInt
        val p2 = split(1)
        val p3 = split(2).toInt
        val p4 = System.currentTimeMillis
        new KafkaTable.Order(p1, p2, p3, p4)
      }
      else {
        val order = JSON.parseObject(s, classOf[KafkaTable.Order])
        if (order.rowtime == null) order.rowtime = System.currentTimeMillis
        order
      }
    }).assignTimestampsAndWatermarks(new KafkaTable.SessionTimeExtract)
    val table = tEnv.fromDataStream(raw, "user,product,amount,rowtime.rowtime")
    tEnv.registerTable("tOrder", table)
    val table1 = tEnv.scan("tOrder")
      .window(Tumble.over("10.second")
        .on("rowtime")
        .as("w"))
      .groupBy("w,user,product")
      .select("user,product,amount.sum as sum_amount,w.start")

    val sql_tumble =
      """
        |select user ,product,sum(amount) as sum_amount
        |from tOrder
        |group by TUMBLE(rowtime, INTERVAL '10' SECOND),user,product""".stripMargin

    val sql_hope =
      """
        |select user ,product,sum(amount) as sum_amount
        |from tOrder
        |group by hop(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND),user,product""".stripMargin

    val sql_sesstion =
      """
        |select user ,product,sum(amount) as sum_amount
        |from tOrder
        |group by session(rowtime, INTERVAL '12' SECOND),user,product""".stripMargin

    val sql_window_start =
      """
        |select tumble_start(rowtime, INTERVAL '10' SECOND) as wStart,user ,product,sum(amount) as sum_amount
        |from tOrder
        |group by TUMBLE(rowtime, INTERVAL '10' SECOND),user,product""".stripMargin

    val table2 = tEnv.sqlQuery(sql_window_start)
    DataStream<Tuple2<Boolean, Result>> resultStream = tEnv.toRetractStream(table2, Result.class);

    resultStream.map(new MapFunction<Tuple2<Boolean, Result>, String>() {
      @Override
      public String map(Tuple2<Boolean, Result> tuple2) throws Exception {
        return "user:" + tuple2.f1.user + "  product:" + tuple2.f1.product + "   amount:" + tuple2.f1.sum_amount + "    wStart:" + tuple2.f1.wStart;
      }
    }).print();
    env.execute();

  }

  class Order() {
    var user: Integer = null
    var product: String = null
    var amount = 0
    var rowtime = 0L

    def this(user: Integer, product: String, amount: Int, rowtime: Long) {
      this()
      this.user = user
      this.product = product
      this.amount = amount
      this.rowtime = rowtime
    }
  }

  class Result() {
    var user: Integer = null
    var product: String = null
    var sum_amount = 0
    var wStart: Timestamp = null
  }

  class SessionTimeExtract extends AssignerWithPeriodicWatermarks[KafkaTable.Order] {
    final private val maxOutOfOrderness = 3500L
    private var currentMaxTimestamp = 0L
    private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    @Nullable override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

    override def extractTimestamp(order: KafkaTable.Order, l: Long): Long = {
      val timestamp = order.rowtime
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
      timestamp
    }
  }

}
