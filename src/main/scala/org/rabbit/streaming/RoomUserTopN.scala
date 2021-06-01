package org.rabbit.streaming

import java.sql.Timestamp
import java.time.Duration
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, FlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.rabbit.config.DevDbConfig
import org.rabbit.models.RoomUserInfo

/**
 * https://www.cnblogs.com/huangqingshi/p/12041032.html
 */
object RoomUserTopN {
  val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
  streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  //    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
  //    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
  //需要设置IDEA Environment variables: HADOOP_USER_NAME=hdfs
  streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

  val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
  streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
  streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20))
  streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(900))


  /**
   * 由于大屏的最大诉求是实时性，等待迟到数据显然不太现实，因此我们采用处理时间作为时间特征，并以1分钟的频率做checkpointing。
   */
  streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  streamExecutionEnv.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
  streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(30 * 1000)

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "screen_flink")
    val topic = "t_room"

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    //        consumer.setStartFromEarliest()
    //    consumer.setCommitOffsetsOnCheckpoints(true)
    consumer.setStartFromLatest()


    /**
     * 给带状态的算子设定算子ID（通过调用uid()方法）是个好习惯，能够保证Flink应用从保存点重启时能够正确恢复状态现场。
     * 为了尽量稳妥，Flink官方也建议为每个算子都显式地设定ID，
     * 参考：https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/savepoints.html#should-i-assign-ids-to-all-operators-in-my-job
     */
    import org.apache.flink.streaming.api.scala._
    val sourceStream = streamExecutionEnv
      .addSource(consumer)
      .setParallelism(1) //设置并行度1，方便测试打印
      .name("source_kafka_bill")
      .uid("source_kafka_bill")

    //    sourceStream.print()


    val dataStream = sourceStream.map(message => JSON.parseObject(message, classOf[RoomUserInfo]))
      .name("map_sub_bill").uid("map_sub_bill")

    val timedData = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[RoomUserInfo](Duration.ofSeconds(3)) //延迟3秒的固定延迟水印
      .withTimestampAssigner(new SerializableTimestampAssigner[RoomUserInfo] { //从数据中抽取eventtime字段
        override def extractTimestamp(element: RoomUserInfo, recordTimestamp: Long): Long = element.ts
      })
      .withIdleness(Duration.ofMinutes(1))) //超时时间内没有记录到达时将一个流标记为空闲,这样就意味着下游的数据不需要等待水印的到来

    val uidDayWindowStream = timedData
      .keyBy(new KeySelector[RoomUserInfo, Long] {
        override def getKey(value: RoomUserInfo): Long = value.roomId
      })
      .timeWindow(Time.minutes(1), Time.seconds(10))


    val uidPhoneAgg = uidDayWindowStream.aggregate(new CountAgg, new RoomUserWindowFunction)
      .name("aggregate_uid_phoneType_count").uid("aggregate_uid_phoneType_count")

    val processData = uidPhoneAgg.keyBy(new KeySelector[RoomUserResult, Long] {
      override def getKey(value: RoomUserResult): Long = value.windowEnd
    })
      .process(new TopNHotItems(3))

    processData.print()
    val resultData = processData.flatMap(new FlatMapFunction[List[RoomUserResult],RoomUserResult] {
      override def flatMap(values: List[RoomUserResult], out: Collector[RoomUserResult]): Unit = {
        values.foreach(v => out.collect(v))
      }
    })

    resultData.print()
    import org.apache.flink.table.api._
    streamTableEnv.createTemporaryView("result_table", resultData, $"roomId",$"windowEnd",$"cnt")


    streamTableEnv.executeSql(
      s"""
         |
         |CREATE TABLE mysql_table (
         |    roomId bigInt,
         |    windowEnd bigInt,
         |    cnt bigInt
         |   -- PRIMARY KEY (rownum, roomId) NOT ENFORCED
         |) WITH (
         |    'connector.type' = 'jdbc',
         |    'connector.url' = '${DevDbConfig.url}',
         |    'connector.table' = 'top_n',
         |    'connector.username' = '${DevDbConfig.user}',
         |    'connector.password' = '${DevDbConfig.password}',
         |    'connector.write.flush.max-rows' = '1'
         |)
         |""".stripMargin)


    streamTableEnv.executeSql(
      """
        |insert into mysql_table
        |SELECT
        | roomId,windowEnd,cnt
        |FROM result_table
        |
        |""".stripMargin)


  }

}


class CountAgg extends AggregateFunction[RoomUserInfo, Long, Long] {

  override def createAccumulator(): Long = {
    0L
  }

  override def merge(a: Long, b: Long): Long = {
    a + b
  }

  override def add(value: RoomUserInfo, acc: Long): Long = {
    value.action + acc
  }

  override def getResult(acc: Long): Long = {
    acc
  }
}

case class RoomUserResult(roomId: Long, windowEnd: Long, cnt: Long)

class RoomUserWindowFunction extends WindowFunction[Long, RoomUserResult, Long, TimeWindow] {
  override def apply(key: Long, //窗口主键即roomId
                     window: TimeWindow,
                     input: scala.Iterable[Long], //集合函数的结果，即count的值
                     out: Collector[RoomUserResult]): Unit = {

    val cnt = input.iterator.next()
    out.collect(RoomUserResult(key, window.getEnd, cnt))
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, RoomUserResult, List[RoomUserResult]] {

  var itemState: ListState[RoomUserResult] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val itemViewStateDesc = new ListStateDescriptor[RoomUserResult]("room-topN", classOf[RoomUserResult])
    itemState = getRuntimeContext.getListState(itemViewStateDesc)

  }

  override def processElement(value: RoomUserResult,
                              ctx: KeyedProcessFunction[Long, RoomUserResult, List[RoomUserResult]]#Context,
                              out: Collector[List[RoomUserResult]]): Unit = {

    //每条数据都保存到状态
    itemState.add(value)
    //注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收集好了所有 windowEnd的商品数据
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, RoomUserResult, List[RoomUserResult]]#OnTimerContext,
                       out: Collector[List[RoomUserResult]]): Unit = {
    //获取收集到的所有商品点击量
    val allItems = new util.ArrayList[RoomUserResult]()

    import scala.collection.JavaConversions._
    for (item <- itemState.get) {
      allItems.add(item)
    }

    import java.util.Comparator
    //提前清除状态中的数据，释放空间//提前清除状态中的数据，释放空间
    itemState.clear()
    //按照点击量从大到小排序
    allItems.sort(new Comparator[RoomUserResult]() {
      override def compare(o1: RoomUserResult, o2: RoomUserResult): Int = (o2.cnt - o1.cnt).asInstanceOf[Int]
    })

    val itemBuyCounts = new util.ArrayList[RoomUserResult]()
    //将排名信息格式化成String，方便打印
    val result = new StringBuilder
    result.append("========================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- 0 until topSize) {
      val currentItem = allItems.get(i)
      result.append("No").append(i).append(":").append("  roomId=").append(currentItem.roomId).append("  人数=").append(currentItem.cnt).append("\n")
      itemBuyCounts.add(currentItem)

    }

    result.append("====================================\n\n")
    println(s"$result")

    out.collect(itemBuyCounts.toList)
  }
}



