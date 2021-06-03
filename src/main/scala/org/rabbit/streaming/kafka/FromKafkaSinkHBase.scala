package org.rabbit.streaming.kafka

import java.time.Duration
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.models.BehaviorData
import org.rabbit.streaming.agg.MultiDimensionalAggregate

/**
 * https://cloud.tencent.com/developer/article/1536148
 */
object FromKafkaSinkHBase {
  val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
  streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  //    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
  //    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
  //需要设置IDEA Environment variables: HADOOP_USER_NAME=hdfs
  streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

  val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
  streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
  streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(20))
  streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(900))





  /**
   * 由于大屏的最大诉求是实时性，等待迟到数据显然不太现实，因此我们采用处理时间作为时间特征，并以1分钟的频率做checkpointing。
   */
  streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  streamExecutionEnv.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
  streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(30 * 1000)

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "screen_flink")
    val topic = "t_user_behavior_1"

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
      .setParallelism(4)
      .name("source_kafka_bill")
      .uid("source_kafka_bill")

//    sourceStream.print()

    val billStream = sourceStream.map(message => JSON.parseObject(message, classOf[BehaviorData]))
      //      .map(data => (data.uid, data.time, data.phoneType, data.clickCount))
      .name("map_sub_bill").uid("map_sub_bill")

    /**
     * 将子订单流按uid分组，开1天的滚动窗口(按照自然日来统计指标)，注意处理时间的时区问题
     * 并同时设定ContinuousProcessingTimeTrigger触发器，以1秒周期触发计算(以1秒的刷新频率呈现在大屏上)。
     */
    val uidDayWindowStream = billStream
      //      .keyBy(0)
      .keyBy("uid", "phoneType")
      .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
      //            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//      .trigger(ContinuousProcessingTimeTrigger.of(Time.minutes(1)));
    .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));

    val uidPhoneAgg = uidDayWindowStream.aggregate(new MultiDimensionalAggregate)
      .name("aggregate_uid_phoneType_count").uid("aggregate_uid_phoneType_count")

    uidPhoneAgg.print()

    import org.apache.flink.table.api._
    streamTableEnv.createTemporaryView("aggTable",uidPhoneAgg,$"uid", $"phone_type", $"cnt")

    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE hbase_table (
        |    rowkey VARCHAR,
        |    cf ROW(uid VARCHAR,phone_type VARCHAR,cnt BIGINT)
        |) WITH (
        |    'connector.type' = 'hbase',
        |    'connector.version' = '2.1.0',
        |    'connector.table-name' = 'ods:behavior_agg',
        |    'connector.zookeeper.quorum' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'connector.zookeeper.znode.parent' = '/hbase',
        |    'connector.write.buffer-flush.max-size' = '10mb',
        |    'connector.write.buffer-flush.max-rows' = '1000',
        |    'connector.write.buffer-flush.interval' = '2s'
        |)
        |""".stripMargin)


    streamTableEnv.executeSql(
      """
        |insert into hbase_table
        |SELECT
        |   CONCAT(SUBSTRING(MD5(CAST(uid AS VARCHAR)), 0, 6), SUBSTRING(MD5(phone_type), 0, 6)) as rowkey,
        |   ROW(uid,phone_type, cnt ) as cf
        |FROM aggTable
        |
        |""".stripMargin)


    streamExecutionEnv.execute()

  }

}




