package org.rabbit.streaming.kafka

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.models.BehaviorData

/**
 * https://cloud.tencent.com/developer/article/1536148
 */
object SinkHbaseBySql {

  val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment



  val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)

  /**
   * 由于大屏的最大诉求是实时性，等待迟到数据显然不太现实，因此我们采用处理时间作为时间特征，并以1分钟的频率做checkpointing。
   */
  streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  streamExecutionEnv.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
  streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(30 * 1000)

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "flink")
    val topic = "user_behavior"

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


    streamTableEnv.createTemporaryView("myTable", billStream)


    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE behavior_hbase1(
        |    rowkey VARCHAR,
        |    cf ROW(`time` VARCHAR, phoneType VARCHAR, clickCount INT)
        |) WITH (
        |    'connector.type' = 'hbase',
        |    'connector.version' = '2.1.0',
        |    'connector.table-name' = 'ods.behavior_hbase1',
        |    'connector.zookeeper.quorum' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'connector.zookeeper.znode.parent' = '/hbase',
        |    'connector.write.buffer-flush.max-size' = '10mb',
        |    'connector.write.buffer-flush.max-rows' = '1000',
        |    'connector.write.buffer-flush.interval' = '2s'
        |)
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |insert into behavior_hbase1
        |SELECT uid,
        |  ROW(`time`, phoneType, clickCount ) as cf
        |  FROM   myTable
        |
        |""".stripMargin)


    streamExecutionEnv.execute()

  }

}

