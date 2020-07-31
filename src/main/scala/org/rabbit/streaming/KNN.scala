package org.rabbit.streaming

import java.time.Duration
import java.util.Properties

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.models.{AnchorLocationInfo, AnchorLocationRow}
import org.rabbit.util.MD5


object KNN {
  val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
  streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  //    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
  //    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
  //需要设置IDEA Environment variables: HADOOP_USER_NAME=hdfs
//  streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

  val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
  streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
  streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20))
  streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(900))



  streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  streamExecutionEnv.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
  streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(30 * 1000)

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "screen_flink")
    val topic = "t_anchor_location"

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    //        consumer.setStartFromEarliest()
    //    consumer.setCommitOffsetsOnCheckpoints(true)
    consumer.setStartFromLatest()



    import org.apache.flink.streaming.api.scala._
    val sourceStream = streamExecutionEnv
      .addSource(consumer)
      .setParallelism(1) //设置并行度1，方便测试打印
      .name("source_kafka_bill")
      .uid("source_kafka_bill")

//        sourceStream.print()



    val dataStream = sourceStream.map(message => JSON.parseObject(message, classOf[AnchorLocationInfo]))
      .name("map_sub_bill").uid("map_sub_bill")

    val timedData = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
      .forBoundedOutOfOrderness[AnchorLocationInfo](Duration.ofSeconds(3)) //延迟3秒的固定延迟水印
      .withTimestampAssigner(new SerializableTimestampAssigner[AnchorLocationInfo] { //从数据中抽取eventtime字段
        override def extractTimestamp(element: AnchorLocationInfo, recordTimestamp: Long): Long = element.ts
      })
      .withIdleness(Duration.ofMinutes(1))) //超时时间内没有记录到达时将一个流标记为空闲,这样就意味着下游的数据不需要等待水印的到来

    val rowSteam = timedData.map{d =>
      val md5 = MD5.mk(d.uid.toString).substring(0,6)
      val rowKey = GeoHash.withCharacterPrecision(d.latitude,d.longitude,12).toBase32 + md5
      AnchorLocationRow(rowKey,d.uid,d.longitude,d.latitude,d.tag1,d.ts)
    }.setParallelism(1) //设置并行度1，方便测试打印

    rowSteam.print()
    import org.apache.flink.table.api._
    streamTableEnv.createTemporaryView("result_table", rowSteam, $"rowKey",$"uid",$"longitude",$"latitude",$"tag1",$"ts")


    streamTableEnv.executeSql(
      s"""
         |
         |CREATE TABLE hbase_table (
         |    rowkey VARCHAR,
         |    cf ROW(uid BIGINT,longitude Double, latitude Double, tag1 VARCHAR, ts BIGINT)
         |) WITH (
         |    'connector.type' = 'hbase',
         |    'connector.version' = '2.1.0',
         |    'connector.table-name' = 'ods:anchor_location',
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
        | rowKey,
        | ROW(uid,longitude, latitude, tag1,ts ) as cf
        |FROM result_table
        |
        |""".stripMargin)


    streamExecutionEnv.execute()

  }

}






object Demo extends App{
  val ss  = "hfugpqpwzrtk"
  val rowKey = GeoHash.withCharacterPrecision(-73.96974759,40.75890919,12).toBase32
  val res =rowKey + 77
  val rowKey1 = GeoHash.withCharacterPrecision(-73.96974759,40.75890919,7).toBase32
  println(rowKey)
//  println(res)
  println(rowKey1)
  println(ss.equals(rowKey))

}



