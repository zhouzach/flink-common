package org.rabbit.streaming.phoenix

import java.sql.PreparedStatement
import java.time.Duration
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.models.UserData

object FromKafkaSinkPhoenixByJdbcSink {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    //    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
    //需要设置IDEA Environment variables: HADOOP_USER_NAME=hdfs
        streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5))
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(900))


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "screen_flink")
    val topic = "t_room"

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    //        consumer.setStartFromEarliest()
    //    consumer.setCommitOffsetsOnCheckpoints(true)
    consumer.setStartFromLatest()

    import org.apache.flink.streaming.api.scala._
    val sourceStream = streamExecutionEnv
      .addSource(consumer)
      .setParallelism(1) //设置并行度1，方便测试打印
      .name("source_kafka")
      .uid("source_kafka")

//        sourceStream.print()


    val dataStream = sourceStream.map(message => {
      try{
        JSON.parseObject(message, classOf[UserData])
      }catch {
        case t: Throwable => {
          println(s"read json failed: ${t.toString}")
          UserData(0L,"",0,0L)
        }
      }
    })
      .name("map_pojo").uid("map_pojo")
//    dataStream.print()

    val timedData = dataStream
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[UserData](Duration.ofSeconds(3))//延迟3秒的固定延迟水印
        .withTimestampAssigner(new SerializableTimestampAssigner[UserData] { //从数据中抽取eventtime字段
          override def extractTimestamp(element: UserData, recordTimestamp: Long): Long = element.created_time
        })
        .withIdleness(Duration.ofMinutes(1))) //超时时间内没有记录到达时将一个流标记为空闲,这样就意味着下游的数据不需要等待水印的到来
      .name("assign_timestamp_watermark").uid("assign_timestamp_watermark")
    timedData.print()

    timedData
      .addSink(JdbcSink.sink(
        "upsert into ODS.USER_FLINK3 values(?,?,?,?)",
        new JdbcStatementBuilder[UserData]() {
          override def accept(ps: PreparedStatement, u: UserData): Unit = {
            ps.setLong(1, u.uid)
            ps.setString(2, u.sex)
            ps.setInt(3, u.age)
            ps.setLong(4, u.created_time)
          }
        },
        JdbcExecutionOptions.builder().withBatchSize(1).build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl(s"jdbc:phoenix:cdh1,cdh2,cdh3:2181:/hbase;autocommit=true")
          .withDriverName("org.apache.phoenix.jdbc.PhoenixDriver")
          //未跑通
//          .withUrl(s"jdbc:phoenix:thin:url=http://cdh3:8765;serialization=PROTOBUF")
//          .withDriverName("org.apache.phoenix.queryserver.client.Driver")
          .build()))
        .name("sink_jdbc").uid("sink_jdbc")

    streamExecutionEnv.execute("kafka sink phoenix")

  }

}
