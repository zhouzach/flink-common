package org.rabbit.streaming.phoenix

import java.time.Duration
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.connectors.phoenix.{PhoenixRichSinkFunctionByPreparedStatement, PhoenixRichSinkFunctionByStatement}
import org.rabbit.models.UserData

object FromKafkaSinkPhoenixByRichFunction {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    //    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
    //需要设置IDEA Environment variables: HADOOP_USER_NAME=hdfs
    //        streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

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

    sourceStream.print()


    val dataStream = sourceStream.map(message => {
      try {
        JSON.parseObject(message, classOf[UserData])
      } catch {
        case t: Throwable => {
          println(s"read json failed: ${t.toString}")
          UserData(0L, "", 0, 0L)
        }
      }
    })
      .name("map_pojo").uid("map_pojo")
    //    dataStream.print()

    val timedData = dataStream
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[UserData](Duration.ofSeconds(3)) //延迟3秒的固定延迟水印
        .withTimestampAssigner(new SerializableTimestampAssigner[UserData] { //从数据中抽取eventtime字段
          override def extractTimestamp(element: UserData, recordTimestamp: Long): Long = element.created_time
        })
        .withIdleness(Duration.ofMinutes(1))) //超时时间内没有记录到达时将一个流标记为空闲,这样就意味着下游的数据不需要等待水印的到来
      .name("assign_timestamp_watermark").uid("assign_timestamp_watermark")
    timedData.print()


    val deploy = "dev"

//    val statementSql = "upsert into user_flink2 values(%d,'%s',%d,%d)"
//    timedData.addSink(new UserPhoenixStatementRichSinkFunction(deploy, statementSql))
//      .name("sink_phoenix").uid("sink_phoenix")

    val preparedStatementSql = "upsert into user_flink2 values(?,?,?,?)"
    timedData.addSink(new UserPhoenixPreparedStatementRichSinkFunction(deploy, preparedStatementSql))
      .name("sink_phoenix").uid("sink_phoenix")

    streamExecutionEnv.execute("kafka sink phoenix")
  }

}

class UserPhoenixStatementRichSinkFunction(deploy: String, sql: String) extends PhoenixRichSinkFunctionByStatement[UserData](deploy: String, sql: String) {

  override def invoke(userData: UserData, context: Context[_]): Unit = {
    try {
      val sqlStr = java.lang.String.format(sql, new java.lang.Long(userData.uid), userData.sex, new Integer(userData.age), new java.lang.Long(userData.created_time))
      stmt.execute(sqlStr)

      conn.commit()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

}

class UserPhoenixPreparedStatementRichSinkFunction(deploy: String, sql: String) extends PhoenixRichSinkFunctionByPreparedStatement[UserData](deploy: String, sql: String) {

  override def invoke(userData: UserData, context: Context[_]): Unit = {
    try {
      //4.组装数据，执行插入操作
      prepareStmt.setLong(1, userData.uid)
      prepareStmt.setString(2, userData.sex)
      prepareStmt.setInt(3, userData.age)

      prepareStmt.setLong(4, userData.created_time)
      prepareStmt.executeUpdate()

//      conn.commit()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

}
