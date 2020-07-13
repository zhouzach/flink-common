package org.rabbit.sql

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * http://wuchong.me/blog/2020/02/25/demo-building-real-time-application-with-flink-sql/
 */
object FlinkSql {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)

    val tableConfig = streamTableEnv.getConfig
    tableConfig.setIdleStateRetentionTime(Time.minutes(1), Time.minutes(6))

    val configuration = tableConfig.getConfiguration
    configuration.setString("table.exec.mini-batch.enabled", "true")
    configuration.setString("table.exec.mini-batch.allow-latency", "5 s")
    configuration.setString("table.exec.mini-batch.size", "5000")





    streamTableEnv.sqlQuery(
      """
        |select SUBSTR(DATE_FORMAT('2020-06-17T18:29:00.000Z', 'HH:mm'),1,4) || '0'
        |
        |""".stripMargin)






    streamTableEnv.execute("from kafka sink mysql")
  }

}
