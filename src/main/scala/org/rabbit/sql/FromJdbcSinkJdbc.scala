package org.rabbit.sql

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html#update-modes
 */
object FromJdbcSinkJdbc {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    //    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
//    streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)


    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE analysis (
        |    gid INT,
        |    gname VARCHAR
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://localhost:3306/dashboard',
        |    'connector.table' = 'analysis',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE analysis1 (
        |    gid INT,
        |    gname VARCHAR
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://localhost:3306/dashboard',
        |    'connector.table' = 'analysis1',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)


    streamTableEnv.executeSql(
      """
        |
        |insert into analysis
        |select * from analysis1
        |
        |""".stripMargin)


//    streamTableEnv.execute("sink mysql")
  }

}
