package org.rabbit.sql

import java.time.Duration

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.config.DevDbConfig

object FromKafkaSinkJdbcForUser {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(20))
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(900))



    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE `user` (
        |    uid BIGINT,
        |    sex VARCHAR,
        |    age INT,
        |    created_time BIGINT,
        |    procTime AS PROCTIME(),
        |    eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(created_time/1000, 'yyyy-MM-dd HH:mm:ss')),
        |    WATERMARK FOR eventTime as eventTime - INTERVAL '3' SECOND
        |) WITH (
        |    'connector' = 'kafka',
        |     'topic' = 'user',
        |    'properties.bootstrap.servers' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    'properties.group.id' = 'user_flink',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'json',
        |    'json.fail-on-missing-field' = 'false',
        |    'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)

    streamTableEnv.executeSql(
      s"""
        |
        |CREATE TABLE user_mysql(
        |    uid BIGINT,
        |    sex VARCHAR,
        |    age INT,
        |    created_time BIGINT
        |) WITH (
        |    'connector' = 'jdbc',
        |    'url' = '${DevDbConfig.url}',
        |    'table-name' = 'user_mysql',
        |    'username' = '${DevDbConfig.user}',
        |    'password' = '${DevDbConfig.password}',
        |    'sink.buffer-flush.max-rows' = '1'
        |)
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |insert into user_mysql
        |select uid,sex,age,created_time
        |from `user`
        |
        |""".stripMargin)



  }

}
