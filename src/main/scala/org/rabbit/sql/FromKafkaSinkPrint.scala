package org.rabbit.sql

import java.time.Duration

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object FromKafkaSinkPrint {

  def main(args: Array[String]): Unit = {

        val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
        streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //idea 需要设置 HADOOP_USER_NAME=hdfs
        streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

        val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
        streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
        streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(20))
        streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(900))


    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE kafka_table (
        |    uid BIGINT,
        |    sex VARCHAR,
        |    age INT,
        |    created_time string,
        |    procTime AS PROCTIME(),
        |    eventTime AS TO_TIMESTAMP(created_time),
        |    WATERMARK FOR eventTime as eventTime - INTERVAL '3' SECOND
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'user',
        |    'properties.bootstrap.servers' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    'properties.group.id' = 'user_flink',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'json',
        |    'json.fail-on-missing-field' = 'false',
        |    'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE print_table
        |(
        |    uid BIGINT,
        |    sex string,
        |    age INT,
        |    created_time string
        |)
        |WITH ('connector' = 'print')
        |
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |insert into print_table
        |SELECT
        |   uid,sex,age,created_time
        |FROM  kafka_table
        |
        |""".stripMargin)


  }

}
