package org.rabbit.sql

import java.time.Duration

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object FromDatagenSinkPrint {

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
        |CREATE TABLE datagen (
        | f_sequence INT,
        | f_random INT,
        | f_random_str STRING,
        | ts AS localtimestamp,
        | WATERMARK FOR ts AS ts
        |) WITH (
        | 'connector' = 'datagen',
        |
        | -- optional options --
        |
        | 'rows-per-second'='5',
        |
        | 'fields.f_sequence.kind'='sequence',
        | 'fields.f_sequence.start'='1',
        | 'fields.f_sequence.end'='1000',
        |
        | 'fields.f_random.min'='1',
        | 'fields.f_random.max'='1000',
        |
        | 'fields.f_random_str.length'='10'
        |)
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE print_table
        |(
        |   f_sequence INT,
        | f_random INT,
        | f_random_str STRING,
        | ts TIMESTAMP(3)
        |)
        |WITH ('connector' = 'print')
        |
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |insert into print_table
        |SELECT
        |   f_sequence,f_random,f_random_str,ts
        |FROM  datagen
        |
        |""".stripMargin)


  }

}
