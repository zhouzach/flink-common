package org.rabbit.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object FromKafkaSinkHbase {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)


    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE `user` (
        |    -- uid VARCHAR,
        |    uid BIGINT,
        |    sex VARCHAR,
        |    age INT,
        |    created_time TIMESTAMP(3),
        |    WATERMARK FOR created_time as created_time - INTERVAL '3' SECOND
        |) WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = 'universal',
        |    -- 'connector.topic' = 'user',
        |    'connector.topic' = 'user_long',
        |    'connector.startup-mode' = 'latest-offset',
        |    'connector.properties.zookeeper.connect' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'connector.properties.bootstrap.servers' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    'connector.properties.group.id' = 'user_flink',
        |    'format.type' = 'json',
        |    'format.derive-schema' = 'true'
        |)
        |""".stripMargin)



    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE hbase_table (
        |    rowkey VARCHAR,
        |    cf ROW(sex VARCHAR, age INT, created_time VARCHAR)
        |) WITH (
        |    'connector.type' = 'hbase',
        |    'connector.version' = '2.1.0',
        |    'connector.table-name' = 'ods:user_hbase2',
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
        |   CONCAT(SUBSTRING(MD5(CAST(uid AS VARCHAR)), 0, 6), cast(CEILING(UNIX_TIMESTAMP(created_time)/60) as string), sex) as uid,
        |   ROW(sex, age, created_time ) as cf
        |FROM  (select uid,sex,age, cast(created_time as VARCHAR) as created_time from `user`)
        |
        |""".stripMargin)


  }

}
