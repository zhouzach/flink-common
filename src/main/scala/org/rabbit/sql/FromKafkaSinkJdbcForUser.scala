package org.rabbit.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

object FromKafkaSinkJdbcForUser {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)


    streamTableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE `user` (
        |    uid VARCHAR,
        |    sex VARCHAR,
        |    age INT,
        |    created_time TIMESTAMP(3),
        |    WATERMARK FOR created_time as created_time - INTERVAL '3' SECOND
        |) WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = 'universal',
        |    'connector.topic' = 'user',
        |    'connector.startup-mode' = 'latest-offset',
        |    'connector.properties.zookeeper.connect' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'connector.properties.bootstrap.servers' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    'connector.properties.group.id' = 'user_flink',
        |    'format.type' = 'json',
        |    'format.derive-schema' = 'true'
        |)
        |""".stripMargin)

    streamTableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE user_mysql(
        |    uid VARCHAR,
        |    sex VARCHAR,
        |    age INT,
        |    created_time TIMESTAMP(3)
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://172.19.245.111:3306/report',
        |    'connector.table' = 'user_mysql',
        |    'connector.username' = 'root',
        |    'connector.password' = 'banban123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    streamTableEnv.sqlUpdate(
      """
        |
        |insert into user_mysql
        |select * from `user`
        |
        |""".stripMargin)



    streamTableEnv.execute("from kafka sink mysql")
  }

}
