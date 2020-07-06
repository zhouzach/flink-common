package org.rabbit.sql

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.java.StreamTableEnvironment


/**
 * http://wuchong.me/blog/2020/02/25/demo-building-real-time-application-with-flink-sql/
 */
object FromKafkaSinkJdbcForUserUV {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)

//    val tableConfig = streamTableEnv.getConfig
//    tableConfig.setIdleStateRetentionTime(Time.minutes(1), Time.minutes(6))
//
//    val configuration = tableConfig.getConfiguration
//    configuration.setString("table.exec.mini-batch.enabled", "true")
//    configuration.setString("table.exec.mini-batch.allow-latency", "5 s")
//    configuration.setString("table.exec.mini-batch.size", "5000")

    streamTableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE `user` (
        |    uid VARCHAR,
        |    sex VARCHAR,
        |    age INT,
        |    created_time TIMESTAMP(3),
        |    proctime as PROCTIME(),
        |    -- declare created_time as event time attribute and use 3 seconds delayed watermark strategy
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
        |CREATE table user_view(
        |    `time` VARCHAR,
        |    cnt bigint
        |)WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = 'universal',
        |    'connector.topic' = 'user_view',
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
        |
        |INSERT INTO user_view
        |SELECT
        |  MAX(SUBSTR(DATE_FORMAT(created_time, 'HH:mm'),1,4) || '0') OVER w AS `time`,
        |  COUNT(DISTINCT uid) OVER w AS cnt
        |FROM `user`
        |WINDOW w AS (ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |
        |
        |""".stripMargin)



    streamTableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE user_uv1(
        |    `time` VARCHAR,
        |    cnt bigint
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://172.19.245.111:3306/report',
        |    'connector.table' = 'user_uv1',
        |    'connector.username' = 'root',
        |    'connector.password' = 'banban123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    streamTableEnv.sqlUpdate(
      """
        |
        |INSERT INTO user_uv1
        |SELECT `time`, MAX(cnt)
        |FROM user_view
        |GROUP BY `time`
        |
        |""".stripMargin)






    streamTableEnv.execute("from kafka sink mysql")
  }

}
