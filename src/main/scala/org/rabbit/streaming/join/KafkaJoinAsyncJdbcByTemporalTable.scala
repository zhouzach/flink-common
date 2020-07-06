package org.rabbit.streaming.join

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.rabbit.connectors.asyncTableSource.MysqlAsyncLookupTableSource

/**
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/joins.html
 */
object KafkaJoinAsyncJdbcByTemporalTable {
  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)

    streamTableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE user_behavior (
        |    uid VARCHAR,
        |    phoneType VARCHAR,
        |    clickCount INT,
        |    `time` TIMESTAMP(3)
        |) WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = 'universal',
        |    'connector.topic' = 'user_behavior',
        |    'connector.startup-mode' = 'earliest-offset',
        |    'connector.properties.0.key' = 'zookeeper.connect',
        |    'connector.properties.0.value' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'connector.properties.1.key' = 'bootstrap.servers',
        |    'connector.properties.1.value' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    'update-mode' = 'append',
        |    'format.type' = 'json',
        |    'format.derive-schema' = 'true'
        |)
        |""".stripMargin)

    streamTableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE user_cnt (
        |    `time` VARCHAR,
        |    sum_age INT
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://localhost:3306/dashboard',
        |    'connector.table' = 'user_cnt',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    val userTableSource = new MysqlAsyncLookupTableSource(
      "user_mysql",
      Array(Types.STRING, Types.STRING, Types.INT, Types.STRING),
      Array("uid", "sex", "age", "created_time"),
      Array())
    streamTableEnv.registerTableSource("users", userTableSource)
    streamTableEnv.sqlUpdate(
      """
        |
        |insert into  user_cnt
        |SELECT
        |  cast(b.`time` as string), u.age
        |FROM
        |  (select * , PROCTIME() AS proctime from user_behavior) AS b
        |  JOIN users FOR SYSTEM_TIME AS OF b.`proctime` AS u
        |  ON b.uid = u.uid
        |
        |""".stripMargin)

    streamTableEnv.execute("Temporal table join")
  }

}
