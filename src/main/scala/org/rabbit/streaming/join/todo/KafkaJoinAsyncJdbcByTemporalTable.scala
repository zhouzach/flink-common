package org.rabbit.streaming.join.todo

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.config.DevDbConfig
import org.rabbit.connectors.asyncTableSource.MysqlAsyncLookupTableSource

/**
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/joins.html
 */
object KafkaJoinAsyncJdbcByTemporalTable {
  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)

    streamTableEnv.executeSql(
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
        |    'connector.topic' = 'user_behavior2',
        |    'connector.properties.zookeeper.connect' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'connector.properties.bootstrap.servers' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    'connector.properties.group.id' = 'testGroup',
        |    'connector.startup-mode' = 'latest-offset',
        |    -- 'connector.startup-mode' = 'earliest-offset',
        |    'update-mode' = 'append',
        |    'format.type' = 'json',
        |    'format.derive-schema' = 'true'
        |)
        |""".stripMargin)

    streamTableEnv.executeSql(
      s"""
        |
        |CREATE TABLE user_cnt (
        |    `time` VARCHAR,
        |    sum_age INT
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = '${DevDbConfig.url}',
        |    'connector.table' = 'user_cnt',
        |    'connector.username' = '${DevDbConfig.user}',
        |    'connector.password' = '${DevDbConfig.password}',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    val userTableSource = new MysqlAsyncLookupTableSource(
      "user_mysql_varchar",
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
