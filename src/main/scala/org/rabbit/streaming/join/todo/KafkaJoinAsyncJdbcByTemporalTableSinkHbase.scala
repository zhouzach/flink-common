package org.rabbit.streaming.join.todo

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.connectors.asyncTableSource.MysqlAsyncLookupTableSource

/**
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/joins.html
 */
object KafkaJoinAsyncJdbcByTemporalTableSinkHbase {
  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)

    streamTableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE user_behavior (
        |    uid VARCHAR,
        |    -- uid BIGINT,
        |    phoneType VARCHAR,
        |    clickCount INT,
        |    `time` TIMESTAMP(3)
        |) WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = 'universal',
        |    'connector.topic' = 'user_behavior',
        |    'connector.startup-mode' = 'latest-offset',
        |    -- 'connector.startup-mode' = 'earliest-offset',
        |    'connector.properties.0.key' = 'zookeeper.connect',
        |    'connector.properties.0.value' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'connector.properties.1.key' = 'bootstrap.servers',
        |    'connector.properties.1.value' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    'update-mode' = 'append',
        |    'format.type' = 'json',
        |    'format.derive-schema' = 'true'
        |)
        |""".stripMargin)


    val userTableSource = new MysqlAsyncLookupTableSource(
      "user_mysql_varchar",
      Array(Types.STRING, Types.STRING, Types.INT, Types.STRING),
      Array("uid", "sex", "age", "created_time"),
      Array())
    streamTableEnv.registerTableSource("users", userTableSource)


    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE user_join_behavior(
        |    rowkey VARCHAR,
        |    cf ROW(phoneType VARCHAR, clickCount INT, `time` VARCHAR, sex VARCHAR,age INT)
        |) WITH (
        |    'connector.type' = 'hbase',
        |    'connector.version' = '2.1.0',
        |    'connector.table-name' = 'ods.user_join_behavior',
        |    'connector.zookeeper.quorum' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'connector.zookeeper.znode.parent' = '/hbase',
        |    'connector.write.buffer-flush.max-size' = '10mb',
        |    'connector.write.buffer-flush.max-rows' = '1000',
        |    'connector.write.buffer-flush.interval' = '2s'
        |)
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |insert into  user_join_behavior
        |SELECT
        |  b.uid,
        |  ROW(phoneType, clickCount, `time`,sex ,age) as cf
        |FROM
        |  (select uid,phoneType,clickCount, cast(`time` as string) as `time`, PROCTIME() AS proctime from user_behavior) AS b
        |  JOIN users FOR SYSTEM_TIME AS OF b.`proctime` AS u
        |  ON b.uid = u.uid
        |
        |""".stripMargin)

    streamTableEnv.execute("Temporal table join")
  }

}
