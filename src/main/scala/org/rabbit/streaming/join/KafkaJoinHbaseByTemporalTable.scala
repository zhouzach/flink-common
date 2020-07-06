package org.rabbit.streaming.join

import org.apache.flink.addons.hbase.HBaseTableSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.descriptors.HBaseValidator
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}

/**
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/temporal_tables.html#defining-temporal-table
 */
object KafkaJoinHbaseByTemporalTable {

  def main(args: Array[String]): Unit = {


    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)

    val hConf = HBaseConfiguration.create()
    hConf.set(HConstants.ZOOKEEPER_QUORUM, "cdh1:2181,cdh2:2181,cdh3:2181")

    val users = new HBaseTableSource(hConf, "user_hbase3")
    users.setRowKey("rowkey", classOf[String]) // currency as the primary key
    users.addColumn("cf", "age", classOf[Integer])

    streamTableEnv.registerTableSource("users", users)


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
        |    'connector.url' = 'jdbc:mysql://172.19.245.111:3306/report',
        |    'connector.table' = 'user_cnt',
        |    'connector.username' = 'root',
        |    'connector.password' = 'banban123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)


    streamTableEnv.sqlQuery(
      """
        |
        |select * from users
        |
        |""".stripMargin).printSchema()

    streamTableEnv.sqlQuery(
      """
        |
        |select * from user_behavior
        |
        |""".stripMargin).printSchema()

    streamTableEnv.sqlUpdate(
      """
        |
        |insert into  user_cnt
        |SELECT
        |  cast(b.`time` as string) as `time`,  u.age
        |FROM
        |  (select * , PROCTIME() AS proctime from user_behavior) AS b
        |  JOIN users FOR SYSTEM_TIME AS OF b.`proctime` AS u
        |  ON b.uid = u.rowkey
        |
        |""".stripMargin)

    streamTableEnv.execute("Temporal table join")
  }
}
