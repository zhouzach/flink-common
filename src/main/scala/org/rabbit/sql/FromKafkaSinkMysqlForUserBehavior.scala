package org.rabbit.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object FromKafkaSinkMysqlForUserBehavior {
  def main(args: Array[String]): Unit = {


    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = TableEnvironment.create(settings)

    tableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE user_log (
        |    uid VARCHAR,
        |    phoneType VARCHAR,
        |    clickCount INT,
        |    `time` TIMESTAMP(3)
        |) WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = 'universal',
        |    'connector.topic' = 't7',
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


    tableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE pvuv_sink (
        |    dt VARCHAR,
        |    pv BIGINT,
        |    uv BIGINT
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://localhost:3306/dashboard',
        |    'connector.table' = 'pvuv_sink',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    tableEnv.sqlUpdate(
      """
        |
        |INSERT INTO pvuv_sink(dt, pv, uv)
        |SELECT
        |  DATE_FORMAT(`time`, 'yyyy-MM-dd HH:00') dt,
        |  COUNT(*) AS pv,
        |  COUNT(DISTINCT uid) AS uv
        |FROM user_log
        |GROUP BY DATE_FORMAT(`time`, 'yyyy-MM-dd HH:00')
        |""".stripMargin)


    tableEnv.execute("from kafka sink mysql")
  }

}
