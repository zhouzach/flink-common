package org.rabbit.streaming.kafka

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation, Types}
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object FromKafkaSinkJdbcByJDBCAppendTableSink {

  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = TableEnvironment.create(settings)

    tableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE user_behavior (
        |    uid VARCHAR,
        |    `time` TIMESTAMP(3),
        |    phoneType VARCHAR,
        |    clickCount INT
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


    val sink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://localhost:3306/dashboard")
      .setUsername("root")
      .setPassword("123456")
      //            .setQuery("INSERT INTO t2(id) VALUES (?)")
      //      .setParameterTypes(BasicTypeInfo.INT_TYPE_INFO)
      .setQuery("INSERT INTO user_behavior (uid, `time`,phoneType,clickCount) values (?,?,?,?)")
      .setParameterTypes(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO)
      .build()

    tableEnv.registerTableSink(
      "jdbcOutputTable",
      // specify table schema
      //      Array[String]("id"),
      //      Array[TypeInformation[_]](Types.INT),

      Array[String]("uid", "`time`", "phoneType","clickCount"),
      Array[TypeInformation[_]](Types.STRING,Types.STRING, Types.STRING, Types.INT),
      sink)

    tableEnv.sqlQuery("select uid, cast(`time` as string),phoneType,clickCount from user_behavior")
      .insertInto("jdbcOutputTable")

    tableEnv.execute("from kafka sink mysql")
  }

}
