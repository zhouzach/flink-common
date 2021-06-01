//package org.rabbit.sql.join
//
//import java.time.Duration
//
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
//import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.table.api.EnvironmentSettings
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.rabbit.config.{DevCDHDbConfig, DevPhoenixConfig}
//
//object KafkaJoinPhoenixSinkMysql {
//
//  def main(args: Array[String]): Unit = {
//
//    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
//
//    streamExecutionEnv.disableOperatorChaining()
//    streamExecutionEnv.setParallelism(1)
//
//    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
////    streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))
//
//    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
//    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
//    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20))
//    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(900))
//
//
//    streamTableEnv.executeSql(
//      """
//        |
//        |CREATE TABLE kafka_table (
//        |    k_uid BIGINT,
//        |    k_sex string,
//        |    k_age INT,
//        |    k_created_time bigint,
//        |    procTime AS PROCTIME()
//        |    --- eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(k_created_time/1000, 'yyyy-MM-dd HH:mm:ss')),
//        |    --- WATERMARK FOR eventTime as eventTime - INTERVAL '3' SECOND
//        |) WITH (
//        |    'connector' = 'kafka',
//        |     'topic' = 't_join_phoenix1',
//        |    'properties.bootstrap.servers' = 'cdh1:9092,cdh2:9092,cdh3:9092',
//        |    'properties.group.id' = 'user_flink',
//        |    'scan.startup.mode' = 'latest-offset',
//        |    'format' = 'json',
//        |    'json.fail-on-missing-field' = 'false',
//        |    'json.ignore-parse-errors' = 'true'
//        |)
//        |""".stripMargin)
//
//
//    streamTableEnv.executeSql(
//      s"""
//         |-- 字段名称及大小写 必须和phoenix表一致
//         |CREATE TABLE user_phoenix(
//         |    ID BIGINT,
//         |    SEX VARCHAR,
//         |    AGE INT,
//         |    CREATED_TIME bigint
//         |) WITH (
//         |    'connector' = 'jdbc',
//         |   'url' = '${DevPhoenixConfig.url}',
//         |   'driver'= '${DevPhoenixConfig.driver}',
//         |   'table-name' = 'ODS.USER_FLINK4'
//         |)
//         |""".stripMargin)
//
////    streamTableEnv.executeSql(
////      s"""
////         |-- 字段名称及大小写 必须和phoenix表一致
////         |CREATE TABLE agg_phoenix(
////         |    ID BIGINT,
////         |    SEX VARCHAR,
////         |    AGE INT,
////         |    CREATED_TIME bigint
////         |) WITH (
////         |    'connector' = 'jdbc',
////         |   'url' = '${DevPhoenixConfig.url}',
////         |   'driver'= '${DevPhoenixConfig.driver}',
////         |   'table-name' = 'ODS.USER_AGG',
////         |   'lookup.cache.max-rows' = '1',
////         |   'lookup.cache.ttl' = '1s',
////         |   'lookup.max-retries' = '3'
////         |)
////         |""".stripMargin)
////
////    streamTableEnv.executeSql(
////      """
////        |
////        |insert into agg_phoenix
////        |select k_uid,SEX,AGE,CREATED_TIME
////        |from kafka_table
////        |JOIN user_phoenix FOR SYSTEM_TIME AS OF kafka_table.`procTime`
////        |  ON kafka_table.k_uid = user_phoenix.ID
////        |
////        |""".stripMargin)
//
//    streamTableEnv.executeSql(
//      s"""
//         |
//         |CREATE TABLE user_mysql(
//         |    id bigint NOT NULL,
//         | uid bigint NOT NULL,
//         | sex STRING,
//         | age INT,
//         | created_time bigint,
//         | PRIMARY KEY (id) NOT ENFORCED
//         |) WITH (
//         |    'connector' = 'jdbc',
//         |    'url' = '${DevCDHDbConfig.url2}',
//         |    'table-name' = 'user_00',
//         |    'username' = '${DevCDHDbConfig.user}',
//         |    'password' = '${DevCDHDbConfig.password}',
//         |    'sink.buffer-flush.max-rows' = '1',
//         |    'sink.buffer-flush.interval' = '1s',
//         |    'sink.max-retries' = '3'
//         |)
//         |""".stripMargin)
//
//
//        streamTableEnv.executeSql(
//          """
//            |
//            |insert into user_mysql
//            |select k_uid,ID,SEX,AGE,CREATED_TIME
//            |from kafka_table
//            |JOIN user_phoenix FOR SYSTEM_TIME AS OF kafka_table.`procTime`
//            |  ON kafka_table.k_uid = user_phoenix.ID
//            |
//            |""".stripMargin)
//
//
//  }
//
//}
