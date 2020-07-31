package org.rabbit.sql

import java.time.Duration

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.catalog.hive.HiveCatalog

object FromKafkaSinkHive {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
//    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
//    streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(20))
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(900))

    val hiveConfDir = "/Users/Zach/dev-apps" // a local path
    val hiveVersion = "2.1.1"

    val catalogName = "my_Catalog"
    val catalog = new HiveCatalog(catalogName, "default", hiveConfDir, hiveVersion)
    streamTableEnv.registerCatalog(catalogName, catalog)
    streamTableEnv.useCatalog(catalogName)

    streamTableEnv.executeSql(
      """
        |create database if not exists stream_tmp
        |
        |""".stripMargin)
    streamTableEnv.executeSql(
      """
        |drop TABLE if exists stream_tmp.kafka_table
        |
        |""".stripMargin)
    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE stream_tmp.kafka_table (
        |    uid BIGINT,
        |    sex string,
        |    age INT,
        |    created_time bigint,
        |    procTime AS PROCTIME(),
        |    eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(created_time/1000, 'yyyy-MM-dd HH:mm:ss')),
        |    WATERMARK FOR eventTime as eventTime - INTERVAL '3' SECOND
        |) WITH (
        |    'connector' = 'kafka',
        |    -- 'connector.version' = 'universal',
        |     'topic' = 't_user_f',
        |    -- 'connector.topic' = 'user_long',
        |    --'connector.properties.zookeeper.connect' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'properties.bootstrap.servers' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    --'properties.bootstrap.servers' = 'pro-kafka-01:9092,pro-kafka-02:9092,pro-kafka-03:9092',
        |    'properties.group.id' = 'user_flink',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'json',
        |    --'format.derive-schema' = 'true'
        |    'json.fail-on-missing-field' = 'false',
        |    'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)


    streamTableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    streamTableEnv.executeSql(
      """
        |create database if not exists hive_tmp
        |
        |""".stripMargin)
    streamTableEnv.executeSql(
      """
        |drop TABLE if exists hive_tmp.hive_table
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE  hive_tmp.hive_table (
        |  uid1 BIGINT,
        |    sex1 string,
        |    age1 INT,
        |    created_time1 bigint
        |) PARTITIONED BY (
        | ts_date STRING,
        | ts_hour STRING,
        | ts_minute string
        | ) STORED AS parquet
        | TBLPROPERTIES (
        |  'partition.time-extractor.timestamp-pattern'='$ts_date $ts_hour:$ts_minute:00',
        |  'sink.partition-commit.trigger'='partition-time',
        |  'sink.partition-commit.delay'='1s',
        |  'sink.partition-commit.policy.kind'='metastore,success-file'
        |)
        |
        |""".stripMargin)

    streamTableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
    streamTableEnv.executeSql(
      """
        |
        |INSERT INTO hive_tmp.hive_table
        |SELECT uid, sex,age,created_time,
        |DATE_FORMAT(eventTime, 'yyyy-MM-dd'),
        |DATE_FORMAT(eventTime, 'HH'),
        |DATE_FORMAT(eventTime, 'mm')
        |FROM stream_tmp.kafka_table
        |
        |""".stripMargin)

  }

}
