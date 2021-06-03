package org.rabbit.sql.join

import java.time.Duration

import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object KafkaJoinHBaseSinkHBase {

  def main(args: Array[String]): Unit = {

        val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
        streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//        streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

        val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
        val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
        streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
        streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(20))
        streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(900))


    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE kafka_table (
        |    uid VARCHAR,
        |    sex VARCHAR,
        |    age INT,
        |    created_time string,
        |    procTime AS PROCTIME(),
        |    eventTime AS TO_TIMESTAMP(created_time),
        |    WATERMARK FOR eventTime as eventTime - INTERVAL '3' SECOND
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 't_user_f',
        |    'properties.bootstrap.servers' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    'properties.group.id' = 'g_user_f',
        |    'scan.startup.mode' = 'latest-offset',
        |    --'scan.startup.mode' = 'specific-offsets',
        |    --'scan.startup.specific-offsets' = 'partition:0,offset:12;partition:1,offset:12;partition:2,offset:12;partition:3,offset:12;partition:4,offset:12;partition:5,offset:12;partition:6,offset:12;partition:7,offset:12;partition:8,offset:12',
        |    'format' = 'json',
        |    'json.fail-on-missing-field' = 'false',
        |    'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin)


    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE hbase_source (
        |    rowkey VARCHAR,
        |    cf ROW(uid VARCHAR,sex VARCHAR, age1 INT, created_time1 VARCHAR)
        |) WITH (
        |    'connector.type' = 'hbase',
        |    'connector.version' = '2.1.0',
        |    'connector.table-name' = 'ods:user_hbase2',
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
        |CREATE TABLE hbase_sink (
        |    rowkey VARCHAR,
        |    cf ROW(uid VARCHAR, age INT, created_time VARCHAR)
        |) WITH (
        |    'connector.type' = 'hbase',
        |    'connector.version' = '2.1.0',
        |    'connector.table-name' = 'ods:user_agg',
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
            |insert into  hbase_sink
            |SELECT
            |   kafka_table.uid,
            |  ROW(rowkey,  age, `created_time` ) as cf
            |FROM
            |  kafka_table
            |  JOIN hbase_source FOR SYSTEM_TIME AS OF kafka_table.`procTime`
            |  ON kafka_table.uid = hbase_source.rowkey
            |
            |""".stripMargin)


    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE print_table
        |(
        |   uid VARCHAR,
        | rowkey VARCHAR,
        | age INT,
        | created_time VARCHAR
        |)
        |WITH ('connector' = 'print')
        |
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |insert into print_table
        |SELECT
        |   kafka_table.uid,rowkey,age,created_time
        |FROM
        | kafka_table
        |  JOIN hbase_source FOR SYSTEM_TIME AS OF kafka_table.`procTime`
        |  ON kafka_table.uid = hbase_source.rowkey
        |
        |""".stripMargin)



  }

}
