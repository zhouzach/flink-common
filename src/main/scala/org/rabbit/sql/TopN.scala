package org.rabbit.sql

import java.time.Duration

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

//https://developer.aliyun.com/article/457445
object TopN {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    //    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)

    //需要设置IDEA Environment variables: HADOOP_USER_NAME=hdf
//    streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(20))
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(900))

    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE datagen (
        | uid INT,
        | roomId INT,
        | action INT,
        | ts AS localtimestamp,
        | WATERMARK FOR ts AS ts
        |) WITH (
        | 'connector' = 'datagen',
        |
        | -- optional options --
        |
        | 'rows-per-second'='1',
        |
        | 'fields.uid.min'='1',
        | 'fields.uid.max'='1000',
        |
        | 'fields.roomId.min'='1',
        | 'fields.roomId.max'='5',
        |
        | 'fields.action.min'='-1',
        | 'fields.action.max'='2'
        |
        |)
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE print_table
        |(
        |  uid INT,
        | roomId INT,
        | action INT,
        | ts TIMESTAMP(3)
        |)
        |WITH ('connector' = 'print')
        |
        |
        |""".stripMargin)



    streamTableEnv.executeSql(
      """
        |CREATE VIEW aggTable AS
        |SELECT
        |second(TUMBLE_START(ts, INTERVAL '2' second)) as minute_time,
        |  roomId,sum(action) as cnt
        |FROM  datagen
        |group by roomId
        |,TUMBLE(ts, INTERVAL '2' second)
        |
        |""".stripMargin)



    streamTableEnv.executeSql(
      """
        |
        |CREATE VIEW tmp_topn AS
        |SELECT *
        |FROM (
        |  SELECT *,
        |    ROW_NUMBER() OVER (PARTITION BY MOD(HASH_CODE(cast(roomId as string)),128) ORDER BY cnt DESC) AS rownum
        |  FROM aggTable)
        |WHERE rownum <= 3
        |
        |""".stripMargin)


    streamTableEnv.executeSql(
      """
        |
        |CREATE VIEW result_table AS
        |SELECT *
        |FROM (
        |  SELECT minute_time,roomId, cnt,
        |    ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rownum
        |  FROM tmp_topn)
        |WHERE rownum <= 3
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE mysql_table (
        |    minute_time bigINT,
        |    roomId INT,
        |    cnt INT,
        |    rownum bigINT
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://localhost:3306/dashboard',
        |    'connector.table' = 'top_n',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    val statementSet = streamTableEnv.createStatementSet()

    val insertPrint =
//    streamTableEnv.executeSql(
      """
        |insert into print_table
        |SELECT
        |  *
        |FROM  datagen
        |
        |""".stripMargin
//    )

    val insertMysql =
//    streamTableEnv.executeSql(
      """
        |insert into mysql_table
        |SELECT
        | *
        |FROM result_table
        |
        |""".stripMargin
//    )

    statementSet.addInsertSql(insertPrint)
    statementSet.addInsertSql(insertMysql)
    statementSet.execute()





  }

}
