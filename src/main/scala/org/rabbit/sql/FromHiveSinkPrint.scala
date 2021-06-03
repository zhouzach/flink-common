package org.rabbit.sql

import java.time.Duration

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.types.Row

object FromHiveSinkPrint {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
//    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
    //需要设置IDEA Environment variables: HADOOP_USER_NAME=hdfs
//    streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(20))
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(900))
    streamTableEnv.getConfig.getConfiguration.setBoolean(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true)

    val hiveConfDir = "/Users/Zach/dev-apps" // a local path
    val hiveVersion = "2.1.1"

    val catalogName = "my_Catalog"
    val catalog = new HiveCatalog(catalogName, "default", hiveConfDir, hiveVersion)
    streamTableEnv.registerCatalog(catalogName, catalog)
    streamTableEnv.useCatalog(catalogName)



    val result = streamTableEnv.executeSql(
      """
        |SELECT
        |   uid1,sex1,age1,created_time1
        |FROM  hive_tmp.hive_table
        |/*+ OPTIONS(
        |       'streaming-source.enable' = 'true',
        |       'streaming-source.monitor-interval' = '1 s',
        |       'streaming-source.consume-start-offset' = '2020-07-28 23:30:00'
        |     ) */
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |drop TABLE if exists print_table1
        |
        |""".stripMargin)
    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE print_table1
        |(
        |    uid BIGINT,
        |    sex string,
        |    age INT,
        |    created_time bigint
        |)
        |WITH ('connector' = 'print')
        |
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |insert into print_table1
        |SELECT
        |   uid1,sex1,age1,created_time1
        |FROM  hive_tmp.hive_table
        |/*+ OPTIONS(
        |       'streaming-source.enable' = 'true',
        |       'streaming-source.monitor-interval' = '1 s'
        |       --'streaming-source.consume-start-offset' = '2020-07-28 23:30:00'
        |     ) */
        |
        |""".stripMargin)



  }

}
