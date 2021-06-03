package org.rabbit.sql.cdc

import java.time.Duration

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.config.DevDbConfig

object FromMysqlSinkMysql {

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
        |-- creates a mysql cdc table source
        |CREATE TABLE user_binlog (
        | id bigint NOT NULL,
        | uid bigint NOT NULL,
        | sex STRING,
        | age INT,
        | created_time bigint,
        | PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        | 'connector' = 'mysql-cdc',
        | 'hostname' = 'localhost',
        | 'port' = '3306',
        | 'username' = 'root',
        | 'password' = '123456',
        | 'database-name' = 'dashboard',
        | 'table-name' = 'user_mysql'
        |)
        |
        |
        |""".stripMargin)




    streamTableEnv.executeSql(
      """
        |
        |CREATE TABLE print_table
        |(
        |    id bigint NOT NULL,
        | uid bigint NOT NULL,
        | sex STRING,
        | age INT,
        | created_time bigint
        |)
        |WITH ('connector' = 'print')
        |
        |
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |insert into print_table
        |SELECT
        |   id,uid,sex,age,created_time
        |FROM  user_binlog
        |
        |""".stripMargin)


    streamTableEnv.executeSql(
      s"""
         |
         |CREATE TABLE user_mysql(
         |    id bigint NOT NULL,
         | uid bigint NOT NULL,
         | sex STRING,
         | age INT,
         | created_time bigint,
         | PRIMARY KEY (id) NOT ENFORCED
         |) WITH (
         |    'connector.type' = 'jdbc',
         |    'connector.url' = '${DevDbConfig.url}',
         |    'connector.table' = 'user_mysql',
         |    'connector.username' = '${DevDbConfig.user}',
         |    'connector.password' = '${DevDbConfig.password}',
         |    'connector.write.flush.max-rows' = '1'
         |)
         |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |INSERT INTO user_mysql
        |SELECT id,uid,sex,age,created_time
        |FROM user_binlog
        |
        |""".stripMargin)






  }

}
