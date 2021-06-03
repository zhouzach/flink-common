package org.rabbit.sql.cdc

import java.time.Duration

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.rabbit.config.DevCDHDbConfig

/**
 * https://jiamaoxiang.top/2020/03/05/%E5%9F%BA%E4%BA%8ECanal%E4%B8%8EFlink%E5%AE%9E%E7%8E%B0%E6%95%B0%E6%8D%AE%E5%AE%9E%E6%97%B6%E5%A2%9E%E9%87%8F%E5%90%8C%E6%AD%A5-%E4%B8%80/
 *
 * 配置MySQL的binlog:
 * 对于自建 MySQL , 需要先开启 Binlog 写入功能，配置 binlog-format 为 ROW 模式，my.cnf 中配置如下
 *
 * [mysqld]
 * log-bin=mysql-bin # 开启 binlog
 * binlog-format=ROW # 选择 ROW 模式
 * server_id=1 # 配置 MySQL replaction 需要定义，不要和 canal 的 slaveId 重复
 *
 * 授权
 * 授权 canal 链接 MySQL 账号具有作为 MySQL slave 的权限, 如果已有账户可直接 grant
 *
 * CREATE USER canal IDENTIFIED BY 'canal';
 * GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
 * -- GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
 * FLUSH PRIVILEGES;
 *
 *
 * https://blog.51cto.com/gfsunny/1554627
 * GRANT ALL ON dashboard.* TO 'root'@'%' IDENTIFIED BY '123456';
 * -- reload 是 administrative 级的权限，即 server administration；这类权限包括：
 * -- CREATE USER, PROCESS, RELOAD, REPLICATION CLIENT, REPLICATION SLAVE, SHOW DATABASES, SHUTDOWN, SUPER
 * -- 这类权限的授权不是针对某个数据库的，因此须使用on *.* 来进行：
 * GRANT RELOAD ON *.* TO 'root'@'%';
 * GRANT SUPER ON *.* TO 'root'@'%';
 * GRANT REPLICATION CLIENT ON *.* TO 'root'@'%';
 * GRANT REPLICATION SLAVE ON *.* TO 'root'@'%';
 * flush privileges;
 */
object FromMultiMysqlSinkMysql {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.disableOperatorChaining()

    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20))
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(900))

    streamTableEnv.executeSql(
      s"""
         |
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
         | 'hostname' = '${DevCDHDbConfig.host2}',
         | 'port' = '3306',
         | 'username' = '${DevCDHDbConfig.user}',
         | 'password' = '${DevCDHDbConfig.password}',
         | 'database-name' = 'dashboard',
         | 'table-name' = 'user_**'
         |)
         |
         |
         |""".stripMargin)

    streamTableEnv.executeSql(
      s"""
         |
         |CREATE TABLE print_table WITH ('connector' = 'print')
         |LIKE user_binlog (EXCLUDING ALL)
         |
         |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |INSERT INTO print_table
        |SELECT id,uid,sex,age,created_time
        |FROM user_binlog
        |
        |""".stripMargin)


  }

}
