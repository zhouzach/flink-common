//package org.rabbit.streaming.join
//
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableSchema}
//
//
///**
// * https://blog.csdn.net/wangpei1949/article/details/103554868
// */
//object KafkaJoinJdbcByTemporalTableSinkHbase {
//  def main(args: Array[String]): Unit = {
//
//    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
//    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
//
//    streamTableEnv.executeSql(
//      """
//        |
//        |CREATE TABLE user_behavior (
//        |    uid VARCHAR,
//        |    phoneType VARCHAR,
//        |    clickCount INT,
//        |    `time` TIMESTAMP(3)
//        |) WITH (
//        |    'connector.type' = 'kafka',
//        |    'connector.version' = 'universal',
//        |    'connector.topic' = 'user_behavior2',
//        |    'connector.startup-mode' = 'latest-offset',
//        |    'connector.properties.0.key' = 'zookeeper.connect',
//        |    'connector.properties.0.value' = 'cdh1:2181,cdh2:2181,cdh3:2181',
//        |    'connector.properties.1.key' = 'bootstrap.servers',
//        |    'connector.properties.1.value' = 'cdh1:9092,cdh2:9092,cdh3:9092',
//        |    'update-mode' = 'append',
//        |    'format.type' = 'json',
//        |    'format.derive-schema' = 'true'
//        |)
//        |""".stripMargin)
//
//
//    val jdbcOptions= JDBCOptions.builder()
//      .setDBUrl(${DevDbConfig.url})
//      .setDriverName("com.mysql.jdbc.Driver")
//      .setUsername(${DevDbConfig.user})
//      .setPassword(${DevDbConfig.password})
//      .setTableName("user_mysql_varchar")
//      .build()
//
//    val mysqlFieldNames = Array("uid", "sex", "age", "created_time")
//    val mysqlFieldTypes = Array(DataTypes.STRING, DataTypes.STRING, DataTypes.INT(), DataTypes.STRING)
//    val mysqlTableSchema = TableSchema.builder.fields(mysqlFieldNames, mysqlFieldTypes).build
//
//    val jdbcLookupOptions = JDBCLookupOptions.builder()
//      .setCacheExpireMs(10 * 1000) //缓存有效期
//      .setCacheMaxSize(10) //最大缓存数据条数
//      .setMaxRetryTimes(3) //最大重试次数
//      .build()
//
//    val userSource = JDBCTableSource.builder()
//      .setOptions(jdbcOptions)
//      .setLookupOptions(jdbcLookupOptions)
//      .setSchema(mysqlTableSchema)
//      .build()
//
//    streamTableEnv.registerTableSource("users", userSource)
//
//
//    streamTableEnv.executeSql(
//      """
//        |
//        |CREATE TABLE user_join_behavior(
//        |    rowkey VARCHAR,
//        |    cf ROW(phoneType VARCHAR, clickCount INT, `time` VARCHAR, sex VARCHAR,age INT)
//        |) WITH (
//        |    'connector.type' = 'hbase',
//        |    'connector.version' = '2.1.0',
//        |    'connector.table-name' = 'ods.user_join_behavior',
//        |    'connector.zookeeper.quorum' = 'cdh1:2181,cdh2:2181,cdh3:2181',
//        |    'connector.zookeeper.znode.parent' = '/hbase',
//        |    'connector.write.buffer-flush.max-size' = '10mb',
//        |    'connector.write.buffer-flush.max-rows' = '1000',
//        |    'connector.write.buffer-flush.interval' = '2s'
//        |)
//        |""".stripMargin)
//
//    streamTableEnv.executeSql(
//      """
//        |
//        |insert into  user_join_behavior
//        |SELECT
//        |  b.uid,
//        |  ROW(phoneType, clickCount, `time`,sex ,age) as cf
//        |FROM
//        |  (select uid,phoneType,clickCount, cast(`time` as string) as `time`, PROCTIME() AS proctime from user_behavior) AS b
//        |  JOIN users FOR SYSTEM_TIME AS OF b.`proctime` AS u
//        |  ON b.uid = u.uid
//        |
//        |""".stripMargin)
//
//
//
//    streamTableEnv.execute("Temporal table join")
//  }
//
//}
