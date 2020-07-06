package org.rabbit.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object FromJdbcSinkJdbcBySqlForLong {

  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = TableEnvironment.create(settings)

    tableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE analysis_gift_consume (
        |    id  bigint,
        |    times INT,
        |    gid INT,
        |    gname VARCHAR,
        |    counts bigint
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://localhost:3306/dashboard',
        |    'connector.table' = 'analysis_gift_consume',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    tableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE analysis_gift_consume1 (
        |    times INT,
        |    gid INT,
        |    gname VARCHAR,
        |    counts DECIMAL(38, 18)
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://localhost:3306/dashboard',
        |    'connector.table' = 'analysis_gift_consume1',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

//    val sink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
//      .setDrivername("com.mysql.jdbc.Driver")
//      .setDBUrl("jdbc:mysql://localhost:3306/dashboard")
//      .setUsername("root")
//      .setPassword("123456")
//      //            .setQuery("INSERT INTO t2(id) VALUES (?)")
//      //      .setParameterTypes(BasicTypeInfo.INT_TYPE_INFO)
//      .setQuery("INSERT INTO analysis_gift_consume1 (id,times, gid,gname,counts) values (?,?,?,?,?)")
//      .setParameterTypes(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,
//        BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO)
//      .build()
//
//    tableEnv.registerTableSink(
//      "jdbcOutputTable",
//      // specify table schema
//      //      Array[String]("id"),
//      //      Array[TypeInformation[_]](Types.INT),
//
//      Array[String]("id","times", "gid", "gname","counts"),
//      Array[TypeInformation[_]](Types.LONG, Types.INT, Types.INT, Types.STRING,Types.LONG),
//      sink)
//
//    tableEnv.sqlQuery("select id,times, gid,gname,counts from analysis_gift_consume")
//      .insertInto("jdbcOutputTable")

    tableEnv.sqlUpdate(
      """
        |
        |insert into analysis_gift_consume1(times,gid,gname,counts)
        |select times,gid,gname,cast(counts as DECIMAL(38, 18)) from analysis_gift_consume
        |
        |""".stripMargin)


    tableEnv.execute("sink mysql")
  }

}
