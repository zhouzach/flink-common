package org.rabbit.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html#update-modes
 */
object FromJdbcSinkJdbc {

  def main(args: Array[String]): Unit = {

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = TableEnvironment.create(settings)

    tableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE analysis (
        |    gid INT,
        |    gname VARCHAR
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://localhost:3306/dashboard',
        |    'connector.table' = 'analysis',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    tableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE analysis1 (
        |    gid INT,
        |    gname VARCHAR
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://localhost:3306/dashboard',
        |    'connector.table' = 'analysis1',
        |    'connector.username' = 'root',
        |    'connector.password' = '123456',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)


    tableEnv.sqlUpdate(
      """
        |
        |insert into analysis1
        |select * from analysis
        |
        |""".stripMargin)


    tableEnv.execute("sink mysql")
  }

}
