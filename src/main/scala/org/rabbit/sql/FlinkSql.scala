package org.rabbit.sql

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * http://wuchong.me/blog/2020/02/25/demo-building-real-time-application-with-flink-sql/
 */
object FlinkSql {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)

    val tableConfig = streamTableEnv.getConfig
    tableConfig.setIdleStateRetentionTime(Time.minutes(1), Time.minutes(6))

    val configuration = tableConfig.getConfiguration
    configuration.setString("table.exec.mini-batch.enabled", "true")
    configuration.setString("table.exec.mini-batch.allow-latency", "5 s")
    configuration.setString("table.exec.mini-batch.size", "5000")



    streamTableEnv.executeSql(
      """
        |select
        |UNIX_TIMESTAMP('2020-07-16 13:39:06.454')
        |,TO_TIMESTAMP('2020-07-16 13:39:06.454') as Timestamp1
        |-- ,TO_TIMESTAMP('2020-07-23T19:53:15.509Z') as Timestamp2  -- 错误的时间字符串
        |,Timestamp '2020-07-16 13:39:06.454' as Timestamp3
        |,Date '2015-10-11' as date1
        |,Time '12:14:50'
        |,YEAR(Date '2015-10-11')
        |,QUARTER(Date '2015-10-11')
        |,MONTH(Date '2015-10-11')
        |,WEEK(Date '2015-10-11')
        |,DAYOFYEAR(Date '2015-10-11')
        |,DAYOFMONTH(Date '2015-10-11')
        |
        |,HOUR(TO_TIMESTAMP('2020-07-16 13:39:06.454')) as Hour1
        |,MINUTE(Timestamp '2015-10-11 12:14:50') as Minute1
        |,SECOND(Timestamp '2015-10-11 12:14:50') as Second1
        |
        |,cast(Timestamp '2020-07-16 13:39:06.454' as string) as ts_str
        |,SUBSTRING(cast(Timestamp '2020-07-16 13:39:06.454' as string),0,10) as date_str
        |,SUBSTRING(cast(Timestamp '2020-07-16 13:39:06.454' as string),12) as time_str
        |
        |""".stripMargin)
      .print()







  }

}
