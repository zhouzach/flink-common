package org.rabbit.cep

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.rabbit.models.BehaviorInfo
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}


/**
 * https://juejin.im/post/5ed368b76fb9a047d3710aa2
 */
object AltertCEP {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)


    streamTableEnv.sqlUpdate(
      """
        |
        |CREATE TABLE user_behavior (
        |    uid VARCHAR,
        |    phoneType VARCHAR,
        |    clickCount INT,
        |      `time` TIMESTAMP(3)
        |) WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = 'universal',
        |    'connector.topic' = 't7',
        |    'connector.startup-mode' = 'latest-offset',
        |    'connector.properties.0.key' = 'zookeeper.connect',
        |    'connector.properties.0.value' = 'cdh1:2181,cdh2:2181,cdh3:2181',
        |    'connector.properties.1.key' = 'bootstrap.servers',
        |    'connector.properties.1.value' = 'cdh1:9092,cdh2:9092,cdh3:9092',
        |    'update-mode' = 'append',
        |    'format.type' = 'json',
        |    'format.derive-schema' = 'true'
        |)
        |""".stripMargin)


    val behaviorTable = streamTableEnv.sqlQuery(
      """
        |select * from `user_behavior`
        |""".stripMargin)


    val inpurtDS = streamTableEnv.toAppendStream[BehaviorInfo](behaviorTable)
    inpurtDS.print()

    val pattern = Pattern.begin[BehaviorInfo]("start")
      .where(_.clickCount > 7)


    val patternStream = CEP.pattern(inpurtDS, pattern)
    val result: DataStream[BehaviorInfo] = patternStream.process(
      new PatternProcessFunction[BehaviorInfo, BehaviorInfo]() {
        override def processMatch(
                                   matchPattern: util.Map[String, util.List[BehaviorInfo]],
                                   ctx: PatternProcessFunction.Context,
                                   out: Collector[BehaviorInfo]): Unit = {
          try {
            println(
              s"""
                 |matchPattern: $matchPattern
                 |util.List[BehaviorInfo]: ${matchPattern.get("start")}
                 |""".stripMargin)
            out.collect(matchPattern.get("start").get(0))
          } catch {
            case exception: Exception =>
              println(exception)
          }
        }
      })
    result.print()



    streamTableEnv.execute("from kafka sink mysql")
  }

}


class MyPatternProcessFunction extends PatternProcessFunction[BehaviorInfo, BehaviorInfo] {
  @throws[Exception]
  override def processMatch(`match`: util.Map[String, util.List[BehaviorInfo]], ctx: PatternProcessFunction.Context,
                            out: Collector[BehaviorInfo]): Unit = {
    val startEvent: BehaviorInfo = `match`.get("start").get(0)
    val endEvent: BehaviorInfo = `match`.get("end").get(0)
    println(
      s"""
         |
         |startEvent: $startEvent
         |""".stripMargin)
    out.collect(startEvent)
  }
}

