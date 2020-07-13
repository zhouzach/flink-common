package org.rabbit.streaming.cep

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.rabbit.config.DevDbConfig
import org.rabbit.connectors.JdbcRichSink
import org.rabbit.models.BehaviorData
import org.rabbit.utils.{MailHelper, MailInfo}


/**
 * https://juejin.im/post/5ed368b76fb9a047d3710aa2
 */
object StreamingCEP {
  val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment

  streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  streamExecutionEnv.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
  streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(30 * 1000)


  val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "flink")
    val topic = "user_behavior2"

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    //        consumer.setStartFromEarliest()
    //    consumer.setCommitOffsetsOnCheckpoints(true)
    consumer.setStartFromLatest()

    import org.apache.flink.streaming.api.scala._
    val sourceStream = streamExecutionEnv
      .addSource(consumer)
      .setParallelism(4)
      .name("source_kafka_bill")
      .uid("source_kafka_bill")

//        sourceStream.print()

    val billStream = sourceStream.map(message => JSON.parseObject(message, classOf[BehaviorData]))
      .name("map_sub_bill").uid("map_sub_bill")


    val startPattern = Pattern.begin[BehaviorData]("start")
      .where(_.clickCount > 7)
      .followedBy("end")
      .where(_.phoneType == "iOS")
      .within(Time.seconds(10))


    val patternStream = CEP.pattern(billStream, startPattern)
    val result = patternStream.process(new MyPatternProcessFunction)
    result.print()
    //        val alerts = patternStream.select(patternSelectFun => {
    //          val startEvent = patternSelectFun.get("start")
    //          val endEvent = patternSelectFun.get("end")
    //          if (startEvent.nonEmpty) {
    //            "数据来了***********" + startEvent.get.mkString(".") + endEvent
    //          } else {
    //            "no data"
    //          }
    //        })
    //    alerts.print

    streamTableEnv.createTemporaryView("myTable", result)

    streamTableEnv.executeSql(
      s"""
        |
        |CREATE TABLE user_behavior(
        |    uid VARCHAR,
        |    created_time VARCHAR,
        |    phoneType VARCHAR,
        |    clickCount INT
        |) WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = '${DevDbConfig.url}',
        |    'connector.table' = 'user_behavior',
        |    'connector.username' = '${DevDbConfig.user}',
        |    'connector.password' = '${DevDbConfig.password}',
        |    'connector.write.flush.max-rows' = '1'
        |)
        |""".stripMargin)

    streamTableEnv.executeSql(
      """
        |
        |insert into user_behavior
        |select * from myTable
        |
        |""".stripMargin)

//    val deploy = "dev"
//    val sinkSql =
//      """
//        |INSERT INTO user_behavior (uid, created_time, phoneType,clickCount)
//        |VALUES (?, ?, ?,?)
//        |ON DUPLICATE KEY UPDATE uid=uid+?
//        |
//        |""".stripMargin
//
//    result.addSink(new BehaviorJdbcRichSink(deploy, sinkSql))



    streamExecutionEnv.execute("from kafka sink mysql")
  }

}

class MyPatternProcessFunction extends PatternProcessFunction[BehaviorData, BehaviorData] {
  override def processMatch(
                             matchPattern: util.Map[String, util.List[BehaviorData]],
                             ctx: PatternProcessFunction.Context,
                             out: Collector[BehaviorData]): Unit = {
    try {
//      println(
//        s"""
//           |matchPattern: $matchPattern
//           |util.List[BehaviorInfo]: ${matchPattern.get("start")}
//           |""".stripMargin)
      val startEvent = matchPattern.get("start")
      val endEvent = matchPattern.get("end").get(0)

      val msg =
        s"""
           |start_event: $startEvent
           |end_event: $endEvent
           |
           |""".stripMargin

      MailHelper.send(MailInfo(
        to = "ispmd@foxmail.com" :: Nil,
        subject = "flink job 任务监控",
        message = msg
      ))

      out.collect(endEvent)
    } catch {
      case exception: Exception =>
        println(exception)
    }
  }

}


class BehaviorJdbcRichSink(deploy: String, sql: String) extends JdbcRichSink[BehaviorData](deploy: String, sql: String){

  override def invoke(behavior: BehaviorData, context: Context[_]): Unit = {
    try {
      //4.组装数据，执行插入操作
      stmt.setString(1, behavior.uid)
      stmt.setString(2, behavior.time)
      stmt.setString(3, behavior.phoneType)
      stmt.setInt(4, behavior.clickCount)
      stmt.setString(5, behavior.uid)
      stmt.executeUpdate()

    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

}




