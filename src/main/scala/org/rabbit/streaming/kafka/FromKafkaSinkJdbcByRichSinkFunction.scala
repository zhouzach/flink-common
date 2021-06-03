package org.rabbit.streaming.kafka

import java.math.BigDecimal
import java.sql.Date
import java.text.SimpleDateFormat

import com.jayway.jsonpath.JsonPath
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.rabbit.connectors.JdbcRichSink
import org.rabbit.streaming.{getEnv, getFlinkKafkaConsumer}

object SinkMysqlByRichSinkFunction {

  def main(args: Array[String]): Unit = {

    val env = getEnv()

    val topic = "t1"
    val deploy = "dev"
    val consumer = getFlinkKafkaConsumer(topic)
    //    consumer.setStartFromEarliest()
    consumer.setStartFromLatest()

//    val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
//    specificStartOffsets.put(new KafkaTopicPartition("treasure_box", 0), 4820L)
    //    specificStartOffsets.put(new KafkaTopicPartition("treasure_box", 1), 31L)
    //    specificStartOffsets.put(new KafkaTopicPartition("treasure_box", 2), 43L)
//    consumer.setStartFromSpecificOffsets(specificStartOffsets)


    val sourceStream = env
      .addSource(consumer)
      .setParallelism(1)
      .name("source_kafka_filebeat")
      .uid("source_kafka_filebeat")
//    sourceStream.print()

//    sourceStream
//        .timeWindowAll(Time.minutes(1), Time.hours(-8))
//      .timeWindowAll(Time.milliseconds(1000))
//      .process(new MyReduceWindowAllFunction())
//      .print()


    val mapStream = sourceStream.map(data => {
      try {
        val array = JsonPath.read[String](data, "$.message")
          .split(",")
        DataInfo(
          array(0).toLong,
          array(1).toLong,
          array(2).toInt,
          new BigDecimal(array(3)))
      } catch {
        case _ => {
          println("read json failed")
          DataInfo( 0L, 0L, 0, new BigDecimal(0))
        }
      }
    })
      .name("map_info").uid("map_info")

    mapStream.print()

    val sql = "insert into t1 (uid,date_of,num,result_credit) values (?,?,?,?) on duplicate key update num=num+?,result_credit=result_credit+?"
    mapStream.addSink(new DataJdbcRichSink(deploy,sql))
      .name("sink_mysql").uid("sink_mysql")

    env.execute()
  }

}

case class DataInfo(
                            uid: Long,
                            dateOf: Long,
                            num: Int,
                            resultCredit: BigDecimal
                          )


class DataJdbcRichSink(deploy: String, sql: String) extends JdbcRichSink[DataInfo](deploy: String, sql: String){

  override def invoke(dataInfo: DataInfo, context: Context[_]): Unit = {
    try {
      //4.组装数据，执行插入操作
      stmt.setLong(1, dataInfo.uid)
      stmt.setLong(2, dataInfo.dateOf)
      stmt.setInt(3, dataInfo.num)
      stmt.setBigDecimal(4, dataInfo.resultCredit)

      stmt.setInt(5, dataInfo.num)
      stmt.setBigDecimal(6, dataInfo.resultCredit)

      stmt.executeUpdate()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

}



class MyReduceWindowAllFunction
  extends ProcessAllWindowFunction[String, ((String, String),  Seq[DataInfo],Int), TimeWindow] {

  override def process(context: Context,
                       elements: Iterable[ String],
                       collector: Collector[((String, String),  Seq[DataInfo],Int)]): Unit = {

    val startTs = context.window.getStart
    val endTs = context.window.getEnd
    val elems = elements.map(data => {

      try {

        val array = JsonPath.read[String](data, "$.message")
          .split(",")
        DataInfo(
          array(0).toLong,
          array(1).toLong,
          0,
          new BigDecimal(array(3)))
      } catch {
        case _ => {
          println("read json failed")
          DataInfo(0L, 0L, 0, new BigDecimal(0))
        }
      }

    })

      val outputKey = (formatTs(startTs), formatTs(endTs))
      collector.collect((outputKey, elems.toSeq, elems.size))

  }

  private def formatTs(ts: Long) = {

    val df = new SimpleDateFormat("yyyyMMddHHmmss")

    df.format(new Date(ts))

  }

}