package org.rabbit.streaming

import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.joda.time.DateTime
import org.rabbit.connectors.{JdbcRichSink, JdbcRichSource}

import scala.util.{Failure, Success, Try}

object FromJdbcSinkJdbc {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)


    val deploy = "dev"
    val sourcSql = "select srcuid,price,id from orders"
    val sinkSql = "INSERT INTO orders_copy (srcuid, price, id) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id=id+?"
    val dataStream = env.addSource(new DataJdbcRichSource(deploy, sourcSql))
    dataStream.print()
    dataStream.addSink(new OrderJdbcRichSink(deploy, sinkSql)) //写入mysql


    env.execute("flink mysql demo") //运行程序
  }

}


class DataJdbcRichSource(deploy: String, sql: String) extends JdbcRichSource[OrderInfo](deploy: String, sql: String) {

  override def run(ctx: SourceFunction.SourceContext[OrderInfo]): Unit = {
    Try {
      val resultSet = stmt.executeQuery()
      while (resultSet.next()) {
        val srcuid = resultSet.getLong("srcuid");
        val price = resultSet.getLong("price");
        val id = resultSet.getInt("id");
        ctx.collect(OrderInfo(srcuid, price, id))//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
      }

    } match {
      case Failure(ex) =>
        logger.error("task fail: ", ex)
        val end = DateTime.now().toString("yyyy-MM-dd HH:mm:ss.sss")
        println("task fail: " + s", on $end")
        println(s"error msg: ${ex.getMessage}")

      case Success(uint) =>
        val end = DateTime.now().toString("yyyy-MM-dd HH:mm:ss.sss")
        println(s"task finished: " + s", on $end")
    }

  }

}

class OrderJdbcRichSink(deploy: String, sql: String) extends JdbcRichSink[OrderInfo](deploy: String, sql: String){

  override def invoke(orderInfo: OrderInfo, context: Context[_]): Unit = {
    try {
      //4.组装数据，执行插入操作
      stmt.setLong(1, orderInfo.srcuid)
      stmt.setLong(2, orderInfo.price)
      stmt.setInt(3, orderInfo.id)

      stmt.setInt(4, orderInfo.id)
      stmt.executeUpdate()

    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

}

case class OrderInfo(srcuid: Long,price: Long, id:Int)