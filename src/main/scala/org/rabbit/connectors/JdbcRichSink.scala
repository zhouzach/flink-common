package org.rabbit.connectors

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.rabbit.config.{DevDbConfig, ProdDbConfig}

abstract class JdbcRichSink[IN](deploy: String, sql: String) extends RichSinkFunction[IN] {

  var conn: Connection = _
  var stmt: PreparedStatement = _

  /**
   * 一、open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
   */
  override def open(parameters: Configuration): Unit = {
    //    super.open(parameters)

    deploy match {
      case "dev" =>
        conn = DriverManager.getConnection(DevDbConfig.url, DevDbConfig.pop)

      case "prod" =>
        conn = DriverManager.getConnection(ProdDbConfig.url, ProdDbConfig.pop)
    }

    stmt = conn.prepareStatement(sql)
  }

  /**
   * 二、每个元素的插入都要调用一次invoke()方法，这里主要进行插入操作
   */


  /**
   * 三、 程序执行完毕就可以进行，关闭连接和释放资源的动作了
   */
  override def close(): Unit = {
    //    super.close()

    if (stmt != null) {
      stmt.close()
    }

    if (conn != null) {
      conn.close()
    }
  }
}