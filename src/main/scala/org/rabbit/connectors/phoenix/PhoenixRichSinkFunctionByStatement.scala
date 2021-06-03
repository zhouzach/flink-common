package org.rabbit.connectors.phoenix

import java.sql.{Connection, DriverManager, Statement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

//https://blog.csdn.net/qq_39140816/article/details/103909946
abstract class PhoenixRichSinkFunctionByStatement[IN](deploy: String, sql: String) extends RichSinkFunction[IN] {

  var conn: Connection = _
  var stmt: Statement = _

  /**
   * 一、open()方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接。
   */
  override def open(parameters: Configuration): Unit = {
    //    super.open(parameters)

//    deploy match {
//
//      case "dev" =>
//        try {
//          conn = DriverManager.getConnection(DevPhoenixConfig.url, DevPhoenixConfig.pop)
//        } catch {
//          case exception: SQLException =>
//            println(exception)
//        }
//
//      case "prod" =>
//        try {
////          conn = DriverManager.getConnection(ProdDbConfig.url, ProdDbConfig.pop)
//        } catch {
//          case exception: SQLException =>
//            println(exception)
//        }
//    }
//
//    stmt = conn.prepareStatement(sql)


    val driver = "org.apache.phoenix.jdbc.PhoenixDriver"
    val url ="jdbc:phoenix:cdh1,cdh2,cdh3:2181"

    Class.forName(driver)
    conn = DriverManager.getConnection(url) // default auto-commit mode is disabled, so conn.commit() must been called in invoke method

    stmt = conn.createStatement()
    stmt.setQueryTimeout(6000)

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