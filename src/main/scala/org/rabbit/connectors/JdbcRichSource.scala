package org.rabbit.connectors

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.rabbit.config.{DevDbConfig, ProdDbConfig}
import org.slf4j.LoggerFactory

abstract class JdbcRichSource[IN](deploy: String, sql: String) extends RichSourceFunction[IN]{
  val logger = LoggerFactory.getLogger(getClass);

  var conn: Connection = _
  var stmt: PreparedStatement = _


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


  override def cancel(): Unit = {
    //    super.close()

    if (stmt != null) {
      stmt.close()
    }

    if (conn != null) {
      conn.close()
    }
  }

}
