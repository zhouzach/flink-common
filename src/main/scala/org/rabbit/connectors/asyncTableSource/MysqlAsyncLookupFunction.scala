package org.rabbit.connectors.asyncTableSource

import java.util.concurrent.CompletableFuture
import java.util.{Collection, Collections, List}

import io.vertx.core.json.JsonObject
import io.vertx.core.{AsyncResult, Handler, Vertx, VertxOptions}
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.{ResultSet, SQLConnection}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists
import org.apache.flink.table.functions.{AsyncTableFunction, FunctionContext}
import org.apache.flink.types.Row
import scala.collection.JavaConversions._

class MysqlAsyncLookupFunction(tableName: String,
                               fieldTypes: Array[TypeInformation[_]],
                               fieldNames: Array[String],
                               connectionField: Array[String]) extends AsyncTableFunction[Row] {
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val URL = "jdbc:mysql://127.0.0.1:3306/dashboard?charset=utf8"

  var jdbcClient: JDBCClient = _

  def getEnclosedField(field: String) = {
    "`" + field + "`"
  }
  def getSelectFromStatement(tableName: String, selectFields: Seq[String], conditionFields: Seq[String]) = {

    val whereClause = if(conditionFields.length > 0){
      " WHERE "+conditionFields.map(cf => s"`$cf` =? ").mkString(",")
    } else {
      ""
    }

    s"""
      |
      |select ${selectFields.map(f => s"`$f`").mkString(",")}
      |from `$tableName`
      |$whereClause
      |
      |""".stripMargin
  }

  def eval(resultFuture: CompletableFuture[Collection[Row]], filterParameter: String): Unit = {
    if(fieldNames.size ==0){
      return
    }

//    val inputParams = new JsonArray();
//    filterParameter.split(",").foreach(p => inputParams.add(p))

    val connectionHandler = new Handler[AsyncResult[SQLConnection]] {
      override def handle(asyncResultSQLConnection: AsyncResult[SQLConnection]): Unit = {
        if (asyncResultSQLConnection.failed()) {
          resultFuture.completeExceptionally(asyncResultSQLConnection.cause());
          return
        }

        val connection = asyncResultSQLConnection.result()
        val sql = getSelectFromStatement(tableName, fieldNames, connectionField);

        val queryHandler = new Handler[AsyncResult[ResultSet]] {
          override def handle(done: AsyncResult[ResultSet]): Unit = {
            if (done.failed()) {
              resultFuture.completeExceptionally(done.cause());
              return
            }

            val resultSize = done.result().getResults.size()
            if (resultSize > 0) {
              val  rowList: List[Row] = Lists.newArrayList();


              for (line <- done.result.getResults) {

                val row = new Row(fieldNames.length)
                for (i <- fieldNames.indices) {
                  row.setField(i, line.getValue(i))
                }

                rowList.add(row)
              }

              resultFuture.complete(rowList)
            } else {
              resultFuture.complete(Collections.emptyList())
            }

            val handler = new Handler[AsyncResult[Void]] {
              override def handle(done: AsyncResult[Void]): Unit = {
                if (done.failed()) {
                  throw new RuntimeException(done.cause())
                }
              }
            }
            connection.close(handler)

          }
        }
        connection.query(sql,queryHandler)

      }
    }
    jdbcClient.getConnection(connectionHandler)
  }


  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(fieldTypes, fieldNames)
  }


  override def open(context: FunctionContext): Unit = {
    // 使用vertx来实现异步jdbc查询
    val mysqlClientConfig = new JsonObject()
    mysqlClientConfig.put("url", URL)
      .put("driver_class", MYSQL_DRIVER)
      .put("user", "root")
      .put("password", "123456");
    System.setProperty("vertx.disableFileCPResolving", "true")

    val vo = new VertxOptions();
    vo.setFileResolverCachingEnabled(false);
    vo.setWarningExceptionTime(60000);
    vo.setMaxEventLoopExecuteTime(60000);
    val vertx = Vertx.vertx(vo);
    jdbcClient = JDBCClient.createNonShared(vertx, mysqlClientConfig)

  }

  override def close(): Unit = {
    jdbcClient.close()
  }

  override def isDeterministic: Boolean = false
}
