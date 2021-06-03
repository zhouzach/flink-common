package org.rabbit.connectors.phoenix

import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.JdbcOutputFormat
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context

class PhoenixSinkFunction[IN](outputFormat: JdbcOutputFormat) extends RichSinkFunction[IN] {

  override def invoke(value: IN, context: Context[_]): Unit = {

  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val ctx = getRuntimeContext
    outputFormat.setRuntimeContext(ctx)
    outputFormat.open(ctx.getIndexOfThisSubtask, ctx.getNumberOfParallelSubtasks)
  }

  override def close(): Unit = {
    super.close()
  }

}
