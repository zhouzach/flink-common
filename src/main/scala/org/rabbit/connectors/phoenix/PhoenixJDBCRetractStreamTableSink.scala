package org.rabbit.connectors.phoenix

import java.lang

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.connector.jdbc.JdbcOutputFormat
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.sinks.{RetractStreamTableSink, TableSink}
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context

class PhoenixJDBCRetractStreamTableSink(tableSchema: TableSchema,
                                        outputformat: JdbcOutputFormat) extends RetractStreamTableSink[Row] with Serializable{

  override def getTableSchema: TableSchema = tableSchema
  override def getRecordType: TypeInformation[Row] = {
    new RowTypeInfo(tableSchema.getFieldTypes,tableSchema.getFieldNames)
  }

  override def consumeDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): DataStreamSink[_] = {
    val phoenixOutputformat = outputformat
    dataStream.addSink(new PhoenixSinkFunction[tuple.Tuple2[lang.Boolean, Row]](phoenixOutputformat){
      override def invoke(value:  tuple.Tuple2[lang.Boolean, Row], context:  Context[_]): Unit = {

        /**
         *  f0==true :插入新数据
         *  f0==false:删除旧数据
         *  由于数据Phoenix可以不考虑删除的操作，所以只用处理Boolean为ture的数据
         */
        if(value.f0){
          phoenixOutputformat.writeRecord(value.f1)
        }
      }
    })
  }

  override def configure(fieldNames: Array[String],
                         fieldTypes: Array[TypeInformation[_]]): TableSink[tuple.Tuple2[lang.Boolean, Row]] = {
    null
  }

}
