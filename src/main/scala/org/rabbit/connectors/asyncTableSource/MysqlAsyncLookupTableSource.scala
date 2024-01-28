//package org.rabbit.connectors.asyncTableSource
//
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.typeutils.RowTypeInfo
//import org.apache.flink.table.api.TableSchema
//import org.apache.flink.table.sources.LookupableTableSource
//import org.apache.flink.table.types.utils.TypeConversions
//import org.apache.flink.types.Row
//
//
//class MysqlAsyncLookupTableSource(tableName: String,
//                                  fieldTypes: Array[TypeInformation[_]],
//                                  fieldNames: Array[String],
//                                  connectionField: Array[String]) extends LookupableTableSource[Row]{
//
//  override def getLookupFunction(lookupKeys: Array[String]) = null
//
//  override def getAsyncLookupFunction(lookupKeys: Array[String]) = new MysqlAsyncLookupFunction(tableName,
//    fieldTypes,fieldNames, connectionField )
//
//  override def isAsyncEnabled: Boolean = true
//
//  override def getTableSchema: TableSchema = TableSchema.builder()
//    .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
//    .build()
//
//  override def getProducedDataType() = {
//    TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(fieldTypes, fieldNames))
//  }
//}
//
//
//
