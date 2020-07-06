package org.rabbit.streaming.kafka

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer

class EventTimeBucketAssigner extends BucketAssigner[String, String] {
  override def getBucketId(element: String, context: BucketAssigner.Context): String = {
    var partitionValue =""
    try partitionValue = getPartitionValue(element)
    catch {
      case e: Exception =>
        partitionValue = "00000000"
    }
    "dt=" + partitionValue //分区目录名称

  }

  override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE

  @throws[Exception]
  private def getPartitionValue(element: String) = { // 取出最后拼接字符串的es字段值，该值为业务时间
    val eventTime = (element.split(",")(1)).toLong
    val eventDate = new Date(eventTime)
    new SimpleDateFormat("yyyyMMdd").format(eventDate)
  }
}
