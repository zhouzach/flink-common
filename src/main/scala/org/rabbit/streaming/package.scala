package org.rabbit

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

package object streaming {

  def getEnv() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
    env
  }

  def getFlinkKafkaConsumer(topic: String)= {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "f1")
    //    properties.put("enable.auto.commit", "false")
    //    properties.setProperty("auto.offset.reset", "earliest")

    val flinkKafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    flinkKafkaConsumer
  }

}
