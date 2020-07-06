package org.rabbit.streaming.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.sink.filesystem.{RollingPolicy, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * https://jiamaoxiang.top/2020/03/24/%E5%9F%BA%E4%BA%8ECanal%E4%B8%8EFlink%E5%AE%9E%E7%8E%B0%E6%95%B0%E6%8D%AE%E5%AE%9E%E6%97%B6%E5%A2%9E%E9%87%8F%E5%90%8C%E6%AD%A5-%E4%BA%8C/
 */
object FromKafkaSinkHdfs {

  val env = StreamExecutionEnvironment.getExecutionEnvironment


  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
  env.getCheckpointConfig.setCheckpointTimeout(30 * 1000)


  val config = env.getCheckpointConfig
  config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "flink11")
    val topic = "canal_user_behavior"

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
            consumer.setStartFromEarliest()

    val sourceStream = env
      .addSource(consumer)
      .setParallelism(4)
      .name("source_kafka_bill")
      .uid("source_kafka_bill")

    sourceStream.print()


    val rollingPolicy:RollingPolicy[String,String] = DefaultRollingPolicy.builder()
      .withRolloverInterval(10L) //滚动写入新文件的时间，默认60s。根据具体情况调节
      .withInactivityInterval( 10L) //默认60秒,未写入数据处于不活跃状态超时会滚动新文件
      .withMaxPartSize( 12L) //设置每个文件的最大大小 ,默认是128M，这里设置为128M
      .build()

    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path("file:///Users/Zach/test/"), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(rollingPolicy)
      .withBucketCheckInterval(100)
//      .withBucketAssigner(new EventTimeBucketAssigner)
      .build()

    sourceStream.addSink(sink)

    env.execute()
  }

}



