package org.rabbit.streaming.kafka

import org.rabbit.streaming.getEnv
import org.rabbit.streaming.getFlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._

object ConsumerDemo {

  def main(args: Array[String]): Unit = {

    val env = getEnv()

    val topic = "t1"
    val consumer = getFlinkKafkaConsumer(topic)
    consumer.setStartFromEarliest()

    /**
     * 给带状态的算子设定算子ID（通过调用uid()方法）是个好习惯，能够保证Flink应用从保存点重启时能够正确恢复状态现场。
     * 为了尽量稳妥，Flink官方也建议为每个算子都显式地设定ID，
     * 参考：https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/savepoints.html#should-i-assign-ids-to-all-operators-in-my-job
     */
    env
      .addSource(consumer)
      .setParallelism(8)
      .name("source_kafka")
      .uid("source_kafka")
      .print()

    env.execute()

  }
}
