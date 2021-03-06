package org.rabbit.streaming.kafka

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.rabbit.models.{BehaviorData, ResultInfo}
import org.rabbit.streaming.agg.MultiDimensionalAggregate

/**
 * https://cloud.tencent.com/developer/article/1536148
 */
object FromKafkaSinkRedis {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  /**
   * 由于大屏的最大诉求是实时性，等待迟到数据显然不太现实，因此我们采用处理时间作为时间特征，并以1分钟的频率做checkpointing。
   */
  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE)
  env.getCheckpointConfig.setCheckpointTimeout(30 * 1000)

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "screen_flink")
    val topic = "t_user_behavior_1"

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
//        consumer.setStartFromEarliest()
//    consumer.setCommitOffsetsOnCheckpoints(true)
    consumer.setStartFromLatest()


    /**
     * 给带状态的算子设定算子ID（通过调用uid()方法）是个好习惯，能够保证Flink应用从保存点重启时能够正确恢复状态现场。
     * 为了尽量稳妥，Flink官方也建议为每个算子都显式地设定ID，
     * 参考：https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/savepoints.html#should-i-assign-ids-to-all-operators-in-my-job
     */
    import org.apache.flink.streaming.api.scala._
    val sourceStream = env
      .addSource(consumer)
      .setParallelism(4)
      .name("source_kafka_bill")
      .uid("source_kafka_bill")

//    sourceStream.print()

    val billStream = sourceStream.map(message => JSON.parseObject(message, classOf[BehaviorData]))
      //      .map(data => (data.uid, data.time, data.phoneType, data.clickCount))
      .name("map_sub_bill").uid("map_sub_bill")

    /**
     * 将子订单流按uid分组，开1天的滚动窗口(按照自然日来统计指标)，注意处理时间的时区问题
     * 并同时设定ContinuousProcessingTimeTrigger触发器，以1秒周期触发计算(以1秒的刷新频率呈现在大屏上)。
     */
    val uidDayWindowStream = billStream
      //      .keyBy(0)
      .keyBy("uid", "phoneType")
      .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
      //            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//      .trigger(ContinuousProcessingTimeTrigger.of(Time.minutes(1)));
    .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));

    val uidPhoneAgg = uidDayWindowStream.aggregate(new MultiDimensionalAggregate)
      .name("aggregate_uid_phoneType_count").uid("aggregate_uid_phoneType_count")

    uidPhoneAgg.print()

    val conf = new FlinkJedisPoolConfig.Builder().setHost("cdh2").build()
    uidPhoneAgg.addSink(new RedisSink[ResultInfo](conf, new PojoRedisMapper))
      .name("sink_redis_user_phone_sum").uid("sink_redis_user_phone_sum")
      .setParallelism(1)


    env.execute()

  }

}

class PojoRedisMapper extends RedisMapper[ResultInfo] {
  private val serialVersionUID = 1L
  val HASH_NAME_PREFIX = "DASHBOARD:USER:PHONE:CNT"

  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, HASH_NAME_PREFIX)
  }

  override def getKeyFromData(data: ResultInfo): String = data.uid + "_" + data.phone_type

  override def getValueFromData(data: ResultInfo): String = String.valueOf(data.cnt)
}

