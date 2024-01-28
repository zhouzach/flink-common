//package org.rabbit.streaming.phoenix
//
//import java.time.Duration
//import java.util.Properties
//
//import com.alibaba.fastjson.JSON
//import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.connector.jdbc.JdbcOutputFormat
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
//import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableSchema}
//import org.rabbit.connectors.phoenix.PhoenixJDBCRetractStreamTableSink
//import org.rabbit.models.UserData
//
////https://blog.csdn.net/u012551524/article/details/106842532
//object FromKafkaSinkPhoenixByRetractStreamTableSink {
//
//  def main(args: Array[String]): Unit = {
//
//    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
//    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    //    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
//    //    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
//    //需要设置IDEA Environment variables: HADOOP_USER_NAME=hdfs
//        streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))
//
//    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
//    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
//    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5))
//    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(900))
//
//
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
//    properties.setProperty("group.id", "screen_flink")
//    val topic = "t_room"
//
//    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
//    //        consumer.setStartFromEarliest()
//    //    consumer.setCommitOffsetsOnCheckpoints(true)
//    consumer.setStartFromLatest()
//
//    import org.apache.flink.streaming.api.scala._
//    val sourceStream = streamExecutionEnv
//      .addSource(consumer)
//      .setParallelism(1) //设置并行度1，方便测试打印
//      .name("source_kafka")
//      .uid("source_kafka")
//
//        sourceStream.print()
//
//
//    val dataStream = sourceStream.map(message => {
//      try{
//        JSON.parseObject(message, classOf[UserData])
//      }catch {
//        case t: Throwable => {
//          println(s"read json failed: ${t.toString}")
//          UserData(0L,"",0,0L)
//        }
//      }
//    })
//      .name("map_pojo").uid("map_pojo")
//    dataStream.print()
//
//    val timedData = dataStream
//      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[UserData](Duration.ofSeconds(3))//延迟3秒的固定延迟水印
//        .withTimestampAssigner(new SerializableTimestampAssigner[UserData] { //从数据中抽取eventtime字段
//          override def extractTimestamp(element: UserData, recordTimestamp: Long): Long = element.created_time
//        })
//        .withIdleness(Duration.ofMinutes(1))) //超时时间内没有记录到达时将一个流标记为空闲,这样就意味着下游的数据不需要等待水印的到来
//      .name("assign_timestamp_watermark").uid("assign_timestamp_watermark")
//    timedData.print()
//
//
//    val tableSche:TableSchema = TableSchema.builder()
//      .field("id",DataTypes.BIGINT().notNull())
//      .field("sex",DataTypes.STRING())
//      .field("age",DataTypes.INT())
//      .field("created_time",DataTypes.BIGINT())
//      .build()
//
//    val jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
//      .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
//      .setDBUrl("jdbc:phoenix:cdh1,cdh2,cdh3:2181:/hbase;autocommit=true")
//      .setQuery("upsert into user_flink values(?,?,?,?)")
//      .setBatchSize(1)
//      .finish()
//
//    val sink = new PhoenixJDBCRetractStreamTableSink(tableSche, jdbcOutputFormat)
//    streamTableEnv.registerTableSink("phoenix_table",sink)
//
//    import org.apache.flink.table.api._
//    val resTable = streamTableEnv.fromDataStream( timedData, $"uid",$"sex",$"age",$"created_time")
//    resTable.executeInsert("phoenix_table")
//
//  }
//
//}
