package org.rabbit.streaming.phoenix

import java.time.Duration
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.JdbcOutputFormat
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableSchema}
import org.apache.flink.types.Row
import org.rabbit.models.UserData

//https://blog.csdn.net/u012551524/article/details/90484124
//https://www.cnblogs.com/029zz010buct/p/10486104.html
object FromKafkaSinkPhoenixByJdbcOutputFormat {

  def main(args: Array[String]): Unit = {

    val streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamExecutionEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    streamExecutionEnv.enableCheckpointing(20 * 1000, CheckpointingMode.EXACTLY_ONCE)
    //    streamExecutionEnv.getCheckpointConfig.setCheckpointTimeout(900 * 1000)
    //需要设置IDEA Environment variables: HADOOP_USER_NAME=hdfs
        streamExecutionEnv.setStateBackend(new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoints"))

    val blinkEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamExecutionEnv, blinkEnvSettings)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5))
    streamTableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(900))


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092")
    properties.setProperty("group.id", "screen_flink")
    val topic = "t_room"

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    //        consumer.setStartFromEarliest()
    //    consumer.setCommitOffsetsOnCheckpoints(true)
    consumer.setStartFromLatest()

    import org.apache.flink.streaming.api.scala._
    val sourceStream = streamExecutionEnv
      .addSource(consumer)
      .setParallelism(1) //设置并行度1，方便测试打印
      .name("source_kafka")
      .uid("source_kafka")

        sourceStream.print()


    val dataStream = sourceStream.map(message => {
      try{
        JSON.parseObject(message, classOf[UserData])
      }catch {
        case t: Throwable => {
          println(s"read json failed: ${t.toString}")
          UserData(0L,"",0,0L)
        }
      }
    })
      .name("map_pojo").uid("map_pojo")
    dataStream.print()

    val timedData = dataStream
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[UserData](Duration.ofSeconds(3))//延迟3秒的固定延迟水印
        .withTimestampAssigner(new SerializableTimestampAssigner[UserData] { //从数据中抽取eventtime字段
          override def extractTimestamp(element: UserData, recordTimestamp: Long): Long = element.created_time
        })
        .withIdleness(Duration.ofMinutes(1))) //超时时间内没有记录到达时将一个流标记为空闲,这样就意味着下游的数据不需要等待水印的到来
      .name("assign_timestamp_watermark").uid("assign_timestamp_watermark")
    timedData.print()
////    timedData
////      .addSink(JdbcSink.sink(
////        "insert into user_flink (uid, sex,age,created_time) values (?,?,?,?)",
////        new JdbcStatementBuilder[UserData]() {
////          override def accept(ps: PreparedStatement, u: UserData): Unit = {
////            ps.setLong(1, u.uid)
////            ps.setString(2, u.sex)
////            ps.setInt(3, u.age)
////            ps.setLong(4, u.created_time)
////          }
////        },
////        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
////          .withUrl(s"${DevDbConfig.url}")
////          .withDriverName("com.mysql.jdbc.Driver")
////          .withUsername(s"${DevDbConfig.user}")
////          .withPassword(s"${DevDbConfig.password}")
////          .build()))
////        .name("sink_jdbc").uid("sink_jdbc")
//
////    val options = JDBCOptions.builder()
////      .setDBUrl("jdbc:phoenix:192.168.10.16:2181:/hbase-unsecure;autocommit=true")
////      .setDriverName("org.apache.phoenix.jdbc.PhoenixDriver")
////      .setTableName("DWR_ACTION_LOG")
////      .build
//
    val tableSche:TableSchema = TableSchema.builder()
      .field("id",DataTypes.BIGINT().notNull())
      .field("sex",DataTypes.STRING())
      .field("age",DataTypes.INT())
      .field("created_time",DataTypes.BIGINT())
      .build()


    val jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
      .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
      .setDBUrl("jdbc:phoenix:cdh1,cdh2,cdh3:2181:/hbase;autocommit=true")
      .setQuery("upsert into user_flink values(?,?,?,?)")
//      .setSqlTypes(Array(
//        Types.INTEGER,
//        Types.VARCHAR,
//        Types.VARCHAR,
//        Types.DOUBLE,
//        Types.INTEGER))
      .setBatchSize(1)
      .finish()

    //JDBCOutputFormat只能处理Row类型
    val rowStream = timedData.map(td => {
      val row = new Row(4)
      row.setField(0,td.uid)
      row.setField(1,td.sex)
      row.setField(2,td.age)
      row.setField(3,td.created_time)
      row
    }).name("row_stream").uid("row_stream")
    rowStream.print()

      rowStream.writeUsingOutputFormat(jdbcOutputFormat).name("write_output").uid("write_output")
//
////    val outputformat = JDBCUpsertOutputFormat.builder()
////      .setOptions(options)
////      .setFieldNames(Array[String]("id","CONTENT","NAME","CREATE_DATE","WRITE_DATE"))
////      .setFieldTypes(Array[Int](java.sql.Types.VARCHAR,java.sql.Types.VARCHAR,java.sql.Types.VARCHAR,java.sql.Types.TIMESTAMP,java.sql.Types.TIMESTAMP))
////      .setMaxRetryTimes(5)
////      .setFlushMaxSize(1000)
////      .setFlushIntervalMills(1000)
////      .setKeyFields(Array[String]("id"))
////      .build()
//
//    val sink = new PhoenixJDBCRetractStreamTableSink(tableSche, jdbcOutputFormat)

////    val resData = timedData.map(d => {
////      val row = new Row(5)
////      row.setField(0, d.uid)
////      row.setField(1, d.sex)
////      row.setField(2, d.age)
////      row.setField(3, d.created_time)
////      new tuple.Tuple2(lang.Boolean.TRUE,row)})
////    sink.consumeDataStream(resData)
//
//    streamTableEnv.registerTableSink("phoenix_table",sink)
//
//    streamTableEnv.createTemporaryView("result_table", timedData, $"uid",$"sex",$"age",$"created_time")
//    val resTable = streamTableEnv.fromDataStream( timedData, $"uid",$"sex",$"age",$"created_time")
//    resTable.executeInsert("phoenix_table")

    streamExecutionEnv.execute("kafka sink phoenix")

  }

}
