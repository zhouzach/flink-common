package org.rabbit.streaming;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.rabbit.models.DayInfo;

import java.time.Duration;
import java.util.Properties;

public class AccumulativeForDayByFlink {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
         env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
         env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092");
        properties.setProperty("group.id", "flink");

        String topic = "day";
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);

        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        flinkKafkaConsumer.setStartFromLatest();

        DataStreamSource<String> sourceStream =  env.addSource(flinkKafkaConsumer);
        sourceStream.setParallelism(3).name("source_kafka").uid("source_kafka");

        SingleOutputStreamOperator<DayInfo> mapStream = sourceStream.map(new MapFunction<String, DayInfo>() {
            public DayInfo map(String value) throws Exception {
                return JSONObject.parseObject(value,DayInfo.class);
            }
        });

        mapStream.setParallelism(3).name("map_data").uid("map_data");

        SingleOutputStreamOperator<DayInfo> timeStream =mapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DayInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withIdleness(Duration.ofSeconds(5))).setParallelism(3)
                .name("watermark_data").uid("watermark_data");

        timeStream.keyBy(DayInfo::getCategory)
                .window(TumblingProcessingTimeWindows.of(Time.days(1),Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new AccumulativeForDayAggregate(),new DayResultProcessWindowFunction());


    }

    private static class AccumulativeForDayAggregate implements AggregateFunction<DayInfo, DayResult, DayResult> {
        @Override
        public DayResult createAccumulator() {
            return new DayResult("",0L);
        }

        @Override
        public DayResult add(DayInfo value, DayResult accumulator) {
            return new DayResult(accumulator.getCategory(), accumulator.getCnt()+value.getValue());
        }

        @Override
        public DayResult getResult(DayResult accumulator) {
            return accumulator;
        }

        @Override
        public DayResult merge(DayResult accumulator1, DayResult accumulator2) {
            return new DayResult(accumulator1.getCategory(), accumulator1.getCnt()+ accumulator2.getCnt());
        }
    }

    public static class DayResult{
        private String category;
        private Long cnt;

        public DayResult(String category, Long cnt) {
            this.category = category;
            this.cnt = cnt;
        }

        public String getCategory() {
            return category;
        }

        public Long getCnt() {
            return cnt;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public void setCnt(Long cnt) {
            this.cnt = cnt;
        }
    }

    public static class WindowDayResult{
        private Long startWindow;
        private Long endWindow;
        private String category;
        private Long cnt;

        public WindowDayResult(Long startWindow, Long endWindow, String category, Long cnt) {
            this.startWindow = startWindow;
            this.endWindow = endWindow;
            this.category = category;
            this.cnt = cnt;
        }
    }

    private static class DayResultProcessWindowFunction
            extends ProcessWindowFunction<DayResult, WindowDayResult, String, TimeWindow> {

        public void process(String key,
                            Context context,
                            Iterable<DayResult> elements,
                            Collector<WindowDayResult> out) {
            long startWindow = context.window().getStart();
            long endWindow = context.window().getEnd();
            DayResult dayResult = elements.iterator().next();
            out.collect(new WindowDayResult(startWindow, endWindow,dayResult.getCategory(), dayResult.getCnt()));
        }
    }
}
