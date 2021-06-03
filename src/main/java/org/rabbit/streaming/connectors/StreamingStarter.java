package org.rabbit.streaming.connectors;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rabbit.models.Record;

public class StreamingStarter {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Record> recordDataStreamSource = executionEnvironment.addSource(new ElasticsearchSourceFunction());
        DataStreamSource<Record> recordDataStreamSource = executionEnvironment.addSource(new ElasticsearchSourceScrollFunction());
        recordDataStreamSource.print();

        executionEnvironment.execute("source es");
    }
}
