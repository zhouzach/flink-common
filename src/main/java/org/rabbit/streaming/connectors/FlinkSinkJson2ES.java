package org.rabbit.streaming.connectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

public class FlinkSinkJson2ES {


    public static void main(String[] args) throws Exception {

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        httpHosts.add(new HttpHost("127.0.0.1", 9201, "http"));
        httpHosts.add(new HttpHost("127.0.0.1", 9202, "http"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
        String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";
        List<String> records = new ArrayList<>();
        records.add(json1);
        records.add(json2);


        SingleOutputStreamOperator<List<String>> source = env.fromElements(records).name("source").uid("source");
        SingleOutputStreamOperator<String> outputStreamOperator =
                source.flatMap(new FlatMapFunction<List<String>, String>() {
                    @Override
                    public void flatMap(List<String> value, Collector<String> out) throws Exception {
                        value.forEach(out::collect);
                    }
                });


        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        return Requests.indexRequest()
                                .index("log5")
                                .source(element, XContentType.JSON);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );


        outputStreamOperator.addSink(esSinkBuilder.build());
//        source.print();

        env.execute("app");

    }

}
