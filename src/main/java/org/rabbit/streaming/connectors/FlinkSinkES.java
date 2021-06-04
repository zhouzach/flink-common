package org.rabbit.streaming.connectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.rabbit.models.Record;

import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkSinkES {


    public static void main(String[] args) throws Exception {

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        httpHosts.add(new HttpHost("127.0.0.1", 9201, "http"));
        httpHosts.add(new HttpHost("127.0.0.1", 9202, "http"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Record> records = new ArrayList<>();
        records.add(new Record("a",10));
        records.add(new Record("b",20));
        SingleOutputStreamOperator<List<Record>> source = env.fromElements(records).name("source").uid("source");
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = source.flatMap(new FlatMapFunction<List<Record>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(List<Record> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                value.forEach(v -> out.collect(new Tuple2<>(v.getName(), v.getNum())));
            }
        });


        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
                    public IndexRequest createIndexRequest(Tuple2<String, Integer> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("name", element.f0);
                        json.put("num", element.f1);

                        return Requests.indexRequest()
                                .index("log3")
                                .source(json);
                    }

                    @Override
                    public void process(Tuple2<String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );


        tuple2SingleOutputStreamOperator.addSink(esSinkBuilder.build());
//        source.print();

        env.execute("app");

    }

}
