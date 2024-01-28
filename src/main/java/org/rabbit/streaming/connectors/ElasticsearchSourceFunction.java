package org.rabbit.streaming.connectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.rabbit.models.Record1;

import java.io.IOException;


public class ElasticsearchSourceFunction extends RichSourceFunction<Record1> {
    RestHighLevelClient client;

    @Override
    public void open(Configuration parameters) throws Exception {

        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http"),
                        new HttpHost("localhost", 9202, "http")));
    }

    @Override
    public void run(SourceContext<Record1> context) throws Exception {

        MultiSearchRequest request = new MultiSearchRequest();
        SearchRequest firstSearchRequest = new SearchRequest("log3");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
//        searchSourceBuilder.sort(new FieldSortBuilder("name").order(SortOrder.ASC));
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "a")
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10));

        firstSearchRequest.source(searchSourceBuilder);
        firstSearchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        request.add(firstSearchRequest);

//        SearchRequest secondSearchRequest = new SearchRequest("log3");
//        searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "b"));
//        secondSearchRequest.source(searchSourceBuilder);
//        request.add(secondSearchRequest);

        MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);


        for ( MultiSearchResponse.Item item : response){
            if(item.getFailure() != null) {
                item.getFailure().printStackTrace();
            } else {
                SearchHit[] results = item.getResponse().getHits().getHits();

                for (SearchHit hit : results) {
                    String sourceAsString = hit.getSourceAsString();
                    if (sourceAsString != null) {
                        ObjectMapper mapper = new ObjectMapper();
                        Record1 obj = mapper.readValue(sourceAsString, Record1.class);
                        context.collect(obj);
                    }
                }
            }
        }

    }

    public void cancel(){

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}