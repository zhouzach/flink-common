package org.rabbit.streaming.connectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.rabbit.models.Record;

import java.io.IOException;


public class ElasticsearchSourceScrollFunction extends RichSourceFunction<Record> {
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
    public void run(SourceContext<Record> context) throws Exception {

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest("log3");
        searchRequest.scroll(scroll);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
//        searchSourceBuilder.sort(new FieldSortBuilder("name").order(SortOrder.ASC));
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "a")
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10));

        searchRequest.source(searchSourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        if(searchHits != null){
            System.out.println("batch 1 searchHits length: " + searchHits.length);
        } else {
            System.out.println("batch 1 searchHits length: 0");
        }

        for (SearchHit hit : searchHits) {
            String sourceAsString = hit.getSourceAsString();
            if (sourceAsString != null) {
                ObjectMapper mapper = new ObjectMapper();
                Record obj = mapper.readValue(sourceAsString, Record.class);
                context.collect(obj);
            }
        }

        int batch = 2;
        String scrollId = searchResponse.getScrollId();
        while (searchHits != null && searchHits.length > 0) {

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            searchHits = searchResponse.getHits().getHits();

            System.out.println("batch "+ batch +" searchHits.length: " + searchHits.length);
            for (SearchHit hit : searchHits) {
                String sourceAsString = hit.getSourceAsString();
                if (sourceAsString != null) {
                    ObjectMapper mapper = new ObjectMapper();
                    Record obj = mapper.readValue(sourceAsString, Record.class);
                    System.out.println(sourceAsString);
                    context.collect(obj);
                }
            }

            batch = batch +1;
            scrollId = searchResponse.getScrollId();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        System.out.println("release the search context: " + succeeded);

    }

    public void cancel() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}