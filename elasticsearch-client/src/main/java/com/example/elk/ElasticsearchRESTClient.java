package com.example.elk;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ElasticsearchRESTClient {

	public RestHighLevelClient getRestClient(){
		RestHighLevelClient client = new RestHighLevelClient(
		        RestClient.builder(
		                new HttpHost(ELKConstant.SERVER_HOST, 9200, "http")));
		return client;
	}
	
	public static Consumer < SearchHit > hitConsumer = ( hit ) -> {
		System.out.println(hit.getSourceAsString());
	};
	
	public static void main(String[] args) throws Exception{
		//tryTermsQuery();
		tryAggregation();
		
	}
	
	public static void tryTermsQuery() throws Exception{
		try (RestHighLevelClient client = new ElasticsearchRESTClient().getRestClient()){
			
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
			sourceBuilder.query(QueryBuilders.termQuery("trackingID", "build_002")); 
			sourceBuilder.from(0); 
			sourceBuilder.size(5); 
			sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
			
			SearchRequest searchRequest = new SearchRequest();
			searchRequest.indices("indy");
			searchRequest.source(sourceBuilder);
			
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			SearchHits hits = searchResponse.getHits();
			
			hits.forEach(hitConsumer);
			
		}catch(Exception e) {
			
		}
	}
	
	public static void tryAggregation() throws Exception{
		try (RestHighLevelClient client = new ElasticsearchRESTClient().getRestClient()){
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
			sourceBuilder.query(QueryBuilders.termQuery("extra.trackingId", "build_002"));
			sourceBuilder.size(0);
			TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_eventType")
			        .field("eventType");
			sourceBuilder.aggregation(aggregation);
			
			SearchRequest searchRequest = new SearchRequest();
			searchRequest.indices("audit");
			searchRequest.source(sourceBuilder);
			
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			SearchHits hits = searchResponse.getHits();
			
			// Retrieving Aggregations
			Aggregations aggregations = searchResponse.getAggregations();
			Terms byTypeAggregation = aggregations.get("by_eventType");
			
			byTypeAggregation.getBuckets().forEach( bucket -> {
				System.out.println(bucket.getKeyAsString() + "|" + bucket.getDocCount());
			});
			
			hits.forEach(hitConsumer);
		}
	}

}
