package com.example.elk;

import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ElasticsearchRESTClient {

	public static void main(String[] args) throws Exception{
		RestHighLevelClient client = null;
		try {
			client = new RestHighLevelClient(
		        RestClient.builder(
		                new HttpHost(ELKConstant.SERVER_HOST, 9200, "http")));
		
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
			hits.forEach(hit->{
				System.out.println(hit.getSourceAsString());
			});
		}catch(Exception e) {
			
		}finally {
			if (client != null) {
				client.close();
			}
		}
	

	}

}
