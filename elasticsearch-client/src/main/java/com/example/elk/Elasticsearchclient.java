package com.example.elk;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Elasticsearchclient {

	public static void main( String[] args ) throws Exception
    {

        Settings settings = Settings.builder().put( "cluster.name", "docker-cluster" ).build();
        Client client = new PreBuiltTransportClient( settings ).addTransportAddress(
                        new TransportAddress( InetAddress.getByName( ELKConstant.SERVER_HOST ), 9300 ) );

        GetResponse getResponse = client.prepareGet( "indy", "tracking_summary", "1" ).get();

        System.out.println( getResponse.toString() );

        String trackingID = (String) getResponse.getSourceAsMap().get( "trackingID" );

        System.out.println( trackingID );

        //SearchResponse response = client.prepareSearch().execute().actionGet();

        SearchResponse response = client.prepareSearch( "indy" )
                                        .setSearchType( SearchType.DFS_QUERY_THEN_FETCH )
                                        .setQuery( QueryBuilders.termQuery( "trackingID", "build_002" ) )
                                        .get();

        List<SearchHit> searchHits = Arrays.asList( response.getHits().getHits() );
        System.out.println( searchHits.size() );

        searchHits.forEach( hit -> {

           System.out.println(hit.getSourceAsString());

        } );


        SearchResponse response2 = client.prepareSearch( "audit" )
                                         .setSearchType( SearchType.DFS_QUERY_THEN_FETCH )
                                         .setQuery( QueryBuilders.constantScoreQuery(
                                                         QueryBuilders.termQuery( "extra.trackingId", "build_002" ) ) )
                                         .addAggregation( AggregationBuilders.count( "doc_count" )
                                                                             .field( "extra.trackingId" ) )
                                         //.setSize( 0 )
                                         .get();

        List<SearchHit> searchHits2 = Arrays.asList( response2.getHits().getHits() );
        System.out.println( searchHits2.size() );
        searchHits2.forEach( hit -> {
            System.out.println( hit.getSourceAsString() );
        } );

        System.out.println( "" + response2.getAggregations().get( "doc_count" ) );
    }

}
