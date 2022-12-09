package org.daniel107x.kafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class OpenSearchConsumer {
    private static final String connectionString = "http://localhost:9200";
    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        // Create an open search client
        RestHighLevelClient restHighLevelClient = createOpenSearchClient();
        // Create the index in OS if doesn't exist
        try(restHighLevelClient) { // If it fails, it will close
            GetIndexRequest getIndexRequest = new GetIndexRequest("wikimedia");
            if(!restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("The wikimedia index has been created");
            }else{
                logger.warn("Wikimedia index already exists");
            }
        }

        // Create kafka clients

        // Main code logic

        // Close things
        restHighLevelClient.close();
    }

    public static RestHighLevelClient createOpenSearchClient(){
        // Build the URI from the conn string
        RestHighLevelClient restHighLevelClient;
        URI connectionURI = URI.create(connectionString);

        // Extract login information if exists
        String userInfo = connectionURI.getUserInfo();
        if(userInfo == null){
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connectionURI.getHost(), connectionURI.getPort(), connectionURI.getScheme())));
        }else{
            String[] auth = userInfo.split(":");
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connectionURI.getHost(), connectionURI.getPort(), connectionURI.getScheme()))
                    .setHttpClientConfigCallback(
                            httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                    )
            );
        }
        return restHighLevelClient;
    }
}
