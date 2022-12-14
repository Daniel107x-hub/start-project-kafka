package org.daniel107x.kafka.opensearch;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    private static final String connectionString = "http://localhost:9200";
    private static final String bootstrapServer = "127.0.0.1:9092";
    private static final String groupId = "consmer-opensearch-demo";
    private static final String index = "wikimedia";
    private static final String topic = "wikimedia.recentchange";

    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        // Create an open search client
        RestHighLevelClient restHighLevelClient = createOpenSearchClient();
        // Create kafka clients
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        // Create the index in OS if doesn't exist
        try(restHighLevelClient; consumer) { // If it fails, it will close, can have multiple dependencies
            GetIndexRequest getIndexRequest = new GetIndexRequest(index);
            if(!restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("The wikimedia index has been created");
            }else{
                logger.warn("Wikimedia index already exists");
            }
            consumer.subscribe(Collections.singleton(topic));
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                logger.info("Received: " + recordCount + " records" );

                //Sending data using bulk requests
                BulkRequest bulkRequest = new BulkRequest();


                for(ConsumerRecord<String, String> record : records){
                    //Send records into OpenSearch
                    /*
                    The consumer is not idempotent because we are not sending the IDs to OpenSearch, so we need
                    to add them
                     */

                    // Strategy 1 - Creating IDs using kafka coordinates
                    String id = record.topic() + "-" + record.partition() + "-" + record.offset();

                    try {
                        // A better strategy would be to use the id contained in the data itself
                        id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest(index)
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        bulkRequest.add(indexRequest);
//                        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
//                        logger.info(response.getId());
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }

                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Inserted: " + response.getItems().length + " records");
                    try{
                        Thread.sleep(1000);
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }

                    // Consume offsets manually after the batch is consumed
                    consumer.commitSync();
                    logger.info("Offsets have been committed");
                }
            }
        }

        // Main code logic

        // Close things
//        restHighLevelClient.close();
    }

    private static String extractId(String jsonString){
        return JsonParser.parseString(jsonString).getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){
        Properties properties =new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //Manually commit offsets
        return new KafkaConsumer<>(properties);
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
