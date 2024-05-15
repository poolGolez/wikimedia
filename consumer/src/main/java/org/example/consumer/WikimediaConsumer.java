package org.example.consumer;

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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class WikimediaConsumer {

    public static final String INDEX_WIKIMEDIA = "wikimedia";
    private static final Logger logger = LoggerFactory.getLogger(WikimediaConsumer.class);
    public static final String TOPIC_WIKIMEDIA = "wikimedia.recentchange";

    public static void main(String[] args) throws IOException {


        try (RestHighLevelClient openSearchClient = createOpenSearchClient()) {

            createIndexIfExists(openSearchClient);

            try (KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer()) {


                Thread mainThread = Thread.currentThread();
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        super.run();
                        kafkaConsumer.wakeup();
                        try {
                            mainThread.join();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

                kafkaConsumer.subscribe(Collections.singletonList(TOPIC_WIKIMEDIA));

                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(5000));

                    BulkRequest bulkRequest = new BulkRequest();

                    for (ConsumerRecord<String, String> record : records) {
                        IndexRequest indexRequest = createIndexRequest(record);
                        bulkRequest.add(indexRequest);
                    }

                    if (bulkRequest.numberOfActions() > 0) {
                        BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        if (bulkResponse.hasFailures()) {
                            logger.error("Error on batch request: {}", bulkResponse.buildFailureMessage());
                        } else {
                            logger.info("Wrote {} records for {} seconds", bulkResponse.getItems().length, bulkResponse.getTook().seconds());
                        }
                    }
                }
            } catch (WakeupException e) {
                logger.info("Shutting down consumer");
            } catch (Exception e) {
                logger.error("Unexpected exception: {}", e.getMessage());
            }
        }
    }

    private static IndexRequest createIndexRequest(ConsumerRecord<String, String> record) {
        JsonObject recordData = (JsonObject) JsonParser.parseString(record.value());
        String documentId = recordData.getAsJsonObject("meta").get("id").getAsString();
        JsonObject indexData = new JsonObject();
        for (String key : List.of("id", "title", "timestamp")) {
            JsonElement node = recordData.get(key);
            if (node != null) {
                indexData.addProperty(key, node.getAsString());
            }
        }

        IndexRequest indexRequest = new IndexRequest(INDEX_WIKIMEDIA)
                .source(record.value(), XContentType.JSON)
                .id(documentId);
        return indexRequest;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:19092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "opensearch-local-sink-autocommit");

        return new KafkaConsumer<>(props);
    }

    private static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        URI connUri = URI.create(connString);

        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            return new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            return new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }
    }

    private static void createIndexIfExists(RestHighLevelClient openSearchClient) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX_WIKIMEDIA);
        boolean exists = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (!exists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_WIKIMEDIA);
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            logger.info("Created new index: " + INDEX_WIKIMEDIA);
        } else {
            logger.debug(String.format("Index %s already exists", INDEX_WIKIMEDIA));
        }
    }

}
