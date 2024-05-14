package org.example.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class WikimediaConsumer {

    public static final String INDEX_WIKIMEDIA = "wikimedia";
    private static final Logger logger = LoggerFactory.getLogger(WikimediaConsumer.class);

    public static void main(String[] args) throws IOException {
        try (RestHighLevelClient openSearchClient = createOpenSearchClient()) {

            createIndexIfExists(openSearchClient);
            IndexRequest indexRequest = new IndexRequest(INDEX_WIKIMEDIA)
                    .source("lastName", "Gibbins")
                    .id("2");

            IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                logger.info("Successfully written document 1");
            } else {
                logger.warn("Something went wrong while writing document 1: {}", indexResponse.getResult());
            }
        }
    }

    private static RestHighLevelClient createOpenSearchClient() {
        String connString = "https://ceq0jowaxd:2weem0iwy7@wikimedia-9115593284.ap-southeast-2.bonsaisearch.net:443";
        // we build a URI from the connection string
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
