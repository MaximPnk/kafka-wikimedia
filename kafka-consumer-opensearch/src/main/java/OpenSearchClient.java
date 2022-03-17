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

public class OpenSearchClient {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchClient.class);

    private final RestHighLevelClient client;

    public OpenSearchClient(String host) throws IOException {
        URI uri = URI.create(host);
        String userInfo = uri.getUserInfo();

        if (userInfo == null) {
            client = new RestHighLevelClient(RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), "http")));
        } else {
            String[] auth = userInfo.split(":");
            CredentialsProvider provider = new BasicCredentialsProvider();
            provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
            client = new RestHighLevelClient(RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()))
                    .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(provider)
                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        createIndex();
    }

    private void createIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("wikimedia");

        if (!client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("The wikimedia index has been created");
        } else {
            log.info("The wikimedia index already exists");
        }
    }

    public RestHighLevelClient getClient() {
        return client;
    }

}
