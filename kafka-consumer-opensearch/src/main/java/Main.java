import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        String openSearchHost = "";

        try (InputStream input = OpenSearchClient.class.getResourceAsStream("opensearch.properties")) {
            Properties properties = new Properties();
            properties.load(input);
            openSearchHost = "http://" + properties.getProperty("opensearch.host");
        } catch (IOException e) {
            log.error("Error while reading props", e);
        }

        OpenSearchClient client = new OpenSearchClient(openSearchHost);
        client.createIndex();
    }

}
