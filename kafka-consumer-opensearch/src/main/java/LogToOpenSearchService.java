import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class LogToOpenSearchService {

    private static final Logger log = LoggerFactory.getLogger(LogToOpenSearchService.class);

    private final KafkaWikimediaConsumer kafkaWikimediaConsumer;
    private final OpenSearchClient openSearchClient;

    public LogToOpenSearchService(KafkaWikimediaConsumer kafkaWikimediaConsumer, OpenSearchClient openSearchClient) {
        this.kafkaWikimediaConsumer = kafkaWikimediaConsumer;
        this.openSearchClient = openSearchClient;
    }

    public void send() throws IOException {
        while (true) {
            ConsumerRecords<String, String> records = kafkaWikimediaConsumer.getConsumer().poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> r : records) {
                try {
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(r.value(), XContentType.JSON);
                    System.out.println(r.value());
                    IndexResponse indexResponse = openSearchClient.getClient().index(indexRequest, RequestOptions.DEFAULT);
                    log.info(indexResponse.getId());
                } catch (Exception ignored) {}
            }
        }
    }

}
