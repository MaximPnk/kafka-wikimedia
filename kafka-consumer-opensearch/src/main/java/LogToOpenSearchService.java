import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
//                String id = r.topic() + "_" + r.partition() + "_" + r.offset();
                String id = extractId(r.value());
                try {
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(r.value(), XContentType.JSON).id(id);
                    IndexResponse indexResponse = openSearchClient.getClient().index(indexRequest, RequestOptions.DEFAULT);
                    log.info(indexResponse.getId());
                } catch (Exception e) {
                    log.error("Fail to send log to elastic", e);
                }
            }
        }
    }

    private String extractId(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

}
