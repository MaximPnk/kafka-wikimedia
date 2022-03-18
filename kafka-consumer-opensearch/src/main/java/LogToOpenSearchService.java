import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
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
            ConsumerRecords<String, String> records = kafkaWikimediaConsumer.getConsumer().poll(Duration.ofMillis(3000));
            BulkRequest bulkRequest = new BulkRequest();
            log.info("Received " + records.count() + " message(s)");
            for (ConsumerRecord<String, String> r : records) {
                String id = extractId(r.value());
                IndexRequest indexRequest = new IndexRequest("wikimedia").source(r.value(), XContentType.JSON).id(id);
                bulkRequest.add(indexRequest);
            }
            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse bulkResponse = openSearchClient.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
                log.info("Sent " + bulkResponse.getItems().length + " message(s)");
            }
            kafkaWikimediaConsumer.getConsumer().commitSync();
            log.info("Batch committed");
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
