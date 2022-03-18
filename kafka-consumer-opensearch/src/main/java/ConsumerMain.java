import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConsumerMain {

    private static final Logger log = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String[] args) {
        String host = "", openSearchHost = "", openSearchPort = "", kafkaHost = "", kafkaPort = "", kafkaTopic = "";

        try (InputStream input = OpenSearchClient.class.getResourceAsStream("consumer.properties")) {
            Properties properties = new Properties();
            properties.load(input);
            host = properties.getProperty("host");
            openSearchPort = properties.getProperty("opensearch.port");
            openSearchHost = "http://" + host + ":" + openSearchPort;
            kafkaPort = properties.getProperty("kafka.port");
            kafkaHost = host + ":" + kafkaPort;
            kafkaTopic = properties.getProperty("kafka.topic");
        } catch (IOException e) {
            log.error("Error while reading properties", e);
        }

        try (KafkaWikimediaConsumer consumer = new KafkaWikimediaConsumer(kafkaHost, kafkaTopic)) {
            OpenSearchClient client = new OpenSearchClient(openSearchHost);
            LogToOpenSearchService service = new LogToOpenSearchService(consumer, client);
            service.send();
        } catch (IOException e) {
            log.error("Error while sending messages to OpenSearch", e);
        }

    }

}
