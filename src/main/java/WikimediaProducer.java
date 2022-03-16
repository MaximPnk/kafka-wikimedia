import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WikimediaProducer {

    private final Logger log = LoggerFactory.getLogger(WikimediaProducer.class);

    private String topic;
    private KafkaProducer<String, String> producer;

    public WikimediaProducer(String bootstrapServer, String topic) {
        Properties kafkaProperties = new Properties();

        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(kafkaProperties);
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

}
