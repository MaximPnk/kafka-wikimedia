import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaWikimediaProducer {

    private final Logger log = LoggerFactory.getLogger(KafkaWikimediaProducer.class);

    private final KafkaProducer<String, String> producer;

    public KafkaWikimediaProducer(String bootstrapServer, String topic) {
        Properties kafkaProperties = new Properties();

        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // задержка перед отправкой для группировки сообщений
        kafkaProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        // размер группы
        kafkaProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));
        // компрессия
        kafkaProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        producer = new KafkaProducer<>(kafkaProperties);
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

}
