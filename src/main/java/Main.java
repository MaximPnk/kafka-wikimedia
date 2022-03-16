import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(WikimediaProducer.class);
    private static String url = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) {

        String bootstrapServer = "", topic = "";

        try (InputStream input = WikimediaProducer.class.getResourceAsStream("kafka.properties")) {
            Properties properties = new Properties();
            properties.load(input);
            bootstrapServer = properties.getProperty("kafka.host");
            topic = properties.getProperty("kafka.topic");
        } catch (IOException e) {
            log.error("Error while reading props", e);
        }

        WikimediaProducer producer = new WikimediaProducer(bootstrapServer, topic);
        EventHandler handler = new WikimediaHandler(producer.getProducer(), topic);
        EventSource source = new EventSource.Builder(handler, URI.create(url)).build();

        source.start();

        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
