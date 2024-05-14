package org.example.producer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;

public class WikimediaProducer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaProducer.class.getName());
    public static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    public static final String TOPIC_NAME = "wikimedia.recentchange";

    public static void main(String[] args) {
        Properties props = getProperties();

        try (EventSource eventSource = new EventSource.Builder(URI.create(WIKIMEDIA_URL)).build()) {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                for (MessageEvent event : eventSource.messages()) {
                    String message = String.format("Got %s: %s", event.getEventName(), event.getData());

                    producer.send(new ProducerRecord<>(TOPIC_NAME, event.getData()));
                    logger.info(message);
                }
            }
        }

    }

    private static @NotNull Properties getProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(20));
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        return props;
    }
}
