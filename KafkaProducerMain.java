package io.conductor.Kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.conductor.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaProducerMain {

    private final static Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());
    private static final String TOPIC_NAME = "udemy-courses";
    private final static String API_URL = "http://localhost:5000/stream/type1";
    private static final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    public static void main(String[] args) {
        log.info("Kafka Producer ");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown triggered. Closing producer!");
            isShuttingDown.set(true); // Mark the shutdown state

        }));

        try {

                List<JsonNode> dataList = getData();

                if (dataList.isEmpty()) {
                    log.warn("No data received from API");
                } else {
                    for (JsonNode eventData : dataList) {

                        if (isShuttingDown.get()) {
                            log.info("Shutdown!!!!Stopped processing!!");
                            break;
                        }

                        String key = eventData.get("id").asText();
                        String value = eventData.toString();

                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, key, value);

                        producer.send(producerRecord, (metadata, exception) -> {
                            if (exception == null) {
                                log.info("Sent message: Key={}, Partition={}, Offset={}", key, metadata.partition(), metadata.offset());
                            } else {
                                log.error("Error while sending message", exception);
                            }
                        });
                    }
                    log.info("All data processed. Sending 'close' message to Kafka.");
                    ProducerRecord<String, String> closeRecord = new ProducerRecord<>(TOPIC_NAME, "close", "close");
                    producer.send(closeRecord, (metadata, exception) -> {
                        if (exception == null) {
                            log.info("Sent 'close' message: Partition={}, Offset={}", metadata.partition(), metadata.offset());
                        } else {
                            log.error("Error while sending 'close' message", exception);
                        }
                    });
                }

        } catch (Exception e) {
            log.error("Error in Kafka Producer", e);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static List<JsonNode> getData() throws Exception {
        log.info("Fetching from API...");
        HttpURLConnection connection = (HttpURLConnection) new URL(API_URL).openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "text/event-stream"); // For SSE streams

        ObjectMapper mapper = new ObjectMapper();
        List<JsonNode> dataList = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line;
            StringBuilder eventData = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("data:")) {
                    eventData.append(line.substring(5).trim());
                } else if (line.isEmpty() && eventData.length() > 0) {
                    String rawData = eventData.toString();
                    eventData.setLength(0);
                    dataList.add(mapper.readTree(rawData));
                }
            }
        }

        return dataList;
    }


}
