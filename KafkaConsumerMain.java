package io.conductor.Kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerMain {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerMain.class.getSimpleName());
    private static final String TOPIC_NAME = "udemy-courses";
    private static final String GROUP_ID = "udemy_group";
    private static final String CSV_FILE_PATH = "udemy_courses.csv";
    private static final String SMALLER_CSV_FILE_PATH = "output/smaller_dataset.csv";
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static final AtomicBoolean running = new AtomicBoolean(true);


    public static void main(String[] args) {
        log.info("Starting Kafka Consumer");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        ObjectMapper mapper = new ObjectMapper();

        List<JsonNode> exportData = new ArrayList<>();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down gracefully...");
            consumer.wakeup();
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }));

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    if ("close".equalsIgnoreCase(key) || "close".equalsIgnoreCase(value)) {
                        log.info("Received shutdown signal from producer. Closing consumer!!!");
                        running.set(false);
                        break; // Exit the loop
                    }
                    try {
                        JsonNode jsonData = mapper.readTree(value);

                        exportData.add(jsonData);


                    } catch (Exception e) {
                        log.error("Failed to parse message value as JSON", e);
                    }
                }

                if (!exportData.isEmpty() && exportData.size() % 10 == 0) {
                    exportData(exportData, CSV_FILE_PATH);
                    if (!exportData.isEmpty() && exportData.size() % 10 == 0) {
                        exportData(exportData, CSV_FILE_PATH);
                        exportSmallerSets(exportData, SMALLER_CSV_FILE_PATH, 100);
                        exportData.clear();
                    }
                    exportData.clear();
                }
            }
        } catch (Exception e) {
            log.error("Unexpected error in Kafka consumer", e);
        } finally {
            consumer.close();
        }
    }

    private static String sanitizeField(String field) {
        if (field == null || field.isEmpty()) {
            return "N/A";
        }
        return field.replace("\"", "\"\"")
                .replace("\n", " ")
                .replace("\r", " ");
    }

    private static void exportData(List<JsonNode> dataList, String filePath) {
        log.info("Exporting data to CSV...");

        boolean writeHeaders = !Files.exists(Paths.get(filePath));

        try (PrintWriter writer = new PrintWriter(new FileWriter(filePath, true))) {
            if (writeHeaders) {
                writer.println("ID,Title,URL,Is Paid,Instructor Names,Category,Headline,Num Subscribers,Rating,Num Reviews,Instructional Level,Objectives,Curriculum");
            }

            // Write data rows
            for (JsonNode data : dataList) {
                String[] row = new String[]{
                        sanitizeField(data.has("id") ? data.get("id").asText() : "N/A"),
                        sanitizeField(data.has("title") ? data.get("title").asText() : "N/A"),
                        sanitizeField(data.has("url") ? data.get("url").asText() : "N/A"),
                        sanitizeField(data.has("is_paid") ? data.get("is_paid").asText() : "N/A"),
                        sanitizeField(data.has("instructor_names") ? data.get("instructor_names").asText() : "N/A"),
                        sanitizeField(data.has("category") ? data.get("category").asText() : "N/A"),
                        sanitizeField(data.has("headline") ? data.get("headline").asText() : "N/A"),
                        sanitizeField(data.has("num_subscribers") ? data.get("num_subscribers").asText() : "N/A"),
                        sanitizeField(data.has("rating") ? data.get("rating").asText() : "N/A"),
                        sanitizeField(data.has("num_reviews") ? data.get("num_reviews").asText() : "N/A"),
                        sanitizeField(data.has("instructional_level") ? data.get("instructional_level").asText() : "N/A"),
                        sanitizeField(data.has("objectives") ? data.get("objectives").asText() : "N/A"),
                        sanitizeField(data.has("curriculum") ? data.get("curriculum").asText() : "N/A")
                };

                if (row.length != 13) {
                    log.warn("Skipping invalid row with incorrect column count: {}", (Object) row);
                    continue;
                }

                writer.printf("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"%n",
                        (Object[]) row);
            }

            log.info("Data exported to CSV file: {}", filePath);
        } catch (Exception e) {
            log.error("Error writing data to CSV file", e);
        }
    }


    private static void exportSmallerSets(List<JsonNode> dataList, String filePath, int chunkSize) {
        log.info("Exporting smaller sets of data to CSV...");

        int chunkCounter = 0;
        int totalChunks = (int) Math.ceil((double) dataList.size() / chunkSize);

        try {
            for (int i = 0; i < totalChunks; i++) {
                int startIdx = i * chunkSize;
                int endIdx = Math.min(startIdx + chunkSize, dataList.size());

                String chunkFilePath = filePath.replace(".csv", "_chunk" + chunkCounter + ".csv");
                try (PrintWriter chunkWriter = new PrintWriter(new FileWriter(chunkFilePath))) {
                    chunkWriter.println("ID,Title,URL,Is Paid,Instructor Names,Category,Headline,Num Subscribers,Rating,Num Reviews,Instructional Level,Objectives,Curriculum");

                    for (int j = startIdx; j < endIdx; j++) {
                        JsonNode data = dataList.get(j);

                        String[] row = new String[]{
                                sanitizeField(data.has("id") ? data.get("id").asText() : "N/A"),
                                sanitizeField(data.has("title") ? data.get("title").asText() : "N/A"),
                                sanitizeField(data.has("url") ? data.get("url").asText() : "N/A"),
                                sanitizeField(data.has("is_paid") ? data.get("is_paid").asText() : "N/A"),
                                sanitizeField(data.has("instructor_names") ? data.get("instructor_names").asText() : "N/A"),
                                sanitizeField(data.has("category") ? data.get("category").asText() : "N/A"),
                                sanitizeField(data.has("headline") ? data.get("headline").asText() : "N/A"),
                                sanitizeField(data.has("num_subscribers") ? data.get("num_subscribers").asText() : "N/A"),
                                sanitizeField(data.has("rating") ? data.get("rating").asText() : "N/A"),
                                sanitizeField(data.has("num_reviews") ? data.get("num_reviews").asText() : "N/A"),
                                sanitizeField(data.has("instructional_level") ? data.get("instructional_level").asText() : "N/A"),
                                sanitizeField(data.has("objectives") ? data.get("objectives").asText() : "N/A"),
                                sanitizeField(data.has("curriculum") ? data.get("curriculum").asText() : "N/A")
                        };

                        if (row.length != 13) {
                            log.warn("Skipping invalid row with incorrect column count: {}", (Object) row);
                            continue;
                        }

                        chunkWriter.printf("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"%n",
                                (Object[]) row);
                    }

                    log.info("Exported chunk {} to file: {}", chunkCounter, chunkFilePath);
                    chunkCounter++;
                }
            }

            log.info("Exported all smaller datasets successfully.");
        } catch (Exception e) {
            log.error("Error exporting smaller datasets", e);
        }
    }


}

