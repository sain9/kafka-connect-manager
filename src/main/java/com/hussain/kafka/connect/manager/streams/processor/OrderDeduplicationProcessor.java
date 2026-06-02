package com.hussain.kafka.connect.manager.streams.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hussain.kafka.connect.manager.streams.model.Order;
import com.hussain.kafka.connect.manager.streams.transformer.DeduplicationTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
//@Component
public class OrderDeduplicationProcessor {

    @Value("${kafka.streams.topics.orders.source:orders-topic}")
    private String sourceTopic;

    @Value("${kafka.streams.topics.orders.cleaned:orders-cleaned-final}")
    private String cleanedTopic;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Autowired
    private KafkaStreamsConfiguration kafkaStreamsConfiguration;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private KafkaStreams kafkaStreams;
    private final AtomicBoolean started = new AtomicBoolean(false);

    @PostConstruct
    public void init() {
        // Start pipeline in background with retry
        new Thread(() -> {
            try {
                waitForTopic(sourceTopic);
                buildAndStartStreams();
                started.set(true);
                log.info("✅ Order Deduplication Pipeline started successfully");
            } catch (Exception e) {
                log.error("Failed to start pipeline after retries", e);
            }
        }).start();
    }

    private void waitForTopic(String topic) {
        int maxRetries = 20;
        int retryCount = 0;
        int delaySeconds = 5;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        log.info("Waiting for topic '{}' to be created...", topic);

        while (retryCount < maxRetries) {
            try (AdminClient admin = AdminClient.create(props)) {
                var topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);

                if (topics.contains(topic)) {
                    log.info("✅ Topic '{}' found after {} attempts", topic, retryCount + 1);
                    return;
                }

                retryCount++;
                log.info("Topic '{}' not found, retry {}/{}. Waiting {} seconds...",
                        topic, retryCount, maxRetries, delaySeconds);

                TimeUnit.SECONDS.sleep(delaySeconds);

            } catch (Exception e) {
                retryCount++;
                log.warn("Error checking topic existence (attempt {}/{}): {}",
                        retryCount, maxRetries, e.getMessage());

                try {
                    TimeUnit.SECONDS.sleep(delaySeconds);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.warn("Topic '{}' not found after {} retries. Starting pipeline anyway...",
                topic, maxRetries);
    }

    private void buildAndStartStreams() {
        try {
            // Build topology
            Topology topology = buildTopology();

            // Get streams properties from configuration
            Properties props = kafkaStreamsConfiguration.asProperties();

            // Create and start KafkaStreams
            kafkaStreams = new KafkaStreams(topology, props);

            // Set exception handler
            kafkaStreams.setUncaughtExceptionHandler((thread, exception) -> {
                log.error("Uncaught exception in Kafka Streams", exception);
                if (exception instanceof org.apache.kafka.streams.errors.MissingSourceTopicException) {
                    log.warn("Source topic missing, will retry...");
                    retryStart();
                }
            });

            // Start the streams
            kafkaStreams.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (kafkaStreams != null) {
                    kafkaStreams.close();
                    log.info("Kafka Streams closed");
                }
            }));

            log.info("✅ Kafka Streams started successfully");

        } catch (Exception e) {
            log.error("Failed to build and start streams", e);
            retryStart();
        }
    }

    private void retryStart() {
        try {
            Thread.sleep(30000);
            log.info("Retrying to start Kafka Streams...");
            buildAndStartStreams();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        log.info("Building Order Deduplication Pipeline");
        log.info("Source: {}, Cleaned: {}", sourceTopic, cleanedTopic);

        String storeName = "order-id-store";

        // Create state store
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(storeName),
                        Serdes.String(),
                        Serdes.Long()
                );
        streamsBuilder.addStateStore(storeBuilder);

        // Read from source topic
        KStream<String, String> stream = streamsBuilder.stream(
                sourceTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Process: Parse JSON -> Deduplicate -> Send to cleaned topic
        stream
                .filter((k, v) -> v != null && !v.isEmpty())
                .mapValues(value -> {
                    try {
                        return objectMapper.readValue(value, Order.class);
                    } catch (Exception e) {
                        log.error("Failed to parse: {}", value);
                        return null;
                    }
                })
                .filter((k, v) -> v != null)
                .transform(() -> new DeduplicationTransformer(storeName), storeName)
                .filter((k, v) -> v != null)
                .mapValues(order -> {
                    try {
                        return objectMapper.writeValueAsString(order);
                    } catch (Exception e) {
                        log.error("Failed to convert to JSON", e);
                        return null;
                    }
                })
                .to(cleanedTopic, Produced.with(Serdes.String(), Serdes.String()));

        log.info("✅ Topology built successfully");

        return streamsBuilder.build();
    }
}