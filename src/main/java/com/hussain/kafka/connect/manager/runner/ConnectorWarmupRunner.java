package com.hussain.kafka.connect.manager.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hussain.kafka.connect.manager.client.KafkaConnectClient;
import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import com.hussain.kafka.connect.manager.service.ConnectorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
@Component
public class ConnectorWarmupRunner implements ApplicationRunner {

    private final ConnectorService connectorService;
    private final KafkaConnectClient kafkaConnectClient;
    private final ObjectMapper objectMapper;

    @Value("${connector.cleanup.on.startup:false}")
    private boolean cleanupOnStartup;

    @Value("${connector.cleanup.skip.patterns:.*-internal$,.*-system$}")
    private String skipPatterns;

    @Value("${connector.cleanup.delay.ms:500}")
    private long cleanupDelayMs;

    public ConnectorWarmupRunner(
            ConnectorService connectorService,
            KafkaConnectClient kafkaConnectClient,
            ObjectMapper objectMapper
    ) {
        this.connectorService = connectorService;
        this.kafkaConnectClient = kafkaConnectClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        log.info("\n========== CONNECTOR WARMUP STARTED ==========\n");

        // Step 1: Clean up existing connectors if enabled
        if (cleanupOnStartup) {
            cleanupExistingConnectors();
        } else {
            log.info("Connector cleanup on startup is DISABLED");
        }

        // Step 2: Get existing connectors after cleanup
        List<String> existingConnectors = connectorService.getAllConnectors();
        log.info("Existing connectors after cleanup: {} \n", existingConnectors);

        // Step 3: Register connectors from config directory
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("classpath:connector-configs/*.json");

        List<String> registeredConnectors = new ArrayList<>();

        for (Resource resource : resources) {
            String fileName = resource.getFilename();
            if (fileName == null) continue;

            String connectorName = fileName.replace(".json", "");

            try {
                // Check if connector already exists
                if (existingConnectors != null && existingConnectors.contains(connectorName)) {
                    log.info("SKIPPED (already exists): {}", connectorName);
                    continue;
                }

                // Load JSON and create connector
                InputStream inputStream = resource.getInputStream();
                ConnectorRequest request = objectMapper.readValue(inputStream, ConnectorRequest.class);
                connectorService.createConnector(request);
                registeredConnectors.add(connectorName);
                log.info("CREATED: {}", connectorName);

            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                    log.info("SKIPPED (already exists): {}", connectorName);
                } else {
                    log.error("FAILED: {}", connectorName, e);
                }
            }
        }

        log.info("\n========== CONNECTOR WARMUP COMPLETED ==========");
        log.info("   Total connectors registered: {}", registeredConnectors.size());
        log.info("   Registered: {}", registeredConnectors);
        log.info("================================================\n");
    }

    private void cleanupExistingConnectors() {
        log.info("========== CLEANUP STARTED ==========");
        log.info("Cleaning up existing connectors...");

        try {
            // Get all existing connectors
            List<String> existingConnectors = connectorService.getAllConnectors();

            if (existingConnectors == null || existingConnectors.isEmpty()) {
                log.info("No existing connectors found to clean up");
                return;
            }

            log.info("Found {} existing connectors: {}", existingConnectors.size(), existingConnectors);

            // Compile skip patterns
            List<Pattern> skipPatternList = new ArrayList<>();
            for (String pattern : skipPatterns.split(",")) {
                skipPatternList.add(Pattern.compile(pattern.trim()));
            }

            // Delete connectors one by one
            for (String connectorName : existingConnectors) {
                // Check if this connector should be skipped
                boolean shouldSkip = false;
                for (Pattern pattern : skipPatternList) {
                    if (pattern.matcher(connectorName).matches()) {
                        log.info("SKIPPING (protected pattern '{}'): {}", pattern.pattern(), connectorName);
                        shouldSkip = true;
                        break;
                    }
                }

                if (shouldSkip) {
                    continue;
                }

                try {
                    log.info("Deleting connector: {}", connectorName);
                    kafkaConnectClient.deleteConnector(connectorName);
                    log.info("✅ Deleted: {}", connectorName);

                    // Add delay between deletions to avoid overwhelming Kafka Connect
                    if (cleanupDelayMs > 0) {
                        Thread.sleep(cleanupDelayMs);
                    }
                } catch (Exception e) {
                    log.warn("Failed to delete connector '{}': {}", connectorName, e.getMessage());
                }
            }

            log.info("========== CLEANUP COMPLETED ==========\n");

        } catch (Exception e) {
            log.error("Error during cleanup: {}", e.getMessage(), e);
        }
    }
}