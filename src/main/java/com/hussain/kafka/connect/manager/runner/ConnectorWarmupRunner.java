package com.hussain.kafka.connect.manager.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import com.hussain.kafka.connect.manager.service.ConnectorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.List;

@Slf4j
@Component
public class ConnectorWarmupRunner implements ApplicationRunner {

    private final ConnectorService connectorService;
    private final ObjectMapper objectMapper;

    public ConnectorWarmupRunner(
            ConnectorService connectorService,
            ObjectMapper objectMapper
    ) {
        this.connectorService = connectorService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        log.info("\n========== CONNECTOR WARMUP STARTED ==========\n");

        PathMatchingResourcePatternResolver resolver =
                new PathMatchingResourcePatternResolver();

        Resource[] resources =
                resolver.getResources("classpath:connector-configs/*.json");

        // Get all existing connectors once to avoid multiple API calls
        List<String> existingConnectors = null;
        try {
            existingConnectors = connectorService.getAllConnectors();
            log.info("Found {} existing connectors in Kafka Connect",
                    existingConnectors != null ? existingConnectors.size() : 0);
        } catch (Exception e) {
            log.warn("Could not fetch existing connectors: {}", e.getMessage());
        }

        for (Resource resource : resources) {

            String fileName = resource.getFilename();

            if (fileName == null) continue;

            String connectorName =
                    fileName.replace(".json", "");

            try {

                // STEP 1: check if connector exists using cached list
                if (existingConnectors != null && existingConnectors.contains(connectorName)) {
                    log.info("SKIPPED (already exists in Kafka Connect): {}", connectorName);
                    continue;
                }

                // STEP 2: load JSON
                InputStream inputStream = resource.getInputStream();

                ConnectorRequest request =
                        objectMapper.readValue(
                                inputStream,
                                ConnectorRequest.class
                        );

                // STEP 3: create connector
                connectorService.createConnector(request);

                log.info("CREATED: {}", connectorName);

            } catch (Exception e) {

                // Check if the error is because connector already exists
                if (e.getMessage() != null &&
                        (e.getMessage().contains("already exists") ||
                                e.getMessage().contains("409 Conflict"))) {
                    log.info("SKIPPED (already exists): {}", connectorName);
                } else {
                    log.error("FAILED: {}", connectorName, e);
                }
            }
        }

        log.info("\n\n========== CONNECTOR WARMUP COMPLETED ==========\n");
    }
}