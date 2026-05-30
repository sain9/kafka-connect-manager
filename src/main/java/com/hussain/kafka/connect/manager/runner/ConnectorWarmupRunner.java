package com.hussain.kafka.connect.manager.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import com.hussain.kafka.connect.manager.service.ConnectorService;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.InputStream;

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

        System.out.println("\n========== CONNECTOR WARMUP STARTED ==========\n");

        PathMatchingResourcePatternResolver resolver =
                new PathMatchingResourcePatternResolver();

        Resource[] resources =
                resolver.getResources("classpath:connector-configs/*.json");

        for (Resource resource : resources) {

            String fileName = resource.getFilename();

            if (fileName == null) continue;

            String connectorName =
                    fileName.replace(".json", "");

            try {

                // STEP 1: check if connector exists
                if (connectorService.connectorExists(connectorName)) {

                    System.out.println(
                            "SKIPPED (already exists): " + connectorName
                    );
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

                System.out.println(
                        "CREATED: " + connectorName
                );

            } catch (Exception e) {

                System.out.println(
                        "FAILED: " + connectorName
                );

                e.printStackTrace();
            }
        }

        System.out.println("\n========== CONNECTOR WARMUP COMPLETED ==========\n");
    }
}