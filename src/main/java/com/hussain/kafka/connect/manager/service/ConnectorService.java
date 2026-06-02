package com.hussain.kafka.connect.manager.service;

import com.hussain.kafka.connect.manager.client.KafkaConnectClient;
import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class ConnectorService {

    private final KafkaConnectClient kafkaConnectClient;

    public ConnectorService(KafkaConnectClient kafkaConnectClient) {
        this.kafkaConnectClient = kafkaConnectClient;
    }

    /**
     * Get all connectors from Kafka Connect
     */
    public List<String> getAllConnectors() {
        try {
            return kafkaConnectClient.getAllConnectors();
        } catch (Exception e) {
            log.warn("Failed to get connectors: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Create connector
     */
    public String createConnector(ConnectorRequest request) {
        try {
            return kafkaConnectClient.createConnector(request);
        } catch (Exception e) {
            // Log but don't rethrow for duplicate connectors
            if (e.getMessage() != null &&
                    (e.getMessage().contains("already exists") ||
                            e.getMessage().contains("409"))) {
                log.info("Connector '{}' already exists, skipping", request.getName());
                return null;
            }
            throw e;
        }
    }

    /**
     * Check if connector already exists
     */
    public boolean connectorExists(String name) {
        List<String> connectors = getAllConnectors();
        return connectors != null && connectors.contains(name);
    }
}