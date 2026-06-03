package com.hussain.kafka.connect.manager.connectorsGenerator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hussain.kafka.connect.manager.client.KafkaConnectClient;
import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConnectorRegistrar {

    private final KafkaConnectClient kafkaConnectClient;
    private final ObjectMapper mapper = new ObjectMapper();

    public List<String> getExistingConnectors() {
        try {
            List<String> connectors = kafkaConnectClient.getAllConnectors();
            return connectors != null ? connectors : Collections.emptyList();
        } catch (Exception e) {
            log.warn("Could not fetch existing connectors: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public void registerConnector(String connectorJson) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> connectorMap = mapper.readValue(connectorJson, Map.class);
        String connectorName = (String) connectorMap.get("name");

        log.info("Registering connector: {}", connectorName);

        // Check if already exists
        List<String> existingConnectors = getExistingConnectors();

        if (existingConnectors.contains(connectorName)) {
            log.info("Connector '{}' already exists, skipping registration", connectorName);
            return;
        }

        // Convert to ConnectorRequest and register
        ConnectorRequest request = mapper.readValue(connectorJson, ConnectorRequest.class);

        try {
            kafkaConnectClient.createConnector(request);
            log.info("✅ Connector '{}' registered successfully", connectorName);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("409")) {
                log.info("Connector '{}' already exists (conflict), skipping", connectorName);
            } else {
                log.error("Failed to register connector '{}': {}", connectorName, e.getMessage());
                throw e;
            }
        }
    }

    public boolean connectorExists(String connectorName) {
        List<String> existingConnectors = getExistingConnectors();
        return existingConnectors.contains(connectorName);
    }
}