package com.hussain.kafka.connect.manager.connectorsGenerator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hussain.kafka.connect.manager.client.KafkaConnectClient;
import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConnectorRegistrar {

    private final KafkaConnectClient kafkaConnectClient;
    private final ObjectMapper mapper = new ObjectMapper();

    public void registerConnector(String connectorJson) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> connectorMap = mapper.readValue(connectorJson, Map.class);
        String connectorName = (String) connectorMap.get("name");

        log.info("Registering connector: {}", connectorName);

        // Check if already exists
        try {
            List<String> existingConnectors = kafkaConnectClient.getAllConnectors();

            if (existingConnectors != null && existingConnectors.contains(connectorName)) {
                log.info("Connector '{}' already exists, skipping registration", connectorName);
                return;
            }
        } catch (Exception e) {
            log.warn("Could not check existing connectors: {}", e.getMessage());
        }

        // Convert to ConnectorRequest and register
        ConnectorRequest request = mapper.readValue(connectorJson, ConnectorRequest.class);

        try {
            kafkaConnectClient.createConnector(request);
            log.info("✅ Connector '{}' registered successfully", connectorName);
        } catch (Exception e) {
            log.error("Failed to register connector '{}': {}", connectorName, e.getMessage());
            throw e;
        }
    }
}