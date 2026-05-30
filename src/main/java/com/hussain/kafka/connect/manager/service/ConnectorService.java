package com.hussain.kafka.connect.manager.service;

import com.hussain.kafka.connect.manager.client.KafkaConnectClient;
import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ConnectorService {

    private final KafkaConnectClient kafkaConnectClient;

    public ConnectorService(KafkaConnectClient kafkaConnectClient) {
        this.kafkaConnectClient = kafkaConnectClient;
    }

    /**
     * Create connector
     */
    public String createConnector(ConnectorRequest request) {
        return kafkaConnectClient.createConnector(request);
    }

    /**
     * Check if connector already exists
     */
    public boolean connectorExists(String name) {

        List<String> connectors =
                kafkaConnectClient.getAllConnectors();

        return connectors != null &&
                connectors.contains(name);
    }
}