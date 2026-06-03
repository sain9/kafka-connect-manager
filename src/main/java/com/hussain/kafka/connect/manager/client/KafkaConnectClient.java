package com.hussain.kafka.connect.manager.client;

import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConnectClient {

    private final RestTemplate restTemplate;

    @Value("${kafka.connect.url}")
    private String kafkaConnectUrl;

    /**
     * Get all connectors from Kafka Connect
     */
    public List<String> getAllConnectors() {
        String url = kafkaConnectUrl + "/connectors";
        ResponseEntity<List> response = restTemplate.getForEntity(url, List.class);
        return response.getBody();
    }

    /**
     * Create connector in Kafka Connect
     */
    public String createConnector(ConnectorRequest request) {
        String url = kafkaConnectUrl + "/connectors";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<ConnectorRequest> entity = new HttpEntity<>(request, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(url, entity, String.class);
        return response.getBody();
    }

    /**
     * Delete connector from Kafka Connect
     */
    public void deleteConnector(String connectorName) {
        String url = kafkaConnectUrl + "/connectors/" + connectorName;

        try {
            // Use exchange with DELETE method and proper headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<?> entity = new HttpEntity<>(headers);

            ResponseEntity<String> response = restTemplate.exchange(
                    url,
                    HttpMethod.DELETE,
                    entity,
                    String.class
            );

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("✅ Deleted connector: {}", connectorName);
            } else {
                log.warn("Failed to delete connector '{}': {}", connectorName, response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Error deleting connector '{}': {}", connectorName, e.getMessage());
            throw e;
        }
    }
}