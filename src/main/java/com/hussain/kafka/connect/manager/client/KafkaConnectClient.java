package com.hussain.kafka.connect.manager.client;

import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;

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

        ResponseEntity<List> response =
                restTemplate.getForEntity(url, List.class);

        return response.getBody();
    }

    /**
     * Create connector in Kafka Connect
     */
    public String createConnector(ConnectorRequest request) {

        String url = kafkaConnectUrl + "/connectors";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<ConnectorRequest> entity =
                new HttpEntity<>(request, headers);

        ResponseEntity<String> response =
                restTemplate.postForEntity(url, entity, String.class);

        return response.getBody();
    }
}