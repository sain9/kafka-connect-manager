package com.hussain.kafka.connect.manager.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;

@Service
public class JsonReaderService {

    private final ObjectMapper objectMapper =
            new ObjectMapper();

    public ConnectorRequest readJson(
            String fileName
    ) {

        try {

            ClassPathResource resource =
                    new ClassPathResource(
                            "connector-configs/" + fileName
                    );

            InputStream inputStream =
                    resource.getInputStream();

            return objectMapper.readValue(
                    inputStream,
                    ConnectorRequest.class
            );

        } catch (IOException e) {

            throw new RuntimeException(
                    "Failed to read JSON file: "
                            + fileName,
                    e
            );
        }
    }
}