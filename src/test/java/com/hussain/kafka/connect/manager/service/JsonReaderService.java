package com.hussain.kafka.connect.manager.service;

import com.hussain.kafka.connect.manager.dto.ConnectorRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;

@Service
public class JsonReaderService {

    private static final String CONFIG_PATH =
            "connector-configs/";

    private final ObjectMapper objectMapper =
            new ObjectMapper();

    public ConnectorRequest readJson(
            String fileName
    ) {

        try {

            File file = new File(
                    CONFIG_PATH + fileName
            );

            return objectMapper.readValue(
                    file,
                    ConnectorRequest.class
            );

        } catch (IOException e) {

            throw new RuntimeException(
                    "Failed to read json file : "
                            + fileName
            );
        }
    }
}