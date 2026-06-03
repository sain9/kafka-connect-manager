package com.hussain.kafka.connect.manager.connectorsGenerator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
@Service
public class ConnectorConfigGenerator {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectMapper prettyMapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    @Value("${kafka.connect.input.path:/tmp/input}")
    private String inputPath;

    @Value("${kafka.connect.finished.path:/tmp/finished}")
    private String finishedPath;

    @Value("${kafka.connect.error.path:/tmp/error}")
    private String errorPath;

    @Value("${connector.configs.save.path:src/main/resources/connector-configs}")
    private String connectorConfigsPath;

    public String generateConnectorConfig(String fileName, Map<String, String> fieldTypes) throws Exception {
        String baseName = fileName.replaceFirst("\\.csv$", "");
        String connectorName = baseName + "-source";
        String topicName = baseName + "-topic";
        String schemaName = toPascalCase(baseName);

        log.info("Generating connector config for: {}", connectorName);
        log.info("Detected field types: {}", fieldTypes);

        // Generate key schema with EMPTY fieldSchemas (like orders-source)
        Map<String, Object> keySchema = new LinkedHashMap<>();
        keySchema.put("name", schemaName);
        keySchema.put("type", "STRUCT");
        keySchema.put("isOptional", true);
        keySchema.put("fieldSchemas", new LinkedHashMap<>()); // Empty fieldSchemas

        // Generate value schema for all columns
        Map<String, Object> valueFieldSchemas = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
            String columnName = entry.getKey();
            String columnType = convertToKafkaType(entry.getValue());

            Map<String, Object> fieldSchema = new LinkedHashMap<>();
            fieldSchema.put("type", columnType);
            fieldSchema.put("isOptional", true);
            valueFieldSchemas.put(columnName, fieldSchema);
        }

        Map<String, Object> valueSchema = new LinkedHashMap<>();
        valueSchema.put("name", schemaName);
        valueSchema.put("type", "STRUCT");
        valueSchema.put("isOptional", false);
        valueSchema.put("fieldSchemas", valueFieldSchemas);

        // Convert schemas to JSON strings WITHOUT pretty printing (single line)
        String keySchemaJson = mapper.writeValueAsString(keySchema);
        String valueSchemaJson = mapper.writeValueAsString(valueSchema);

        log.info("Generated key schema: {}", keySchemaJson);
        log.info("Generated value schema: {}", valueSchemaJson);

        // Build connector config
        Map<String, String> config = new LinkedHashMap<>();
        config.put("connector.class", "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector");
        config.put("tasks.max", "1");
        config.put("topic", topicName);
        config.put("input.path", inputPath);
        config.put("finished.path", finishedPath);
        config.put("error.path", errorPath);
        config.put("input.file.pattern", baseName + ".*\\.csv");
        config.put("csv.first.row.as.header", "true");
        config.put("schema.generation.enabled", "false");
        config.put("key.schema", keySchemaJson);
        config.put("value.schema", valueSchemaJson);

        Map<String, Object> connector = new LinkedHashMap<>();
        connector.put("name", connectorName);
        connector.put("config", config);

        // For display/saving, use pretty printed JSON
        String connectorJsonPretty = prettyMapper.writerWithDefaultPrettyPrinter().writeValueAsString(connector);
        String connectorJsonCompact = mapper.writeValueAsString(connector);

        // Save the connector config to file (pretty printed for readability)
        saveConnectorConfigToFile(baseName, connectorJsonPretty);

        // Return compact JSON for API call (no extra spaces/newlines)
        return connectorJsonCompact;
    }

    private String convertToKafkaType(String detectedType) {
        switch (detectedType) {
            case "INT32":
                return "INT32";
            case "INT64":
                return "INT64";
            case "FLOAT64":
                return "FLOAT64";
            case "BOOLEAN":
                return "BOOLEAN";
            case "STRING":
            default:
                return "STRING";
        }
    }

    private void saveConnectorConfigToFile(String baseName, String connectorJson) {
        try {
            // Create directory if it doesn't exist
            Path configDir = Paths.get(connectorConfigsPath);
            Files.createDirectories(configDir);

            // Save to main directory (overwrite if exists)
            Path mainConfigFile = configDir.resolve(baseName + "-source.json");
            Files.writeString(mainConfigFile, connectorJson);
            log.info("✅ Connector config saved to: {}", mainConfigFile);

        } catch (IOException e) {
            log.warn("Failed to save connector config to file: {}", e.getMessage());
        }
    }

    private String toPascalCase(String value) {
        String[] parts = value.split("[_\\-\\s]+");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (part != null && !part.isBlank()) {
                sb.append(Character.toUpperCase(part.charAt(0)));
                if (part.length() > 1) {
                    sb.append(part.substring(1).toLowerCase());
                }
            }
        }
        return sb.toString();
    }
}