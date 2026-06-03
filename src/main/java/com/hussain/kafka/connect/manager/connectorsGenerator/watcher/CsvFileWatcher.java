package com.hussain.kafka.connect.manager.connectorsGenerator.watcher;

import com.hussain.kafka.connect.manager.connectorsGenerator.service.ConnectorConfigGenerator;
import com.hussain.kafka.connect.manager.connectorsGenerator.service.ConnectorRegistrar;
import com.hussain.kafka.connect.manager.connectorsGenerator.service.CsvSchemaDetector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class CsvFileWatcher {

    private final CsvSchemaDetector schemaDetector;
    private final ConnectorConfigGenerator configGenerator;
    private final ConnectorRegistrar connectorRegistrar;

    @Value("${csv.watch.directory:/home/hussain/CodeBase/ps-sql/kafka/data/upload}")
    private String watchDirectory;

    @Value("${csv.input.directory:/home/hussain/CodeBase/ps-sql/kafka/data/input}")
    private String inputDirectory;

    @PostConstruct
    public void startWatching() {
        Thread watcherThread = new Thread(() -> {
            try {
                Path watchPath = Paths.get(watchDirectory);
                Path inputPath = Paths.get(inputDirectory);

                // Create directories if they don't exist
                Files.createDirectories(watchPath);
                Files.createDirectories(inputPath);

                log.info("========================================");
                log.info("CSV File Watcher Started");
                log.info("Watching: {}", watchDirectory);
                log.info("Input directory: {}", inputDirectory);
                log.info("========================================");

                // Process any existing files first
                processExistingFiles(watchPath);

                try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
                    watchPath.register(watchService,
                            StandardWatchEventKinds.ENTRY_CREATE,
                            StandardWatchEventKinds.ENTRY_MODIFY);

                    while (true) {
                        WatchKey key = watchService.poll(5, TimeUnit.SECONDS);
                        if (key != null) {
                            for (WatchEvent<?> event : key.pollEvents()) {
                                WatchEvent.Kind<?> kind = event.kind();

                                if (kind == StandardWatchEventKinds.OVERFLOW) {
                                    continue;
                                }

                                Path filename = (Path) event.context();
                                String fileName = filename.toString();

                                if (fileName.toLowerCase().endsWith(".csv")) {
                                    Path fullPath = watchPath.resolve(filename);

                                    // Wait for file to be completely written
                                    TimeUnit.SECONDS.sleep(2);

                                    // Verify it's a file, not a directory
                                    if (Files.isRegularFile(fullPath) && !Files.isDirectory(fullPath)) {
                                        log.info("📄 New CSV detected: {}", fileName);

                                        try {
                                            processCsvFile(fullPath);
                                        } catch (Exception e) {
                                            log.error("Failed to process: {}", fileName, e);
                                        }
                                    } else {
                                        log.debug("Skipping non-file entry: {}", fileName);
                                    }
                                }
                            }
                            key.reset();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("File watcher failed", e);
            }
        });
        watcherThread.setDaemon(true);
        watcherThread.start();
    }

    private void processExistingFiles(Path watchPath) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(watchPath, "*.csv")) {
            for (Path filePath : stream) {
                if (Files.isRegularFile(filePath) && !Files.isDirectory(filePath)) {
                    log.info("📄 Processing existing file: {}", filePath.getFileName());
                    try {
                        processCsvFile(filePath);
                    } catch (Exception e) {
                        log.error("Failed to process existing file: {}", filePath.getFileName(), e);
                    }
                }
            }
        }
    }

    private void processCsvFile(Path csvFilePath) throws Exception {
        String fileName = csvFilePath.getFileName().toString();
        String baseName = fileName.replaceFirst("\\.csv$", "");

        log.info("========================================");
        log.info("Processing: {}", fileName);
        log.info("========================================");

        if (!Files.isRegularFile(csvFilePath) || Files.isDirectory(csvFilePath)) {
            log.error("Not a regular file: {}", fileName);
            return;
        }

        // Get existing connectors
        List<String> existingConnectors = connectorRegistrar.getExistingConnectors();

        log.info("Existing connectors: {}", existingConnectors);

        // Check if we should create a connector for this CSV
        if (!shouldCreateConnector(baseName, existingConnectors)) {
            // Find which existing connector this file belongs to
            String targetConnector = findTargetConnector(baseName, existingConnectors);
            String targetTopic = targetConnector.replace("-source", "-topic");

            log.info("⏭️ Skipping connector creation for '{}' - similar connector already exists", fileName);
            log.info("   📌 This CSV data will be processed by existing connector: {}", targetConnector);
            log.info("   📌 Data will be published to Kafka topic: {}", targetTopic);
            log.info("   Moving CSV to input directory for existing connector to process...");

            // Still move the CSV to input directory for existing connector to process
            Path inputPath = Paths.get(inputDirectory);
            Files.createDirectories(inputPath);
            Path targetPath = inputPath.resolve(fileName);
            Files.move(csvFilePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            log.info("✅ CSV moved to input directory: {}", targetPath);
            return;
        }

        // Step 1: Detect schema
        log.info("Step 1: Detecting schema...");
        File csvFile = csvFilePath.toFile();
        Map<String, String> schema = schemaDetector.detectSchema(csvFile);
        log.info("✅ Schema detected with {} columns", schema.size());
        schema.forEach((col, type) -> log.info("   - {}: {}", col, type));

        // Step 2: Generate and save connector config
        log.info("Step 2: Generating and saving connector config...");
        String connectorConfig = configGenerator.generateConnectorConfig(fileName, schema);
        log.info("✅ Connector config generated and saved");

        // Step 3: Register connector
        log.info("Step 3: Registering connector in Kafka Connect...");
        connectorRegistrar.registerConnector(connectorConfig);
        log.info("✅ Connector registered");

        // Step 4: Wait for connector to be fully initialized
        log.info("Step 4: Waiting 15 seconds for connector to be fully ready...");
        TimeUnit.SECONDS.sleep(15);

        // Step 5: MOVE CSV to input directory
        log.info("Step 5: Moving CSV to input directory...");
        Path inputPath = Paths.get(inputDirectory);
        Files.createDirectories(inputPath);

        Path targetPath = inputPath.resolve(fileName);
        Files.move(csvFilePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        log.info("✅ CSV moved to: {}", targetPath);

        log.info("========================================");
        log.info("✅ SUCCESS! New connector created and CSV moved");
        log.info("   Connector: {}-source", baseName);
        log.info("   Topic: {}-topic", baseName);
        log.info("========================================");
    }

    private String findTargetConnector(String baseName, List<String> existingConnectors) {
        // First, check for exact match (should not happen here, but just in case)
        String exactMatch = baseName + "-source";
        if (existingConnectors.contains(exactMatch)) {
            return exactMatch;
        }

        // Find which existing connector this is a variation of
        for (String existing : existingConnectors) {
            String existingBase = existing.replace("-source", "");

            // Check if current file is variation of this existing connector
            if (isVariationOf(baseName, existingBase)) {
                return existing;
            }

            // Also check reverse (if existing is variation of current)
            if (isVariationOf(existingBase, baseName)) {
                return existing;
            }
        }

        // If no match found, return the base name as fallback
        return baseName + "-source";
    }

    private boolean shouldCreateConnector(String baseName, List<String> existingConnectors) {
        String connectorName = baseName + "-source";

        // If exact connector already exists, skip
        if (existingConnectors.contains(connectorName)) {
            log.info("Connector '{}' already exists, skipping", connectorName);
            return false;
        }

        // Check if this is a variation of any existing connector
        for (String existing : existingConnectors) {
            String existingBase = existing.replace("-source", "");

            if (isVariationOf(baseName, existingBase)) {
                log.info("'{}' is a variation of existing connector '{}', skipping connector creation", baseName, existingBase);
                return false;
            }

            // Also check reverse: if existing is variation of new (should not happen but just in case)
            if (isVariationOf(existingBase, baseName)) {
                log.info("Existing connector '{}' is a variation of '{}', skipping", existingBase, baseName);
                return false;
            }
        }

        log.info("No existing connector found for '{}', will create new connector", baseName);
        return true;
    }

    private boolean isVariationOf(String newName, String existingName) {
        // Exact match
        if (newName.equals(existingName)) {
            return true;
        }

        // Check: abc_2 is variation of abc
        if (newName.matches(existingName + "_\\d+$")) {
            log.debug("'{}' matches pattern '{}_\\d+'", newName, existingName);
            return true;
        }

        // Check: abc2 is variation of abc
        if (newName.matches(existingName + "\\d+$")) {
            log.debug("'{}' matches pattern '{}\\d+'", newName, existingName);
            return true;
        }

        // Check: abc_copy, abc_backup are variations
        if (newName.matches(existingName + "_(copy|backup|old|test|temp)$")) {
            log.debug("'{}' matches pattern '{}_(copy|backup|old|test|temp)'", newName, existingName);
            return true;
        }

        // Check: abc_copy_2, abc_backup_1 are variations
        if (newName.matches(existingName + "_(copy|backup|old|test|temp)_\\d+$")) {
            log.debug("'{}' matches pattern '{}_(copy|backup|old|test|temp)_\\d+'", newName, existingName);
            return true;
        }

        return false;
    }

    private String extractRootName(String name) {
        // Remove common suffixes
        return name.replaceAll("(\\d+)$", "")         // Remove trailing numbers like "2" from "felas2"
                .replaceAll("(_\\d+)$", "")        // Remove _1, _2, _3, _10
                .replaceAll("(_test)$", "")        // Remove _test
                .replaceAll("(_copy)$", "")        // Remove _copy
                .replaceAll("(_backup)$", "")      // Remove _backup
                .replaceAll("(_new)$", "")         // Remove _new
                .replaceAll("(_old)$", "")         // Remove _old
                .replaceAll("(_v\\d+)$", "")       // Remove _v1, _v2
                .replaceAll("_\\w+$", "");         // Remove any other suffix
    }
}