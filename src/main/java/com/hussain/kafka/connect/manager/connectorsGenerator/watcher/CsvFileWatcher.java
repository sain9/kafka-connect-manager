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

        // Verify it's a regular file
        if (!Files.isRegularFile(csvFilePath) || Files.isDirectory(csvFilePath)) {
            log.error("Not a regular file: {}", fileName);
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

        // Step 5: MOVE CSV to input directory (not copy)
        log.info("Step 5: Moving CSV to input directory...");
        Path inputPath = Paths.get(inputDirectory);
        Files.createDirectories(inputPath);

        Path targetPath = inputPath.resolve(fileName);

        // MOVE the file (cut and paste) from upload to input
        Files.move(csvFilePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        log.info("✅ CSV moved to: {}", targetPath);

        // Verify the moved file is valid
        if (Files.exists(targetPath) && Files.isRegularFile(targetPath) && Files.size(targetPath) > 0) {
            log.info("✅ File verified at: {} (size: {} bytes)", targetPath, Files.size(targetPath));
        } else {
            log.error("❌ File move failed!");
            return;
        }

        log.info("========================================");
        log.info("✅ SUCCESS! CSV moved to input directory");
        log.info("   Connector: {}-source", baseName);
        log.info("   Topic: {}-topic", baseName);
        log.info("   Config saved: src/main/resources/connector-configs/{}-source.json", baseName);
        log.info("   CSV location: {}", targetPath);
        log.info("========================================");
        log.info("   Kafka Connect will now:");
        log.info("   1. Read the CSV from input directory");
        log.info("   2. Publish messages to {}-topic", baseName);
        log.info("   3. Move the file to /tmp/finished when complete");
        log.info("========================================");
    }
}