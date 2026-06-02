package com.hussain.kafka.connect.manager.connectorsGenerator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

@Slf4j
@Service
public class CsvSchemaDetector {

 public Map<String, String> detectSchema(File csvFile) throws Exception {
  if (!csvFile.isFile()) {
   throw new IllegalArgumentException("Not a regular file: " + csvFile.getName());
  }

  log.info("Detecting schema from: {}", csvFile.getName());

  Map<String, String> fieldTypes = new LinkedHashMap<>();

  try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
   // Read header
   String headerLine = reader.readLine();
   if (headerLine == null || headerLine.trim().isEmpty()) {
    throw new RuntimeException("CSV file is empty");
   }

   String[] headers = headerLine.split(",");
   if (headers.length == 0) {
    throw new RuntimeException("No headers found in CSV");
   }

   // Read sample rows for type detection
   List<String[]> sampleRows = new ArrayList<>();
   String line;
   int rowCount = 0;
   while ((line = reader.readLine()) != null && rowCount < 100) {
    if (!line.trim().isEmpty()) {
     sampleRows.add(line.split(","));
     rowCount++;
    }
   }

   // Detect type for each column
   for (int i = 0; i < headers.length; i++) {
    String columnName = headers[i].trim();
    String detectedType = null;
    boolean isAllNull = true;

    for (String[] row : sampleRows) {
     if (i < row.length) {
      String value = row[i].trim();
      if (!value.isEmpty()) {
       isAllNull = false;
       String type = inferType(value);
       if (detectedType == null) {
        detectedType = type;
       } else {
        detectedType = mergeTypes(detectedType, type);
       }
      }
     }
    }

    if (isAllNull || detectedType == null) {
     detectedType = "STRING";
    }

    fieldTypes.put(columnName, detectedType);
    log.debug("  Column '{}' -> Type: {}", columnName, detectedType);
   }
  }

  return fieldTypes;
 }

 private String inferType(String value) {
  // Check boolean
  if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
   return "BOOLEAN";
  }

  // Check integer
  if (value.matches("-?\\d+")) {
   try {
    int intVal = Integer.parseInt(value);
    // Check if fits in INT32
    if (intVal >= -2147483648 && intVal <= 2147483647) {
     return "INT32";
    }
    return "INT64";
   } catch (NumberFormatException e) {
    return "STRING";
   }
  }

  // Check decimal/float
  if (value.matches("-?\\d+\\.\\d+")) {
   return "FLOAT64";
  }

  // Default to string
  return "STRING";
 }

 private String mergeTypes(String existing, String incoming) {
  if ("STRING".equals(existing) || "STRING".equals(incoming)) {
   return "STRING";
  }
  if ("FLOAT64".equals(existing) || "FLOAT64".equals(incoming)) {
   return "FLOAT64";
  }
  if ("INT64".equals(existing) || "INT64".equals(incoming)) {
   return "INT64";
  }
  if ("BOOLEAN".equals(existing) && "BOOLEAN".equals(incoming)) {
   return "BOOLEAN";
  }
  if ("INT32".equals(existing) && "INT32".equals(incoming)) {
   return "INT32";
  }
  return "STRING";
 }
}