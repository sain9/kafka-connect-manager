//package com.hussain.kafka.connect.manager.controller;
//
//import com.hussain.kafka.connect.manager.service.ConnectorService;
//import lombok.RequiredArgsConstructor;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.*;
//
//@RestController
//@RequestMapping("/api/connectors")
//@RequiredArgsConstructor
//public class ConnectorController {
//
//    private final ConnectorService connectorService;
//
//    @PostMapping("/create/{fileName}")
//    public ResponseEntity<String> createConnector(
//            @PathVariable String fileName
//    ) {
//
//        String response =
//                connectorService.createConnector(fileName);
//
//        return ResponseEntity.ok(response);
//    }
//}