package com.hussain.kafka.connect.manager.dto;

import lombok.Data;

import java.util.Map;

@Data
public class ConnectorRequest {

    private String name;

    private Map<String, String> config;
}