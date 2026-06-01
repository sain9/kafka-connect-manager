package com.hussain.kafka.connect.manager.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    @JsonProperty("order_id")
    private Long orderId;

    @JsonProperty("customer_name")
    private String customerName;

    @JsonProperty("amount")
    private Long amount;
}