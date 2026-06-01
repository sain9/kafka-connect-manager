package com.hussain.kafka.connect.manager.streams.transformer;

import com.hussain.kafka.connect.manager.streams.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class DeduplicationTransformer implements Transformer<String, Order, KeyValue<String, Order>> {

    private final String storeName;
    private KeyValueStore<String, Long> stateStore;

    public DeduplicationTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.stateStore = context.getStateStore(storeName);
        log.info("Deduplication transformer initialized");
    }

    @Override
    public KeyValue<String, Order> transform(String key, Order order) {
        if (order == null || order.getOrderId() == null) {
            return null;
        }

        String dedupKey = String.valueOf(order.getOrderId());
        Long firstSeen = stateStore.get(dedupKey);

        if (firstSeen == null) {
            // First time - keep it
            stateStore.put(dedupKey, System.currentTimeMillis());
            log.info("✅ ACCEPTED: Order {}", order.getOrderId());
            return new KeyValue<>(dedupKey, order);
        } else {
            // Duplicate - reject it
            log.warn("❌ REJECTED: Duplicate Order {}", order.getOrderId());
            return null;
        }
    }

    @Override
    public void close() {
        // Cleanup
    }
}