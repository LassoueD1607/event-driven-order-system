package com.example.inventoryservice.producer;

import com.example.inventoryservice.config.KafkaTopicConfig;
import com.example.inventoryservice.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class StockEventProducer {

    private static final Logger log = LoggerFactory.getLogger(StockEventProducer.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public StockEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(OrderEvent event) {
        log.info("Publishing stock event: {}", event);
        kafkaTemplate.send(KafkaTopicConfig.STOCK_EVENTS_TOPIC, event.getOrderId(), event);
    }
}
