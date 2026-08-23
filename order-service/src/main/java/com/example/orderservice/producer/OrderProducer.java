package com.example.orderservice.producer;

import com.example.orderservice.config.KafkaTopicConfig;
import com.example.orderservice.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Order order) {
        log.info("Publishing order: {}", order);
        // Keyed by orderId so events for the same order share a partition and stay ordered.
        kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, order.getOrderId(), order);
    }
}
