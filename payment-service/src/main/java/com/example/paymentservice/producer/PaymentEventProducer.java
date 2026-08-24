package com.example.paymentservice.producer;

import com.example.paymentservice.config.KafkaTopicConfig;
import com.example.paymentservice.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(OrderEvent event) {
        log.info("Publishing payment event: {}", event);
        kafkaTemplate.send(KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, event.getOrderId(), event);
    }
}
