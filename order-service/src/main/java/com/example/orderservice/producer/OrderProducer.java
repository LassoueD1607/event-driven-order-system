package com.example.orderservice.producer;                       // Producer package.

import com.example.orderservice.config.KafkaTopicConfig;         // Our topic-name constant.
import com.example.orderservice.model.Order;                     // Our data object.
import org.slf4j.Logger;                                         // Logging interface.
import org.slf4j.LoggerFactory;                                  // Creates loggers.
import org.springframework.kafka.core.KafkaTemplate;             // Spring's tool for SENDING to Kafka.
import org.springframework.stereotype.Service;                   // Marks this as a managed service bean.

/**
 * Sends Order objects to Kafka. The JsonSerializer (see application.yml) turns each Order into JSON text.
 */
@Service                                                         // Spring creates one instance and injects it where needed.
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class); // One shared logger for this class.

    private final KafkaTemplate<String, Order> kafkaTemplate;    // The send tool. <String, Order> = key is String, value is Order.

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) { // Constructor injection: Spring supplies a ready KafkaTemplate...
        this.kafkaTemplate = kafkaTemplate;                      // ...built from our application.yml settings. We just store it.
    }

    public void send(Order order) {                              // Send one order. Returns nothing (void).
        log.info("➡️  Publishing order: {}", order);            // Log it first ({} is replaced by order.toString()).
        kafkaTemplate.send(                                      // The actual send (fire-and-forget / async):
                KafkaTopicConfig.TOPIC_NAME,                     //   1) topic = "orders"
                order.getOrderId(),                              //   2) KEY = orderId  -> same key = same partition = ordered (Phase 2)
                order);                                          //   3) VALUE = the Order (auto-converted to JSON)
    }
}
