package com.example.shippingservice.consumer;

import com.example.shippingservice.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "orders", groupId = "shipping-group")
    public void handleOrder(Order order) {
        log.info("Received order {} - shipping {} x '{}' (total ${})",
                order.getOrderId(),
                order.getQuantity(),
                order.getProduct(),
                order.getQuantity() * order.getPrice());
    }
}
