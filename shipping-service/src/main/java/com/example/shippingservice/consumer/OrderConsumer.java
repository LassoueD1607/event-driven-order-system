package com.example.shippingservice.consumer;

import com.example.shippingservice.model.Order;
import com.example.shippingservice.service.ShippingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Now the third hop of the saga: listens to "payment-events" (not "orders" directly) so
 * shipping only happens once stock was reserved AND payment actually completed.
 */
@Service
public class OrderConsumer {

    private final ShippingService shippingService;

    public OrderConsumer(ShippingService shippingService) {
        this.shippingService = shippingService;
    }

    @KafkaListener(topics = "payment-events", groupId = "${app.kafka.group:shipping-group}")
    public void handlePaymentEvent(Order order) {
        if (!"COMPLETED".equals(order.getStatus())) {
            return; // failed payments never ship
        }
        if (order.getQuantity() <= 0) {
            throw new IllegalArgumentException(
                    "Invalid order " + order.getOrderId() + ": quantity must be > 0");
        }
        shippingService.ship(order);
    }
}
