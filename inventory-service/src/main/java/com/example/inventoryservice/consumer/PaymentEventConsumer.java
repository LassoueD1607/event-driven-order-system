package com.example.inventoryservice.consumer;

import com.example.inventoryservice.model.OrderEvent;
import com.example.inventoryservice.service.InventoryService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * The saga's compensating step: if payment fails after stock was reserved, release it.
 * COMPLETED payments are ignored here — nothing to compensate for those.
 */
@Service
public class PaymentEventConsumer {

    private final InventoryService inventoryService;

    public PaymentEventConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "payment-events", groupId = "${app.kafka.group:inventory-group}")
    public void handlePaymentEvent(OrderEvent event) {
        if ("FAILED".equals(event.getStatus())) {
            inventoryService.releaseStock(event.getOrderId());
        }
    }
}
