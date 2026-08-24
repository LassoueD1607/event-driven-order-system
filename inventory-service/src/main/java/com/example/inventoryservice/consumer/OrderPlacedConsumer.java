package com.example.inventoryservice.consumer;

import com.example.inventoryservice.model.OrderEvent;
import com.example.inventoryservice.producer.StockEventProducer;
import com.example.inventoryservice.service.InventoryService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/** First hop of the saga: reserve (or reject) stock for a newly placed order. */
@Service
public class OrderPlacedConsumer {

    private final InventoryService inventoryService;
    private final StockEventProducer stockEventProducer;

    public OrderPlacedConsumer(InventoryService inventoryService, StockEventProducer stockEventProducer) {
        this.inventoryService = inventoryService;
        this.stockEventProducer = stockEventProducer;
    }

    @KafkaListener(topics = "orders", groupId = "${app.kafka.group:inventory-group}")
    public void handleOrderPlaced(OrderEvent order) {
        OrderEvent outcome = inventoryService.reserveStock(order);
        if (outcome != null) {
            stockEventProducer.send(outcome);
        }
    }
}
