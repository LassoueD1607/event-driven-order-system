package com.example.paymentservice.consumer;

import com.example.paymentservice.model.OrderEvent;
import com.example.paymentservice.producer.PaymentEventProducer;
import com.example.paymentservice.service.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/** Second hop of the saga: charge the customer once stock has been reserved. */
@Service
public class StockEventConsumer {

    private final PaymentService paymentService;
    private final PaymentEventProducer paymentEventProducer;

    public StockEventConsumer(PaymentService paymentService, PaymentEventProducer paymentEventProducer) {
        this.paymentService = paymentService;
        this.paymentEventProducer = paymentEventProducer;
    }

    @KafkaListener(topics = "stock-events", groupId = "${app.kafka.group:payment-group}")
    public void handleStockEvent(OrderEvent event) {
        if (!"RESERVED".equals(event.getStatus())) {
            return; // rejected orders never reach payment
        }
        OrderEvent outcome = paymentService.charge(event);
        if (outcome != null) {
            paymentEventProducer.send(outcome);
        }
    }
}
