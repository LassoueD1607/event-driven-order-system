package com.example.orderservice.controller;

import com.example.orderservice.model.Order;
import com.example.orderservice.producer.OrderProducer;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public String placeOrder(@RequestBody Order order) {
        order.setOrderId(UUID.randomUUID().toString());
        producer.send(order);
        return "Order placed & published: " + order.getOrderId();
    }
}
