package com.example.orderservice.controller;                     // Controller package.

import com.example.orderservice.model.Order;                     // Our data object.
import com.example.orderservice.producer.OrderProducer;          // The producer we call.
import org.springframework.web.bind.annotation.*;                // All web annotations (* = everything in that package).

import java.util.UUID;                                           // For generating unique ids.

/**
 * REST door for placing orders. Accepts JSON like: { "product": "Laptop", "quantity": 3, "price": 1200.0 }
 */
@RestController                                                  // Methods handle HTTP requests; return values become the HTTP response.
@RequestMapping("/api/orders")                                  // Base URL: every endpoint here starts with /api/orders.
public class OrderController {

    private final OrderProducer producer;                       // The producer this controller uses.

    public OrderController(OrderProducer producer) {            // Constructor injection: Spring supplies the OrderProducer.
        this.producer = producer;
    }

    @PostMapping                                                 // Handle HTTP POST to /api/orders.
    public String placeOrder(@RequestBody Order order) {        // @RequestBody = convert the JSON request body into an Order object.
        order.setOrderId(UUID.randomUUID().toString());         // Server assigns a unique id (client didn't send one).
        producer.send(order);                                   // Publish the order to Kafka.
        return "Order placed & published: " + order.getOrderId(); // This text becomes the HTTP response body.
    }
}
