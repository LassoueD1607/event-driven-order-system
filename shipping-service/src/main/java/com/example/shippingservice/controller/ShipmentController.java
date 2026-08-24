package com.example.shippingservice.controller;

import com.example.shippingservice.model.Order;
import com.example.shippingservice.service.ShippingService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only view of recently shipped orders, for demoing the happy path end to end. */
@RestController
@RequestMapping("/api/shipments")
public class ShipmentController {

    private final ShippingService shippingService;

    public ShipmentController(ShippingService shippingService) {
        this.shippingService = shippingService;
    }

    @GetMapping
    public List<Order> recentShipments() {
        return shippingService.recentShipments();
    }
}
