package com.example.paymentservice.controller;

import com.example.paymentservice.model.OrderEvent;
import com.example.paymentservice.service.PaymentService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only view of recent payment attempts, for demoing successes and failures. */
@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @GetMapping
    public List<OrderEvent> recentPayments() {
        return paymentService.recentHistory();
    }
}
