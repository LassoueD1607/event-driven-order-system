package com.example.paymentservice.service;

import com.example.paymentservice.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simulated payment gateway. Charges fail if the product name contains "fail" — a
 * deterministic hook for demoing the compensating transaction on purpose — or on a random
 * ~15% chance, to also show an unprompted failure path. No real card data is ever involved.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final int HISTORY_LIMIT = 50;
    private static final double RANDOM_FAILURE_RATE = 0.15;

    private final Set<String> processed = ConcurrentHashMap.newKeySet();
    private final Deque<OrderEvent> history = new ArrayDeque<>();

    public synchronized OrderEvent charge(OrderEvent stockReserved) {
        if (!processed.add(stockReserved.getOrderId())) {
            log.info("Order {} already charged — ignoring duplicate delivery", stockReserved.getOrderId());
            return null;
        }

        boolean forcedFailure = stockReserved.getProduct() != null
                && stockReserved.getProduct().toLowerCase().contains("fail");
        boolean randomFailure = Math.random() < RANDOM_FAILURE_RATE;

        OrderEvent result;
        if (forcedFailure || randomFailure) {
            String reason = forcedFailure ? "card declined (forced demo failure)" : "card declined";
            log.info("Payment FAILED for order {}: {}", stockReserved.getOrderId(), reason);
            result = new OrderEvent(stockReserved.getOrderId(), stockReserved.getProduct(),
                    stockReserved.getQuantity(), stockReserved.getPrice(), "FAILED", reason);
        } else {
            log.info("Payment COMPLETED for order {}: ${}", stockReserved.getOrderId(),
                    stockReserved.getQuantity() * stockReserved.getPrice());
            result = new OrderEvent(stockReserved.getOrderId(), stockReserved.getProduct(),
                    stockReserved.getQuantity(), stockReserved.getPrice(), "COMPLETED", null);
        }

        history.addFirst(result);
        while (history.size() > HISTORY_LIMIT) {
            history.removeLast();
        }
        return result;
    }

    public List<OrderEvent> recentHistory() {
        return List.copyOf(history);
    }
}
