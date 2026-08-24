package com.example.inventoryservice.service;

import com.example.inventoryservice.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory stock ledger. A real system would back this with a database and derive starting
 * stock from a ProductCreated event or an admin API — seeding it on first sight is a learning
 * simplification (same approach used elsewhere in this project's sibling learning repos).
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private static final int DEFAULT_INITIAL_STOCK = 100;

    private final Map<String, Integer> stockByProduct = new ConcurrentHashMap<>();
    private final Map<String, Reservation> reservationsByOrderId = new ConcurrentHashMap<>();

    // Kafka is at-least-once: a redelivered message must not be reprocessed.
    private final Set<String> reservationsHandled = ConcurrentHashMap.newKeySet();
    private final Set<String> releasesHandled = ConcurrentHashMap.newKeySet();

    /** Reserves stock for an order, or rejects it if there isn't enough. Idempotent per orderId. */
    public OrderEvent reserveStock(OrderEvent order) {
        if (!reservationsHandled.add(order.getOrderId())) {
            log.info("Order {} already processed for reservation — ignoring duplicate delivery", order.getOrderId());
            return null;
        }

        int available = stockByProduct.computeIfAbsent(order.getProduct(), p -> DEFAULT_INITIAL_STOCK);
        if (available < order.getQuantity()) {
            log.info("Rejecting order {}: requested {} x '{}', only {} in stock",
                    order.getOrderId(), order.getQuantity(), order.getProduct(), available);
            return new OrderEvent(order.getOrderId(), order.getProduct(), order.getQuantity(),
                    order.getPrice(), "REJECTED", "insufficient stock");
        }

        stockByProduct.put(order.getProduct(), available - order.getQuantity());
        reservationsByOrderId.put(order.getOrderId(), new Reservation(order.getProduct(), order.getQuantity()));
        log.info("Reserved {} x '{}' for order {} ({} left in stock)",
                order.getQuantity(), order.getProduct(), order.getOrderId(),
                stockByProduct.get(order.getProduct()));
        return new OrderEvent(order.getOrderId(), order.getProduct(), order.getQuantity(),
                order.getPrice(), "RESERVED", null);
    }

    /** The saga's compensating transaction: releases previously reserved stock after a payment failure. */
    public void releaseStock(String orderId) {
        if (!releasesHandled.add(orderId)) {
            log.info("Release for order {} already processed — ignoring duplicate delivery", orderId);
            return;
        }

        Reservation reservation = reservationsByOrderId.remove(orderId);
        if (reservation == null) {
            log.warn("Payment failed for order {} but no reservation was found to release", orderId);
            return;
        }

        stockByProduct.merge(reservation.product(), reservation.quantity(), Integer::sum);
        log.info("Compensated order {}: released {} x '{}' back to stock ({} now available)",
                orderId, reservation.quantity(), reservation.product(), stockByProduct.get(reservation.product()));
    }

    public Map<String, Integer> currentStock() {
        return Map.copyOf(stockByProduct);
    }

    private record Reservation(String product, int quantity) {
    }
}
