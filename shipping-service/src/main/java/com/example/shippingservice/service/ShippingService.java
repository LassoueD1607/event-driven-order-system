package com.example.shippingservice.service;

import com.example.shippingservice.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Final hop of the saga: ship the order once payment has actually completed. */
@Service
public class ShippingService {

    private static final Logger log = LoggerFactory.getLogger(ShippingService.class);
    private static final int HISTORY_LIMIT = 50;

    private final Set<String> shipped = ConcurrentHashMap.newKeySet();
    private final Deque<Order> history = new ArrayDeque<>();

    public synchronized void ship(Order order) {
        if (!shipped.add(order.getOrderId())) {
            log.info("Order {} already shipped — ignoring duplicate delivery", order.getOrderId());
            return;
        }

        log.info("Shipping order {}: {} x '{}' (total ${})",
                order.getOrderId(), order.getQuantity(), order.getProduct(),
                order.getQuantity() * order.getPrice());

        history.addFirst(order);
        while (history.size() > HISTORY_LIMIT) {
            history.removeLast();
        }
    }

    public List<Order> recentShipments() {
        return List.copyOf(history);
    }
}
