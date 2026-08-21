package com.example.shippingservice.consumer;                    // Consumer package.

import com.example.shippingservice.model.Order;                  // THIS service's own Order class.
import org.slf4j.Logger;                                         // Logging interface.
import org.slf4j.LoggerFactory;                                  // Creates loggers.
import org.springframework.kafka.annotation.KafkaListener;       // The annotation that RECEIVES messages.
import org.springframework.stereotype.Service;                   // Marks this as a managed service bean.

/**
 * Listens for Order events and "ships" them.
 * The JsonDeserializer (see application.yml) turns incoming JSON text back into an Order.
 */
@Service                                                         // Spring manages one instance of this consumer.
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class); // One shared logger.

    @KafkaListener(topics = "orders", groupId = "shipping-group")// Spring auto-calls the method below for each message in "orders".
    public void handleOrder(Order order) {                       // Parameter is an Order (not String!) - deserialized from JSON automatically.
        log.info("📦 Received order {} -> shipping {} x '{}' (total ${})", // 4 placeholders, filled in order below:
                order.getOrderId(),                              //   {} #1 = order id
                order.getQuantity(),                             //   {} #2 = quantity
                order.getProduct(),                              //   {} #3 = product name
                order.getQuantity() * order.getPrice());         //   {} #4 = computed total (qty x price)
    }
}
