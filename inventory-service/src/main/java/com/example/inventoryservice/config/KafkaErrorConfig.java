package com.example.inventoryservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Same retry+DLQ strategy as the rest of this project: retry twice, one second apart, then
 * quarantine to "<topic>.DLT" so one poison message can't block the partition. This service
 * consumes two topics ("orders" and "payment-events"), so it declares a DLT for each.
 */
@Configuration
public class KafkaErrorConfig {

    @Bean
    public NewTopic ordersDlt() {
        return TopicBuilder.name("orders.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentEventsDlt() {
        return TopicBuilder.name("payment-events.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
