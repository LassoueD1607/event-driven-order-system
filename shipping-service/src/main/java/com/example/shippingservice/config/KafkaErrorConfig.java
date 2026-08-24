package com.example.shippingservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Error handling for the consumer: retry a failed message a few times, then send it
 * to a dead-letter topic ("payment-events.DLT") so a poison message never blocks the partition.
 */
@Configuration
public class KafkaErrorConfig {

    /** Quarantine topic for messages that keep failing. Same partition count as "payment-events". */
    @Bean
    public NewTopic paymentEventsDlt() {
        return TopicBuilder.name("payment-events.DLT")
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Spring Boot auto-applies a single CommonErrorHandler bean to the listener container.
     * Retry twice, one second apart; if it still fails, publish to "<topic>.DLT".
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
