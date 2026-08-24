package com.example.inventoryservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Declares the "stock-events" topic so it exists with an explicit partition/replica count. */
@Configuration
public class KafkaTopicConfig {

    public static final String STOCK_EVENTS_TOPIC = "stock-events";

    @Bean
    public NewTopic stockEventsTopic() {
        return TopicBuilder.name(STOCK_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
