package com.example.orderservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the "orders" topic so it is created at startup with an explicit
 * partition/replica count, rather than relying on broker auto-creation defaults.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String TOPIC_NAME = "orders";

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
