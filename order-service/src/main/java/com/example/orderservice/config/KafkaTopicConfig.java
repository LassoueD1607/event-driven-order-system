package com.example.orderservice.config;                         // Config package.

import org.apache.kafka.clients.admin.NewTopic;                  // Kafka's "topic definition" object.
import org.springframework.context.annotation.Bean;             // Marks a method whose result Spring should manage.
import org.springframework.context.annotation.Configuration;    // Marks a class that defines beans.
import org.springframework.kafka.config.TopicBuilder;           // Helper to build a NewTopic fluently.

/**
 * The order-service OWNS the "orders" topic, so it creates it on startup.
 * This is the code equivalent of: kafka-topics.sh --create --topic orders ...
 */
@Configuration                                                   // "This class defines beans (objects Spring creates & manages)."
public class KafkaTopicConfig {

    public static final String TOPIC_NAME = "orders";           // Topic name in ONE place. public/static/final = shared, unchangeable constant.

    @Bean                                                        // Spring calls this at startup and manages the returned object.
    public NewTopic ordersTopic() {                             // Spring sees the NewTopic and asks Kafka to create it if missing.
        return TopicBuilder.name(TOPIC_NAME)                    // Topic named "orders"...
                .partitions(1)                                   // ...with 1 partition (one lane) - we grow this in Phase 5.
                .replicas(1)                                     // ...and 1 copy (we only have 1 broker).
                .build();                                        // Finalize the topic definition.
    }
}
