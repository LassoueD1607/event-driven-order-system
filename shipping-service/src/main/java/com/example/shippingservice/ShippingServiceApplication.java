package com.example.shippingservice;                                // This service's base package.

import org.springframework.boot.SpringApplication;                  // Boots a Spring Boot app.
import org.springframework.boot.autoconfigure.SpringBootApplication;// The all-in-one Spring Boot annotation.

/**
 * The entry point of the shipping-service.
 */
@SpringBootApplication
public class ShippingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShippingServiceApplication.class, args);
    }
}
