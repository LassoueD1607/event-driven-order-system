package com.example.orderservice;

import org.springframework.boot.SpringApplication;                  // Import: the class that boots a Spring Boot app.
import org.springframework.boot.autoconfigure.SpringBootApplication;// Import: the all-in-one Spring Boot annotation.

@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
