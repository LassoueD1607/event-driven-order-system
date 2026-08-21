package com.example.orderservice.model;                          // Sub-package for data models (just organization).

/**
 * The Order EVENT we send through Kafka.
 * A plain data object (POJO) that gets converted to JSON before sending, e.g.:
 *   {"orderId":"...","product":"Laptop","quantity":3,"price":1200.0}
 * Jackson (the JSON library) needs a no-arg constructor + getters/setters to do the conversion.
 */
public class Order {                                             // The blueprint for an order object.

    private String orderId;                                      // Field: unique id (text). private = only this class touches it directly.
    private String product;                                      // Field: product name (text).
    private int quantity;                                        // Field: how many (whole number).
    private double price;                                        // Field: unit price (decimal number).

    public Order() {                                             // No-arg constructor. Looks empty, but Jackson REQUIRES it to rebuild from JSON.
    }

    public Order(String orderId, String product,                // Convenience constructor: build a fully-filled Order in one line.
                 int quantity, double price) {
        this.orderId = orderId;                                  // "this.orderId" = the field; "orderId" = the parameter passed in.
        this.product = product;
        this.quantity = quantity;
        this.price = price;
    }

    // --- Getters (read a field) and setters (change a field). Jackson uses these for JSON <-> object. ---
    public String getOrderId() { return orderId; }              // Read orderId.
    public void setOrderId(String orderId) { this.orderId = orderId; }   // Write orderId.

    public String getProduct() { return product; }             // Read product.
    public void setProduct(String product) { this.product = product; }   // Write product.

    public int getQuantity() { return quantity; }              // Read quantity.
    public void setQuantity(int quantity) { this.quantity = quantity; }  // Write quantity.

    public double getPrice() { return price; }                 // Read price.
    public void setPrice(double price) { this.price = price; }  // Write price.

    @Override                                                   // We replace Java's default toString()...
    public String toString() {                                  // ...so logging shows readable text instead of "Order@1a2b3c".
        return "Order{orderId='" + orderId + "', product='" + product +
               "', quantity=" + quantity + ", price=" + price + '}';
    }
}
