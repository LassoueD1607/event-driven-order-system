package com.example.orderservice.model;

/** Order event published to Kafka, serialized as JSON. */
public class Order {

    private String orderId;
    private String product;
    private int quantity;
    private double price;

    public Order() {
        // Required by Jackson for JSON (de)serialization.
    }

    public Order(String orderId, String product, int quantity, double price) {
        this.orderId = orderId;
        this.product = product;
        this.quantity = quantity;
        this.price = price;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }

    @Override
    public String toString() {
        return "Order{orderId='" + orderId + "', product='" + product +
               "', quantity=" + quantity + ", price=" + price + '}';
    }
}
