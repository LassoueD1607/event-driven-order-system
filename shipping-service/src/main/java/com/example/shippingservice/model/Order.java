package com.example.shippingservice.model;                      // NOTE: shippingservice package (NOT orderservice) - this is a SEPARATE copy.

/**
 * A separate copy of Order living in shipping-service's OWN package.
 * Why duplicate instead of share code? Because these are independent microservices:
 * they agree on the JSON SHAPE (the contract), not on shared Java classes. shipping-service
 * could be rewritten in Python and nothing would break. This is real microservice thinking.
 */
public class Order {

    private String orderId;                                      // Same four fields as the producer's Order...
    private String product;
    private int quantity;
    private double price;

    public Order() {}
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }

    @Override                                                   // Readable logging output.
    public String toString() {
        return "Order{orderId='" + orderId + "', product='" + product +
               "', quantity=" + quantity + ", price=" + price + '}';
    }
}
