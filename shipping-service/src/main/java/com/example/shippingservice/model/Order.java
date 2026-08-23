package com.example.shippingservice.model;

/**
 * Order event as consumed by shipping-service. Deliberately a separate copy from
 * order-service's class: the services share a JSON contract, not Java code.
 */
public class Order {

    private String orderId;
    private String product;
    private int quantity;
    private double price;

    public Order() {
        // Required by Jackson for JSON deserialization.
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
