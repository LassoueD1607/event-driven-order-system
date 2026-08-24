package com.example.inventoryservice.model;

/**
 * Shape shared, by convention, across "orders", "stock-events" and "payment-events" —
 * not a shared library on purpose (each service keeps its own copy, per the JSON-contract-only
 * rule already used elsewhere in this project). status/reason are simply absent on messages
 * from a stage that doesn't set them (e.g. the original "orders" message has status = null).
 */
public class OrderEvent {

    private String orderId;
    private String product;
    private int quantity;
    private double price;
    private String status;
    private String reason;

    public OrderEvent() {
        // Required by Jackson for JSON deserialization.
    }

    public OrderEvent(String orderId, String product, int quantity, double price, String status, String reason) {
        this.orderId = orderId;
        this.product = product;
        this.quantity = quantity;
        this.price = price;
        this.status = status;
        this.reason = reason;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    @Override
    public String toString() {
        return "OrderEvent{orderId='" + orderId + "', product='" + product +
               "', quantity=" + quantity + ", price=" + price +
               ", status='" + status + "', reason='" + reason + "'}";
    }
}
