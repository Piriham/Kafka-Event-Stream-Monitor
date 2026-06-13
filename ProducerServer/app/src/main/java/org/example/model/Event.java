package org.example.model;

public class Event {
    private String type;   // e.g. "userClick", "pageView", "addToCart", "checkout"
    private String payload; // any extra data

    public Event() {}

    public Event(String type, String payload) {
        this.type = type;
        this.payload = payload;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    @Override
    public String toString() {
        return "Event{type='" + type + "', payload='" + payload + "'}";
    }
}