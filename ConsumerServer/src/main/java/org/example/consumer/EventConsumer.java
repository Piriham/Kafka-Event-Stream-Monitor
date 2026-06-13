package org.example.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.example.model.Event;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class EventConsumer {

    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    public EventConsumer(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.objectMapper = new ObjectMapper();
    }

    @KafkaListener(topics = "testy", groupId = "metrics-consumer-group")
    public void consume(String message) {
        try {
            Event event = objectMapper.readValue(message, Event.class);
            System.out.println("Consumed event: " + event);

            Counter.builder("consumer_events_received_total")
                    .tag("event_type", event.getType())
                    .description("Total events received from Kafka by type")
                    .register(meterRegistry)
                    .increment();

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());

            Counter.builder("consumer_events_failed_total")
                    .description("Total events that failed to process")
                    .register(meterRegistry)
                    .increment();
        }
    }
}