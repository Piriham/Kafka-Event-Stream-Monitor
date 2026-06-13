package org.example.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.example.model.Event;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class ProducerController {

    private static final String TOPIC_NAME = "testy";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public ProducerController(KafkaTemplate<String, String> kafkaTemplate,
                              MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
        this.meterRegistry = meterRegistry;
    }

    @PostMapping("/event")
    public ResponseEntity<String> sendEvent(@RequestBody Event event) {
        try {
            if (event.getType() == null || event.getType().isBlank()) {
                return ResponseEntity.badRequest().body("Event type is required");
            }

            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(TOPIC_NAME, event.getType(), payload);

            Counter.builder("producer_events_sent_total")
                    .tag("event_type", event.getType())
                    .description("Total events sent to Kafka by type")
                    .register(meterRegistry)
                    .increment();

            return ResponseEntity.ok("Event sent: " + event.getType());
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body("Failed to send event: " + e.getMessage());
        }
    }
}