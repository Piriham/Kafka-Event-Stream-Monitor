# Request Flows

## Flow 1 — Happy Path: User Clicks "Add to Cart"

```
User clicks "🛒 Add to Cart" button
        │
        ▼  [LandingPage.js : emitEvent("addToCart")]
setStatuses(prev → { addToCart: 'loading' })       // button shows "Sending..."
        │
        ▼  fetch("http://localhost:8080/producer/event", POST)
           body: { "type": "addToCart", "payload": "react-ui" }
           headers: Content-Type: application/json
        │
        ▼  [ProducerController.java : sendEvent(Event)]
           Spring deserializes JSON body → Event POJO
           Validates: event.getType() != null && !blank  ✓
           objectMapper.writeValueAsString(event)
           → '{"type":"addToCart","payload":"react-ui"}'
        │
        ▼  kafkaTemplate.send("testy", "addToCart", '{"type":"addToCart","payload":"react-ui"}')
           Key   = "addToCart"   (used for Kafka partition routing)
           Value = JSON string
        │
        ▼  Micrometer:
           Counter "producer_events_sent_total" tag(event_type=addToCart) .increment()
        │
        ▼  ResponseEntity.ok("Event sent: addToCart")   →   HTTP 200
        │
        ▼  [LandingPage.js]
           counts.addToCart += 1
           statuses.addToCart = 'success'              // card glows green
           setTimeout 1500ms → statuses.addToCart = null
        │
        ▼  [ConsumerServer EventConsumer.java : consume(String message)]  ← async
           objectMapper.readValue(message, Event.class)
           → Event{type='addToCart', payload='react-ui'}
           System.out.println("Consumed event: ...")
           Counter "consumer_events_received_total" tag(event_type=addToCart) .increment()
        │
        ▼  [Prometheus scrape — every N seconds]
           GET http://localhost:8080/actuator/prometheus
           GET http://localhost:8081/actuator/prometheus
        │
        ▼  Grafana queries Prometheus
           panel shows producer_events_sent_total{event_type="addToCart"} = 1
           panel shows consumer_events_received_total{event_type="addToCart"} = 1
```

---

## Flow 2 — Validation Failure: Missing Event Type

```
Client sends: POST /producer/event   body: { "payload": "react-ui" }
        │
        ▼  [ProducerController.java : sendEvent(Event)]
           event.getType() == null   →  condition true
        │
        ▼  return ResponseEntity.badRequest().body("Event type is required")
           HTTP 400 Bad Request
        │
        ▼  [LandingPage.js]
           response.ok == false  →  throw new Error('Failed')
           statuses[eventType] = 'error'   // card glows red
```

---

## Flow 3 — Kafka Serialization Failure (Edge Case)

```
[ProducerController.java : sendEvent(Event)]
        │
        ▼  objectMapper.writeValueAsString(event)   throws JsonProcessingException
        │
        ▼  catch (Exception e)
           return ResponseEntity.internalServerError()
                  .body("Failed to send event: " + e.getMessage())
           HTTP 500
```

---

## Flow 4 — Consumer Deserialization Failure

```
[ConsumerServer EventConsumer.java : consume(String message)]
        │  message is malformed / not valid JSON
        ▼  objectMapper.readValue(message, Event.class)  throws JsonProcessingException
        │
        ▼  catch (Exception e)
           System.err.println("Error processing message: " + e.getMessage())
           Counter "consumer_events_failed_total" .increment()
           (no re-throw → message is acknowledged to Kafka, NOT retried)
```

---

## Flow 5 — Prometheus Metric Scrape

```
Prometheus scraper (configured externally)
        │
        ▼  GET http://localhost:8080/actuator/prometheus
           Spring Actuator + Micrometer-Prometheus registry
           returns text/plain OpenMetrics format:
           producer_events_sent_total{event_type="pageView",...} 3.0
           producer_events_sent_total{event_type="checkout",...} 1.0
           ...
        │
        ▼  GET http://localhost:8081/actuator/prometheus
           consumer_events_received_total{event_type="pageView",...} 3.0
           consumer_events_failed_total{...} 0.0
           ...
        │
        ▼  Prometheus stores time-series in local TSDB
        │
        ▼  Grafana queries via PromQL
           e.g.  rate(producer_events_sent_total[1m])
```

---

## Key Classes and Methods Referenced

| File | Class | Method | Role |
|---|---|---|---|
| `landing-page/src/LandingPage.js` | `LandingPage` (FC) | `emitEvent(eventType)` | Fires fetch, manages UI state |
| `ProducerServer/app/src/main/java/org/example/controller/ProducerController.java` | `ProducerController` | `sendEvent(Event)` | REST endpoint, validates, publishes to Kafka |
| `ProducerServer/app/src/main/java/org/example/model/Event.java` | `Event` | getters/setters | Data model shared across HTTP and Kafka layers |
| `ConsumerServer/src/main/java/org/example/consumer/EventConsumer.java` | `EventConsumer` | `consume(String)` | Kafka listener, deserializes, emits counter |
| `ProducerServer/app/src/main/java/org/example/config/CorsConfig.java` | `CorsConfig` | `corsFilter()` | Spring bean: allows cross-origin from :3001 |
