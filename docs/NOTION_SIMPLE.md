# Kafka Event Tracking Pipeline

A learning project that connects a React UI → Spring Boot → Apache Kafka → Prometheus → Grafana to understand how event-driven systems work end to end.

---

## What It Does

When a user clicks a button on a course-platform landing page (e.g. "Add to Cart"), that action is sent to a backend, published to Kafka, consumed by another service, and shows up as a metric in Grafana — all within seconds.

---

## Services & Ports

| Service | Port | Role |
| --- | --- | --- |
| React UI | 3000 | Landing page with 4 event buttons |
| ProducerServer | 8080 | REST API → publishes to Kafka |
| ConsumerServer | 8081 | Reads from Kafka → emits metrics |
| Kafka Broker | 9092 | Message broker (WSL2 IP) |
| Prometheus | 9090 | Scrapes metrics from both servers |
| Grafana | 3001 | Visualises the Prometheus metrics |

---

## Architecture

```
React UI (3000)
    │
    │  POST /producer/event
    ▼
ProducerServer (8080)
    │
    │  kafkaTemplate.send("testy", eventType, json)
    ▼
Kafka — topic: "testy"
    │
    │  @KafkaListener
    ▼
ConsumerServer (8081)
    │
    │  counter.increment()
    ▼
/actuator/prometheus  ◄──── Prometheus (9090) ◄──── Grafana (3001)
```

---

## The 4 Event Types

These are simulated button clicks — no real cart or purchase flow exists.

| Event | What it represents |
| --- | --- |
| `pageView` | User visited the page |
| `userClick` | User clicked on a course |
| `addToCart` | User added a course to cart |
| `checkout` | User completed a purchase |

---

## Key Files

### React — `landing-page/src/LandingPage.js`
The entire UI. Four buttons, each calling `emitEvent(type)` which fires a `fetch` POST to the producer. Local state tracks how many of each event were sent. All state resets on page refresh.

### Producer — `ProducerController.java`
The only REST endpoint. Receives the event, checks the type isn't blank, serializes it to JSON, and sends it to Kafka. Also increments a Micrometer counter tagged with the event type.

```java
kafkaTemplate.send("testy", event.getType(), jsonPayload);
Counter.builder("producer_events_sent_total")
       .tag("event_type", event.getType())
       .register(meterRegistry).increment();
```

### Consumer — `EventConsumer.java`
Listens to the Kafka topic. When a message arrives, it deserializes it back to an Event object and increments its own counter. If parsing fails, it logs the error and increments a failure counter.

```java
@KafkaListener(topics = "testy", groupId = "metrics-consumer-group")
public void consume(String message) {
    Event event = objectMapper.readValue(message, Event.class);
    Counter.builder("consumer_events_received_total")
           .tag("event_type", event.getType())
           .register(meterRegistry).increment();
}
```

### Both Servers — `application.properties`

```properties
# ProducerServer
server.port=8080
spring.kafka.bootstrap-servers=172.24.236.246:9092
management.endpoints.web.exposure.include=*
management.endpoint.prometheus.enabled=true

# ConsumerServer
server.port=8081
spring.kafka.consumer.group-id=metrics-consumer-group
spring.kafka.consumer.auto-offset-reset=earliest
```

---

## How the Kafka Message Looks

```
Key:   addToCart
Value: {"type":"addToCart","payload":"react-ui"}
```

The **key** determines which Kafka partition the message goes to. Using the event type as the key means all `checkout` events land on the same partition — preserving their order.

---

## How Metrics Flow

Both Spring Boot servers expose `/actuator/prometheus` for free — just by adding the Micrometer Prometheus dependency. Prometheus scrapes that endpoint on a schedule. Grafana queries Prometheus and shows the counters on a dashboard.

**Metrics created:**

- `producer_events_sent_total{event_type="checkout"}` — on the producer
- `consumer_events_received_total{event_type="checkout"}` — on the consumer
- `consumer_events_failed_total` — when a message can't be parsed

---

## Concept Map

| Concept | Where it appears |
| --- | --- |
| REST API | `POST /producer/event` in ProducerController |
| Kafka Producer | `KafkaTemplate.send()` in ProducerController |
| Kafka Consumer | `@KafkaListener` in EventConsumer |
| Kafka Topic | `"testy"` — one topic, all event types |
| Consumer Group | `metrics-consumer-group` — one group, one instance |
| Micrometer Counter | `Counter.builder(...).tag(...).increment()` in both services |
| Prometheus Scrape | `/actuator/prometheus` on both servers |
| CORS | `CorsConfig.java` in both servers — allows cross-origin requests |
| Dependency Injection | Spring constructor injection in both controllers/services |

---

## Known Gaps (Good to Know)

**1. Kafka delivery not confirmed**
`kafkaTemplate.send()` returns a future that is never awaited. The API returns 200 OK even if Kafka never received the message.

**2. Failed consumer messages are dropped**
When the consumer can't parse a message, it catches the error and moves on. The message is lost — there's no retry or dead letter queue.

**3. CORS is misconfigured**
The CORS config whitelists `localhost:3001` (Grafana) but the React app runs on `localhost:3000`. This should also be whitelisted.

**4. Hardcoded Kafka IP**
`172.24.236.246:9092` is a WSL2 IP that changes on reboot. Should be an environment variable.

**5. Dead dependencies**
Lombok, MySQL driver, Guava, and Spring Cloud Bootstrap are all declared in `build.gradle` but never used.

---

## How to Run It

```bash
# 1. Start Kafka (Docker)
docker run -d -p 9092:9092 bitnami/kafka:latest

# 2. Start ProducerServer
cd ProducerServer && ./gradlew :app:bootRun

# 3. Start ConsumerServer
cd ConsumerServer && ./gradlew bootRun

# 4. Start React UI
cd landing-page && npm install && npm start

# 5. Test manually
curl -X POST http://localhost:8080/producer/event \
  -H "Content-Type: application/json" \
  -d '{"type":"checkout","payload":"test"}'

# 6. Check metrics
curl http://localhost:8080/actuator/prometheus | grep producer_events
curl http://localhost:8081/actuator/prometheus | grep consumer_events
```
