# 📡 Kafka Event Tracking Pipeline — Complete Project Reference

> A real-time event-tracking pipeline: React UI → Spring Boot → Apache Kafka → Prometheus → Grafana

---

# 🗂 Table of Contents

1. Project Overview
2. Architecture
3. Folder Structure
4. Request Flows
5. File-by-File Breakdown
6. APIs
7. Messaging (Kafka)
8. Database
9. Tech Stack
10. Security
11. Production Readiness
12. Commands (How to Run Everything)
13. Interview Q&A (105 Questions)
14. Interview Scripts (30s / 2min / 5min / 10min)
15. Things Not in the Code

---

# 1. 🎯 Project Overview

## What It Does

A real-time **event-tracking pipeline** modelled after analytics infrastructure used by e-learning platforms. When a user clicks a button on a course-platform landing page, the action streams through Apache Kafka, gets consumed by a metrics service, and appears in Grafana within seconds.

## Who Uses It

| Actor | Role |
| --- | --- |
| End-user | Clicks buttons on the landing page (pageView, userClick, addToCart, checkout) |
| Operations / Analyst | Views Grafana dashboards to see event counts in real time |
| Developer | Learns how to wire a full event-driven pipeline |

## Features

- **Event Emission UI** — React SPA with 4 event-type buttons, local send counts, loading/success/error states
- **REST Producer API** — `POST /producer/event` validates and publishes to Kafka topic `testy`
- **Kafka Messaging** — Topic `testy`, string key/value, consumer group `metrics-consumer-group`
- **Metrics Collection** — Both servers expose `/actuator/prometheus`; counters tagged by `event_type`
- **Observability Stack** — Prometheus scrapes both servers; Grafana visualizes counters
- **CORS Configuration** — Both Spring Boot servers whitelist `http://localhost:3001`

## Architecture Style

**Event-Driven Microservices** — three independent processes communicate asynchronously via Kafka. React → Producer is synchronous HTTP; everything downstream is asynchronous.

## Port Map

| Service | Port |
| --- | --- |
| React UI | 3000 |
| ProducerServer | 8080 |
| ConsumerServer | 8081 |
| Kafka Broker | 172.24.236.246:9092 |
| Prometheus | 9090 |
| Grafana | 3001 |

## Tech Stack Summary

| Layer | Technology | Version |
| --- | --- | --- |
| Frontend | React (Create React App) | 18.2 |
| Producer backend | Spring Boot | 3.2.2 |
| Consumer backend | Spring Boot | 3.2.2 |
| Message broker | Apache Kafka | External |
| Metrics | Micrometer + Prometheus registry | Spring Boot bundled |
| Metrics scraper | Prometheus | Port 9090 |
| Dashboard | Grafana | Port 3001 |
| Build tool | Gradle | 8.6 |
| Language | Java | 21 |
| JSON | Jackson Databind | 2.16.1 |

---

# 2. 🏗 Architecture

## System Diagram (Text)

```
┌────────────────────────────────────────────────────────────────┐
│                       Developer Machine                        │
│                                                                │
│  ┌─────────────┐   HTTP POST    ┌─────────────────┐           │
│  │  React UI   │ ─────────────► │  ProducerServer │           │
│  │ (port 3000) │                │  (port 8080)    │           │
│  └─────────────┘                └────────┬────────┘           │
│                                          │ KafkaTemplate      │
│                                          ▼                    │
│                                 ┌─────────────────┐           │
│                                 │  Apache Kafka   │           │
│                                 │ topic: "testy"  │           │
│                                 │ :9092           │           │
│                                 └────────┬────────┘           │
│                                          │ @KafkaListener     │
│                                          ▼                    │
│                                 ┌─────────────────┐           │
│                                 │  ConsumerServer │           │
│                                 │  (port 8081)    │           │
│                                 └─────────────────┘           │
│                                                                │
│  ┌──────────────┐  scrape /actuator/prometheus                │
│  │  Prometheus  │ ──────────────────────────────► :8080/:8081 │
│  │  (port 9090) │                                             │
│  └──────┬───────┘                                             │
│         │ datasource                                          │
│         ▼                                                     │
│  ┌──────────────┐                                             │
│  │   Grafana    │◄── React footer link                        │
│  │  (port 3001) │                                             │
│  └──────────────┘                                             │
└────────────────────────────────────────────────────────────────┘
```

## Architectural Patterns Used

| Pattern | Where | Why |
| --- | --- | --- |
| Event-Driven Architecture | Kafka producer/consumer | Decouples emission from processing |
| Microservices | Two separate Spring Boot apps | Independent scaling and deployment |
| Dependency Injection | Spring constructor injection | Testability, Spring manages lifecycle |
| MVC (partial) | ProducerServer: Controller + Model | Standard REST pattern |
| Singleton | MeterRegistry, KafkaTemplate | Spring bean scope = singleton |
| Observer (via Kafka) | @KafkaListener | Consumer notified when producer publishes |
| CORS Filter | CorsConfig in both servers | Security boundary for cross-origin HTTP |

---

# 3. 📁 Folder Structure

```
Kafka Mini Proj/
│
├── ProducerServer/                       Spring Boot REST + Kafka producer
│   ├── app/
│   │   ├── build.gradle                  deps: spring-web, kafka, actuator, micrometer
│   │   └── src/main/java/org/example/
│   │       ├── App.java                  @SpringBootApplication entry point
│   │       ├── config/
│   │       │   └── CorsConfig.java       CORS filter bean (whitelist :3001)
│   │       ├── controller/
│   │       │   └── ProducerController.java   POST /producer/event
│   │       └── model/
│   │           └── Event.java            POJO: type + payload
│   │   └── src/main/resources/
│   │       └── application.properties    port 8080, kafka, actuator config
│   ├── gradle/libs.versions.toml         version catalog (guava, junit)
│   └── settings.gradle                   root = ProducerConsumer, includes 'app'
│
├── ConsumerServer/                       Spring Boot Kafka consumer + metrics
│   ├── build.gradle                      deps: spring-web, kafka, actuator, micrometer
│   └── src/main/java/org/example/
│       ├── App.java                      @SpringBootApplication entry point
│       ├── config/
│       │   └── CorsConfig.java           identical CORS filter
│       ├── consumer/
│       │   └── EventConsumer.java        @KafkaListener on topic "testy"
│       └── model/
│           └── Event.java                identical POJO
│       └── src/main/resources/
│           └── application.properties    port 8081, kafka consumer group config
│
└── landing-page/                         React CRA frontend
    ├── package.json                      React 18, react-scripts 5
    └── src/
        ├── App.js                        root component → LandingPage
        ├── LandingPage.js                event buttons, fetch calls, counters
        └── LandingPage.css               dark-theme grid layout
```

### Folder Responsibilities

| Folder | Layer | Responsibility |
| --- | --- | --- |
| `controller/` | Web (MVC Controller) | Receives HTTP, validates, delegates to Kafka |
| `config/` | Cross-cutting | CORS configuration applied globally |
| `model/` | Domain | Event POJO used across HTTP and Kafka |
| `consumer/` | Messaging | Kafka listener, deserializes, emits metrics |
| `src/` (React) | Presentation | UI, state management, fetch calls |
| `resources/` | Config | application.properties for each service |

---

# 4. 🔄 Request Flows

## Flow 1 — Happy Path: User Clicks "Add to Cart"

```
1. User clicks "🛒 Add to Cart"
   └─ LandingPage.js: emitEvent("addToCart")
      └─ setStatuses({ addToCart: 'loading' })   // button = "Sending..."

2. fetch("http://localhost:8080/producer/event", POST)
   body: { "type": "addToCart", "payload": "react-ui" }

3. ProducerController.java: sendEvent(Event event)
   ├─ Spring deserializes JSON body → Event POJO
   ├─ Validates: event.getType() not null and not blank ✓
   ├─ objectMapper.writeValueAsString(event)
   │  → '{"type":"addToCart","payload":"react-ui"}'
   ├─ kafkaTemplate.send("testy", "addToCart", jsonString)
   └─ Counter("producer_events_sent_total").tag("event_type","addToCart").increment()
   └─ return HTTP 200 "Event sent: addToCart"

4. LandingPage.js receives 200
   ├─ counts.addToCart += 1
   ├─ statuses.addToCart = 'success'    // card glows green
   └─ setTimeout 1500ms → statuses.addToCart = null

5. [Async] EventConsumer.java: consume(String message)
   ├─ objectMapper.readValue(message, Event.class)
   │  → Event{type='addToCart', payload='react-ui'}
   ├─ System.out.println("Consumed event: ...")
   └─ Counter("consumer_events_received_total").tag("event_type","addToCart").increment()

6. [Periodic] Prometheus scrapes /actuator/prometheus
   └─ Grafana panel shows both counters = 1
```

## Flow 2 — Validation Failure: Missing Event Type

```
POST /producer/event   body: { "payload": "react-ui" }
   └─ ProducerController: event.getType() == null → true
      └─ return HTTP 400 "Event type is required"
         └─ LandingPage: response.ok = false → throw Error
            └─ statuses[eventType] = 'error'   // card glows red
```

## Flow 3 — Consumer Deserialization Failure

```
Kafka delivers malformed message to EventConsumer
   └─ objectMapper.readValue() throws JsonProcessingException
      └─ catch block:
         ├─ System.err.println("Error processing message: ...")
         └─ Counter("consumer_events_failed_total").increment()
            └─ ⚠️ Message offset is committed — message is LOST, not retried
```

## Flow 4 — Prometheus Scrape

```
Prometheus (every N seconds):
   GET http://localhost:8080/actuator/prometheus
   GET http://localhost:8081/actuator/prometheus
   └─ Returns OpenMetrics text:
      producer_events_sent_total{event_type="addToCart",...} 3.0
      consumer_events_received_total{event_type="addToCart",...} 3.0
      └─ Stored in Prometheus TSDB
         └─ Grafana queries via PromQL → panels update
```

---

# 5. 📄 File-by-File Breakdown

## ProducerServer

### `App.java`

```java
@SpringBootApplication
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}
```

- `@SpringBootApplication` = `@Configuration` + `@EnableAutoConfiguration` + `@ComponentScan`
- Triggers classpath scan of `org.example` and all subpackages
- Auto-configures `KafkaTemplate`, embedded Tomcat, Actuator, Micrometer

### `CorsConfig.java`

```java
@Configuration
public class CorsConfig {
    @Bean
    public CorsFilter corsFilter() {
        config.addAllowedOrigin("http://localhost:3001"); // ⚠️ Should also include :3000
        config.addAllowedHeader("*");
        config.addAllowedMethod("*");
        config.setAllowCredentials(true);
    }
}
```

- Applies CORS rules globally to all endpoints (`/**`)
- Uses `CorsFilter` bean (not `@CrossOrigin`) so it runs before Spring Security
- **Bug:** React UI is on `:3000` but only `:3001` (Grafana) is whitelisted

### `ProducerController.java` — The Core

```java
@RestController
@RequestMapping("/producer")
public class ProducerController {

    private static final String TOPIC_NAME = "testy";  // line 15 — hardcoded

    // Constructor injection — testable, makes dependencies explicit
    public ProducerController(KafkaTemplate<String, String> kafkaTemplate,
                              MeterRegistry meterRegistry) { ... }

    @PostMapping("/event")
    public ResponseEntity<String> sendEvent(@RequestBody Event event) {
        // Line 31: validation
        if (event.getType() == null || event.getType().isBlank())
            return ResponseEntity.badRequest().body("Event type is required");

        // Line 35: serialize
        String payload = objectMapper.writeValueAsString(event);

        // Line 36: publish — returns CompletableFuture (NOT awaited — bug)
        kafkaTemplate.send(TOPIC_NAME, event.getType(), payload);

        // Lines 38-42: metric
        Counter.builder("producer_events_sent_total")
               .tag("event_type", event.getType())
               .register(meterRegistry).increment();

        return ResponseEntity.ok("Event sent: " + event.getType());
    }
}
```

**Key insight:** `kafkaTemplate.send()` is non-blocking. The future is ignored, so a Kafka failure is invisible to the caller — the API still returns 200 OK.

### `Event.java` (shared model)

```java
public class Event {
    private String type;    // "pageView" | "userClick" | "addToCart" | "checkout"
    private String payload; // arbitrary metadata ("react-ui" hardcoded from UI)

    // manual getters, setters, toString — Lombok is declared but unused
}
```

**Note:** Identical class exists in both ProducerServer and ConsumerServer. In production this would be a shared library or Avro schema.

### `application.properties` — ProducerServer

```properties
server.port=8080
spring.kafka.bootstrap-servers=172.24.236.246:9092  # hardcoded WSL2 IP
spring.kafka.producer.key-serializer=...StringSerializer
spring.kafka.producer.value-serializer=...StringSerializer
management.endpoints.web.exposure.include=*          # exposes everything — dev only
management.endpoint.prometheus.enabled=true
management.metrics.export.prometheus.enabled=true
```

---

## ConsumerServer

### `EventConsumer.java` — The Core

```java
@Service
public class EventConsumer {

    @KafkaListener(topics = "testy", groupId = "metrics-consumer-group")
    public void consume(String message) {
        try {
            Event event = objectMapper.readValue(message, Event.class);
            System.out.println("Consumed event: " + event);  // no SLF4J — dev only

            Counter.builder("consumer_events_received_total")
                   .tag("event_type", event.getType())
                   .register(meterRegistry).increment();

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());

            // ⚠️ No re-throw = offset committed = message silently DROPPED
            Counter.builder("consumer_events_failed_total")
                   .register(meterRegistry).increment();
        }
    }
}
```

**What Spring Kafka does automatically:**
- Creates `ConcurrentMessageListenerContainer`
- Runs background thread polling the broker
- Handles partition assignment within consumer group
- Commits offsets after `consume()` returns

### `application.properties` — ConsumerServer

```properties
server.port=8081
spring.kafka.bootstrap-servers=172.24.236.246:9092
spring.kafka.consumer.group-id=metrics-consumer-group
spring.kafka.consumer.auto-offset-reset=earliest   # replay from beginning on first start
spring.kafka.consumer.key-deserializer=...StringDeserializer
spring.kafka.consumer.value-deserializer=...StringDeserializer
management.endpoints.web.exposure.include=*
management.endpoint.prometheus.enabled=true
management.metrics.export.prometheus.enabled=true
```

---

## Frontend

### `LandingPage.js` — The Entire UI

```javascript
const EVENTS = [
  { type: 'pageView',  label: '👁 Page View',  desc: 'Track a page visit' },
  { type: 'userClick', label: '🖱 User Click',  desc: 'Buy a course click' },
  { type: 'addToCart', label: '🛒 Add to Cart', desc: 'Add course to cart' },
  { type: 'checkout',  label: '💳 Checkout',    desc: 'Complete a purchase' },
];

const emitEvent = async (eventType) => {
    setStatuses(prev => ({ ...prev, [eventType]: 'loading' }));
    try {
        const response = await fetch('http://localhost:8080/producer/event', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type: eventType, payload: 'react-ui' }),
        });
        if (!response.ok) throw new Error('Failed');
        setCounts(prev => ({ ...prev, [eventType]: (prev[eventType] || 0) + 1 }));
        setStatuses(prev => ({ ...prev, [eventType]: 'success' }));
    } catch (err) {
        setStatuses(prev => ({ ...prev, [eventType]: 'error' }));
    } finally {
        setTimeout(() => setStatuses(prev => ({ ...prev, [eventType]: null })), 1500);
    }
};
```

**State design:**
- `counts` — `{ addToCart: 3, checkout: 1 }` — resets on page refresh
- `statuses` — `{ addToCart: 'loading' | 'success' | 'error' | null }` — drives button visual

**Footer links:** `localhost:3001` → Grafana | `localhost:9090` → Prometheus

---

# 6. 🌐 APIs

## POST /producer/event

**Base URL:** `http://localhost:8080`

**Request:**

```json
POST /producer/event
Content-Type: application/json

{
  "type": "checkout",
  "payload": "react-ui"
}
```

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `type` | String | Yes | Used as Kafka key + metric tag |
| `payload` | String | No | Arbitrary metadata |

**Responses:**

| Status | Body | Condition |
| --- | --- | --- |
| 200 OK | `"Event sent: checkout"` | Kafka publish attempted |
| 400 Bad Request | `"Event type is required"` | type is null or blank |
| 500 Internal Server Error | `"Failed to send event: ..."` | Serialization or sync Kafka failure |

**⚠️ Important:** 200 OK does NOT guarantee Kafka delivery — the `CompletableFuture` from `kafkaTemplate.send()` is not awaited.

## Spring Actuator Endpoints (both servers)

| Endpoint | Description |
| --- | --- |
| `GET /actuator/health` | App health status |
| `GET /actuator/prometheus` | **Primary** — OpenMetrics scrape target |
| `GET /actuator/metrics` | All registered metric names |
| `GET /actuator/env` | ⚠️ Exposes environment variables — disable in prod |

---

# 7. 📨 Messaging (Kafka)

## Topic Configuration

| Property | Value |
| --- | --- |
| Topic name | `testy` |
| Broker | `172.24.236.246:9092` |
| Key serializer | `StringSerializer` |
| Value serializer | `StringSerializer` |
| Consumer group | `metrics-consumer-group` |
| Auto offset reset | `earliest` |

## Message Format

```
Key:   addToCart
Value: {"type":"addToCart","payload":"react-ui"}
```

- **Key** = `event.getType()` → Kafka uses this for partition routing (consistent hashing)
- **Value** = JSON-serialized Event object
- Same key → same partition → ordering guaranteed per event type

## Event Types

| Type | Description |
| --- | --- |
| `pageView` | User visited the page |
| `userClick` | User clicked a course |
| `addToCart` | User added course to cart |
| `checkout` | User completed a purchase |

## Metrics Created

**ProducerServer:**

```
producer_events_sent_total{event_type="pageView|userClick|addToCart|checkout"}
```

**ConsumerServer:**

```
consumer_events_received_total{event_type="..."}
consumer_events_failed_total{}
```

## Error Handling Gaps

| Component | Current Behavior | Production Fix |
| --- | --- | --- |
| Producer | `CompletableFuture` ignored — silent failure | `.whenComplete((r,ex) -> log/alert)` |
| Consumer | Bad message logged, offset committed — message lost | `@RetryableTopic` with DLT |
| Consumer | No retry on deserialization failure | Configure retry with backoff |

## Why Kafka vs Alternatives

| Criteria | Kafka | RabbitMQ | SQS |
| --- | --- | --- | --- |
| Message retention | ✅ Configurable | ❌ Deleted on ACK | ✅ 14 days |
| Replay | ✅ Yes | ❌ No | ❌ No |
| Multiple consumers | ✅ Consumer groups | ⚠️ Exchange bindings | ⚠️ SNS fan-out |
| Ordering | ✅ Per partition | ✅ Per queue | ❌ Best-effort |
| Throughput | ✅ Very high | ✅ High | ✅ High |

---

# 8. 🗄 Database

**This project has no database.** All state is ephemeral.

The `build.gradle` includes `mysql-connector-java:8.0.33` — this is a **dead dependency**. No JPA, no datasource URL, no entities exist anywhere.

| Data | Lives In | Durability |
| --- | --- | --- |
| Event counts | React `useState` | Lost on page refresh |
| Kafka messages | Kafka topic "testy" | Until retention expires |
| Micrometer metrics | In-memory MeterRegistry | Lost on JVM restart |
| Prometheus time-series | Prometheus TSDB | Durable per Prometheus config |

**Why no database?** Prometheus serves as the time-series store. The goal is to demonstrate the streaming pipeline, not persistence.

---

# 9. ⚙️ Tech Stack Deep Dive

## Spring Boot 3.2.2

- `@SpringBootApplication` → component scan + auto-configure + start embedded Tomcat
- Auto-configures `KafkaTemplate` when `spring-kafka` is on classpath + `bootstrap-servers` is set
- **Why here:** Minimal boilerplate; starters handle Kafka, Actuator, Jackson with zero config

## Apache Kafka

- Distributed, append-only, partitioned log
- Producer: `kafkaTemplate.send(topic, key, value)` → message appended to partition
- Consumer: polls broker with stored offset; Spring Kafka handles the loop automatically
- **Key interview concepts:** consumer group, offset, partition, at-least-once delivery, log compaction

## Micrometer

- Vendor-neutral metrics facade (like SLF4J but for metrics)
- Write `Counter.builder().tag().register().increment()` once — works with any backend
- Switch Prometheus → Datadog by changing one dependency, zero code changes
- **Pattern used:** Adapter (abstracts away the backend)

## Prometheus

- Pull-based time-series database — scrapes `/actuator/prometheus` every N seconds
- PromQL examples for this project:
    - `producer_events_sent_total` — raw total
    - `rate(producer_events_sent_total[1m])` — per-second rate
    - `sum by(event_type)(consumer_events_received_total)` — grouped by type

## React 18 + Create React App

- `useState` manages local event counts and button states — no Redux needed
- `fetch` API fires HTTP POST to ProducerServer
- State is in-memory only — resets on page refresh
- **CRA 5** = Webpack 5 + Babel + ESLint bundled

## Gradle 8.6

- ProducerServer: multi-module setup (`settings.gradle` includes `app`)
- ConsumerServer: single-module
- Version catalog (`libs.versions.toml`) centralizes dependency versions

## Dead / Unused Dependencies

| Dependency | Declared | Used | Verdict |
| --- | --- | --- | --- |
| `lombok` | Both servers | ❌ No | Remove |
| `mysql-connector-java` | ProducerServer | ❌ No | Remove |
| `guava` | ProducerServer | ❌ No | Remove |
| `spring-cloud-starter-bootstrap` | ProducerServer | ❌ No | Remove |
| `io.prometheus:simpleclient` | ProducerServer | ❌ Redundant | Remove (Micrometer wraps it) |

---

# 10. 🔒 Security

## Current Status: Development-Grade Only

### Risk Summary

| Risk | Severity | Fix |
| --- | --- | --- |
| No authentication on API | 🔴 High | Add JWT or API key |
| No Kafka authentication | 🔴 High | SASL + SSL on broker |
| CORS wrong origin (:3000 not whitelisted) | 🟡 Medium | Add localhost:3000 |
| Hardcoded Kafka broker IP | 🟡 Medium | Use env var `${KAFKA_BOOTSTRAP_SERVERS}` |
| All Actuator endpoints exposed | 🟡 Medium | Limit to `health,prometheus` |
| No input size validation | 🟡 Medium | Bean Validation `@Size` on Event fields |
| No TLS on HTTP or Kafka | 🟡 Medium | nginx reverse proxy with TLS |
| Error message leaks internals | 🟢 Low | Return generic "Internal server error" |

### The CORS Bug

```java
// CorsConfig.java in both servers
config.addAllowedOrigin("http://localhost:3001");  // Grafana ✅
// Missing:
// config.addAllowedOrigin("http://localhost:3000");  // React UI ❌
```

React at `:3000` calling Spring at `:8080` = cross-origin. Without `:3000` in the whitelist, strict browsers block the request.

### Actuator Exposure Fix (for production)

```properties
# Replace this:
management.endpoints.web.exposure.include=*

# With this:
management.endpoints.web.exposure.include=health,prometheus
management.endpoints.web.exposure.exclude=env,heapdump,threaddump
```

### API Security Fix (production)

Add `spring-boot-starter-security` + `spring-security-oauth2-resource-server`, then require Bearer token on `/producer/event`.

---

# 11. 🚀 Production Readiness

## Strengths

- ✅ Separation of concerns — Producer and Consumer are fully independent services
- ✅ Metrics tagged from day one — easy Grafana dashboards without metric redesign
- ✅ `auto-offset-reset=earliest` — no events missed on consumer restart
- ✅ Input validation exists on required field
- ✅ Java 21 LTS — supports virtual threads if needed
- ✅ Clean, readable code — small classes, single responsibility

## Gaps (Prioritized)

### Priority 1 — Correctness (Fix Before Showing to Anyone)

- ❌ `kafkaTemplate.send()` future not awaited — 200 OK ≠ event delivered
- ❌ Consumer swallows bad messages — data lost silently
- ❌ CORS wrong origin — React UI calls may fail in strict browsers
- ❌ `AppTest.java` calls `getGreeting()` which doesn't exist — broken test

### Priority 2 — Operability

- ❌ `System.out.println` — no log levels, no structured logs
- ❌ No Docker Compose — manual 6-step startup
- ❌ Hardcoded Kafka IP — breaks on every machine
- ❌ Dead dependencies (Lombok, MySQL, Guava, Spring Cloud Bootstrap)

### Priority 3 — Security

- ❌ No auth on REST API
- ❌ All Actuator endpoints open
- ❌ No input size limits

### Priority 4 — Scale

- ❌ Topic likely has 1 partition (default) — no parallelism
- ❌ No consumer lag monitoring
- ❌ No Dead Letter Topic

## If I Built This for Production

```
Producer (3 instances) → Load Balancer
   ↓
Kafka Cluster (3 brokers, 10 partitions)
   ↓              ↓ Dead Letter Topic
Consumer Group    DLT Consumer
(10 instances)       ↓
   ↓             Alert/Replay Service
PostgreSQL (audit log)
   ↓
Prometheus → Grafana → AlertManager → PagerDuty
```

---

# 12. 💻 Commands

## Start Kafka (Docker)

```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  bitnami/kafka:latest
```

## Create the Kafka Topic

```bash
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --topic testy

# Verify
kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Run ProducerServer

```bash
cd ProducerServer
./gradlew :app:bootRun

# Or via JAR
./gradlew build
java -jar app/build/libs/app.jar

# Override Kafka broker
java -jar app/build/libs/app.jar --spring.kafka.bootstrap-servers=localhost:9092
```

**Starts on:** `http://localhost:8080`

## Run ConsumerServer

```bash
cd ConsumerServer
./gradlew bootRun
```

**Starts on:** `http://localhost:8081`

## Run React Frontend

```bash
cd landing-page
npm install
npm start
```

**Opens on:** `http://localhost:3000`

## Start Prometheus

Create `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'producer-server'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: /actuator/prometheus

  - job_name: 'consumer-server'
    static_configs:
      - targets: ['localhost:8081']
    metrics_path: /actuator/prometheus
```

```bash
docker run -d --name prometheus -p 9090:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus
```

**UI:** `http://localhost:9090`

## Start Grafana

```bash
docker run -d --name grafana -p 3001:3000 grafana/grafana
```

**UI:** `http://localhost:3001` (login: admin/admin)
Add Prometheus datasource: `http://localhost:9090`

## Test the API

```bash
# Send a checkout event
curl -X POST http://localhost:8080/producer/event \
  -H "Content-Type: application/json" \
  -d '{"type":"checkout","payload":"curl-test"}'

# Check ProducerServer metrics
curl http://localhost:8080/actuator/prometheus | grep producer_events

# Check ConsumerServer metrics
curl http://localhost:8081/actuator/prometheus | grep consumer_events

# Watch Kafka topic live
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic testy \
  --from-beginning \
  --property print.key=true
```

---

# 13. 🎓 Interview Q&A (105 Questions)

## Beginner (Spring Boot / Java)

**Q: What does `@SpringBootApplication` do?**
It's a composed annotation = `@Configuration` + `@EnableAutoConfiguration` + `@ComponentScan`. It tells Spring to scan the current package for beans, auto-configure based on classpath, and mark the class as a configuration source.

**Q: What is `@RestController` vs `@Controller`?**
`@RestController` = `@Controller` + `@ResponseBody`. Every method return value is serialized directly to the HTTP response body via Jackson. `@Controller` is for MVC returning view names.

**Q: Why use constructor injection instead of `@Autowired` field injection?**
Constructor injection is testable (pass mocks directly), makes dependencies explicit and required, and supports `final` fields for immutability.

**Q: What does `@RequestBody` do?**
Tells Spring MVC to deserialize the HTTP request body (JSON) into the annotated parameter type using Jackson's `ObjectMapper`.

**Q: What is `ResponseEntity`?**
A Spring class representing a full HTTP response — status code, headers, and body. Allows fine-grained control: `ResponseEntity.ok("message")`, `ResponseEntity.badRequest().body("error")`.

**Q: What does the Actuator do?**
Adds production-ready endpoints: `/actuator/health`, `/actuator/metrics`, `/actuator/prometheus`. Enabled by adding `spring-boot-starter-actuator` to the build.

---

## Beginner (Kafka)

**Q: What is Apache Kafka?**
A distributed, fault-tolerant message broker. Records are written to topics (organized into partitions) and retained for a configurable period. Consumers read at their own pace using stored offsets.

**Q: What is a Kafka partition?**
A topic is split into N partitions. Each partition is an ordered, immutable sequence of records. Records with the same key always go to the same partition (consistent hashing of the key).

**Q: Why is `event.getType()` used as the Kafka message key?**
To ensure all events of the same type land on the same partition, preserving per-type ordering. All `checkout` events are processed in the order they were created.

**Q: What is a Consumer Group?**
A set of consumer instances that collectively consume all partitions of a topic. Each partition is assigned to exactly one consumer in the group. Scale by adding more instances.

**Q: What is `auto-offset-reset=earliest`?**
If no committed offset exists for this group, start from the oldest message in the topic. Alternative is `latest` (only new messages). Used here to ensure no events are missed on first startup.

**Q: What does `@KafkaListener` do?**
Spring Kafka annotation marking a method as a Kafka message handler. Spring creates a listener container that polls the broker and calls the annotated method with each record value.

**Q: What is `KafkaTemplate`?**
A Spring wrapper around the native Kafka `Producer`. Provides `send(topic, key, value)` and integrates with Spring's observability.

---

## Beginner (Metrics / React)

**Q: What is Micrometer?**
A vendor-neutral metrics facade. You write `Counter.builder("name").tag().register(registry).increment()` once; the registry formats it for whichever backend (Prometheus, Datadog) is on the classpath.

**Q: What is a Counter in Micrometer?**
A monotonically increasing metric. Only goes up. Used for total events, errors, requests.

**Q: Why tag metrics with `event_type`?**
Tags add dimensions. Without tags: only know total events. With `event_type` tag: query `producer_events_sent_total{event_type="checkout"}` independently.

**Q: What is `useState` in React?**
Hook for local component state. `const [counts, setCounts] = useState({})` — `counts` is current state, `setCounts` is the updater. State is in-memory; page refresh resets it.

**Q: Why does refreshing the React page reset event counts?**
Counts live in `useState` — React component state is not persisted. A page refresh creates a new component instance with `useState({})` starting fresh.

---

## Intermediate

**Q: Explain the CORS bug in this project.**
`CorsConfig` whitelists `http://localhost:3001` (Grafana), but React runs on `http://localhost:3000`. Browser fetch calls from React should be blocked by CORS pre-flight. Fix: add `localhost:3000` to allowed origins in both servers' `CorsConfig`.

**Q: `kafkaTemplate.send()` is non-blocking. What does that mean for error handling?**
`send()` returns a `CompletableFuture<SendResult>`. The current code ignores this future — Kafka failures are silently dropped and the API returns 200 OK. Fix: `.whenComplete((result, ex) -> { if (ex != null) log.error("Failed", ex); })`.

**Q: What is the difference between at-least-once, at-most-once, and exactly-once Kafka delivery?**
- **At-most-once:** Offset committed before processing — message may be lost if consumer crashes
- **At-least-once (default here):** Offset committed after processing — message may be redelivered if consumer crashes after processing but before commit
- **Exactly-once:** Requires Kafka transactions (`enable.idempotence=true`, `isolation.level=read_committed`)

**Q: What is a Dead Letter Topic and why is it missing here?**
A DLT is a Kafka topic where messages that fail processing are sent after N retries. Here, when `readValue()` throws, the exception is caught and the message is silently dropped. Fix: use `@RetryableTopic` or `DeadLetterPublishingRecoverer`.

**Q: Why are there two identical `Event.java` files?**
Both services need the model to serialize/deserialize Kafka messages. This is a maintenance risk — if you add a field in one but not the other, Jackson silently ignores the extra field. Production fix: shared library or Avro schema in Schema Registry.

**Q: What is `ConcurrentMessageListenerContainer`?**
Spring Kafka's default container for `@KafkaListener`. By default creates 1 thread. If topic has 3 partitions and `concurrency=3`, three threads each handle one partition.

**Q: Why does ConsumerServer have `server.port=8081` if it has no REST API?**
To expose `/actuator/prometheus` for Prometheus scraping. Without the web server, Prometheus can't scrape consumer metrics.

**Q: What happens if two ConsumerServer instances start in the same consumer group?**
Kafka triggers a rebalance and redistributes partitions between the consumers. This is how horizontal scaling works — add more instances to handle more partitions.

**Q: Why is `new ObjectMapper()` an anti-pattern in Spring?**
Spring Boot auto-configures an `ObjectMapper` bean with consistent settings (date format, modules, features). Using `new ObjectMapper()` bypasses these, creating potential serialization inconsistencies.

**Q: What does `management.metrics.export.prometheus.enabled=true` do?**
Registers the `PrometheusMeterRegistry` as the active Micrometer registry. All counters, gauges, and timers will be formatted in Prometheus exposition format at `/actuator/prometheus`.

---

## Advanced

**Q: Design a schema evolution strategy for the Event model.**
Use Apache Avro with Confluent Schema Registry. Define Event schema in `.avsc`. Use `AvroSerializer/AvroDeserializer`. Registry enforces backward/forward compatibility. Adding a new optional field is backward-compatible — old consumers ignore it.

**Q: How would you implement exactly-once semantics?**
Producer: `enable.idempotence=true`, `transactional.id` set. Consumer: `isolation.level=read_committed`, manual offset commit inside a Kafka transaction. Spring Kafka `@Transactional` with `KafkaTransactionManager`.

**Q: How would you scale to 100,000 events/second?**
1. Increase topic partitions to 10-20. 2. Run 10-20 ConsumerServer instances (each gets partitions). 3. Run 3-5 ProducerServer instances behind a load balancer. 4. Configure Kafka producer batching (`batch.size`, `linger.ms`). 5. Enable virtual threads: `spring.threads.virtual.enabled=true`.

**Q: The consumer uses `System.out.println`. What are the implications?**
No log levels (can't filter INFO vs ERROR), no structured logging (hard to parse/search), no correlation IDs (can't trace a single event). Fix: SLF4J + Logback, output JSON with `logstash-logback-encoder`.

**Q: How would you add distributed tracing?**
Add `micrometer-tracing-bridge-brave` (Zipkin) or `micrometer-tracing-bridge-otel` (OpenTelemetry). Micrometer Tracing auto-instruments Spring MVC and Spring Kafka. A trace ID propagates via Kafka record headers. Grafana Tempo visualizes the full span.

**Q: What is consumer group rebalancing and what problems can it cause?**
Triggered when consumers join/leave or partition count changes. During rebalancing, all consumption stops. Long rebalances cause cascading failures. Fix: use `CooperativeStickyAssignor`, keep `max.poll.records` small.

**Q: How would you implement a Dead Letter Topic?**
Option 1: `@RetryableTopic(attempts = 3, backoff = @Backoff(delay = 1000))` — Spring Kafka auto-creates retry and DLT topics.
Option 2: `SeekToCurrentErrorHandler` with `DeadLetterPublishingRecoverer`.

**Q: The Kafka topic name is hardcoded. How would you externalize it?**

```properties
# application.properties
kafka.topic.events=testy
```

```java
@Value("${kafka.topic.events}")
private String topicName;
```

**Q: What is Kafka's log compaction and would it help here?**
Log compaction retains only the latest record per key. **Not useful here** — events are facts, not state updates. Every `checkout` is meaningful. Log compaction is for changelog topics (CDC streams).

**Q: How would you add a new metric for average payload size?**

```java
DistributionSummary.builder("event_payload_size_bytes")
    .tag("event_type", event.getType())
    .register(meterRegistry)
    .record(payload.getBytes().length);
```

PromQL: `event_payload_size_bytes_sum / event_payload_size_bytes_count`

---

## System Design

**Q: Design the full observability stack.**
Metrics: Micrometer → Prometheus → Grafana (in place). Logging: SLF4J + Logback → Elasticsearch/Loki → Grafana. Tracing: Micrometer Tracing + Zipkin/Jaeger → Grafana Tempo. Alerting: Prometheus AlertManager → PagerDuty/Slack.

**Q: How would you support 10 different event consumers?**
Each gets its own consumer group. Kafka delivers each message to all consumer groups independently. Add `analytics-group`, `fraud-group`, `db-writer-group` — all receive all messages without changing the producer.

**Q: Design a replay mechanism for failed events.**
Store raw Kafka messages to S3 via Kafka Connect S3 Sink Connector. To replay: use Kafka's `--from-beginning` or `consumer.offsetsForTimes()`. Alternatively, emit failed events to `testy.DLT` which a replay service republishes to `testy`.

**Q: How would you make this system GDPR-compliant?**
Personal data should not be in the Kafka payload. If user IDs are added: use crypto-shredding (encrypt each user's events with a per-user key, delete the key for erasure). Add topic-level retention policy (`retention.ms`).

**Q: How would you monitor consumer lag?**
`kafka_consumer_fetch_manager_records_lag` auto-exported by Micrometer Kafka binder. Or `kafka-consumer-groups.sh --describe`. Alert in Prometheus when lag > 10,000 messages.

---

## Follow-Up / Curveball

**Q: "Won't `auto-offset-reset=earliest` cause duplicate processing every restart?"**
Only on the first run (before any offset is committed). After the first message is consumed, the group's offset is committed to `__consumer_offsets`. On subsequent restarts, Kafka resumes from the last committed offset.

**Q: "Micrometer counters reset on JVM restart. How does Prometheus handle that?"**
Prometheus detects counter resets (when current value < previous value). `rate()` and `increase()` functions account for resets automatically. Your data is not lost — Prometheus handles the math.

**Q: "What would break first if you ran 3 ConsumerServer instances against 1 Kafka partition?"**
Two of three consumers would be idle. Kafka assigns at most one consumer per partition per group. To parallelize: create the topic with 3 partitions.

**Q: "Why not use WebSockets instead of REST + Kafka?"**
WebSockets are browser ↔ server. Kafka is server ↔ server. They solve different problems. You could add WebSockets to push Kafka events to the browser in real-time, but Kafka is still the right choice for server-side fan-out.

**Q: "What is the biggest design flaw in this project?"**
The producer returns 200 OK before confirming Kafka delivery. `kafkaTemplate.send()` is fire-and-forget. The React UI shows ✅ Sent! but no event may have been published. Fix: await the `CompletableFuture` or add `.whenComplete()` callback and return 500 on failure.

**Q: "How would you containerize this with Docker?"**

```dockerfile
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY app/build/libs/app.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Then `docker-compose.yml` linking kafka, producer, consumer, prometheus, grafana with environment variables for `KAFKA_BOOTSTRAP_SERVERS`.

**Q: "What would you add to make this project showcase-ready on your resume?"**
1. Docker Compose (one-command startup). 2. Pre-built Grafana dashboard (JSON provisioning). 3. GitHub Actions CI (`./gradlew test`). 4. Fix the CORS bug. 5. Await Kafka future in producer. 6. `@RetryableTopic` for DLT. 7. Replace `println` with structured logging. 8. Remove dead dependencies. 9. Integration tests with `@EmbeddedKafka`. 10. README with architecture diagram.

---

# 14. 🎤 Interview Scripts

## 30-Second Elevator Pitch

> "I built a real-time event tracking pipeline for a course platform. Users click buttons on a React UI — things like 'Add to Cart' or 'Checkout' — and those events flow through Apache Kafka to a Spring Boot consumer that exposes Prometheus metrics. The whole thing is visible in a Grafana dashboard within seconds. The goal was to learn how event-driven systems work end-to-end, from the browser all the way to the observability stack."

---

## 2-Minute Summary

> "I built a full-stack event streaming system to understand how production analytics pipelines work in practice.
>
> The system has three main components. A React frontend that simulates a course platform where users can trigger four types of events — page views, clicks, add-to-cart, and checkout. Each button click sends a POST request to my Spring Boot producer server.
>
> The producer validates the event, serializes it to JSON, and publishes it to a Kafka topic called 'testy', using the event type as the message key. Using the key ensures that all checkout events go to the same partition, preserving ordering per event type.
>
> On the other side, a second Spring Boot service listens to that Kafka topic and increments Micrometer counters tagged by event type. Both services expose a Prometheus scrape endpoint. Prometheus scrapes them, and Grafana shows the data in real time.
>
> One thing I discovered: `kafkaTemplate.send()` is non-blocking — it returns a `CompletableFuture`. In my current implementation I don't await it, which means a Kafka failure is invisible to the caller. In production I'd add a completion callback and handle delivery failures explicitly."

---

## 5-Minute Deep Dive

> "I built this to get hands-on experience with an event-driven microservices architecture.
>
> **The Problem:** Modern platforms generate thousands of user events per second. You need to capture events without blocking the user, process them independently, and make them visible to analysts in real time. Kafka solves this by acting as a durable buffer between event producers and consumers.
>
> **The Architecture:** Three services: React UI, Spring Boot producer on port 8080, Spring Boot consumer on port 8081. Kafka is the backbone.
>
> The React UI has four event-type buttons. Each click fires a POST to the producer. The producer validates that event type is not blank — since it's the Kafka key and Prometheus metric tag — serializes the Event object to JSON via Jackson, and calls `kafkaTemplate.send('testy', eventType, jsonPayload)`. I then increment a Micrometer counter tagged with the event type and return 200 OK.
>
> The consumer uses `@KafkaListener`. Spring Kafka handles all the polling, partition assignment, and offset management automatically. Inside `consume()`, I deserialize the JSON back to an Event POJO and increment a counter. If deserialization fails, I catch the exception, log it, and increment a failure counter. The trade-off: failed messages are silently dropped. In production I'd fix this using `@RetryableTopic` with a Dead Letter Topic.
>
> **Observability:** Both servers expose `/actuator/prometheus`. By adding the Micrometer Prometheus dependency and two properties in `application.properties`, Spring Boot automatically formats all metrics in Prometheus text format. Prometheus scrapes both servers; Grafana shows real-time breakdowns by event type.
>
> **What I'd change for production:** Await the Kafka `CompletableFuture` in the producer, add Dead Letter Topic handling, add JWT auth to the REST API, extract the topic name and broker address to environment variables, and write integration tests using `@EmbeddedKafka`."

---

## 10-Minute Architecture Discussion

> *(Deliver the 5-minute version, then continue:)*
>
> **Why Kafka vs RabbitMQ:** Kafka retains messages on disk regardless of consumption — if I add a new consumer service later, it can replay all historical events. RabbitMQ deletes messages after acknowledgment. Kafka's consumer group model lets me scale consumers by adding instances — each gets assigned a partition automatically.
>
> **JSON vs Avro:** I chose JSON strings for simplicity — human-readable, easy to debug with `kafka-console-consumer`. The tradeoff: JSON is verbose. In a production high-throughput system I'd use Avro with Confluent Schema Registry for schema evolution guarantees and 10x better compression.
>
> **The CORS Discovery:** My CORS config whitelists `http://localhost:3001` (Grafana), but React runs on `http://localhost:3000`. Browser fetch calls from React should fail the CORS preflight. In practice development servers are sometimes lenient about localhost, but this is a real bug that would surface in production.
>
> **Micrometer as Abstraction:** I used Micrometer rather than the Prometheus Java client directly. It's the vendor-neutral metrics facade — like SLF4J for metrics. If I wanted to switch from Prometheus to Datadog tomorrow, I'd change one dependency and one config property, with zero code changes. The `Counter.builder().tag().register().increment()` pattern is backend-agnostic.
>
> **Scaling Path:** 10,000 events/second: increase topic partitions to 10-20, run 10-20 consumer instances (each gets a partition), run 3-5 producer instances behind a load balancer, enable Kafka producer batching with `linger.ms=5`. 100,000 events/second: move to Avro serialization, add consumer lag alerting, consider Kafka Streams for real-time aggregations.
>
> **What's Next:** A persistence layer — a ConsumerServer variant that writes events to PostgreSQL for business intelligence queries. A Kafka Streams application to compute real-time rolling 5-minute conversion rates using windowed aggregations."

---

# 15. ❓ Things Not in the Code

## Unknowns About the Infrastructure

- How is the Kafka broker at `172.24.236.246` configured? (WSL2, Docker, remote VM?)
- What Kafka version? (Affects KRaft vs Zookeeper support)
- How many partitions does `testy` have? (Likely 1 — the default)
- What is the message retention policy?
- Is Prometheus actually configured to scrape both servers? (No `prometheus.yml` in repo)
- What Grafana dashboards were built? (No dashboard JSON in repo)
- Why port 3001 for Grafana instead of the default 3000?

## Business Context Questions

- Is this a learning project or a real product foundation?
- What does "course platform" mean — is there a real e-commerce flow planned?
- Why is the payload hardcoded to `"react-ui"` — is this meant to identify the source system?
- Were the dead dependencies (Lombok, MySQL, Spring Cloud) planned for future features?

## Assumptions Made in This Documentation

1. The system was built as a **learning project**
2. The broker at `172.24.236.246:9092` is a **local WSL2** instance
3. Topic `testy` has **1 partition** (the default when created without specifying)
4. **No authentication** is configured on Kafka or the Spring Boot services
5. Prometheus and Grafana are run **externally**, not managed by this repo
6. `AppTest.java` was **never fixed** — it's a discarded Gradle scaffold

## Questions for the Original Developer

1. Was Kafka ever successfully connected and were events visible in Grafana end-to-end?
2. What is the next planned feature — database persistence, real auth, real cart flow?
3. Why React (CRA) rather than a simpler HTML+JS page — was a richer UI planned?
4. Was `testy` meant as a temporary name that was never changed?

---

# 📌 Pre-Interview Cheat Sheet

## The 3 Things That Impress Interviewers

**1. Kafka delivery guarantee insight:**
> "I noticed `kafkaTemplate.send()` returns a `CompletableFuture` that I don't await — so the producer returns 200 OK even if Kafka is unreachable. In production I'd handle that with `.whenComplete()` and return 500 on delivery failure."

**2. Consumer error handling:**
> "The consumer silently drops malformed messages because the catch block doesn't re-throw. A production fix would use `@RetryableTopic` to route failures to a Dead Letter Topic after N retries, so no events are permanently lost."

**3. Kafka partitioning knowledge:**
> "I use the event type as the Kafka message key, which guarantees all checkout events land on the same partition and are processed in order. That's the key design choice for any consumer that needs sequential state per event type."

## Key Numbers to Remember

| Thing | Value |
| --- | --- |
| Producer port | 8080 |
| Consumer port | 8081 |
| Kafka topic | testy |
| Consumer group | metrics-consumer-group |
| Producer metric | producer_events_sent_total |
| Consumer metric | consumer_events_received_total |
| Event types | pageView, userClick, addToCart, checkout |
| Spring Boot version | 3.2.2 |
| Java version | 21 |

## If You Have 1 Hour Before the Interview

1. Re-read this document top to bottom (30 min)
2. Practice the 2-minute pitch out loud until smooth (10 min)
3. Be able to draw the architecture diagram from memory (5 min)
4. Memorize the 3 impressive insights above (5 min)
5. Be ready to say "the biggest gap is the un-awaited Kafka future" (10 min practice)
