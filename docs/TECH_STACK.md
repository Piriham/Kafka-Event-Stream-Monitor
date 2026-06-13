# Technology Stack

---

## 1. Spring Boot 3.2.2

### What is it?
A convention-over-configuration framework that turns a plain Java application into a production-ready server with minimal boilerplate. It auto-configures Kafka, Actuator, Jackson, and embedded Tomcat based on classpath presence.

### Why used here?
- One annotation (`@SpringBootApplication`) starts the entire application context.
- `spring-kafka` starter auto-creates `KafkaTemplate` and listener containers from `application.properties`.
- `spring-boot-starter-actuator` exposes `/actuator/prometheus` for free.

### Internal Working
1. `SpringApplication.run(App.class, args)` triggers classpath scanning.
2. `@SpringBootApplication` = `@Configuration` + `@EnableAutoConfiguration` + `@ComponentScan`.
3. Auto-configuration detects `spring-kafka` on classpath → creates `KafkaTemplate` bean.
4. Embedded Tomcat starts on configured port.

### Alternatives
- Quarkus (faster startup, GraalVM native), Micronaut (compile-time DI), plain Spring MVC.

### Interview Questions
- "What does `@SpringBootApplication` actually do?" → It's a composite annotation enabling component scan, auto-configuration, and marking the class as a configuration source.
- "How does Spring Boot know to create a KafkaTemplate?" → Auto-configuration class `KafkaAutoConfiguration` is triggered when `spring-kafka` is on the classpath and `spring.kafka.bootstrap-servers` is set.

---

## 2. Apache Kafka

### What is it?
A distributed, append-only, partitioned log used as a message broker. Producers write records to topics; consumers read at their own pace using stored offsets.

### Why used here?
- Decouples event emission (producer) from processing (consumer).
- Kafka retains messages — consumers can be restarted and replay from any offset.
- Scales horizontally by adding partitions and consumer instances.

### Internal Working
1. Producer sends `ProducerRecord(topic, key, value)` to a broker partition (key → hash → partition index).
2. Broker appends record to the partition log on disk.
3. Consumer polls broker with its group offset; broker returns new records.
4. Consumer commits offset after processing.

### Key Concepts for Interviews
- **Consumer Group**: Multiple consumers sharing a group each get exclusive access to a subset of partitions. Here `metrics-consumer-group` has one consumer reading all partitions.
- **Offset**: Each record in a partition has a monotonically increasing offset. `auto-offset-reset=earliest` means start from offset 0 on first run.
- **Retention**: By default Kafka keeps messages for 7 days regardless of consumption.
- **At-least-once delivery**: The current consumer acknowledges after the listener method returns (Spring default `ack-mode=BATCH`). If the JVM crashes mid-processing, the message may be re-delivered.

### Alternatives
- RabbitMQ (AMQP, push-based, better for task queues), AWS SQS/SNS (managed, serverless), Redis Streams (lightweight), Google PubSub.

---

## 3. Micrometer + Prometheus Registry

### What is it?
**Micrometer** is a vendor-neutral metrics facade (like SLF4J for logging, but for metrics). The `micrometer-registry-prometheus` adapter formats metrics in the Prometheus text exposition format.

### Why used here?
- Zero-config integration with Spring Boot Actuator — add the dependency and `/actuator/prometheus` lights up.
- Metrics are tagged (dimensional) — `producer_events_sent_total{event_type="checkout"}` is independently queryable.
- Industry standard for cloud-native observability.

### Metrics Created in This Project

**ProducerServer:**
```
producer_events_sent_total{event_type="pageView|userClick|addToCart|checkout",...}
```

**ConsumerServer:**
```
consumer_events_received_total{event_type="..."}
consumer_events_failed_total{}
```

Both created with the `Counter.builder(...).tag(...).register(meterRegistry).increment()` pattern.

### Internal Working
1. `MeterRegistry` (injected by Spring) holds all registered meters in memory.
2. `Counter.builder()` creates or retrieves an existing counter by name+tags.
3. `.increment()` atomically increments the counter value.
4. At scrape time, `/actuator/prometheus` serializes all meters to text format.

### Alternatives
- Micrometer with InfluxDB, Datadog, CloudWatch, or StatsD registries.
- Dropwizard Metrics (older, less Spring-integrated).

---

## 4. Prometheus

### What is it?
A time-series database and monitoring system that **pulls** (scrapes) metrics from HTTP endpoints at a configured interval (default 15s).

### Why used here?
- Pull model means services don't need to know about the monitoring server.
- PromQL is a powerful query language for aggregations, rates, and alerting.
- De-facto standard for Kubernetes/cloud-native monitoring.

### Key PromQL Examples for This Project
```promql
# Total events sent per type
producer_events_sent_total

# Rate of events per second over last 1 minute
rate(producer_events_sent_total[1m])

# Consumer lag (failed vs received)
consumer_events_failed_total / consumer_events_received_total
```

### Alternatives
- InfluxDB + Telegraf (push model), Datadog (SaaS), AWS CloudWatch.

---

## 5. Grafana

### What is it?
A dashboard and visualization platform. Queries Prometheus (or other datasources) and renders time-series graphs, counters, heatmaps.

### Why used here?
- Connects to Prometheus as a datasource.
- Provides pre-built panel types for counters (stat, time series, bar gauge).
- Standard in the Prometheus ecosystem.

### Configuration in Project
Running on port 3001 (non-default; default is 3000, which conflicts with React). Linked from the React UI footer.

---

## 6. React 18 + Create React App

### What is it?
A JavaScript UI library for building component-based UIs. CRA provides zero-config Webpack/Babel setup.

### Why used here?
- Quick to scaffold (no build configuration needed).
- `useState` hook manages local event counts and button states without a state management library.
- `fetch` API sends HTTP POST events to the ProducerServer.

### Key Design Decisions
- State is **local** — refreshing the page resets all counts.
- No Redux or context — the app is small enough for `useState`.
- Hardcoded `payload: "react-ui"` — payload is not configurable from the UI.

---

## 7. Gradle 8.6

### What is it?
A JVM build tool using Groovy/Kotlin DSL. Manages dependencies, compilation, testing, and packaging.

### Project Structure
- **ProducerServer**: Multi-module (`settings.gradle` includes `app`). Uses version catalog (`libs.versions.toml`).
- **ConsumerServer**: Single-module build.

### Key Commands
```bash
./gradlew bootRun        # start the Spring Boot app
./gradlew build          # compile + test + package JAR
./gradlew dependencies   # show dependency tree
```

---

## 8. Java 21

### What is it?
LTS version of Java. Spring Boot 3.x requires Java 17+.

### Used Features in This Project
- Standard OOP (classes, interfaces)
- No Java 21-specific features (records, sealed classes, virtual threads) are used, though Spring Boot 3.2 supports virtual threads via `spring.threads.virtual.enabled=true`

---

## 9. Jackson Databind 2.16.1

### What is it?
The de-facto Java JSON library. Serializes Java objects to JSON and deserializes JSON to Java objects.

### How Used
```java
// Producer: serialize Event → JSON string for Kafka
String payload = objectMapper.writeValueAsString(event);

// Consumer: deserialize Kafka message JSON → Event object
Event event = objectMapper.readValue(message, Event.class);
```

Both sides use `new ObjectMapper()` directly (not a Spring-managed bean). This is a minor anti-pattern — Spring Boot auto-configures an `ObjectMapper` bean with proper settings; using `new ObjectMapper()` bypasses those.

---

## 10. Lombok 1.18.30

### What is it?
Annotation processor that generates boilerplate Java code (getters, setters, constructors, toString) at compile time.

### Used in This Project?
**Declared in both build files, but never used.** The `Event.java` model manually defines getters, setters, and `toString()` instead of using `@Data` or `@Getter`. Dead dependency.
