# Interview Preparation Guide

---

## Beginner Questions (30)

### Spring Boot / Java

**Q1: What does `@SpringBootApplication` do?**
> It is a composed annotation equivalent to `@Configuration + @EnableAutoConfiguration + @ComponentScan`. It tells Spring to scan the current package and subpackages for beans, auto-configure based on classpath, and mark the class as a configuration source.

**Q2: What is `@RestController` vs `@Controller`?**
> `@RestController` = `@Controller + @ResponseBody`. Every method return value is serialized (by Jackson) directly to the HTTP response body. `@Controller` is for MVC returning view names.

**Q3: What is Dependency Injection and how does Spring do it?**
> DI means a class's dependencies are provided externally rather than instantiated internally. Spring manages a container of beans and injects them via constructor injection (used here in `ProducerController`) or `@Autowired` field injection.

**Q4: What is a Spring Bean?**
> An object whose lifecycle is managed by the Spring IoC container. Beans are singletons by default. `KafkaTemplate` and `MeterRegistry` are both Spring-managed beans.

**Q5: Why use constructor injection instead of field injection (`@Autowired`)?**
> Constructor injection is testable (you can pass mock objects directly), makes dependencies explicit and required, and supports immutability (`final` fields).

**Q6: What is `application.properties` used for?**
> Externalized configuration. Spring Boot reads it to configure server port, Kafka bootstrap servers, actuator settings, etc. Follows 12-factor app principles.

**Q7: What does `@RequestBody` do?**
> Tells Spring MVC to deserialize the HTTP request body (JSON) into the annotated parameter type using Jackson's `ObjectMapper`.

**Q8: What is `ResponseEntity`?**
> A Spring class that represents an HTTP response, including status code, headers, and body. Allows fine-grained control: `ResponseEntity.ok("message")`, `ResponseEntity.badRequest().body("error")`.

**Q9: What does Jackson's `ObjectMapper.writeValueAsString()` do?**
> Serializes a Java object to a JSON string. Uses reflection to read field values (or getter methods).

**Q10: What is the Actuator?**
> Spring Boot Actuator adds production-ready endpoints to monitor and manage your app: `/actuator/health`, `/actuator/metrics`, `/actuator/prometheus`, etc. Enabled by adding `spring-boot-starter-actuator` to the build.

---

### Kafka

**Q11: What is Apache Kafka?**
> A distributed, fault-tolerant, high-throughput message broker. Records are written to topics (organized into partitions) and retained for a configurable period. Consumers read at their own pace using stored offsets.

**Q12: What is a Kafka topic?**
> A named, ordered, and partitioned log of records. In this project, the topic is `"testy"`. All events regardless of type go to this one topic.

**Q13: What is a Kafka partition?**
> A topic is split into N partitions. Each partition is an ordered, immutable sequence of records. Records with the same key always go to the same partition (consistent hashing of the key).

**Q14: Why is `event.getType()` used as the Kafka message key?**
> To ensure all events of the same type land on the same partition, preserving per-type ordering. E.g., all `checkout` events are processed in the order they were created.

**Q15: What is a Consumer Group?**
> A set of consumer instances that collectively consume all partitions of a topic. Each partition is assigned to exactly one consumer in the group. Here `metrics-consumer-group` has one instance reading all partitions.

**Q16: What is `auto-offset-reset=earliest`?**
> Tells the consumer: if no committed offset exists for this group, start from the oldest message in the topic. The alternative is `latest` (start from messages published after the consumer starts).

**Q17: What is `@KafkaListener`?**
> Spring Kafka annotation that marks a method as a Kafka message handler. Spring creates a listener container that polls the broker and calls the annotated method with each record value.

**Q18: What does `StringSerializer` / `StringDeserializer` do?**
> Kafka messages are bytes. These classes convert Java `String` to/from byte arrays using UTF-8 encoding, so you can produce and consume plain text or JSON strings.

**Q19: What happens when the ConsumerServer restarts?**
> Kafka stores the committed offset for `metrics-consumer-group` on the broker. On restart, the consumer resumes from the last committed offset, so no messages are lost or re-processed (assuming the consumer committed before crashing).

**Q20: What is `KafkaTemplate`?**
> A Spring wrapper around the native Kafka `Producer`. Provides template methods like `send(topic, key, value)` and integrates with Spring's transaction management and observability.

---

### Micrometer / Prometheus / Grafana

**Q21: What is Micrometer?**
> A vendor-neutral metrics facade. You write `Counter.builder("name").tag("k","v").register(registry).increment()` once; the registry formats it for whichever backend (Prometheus, Datadog, InfluxDB) is on the classpath.

**Q22: What is a Counter in Micrometer?**
> A monotonically increasing metric. Only goes up. Used for total events processed, errors, requests. Cannot decrease (even if you restart — Prometheus handles rate calculations).

**Q23: Why tag metrics with `event_type`?**
> Tags (labels in Prometheus) add dimensions to metrics. Without tags you only know total events sent. With `event_type` tag, you can query `producer_events_sent_total{event_type="checkout"}` to see checkout-specific counts.

**Q24: What is Prometheus's scraping model?**
> Pull-based: Prometheus contacts each target's `/metrics` endpoint (here `/actuator/prometheus`) at a configured interval (default 15s) and ingests the metrics into its time-series database.

**Q25: What is PromQL?**
> Prometheus Query Language. Used in Grafana to query time-series data. Examples: `rate(producer_events_sent_total[1m])` = per-second rate over 1 minute. `sum by(event_type)(producer_events_sent_total)` = totals grouped by event type.

---

### React / Frontend

**Q26: What is `useState` and how is it used here?**
> React hook for local component state. `const [counts, setCounts] = useState({})` — `counts` is the current state, `setCounts` is the updater. Used to track how many of each event type have been sent and the loading/success/error status of each button.

**Q27: What is `fetch` and how is it used?**
> Browser-native HTTP client (Promise-based). `fetch(url, {method: 'POST', ...})` sends an async HTTP request. `await response.ok` checks HTTP status code.

**Q28: Why does the React app refresh reset the event counts?**
> The counts live in `useState` — React component state is in-memory and not persisted. A page refresh creates a new React component instance with `useState({})` starting fresh.

**Q29: What is Create React App?**
> A zero-config toolchain (Webpack + Babel + ESLint) for bootstrapping React projects. `npm start` starts a dev server with hot reload. The project uses CRA 5 with React 18.

**Q30: What would you change in the frontend for production?**
> Move the API URL from hardcoded `http://localhost:8080` to an environment variable (`process.env.REACT_APP_API_URL`). Add error boundary components. Use React Query or SWR for data fetching with caching. Add proper logging.

---

## Intermediate Questions (30)

**Q31: Explain the CORS issue in this project.**
> The `CorsConfig` whitelists `http://localhost:3001` (Grafana), but the React app runs on `http://localhost:3000`. Browser fetch calls from React should be blocked by CORS. The fix is to add `localhost:3000` to the allowed origins list in both servers' `CorsConfig`.

**Q32: `kafkaTemplate.send()` is non-blocking. What does that mean for error handling?**
> `send()` returns a `CompletableFuture<SendResult>`. The current code ignores this future, so if Kafka rejects the message (e.g., broker is unreachable after buffer timeout), the exception is silently dropped and the API still returns 200 OK. The fix: `.whenComplete((result, ex) -> { if (ex != null) log.error("Failed", ex); })`.

**Q33: What is the difference between at-least-once, at-most-once, and exactly-once delivery in Kafka?**
> - **At-most-once**: Offset committed before processing. If consumer crashes after commit but before processing, message is lost.
> - **At-least-once** (default here): Offset committed after processing. If consumer crashes after processing but before commit, message is re-delivered and processed again.
> - **Exactly-once**: Requires Kafka transactions (`isolation.level=read_committed`, `enable.idempotence=true`). Most complex but guarantees each message processed exactly once.

**Q34: What is a Dead Letter Topic and why is it missing here?**
> A DLT is a separate Kafka topic where messages that fail processing are sent after N retries. Here, when `objectMapper.readValue()` throws, the exception is caught, logged, and the message is silently dropped. A production system would route failed messages to `testy.DLT` using Spring Kafka's `@RetryableTopic` or `DeadLetterPublishingRecoverer`.

**Q35: Why are there two identical `Event.java` files in Producer and Consumer?**
> Both services need the model to serialize/deserialize Kafka messages. In a monorepo or shared library, this would be one class. The current duplication is a maintenance risk — if you add a field to Event in the producer but not the consumer, Jackson deserialization will silently ignore the extra field (or fail if configured strictly).

**Q36: What is the Spring Kafka auto-configuration doing behind the scenes?**
> When `spring-kafka` is on the classpath and `spring.kafka.bootstrap-servers` is set, `KafkaAutoConfiguration` creates: `KafkaTemplate<?, ?>` bean, `ConsumerFactory` bean, `KafkaListenerContainerFactory` bean, and triggers `@KafkaListener` processing via `KafkaListenerAnnotationBeanPostProcessor`.

**Q37: What is `ConcurrentMessageListenerContainer` and how many threads does it use?**
> It's Spring Kafka's default container for `@KafkaListener`. By default it creates `concurrency=1` thread per listener. The thread runs a loop calling `consumer.poll()`. If you have 3 partitions and `concurrency=3`, three threads each handle one partition.

**Q38: What does `management.metrics.export.prometheus.enabled=true` do?**
> Registers the `PrometheusMeterRegistry` as the active Micrometer registry. All counters, gauges, and timers registered with `MeterRegistry` will be formatted in Prometheus exposition format at `/actuator/prometheus`.

**Q39: Why does the consumer have `server.port=8081`? The consumer doesn't serve any REST API.**
> To expose `/actuator/prometheus` for Prometheus scraping. Without the web server, Prometheus can't scrape metrics from the consumer. The web server also exposes `/actuator/health` for health checks.

**Q40: What happens if two instances of ConsumerServer are started in the same consumer group?**
> Kafka triggers a **rebalance**. It redistributes partitions among the active consumers in the group. If the topic has 3 partitions and 2 consumers, one gets 2 partitions and the other gets 1. This is how horizontal scaling works — add more consumer instances to increase throughput.

**Q41: What is the difference between `Counter.builder(...).register(meterRegistry)` and caching the Counter?**
> `Counter.builder().register()` is idempotent — if a meter with that name+tags already exists, it returns the existing one. But it does involve a map lookup on every event. For high-throughput scenarios, cache the counter in a field or `ConcurrentHashMap<String, Counter>` keyed by event type.

**Q42: Why is `new ObjectMapper()` an anti-pattern in Spring?**
> Spring Boot auto-configures an `ObjectMapper` bean with consistent settings (date format, property naming, feature flags). Using `new ObjectMapper()` bypasses these, creating potential inconsistency — e.g., the auto-configured mapper might include JavaTimeModule for date handling, but `new ObjectMapper()` won't.

**Q43: What is `spring-cloud-starter-bootstrap` doing in the ProducerServer build.gradle?**
> It enables the legacy Spring Cloud bootstrap context (loads `bootstrap.properties` before `application.properties`). This is used when connecting to Spring Cloud Config Server. There is no `bootstrap.properties` and no Config Server in this project — it's an unused dependency.

**Q44: Explain the Kafka message key and its effect on partitioning.**
> Kafka uses `murmur2(key) % numPartitions` to determine the target partition. Using `event.getType()` as key means: all `checkout` events go to the same partition. This guarantees order within a single event type. Without a key, messages are round-robined across partitions.

**Q45: What would happen if you renamed `Event.type` to `Event.eventType` in the Producer but not the Consumer?**
> The Kafka message would be `{"eventType":"checkout","payload":"..."}`. The Consumer's `objectMapper.readValue()` would deserialize this into `Event{type=null, payload="..."}` (Jackson doesn't find a `type` field in the JSON). The counter would be tagged with `event_type=null`. Incrementing `consumer_events_received_total{event_type="null"}` — metrics would appear broken.

**Q46: How does Spring Boot know the main class to run?**
> Spring Boot plugin looks for `mainClass` in `build.gradle` (`application { mainClass = 'org.example.App' }`) or uses the class annotated with `@SpringBootApplication` auto-detection.

**Q47: What is `micrometer-registry-prometheus` vs `io.prometheus:simpleclient`?**
> Both are in ProducerServer's build.gradle. `micrometer-registry-prometheus` is the Micrometer bridge that adapts Micrometer's API to Prometheus. `io.prometheus:simpleclient` is the raw Prometheus Java client — lower level. Having both is redundant; Micrometer's registry already wraps simpleclient internally.

**Q48: What is CORS and why is it needed?**
> Cross-Origin Resource Sharing. Browsers block JavaScript from making HTTP requests to a different origin (different host or port) unless the server includes `Access-Control-Allow-Origin` response headers. The React app (port 3000) making requests to the Spring server (port 8080) is cross-origin, so CORS headers are required.

**Q49: What is the `@Service` annotation on `EventConsumer`?**
> Marks the class as a Spring service-layer bean. Functionally equivalent to `@Component`, but conveys semantic meaning. Spring will create one instance and manage its lifecycle. `@KafkaListener` methods are only processed on Spring-managed beans.

**Q50: How does the React UI provide visual feedback without a database?**
> Through React's `useState`. The `counts` state object maps event types to local send counts. The `statuses` state tracks the current HTTP request state per event type. A 1500ms `setTimeout` resets the status. All state is lost on page refresh.

---

## Advanced Questions (30)

**Q51: Design a schema evolution strategy for the Event model.**
> Use Apache Avro with Confluent Schema Registry. Define Event schema in `.avsc`. Use `AvroSerializer/AvroDeserializer`. Schema Registry enforces backward/forward compatibility. Adding a new optional field (`userId`, `sessionId`) is backward-compatible — old consumers ignore it, new consumers use it.

**Q52: How would you implement exactly-once semantics in this pipeline?**
> On producer: `enable.idempotence=true`, `transactional.id` set. On consumer: `isolation.level=read_committed`, manual offset commit inside a Kafka transaction. Spring Kafka `@Transactional` with `KafkaTransactionManager`. The consumer would atomically process the message + commit offset in one transaction.

**Q53: How would you scale this system to handle 100,000 events per second?**
> 1. Increase topic partitions to 10-20. 2. Run 10-20 ConsumerServer instances (each gets partitions). 3. Run 3-5 ProducerServer instances behind a load balancer. 4. Configure Kafka producer batching (`batch.size`, `linger.ms`). 5. Use async callbacks instead of synchronous `get()`. 6. Move Prometheus scraping to push (Prometheus Pushgateway) if scrape interval becomes a bottleneck.

**Q54: The consumer currently uses `System.out.println`. What are the implications?**
> stdout is unbuffered or line-buffered. In a containerized environment, stdout/stderr are captured by the container runtime (Docker/k8s) and forwarded to a logging driver. This works but: (1) no log levels (can't filter INFO vs ERROR), (2) no structured logging (hard to parse/search), (3) no correlation IDs (can't trace a single event through producer → consumer). Fix: SLF4J + Logback, output JSON with `logstash-logback-encoder`.

**Q55: How would you add distributed tracing to this system?**
> Add `micrometer-tracing-bridge-brave` (Zipkin) or `micrometer-tracing-bridge-otel` (OpenTelemetry). Micrometer Tracing auto-instruments Spring MVC (incoming HTTP) and Spring Kafka (producer/consumer). A trace ID is propagated via Kafka record headers. In Grafana/Zipkin, you can see the full span: HTTP request → Kafka produce → Kafka consume.

**Q56: What is consumer group rebalancing and what problems can it cause?**
> Rebalancing is triggered when consumers join or leave the group, or when partition count changes. During rebalancing, all consumption stops (stop-the-world in Kafka). Long rebalances (caused by slow `poll()`) can cause cascading failures. Fix: Use incremental cooperative rebalancing (`partition.assignment.strategy=CooperativeStickyAssignor`), keep `max.poll.records` small, ensure `max.poll.interval.ms` is longer than your processing time.

**Q57: How would you implement a Dead Letter Topic for the consumer?**
> Option 1: `@RetryableTopic(attempts = 3, backoff = @Backoff(delay = 1000))` on `consume()` — Spring Kafka auto-creates `testy-retry-0`, `testy-retry-1`, `testy-dlt` topics and handles retry routing.
> Option 2: `SeekToCurrentErrorHandler` with `DeadLetterPublishingRecoverer` — after N retries, publishes the failed record to `testy.DLT`.

**Q58: How would you test the ProducerController without a real Kafka broker?**
> Use `@EmbeddedKafka` from `spring-kafka-test`. Spring starts an embedded Kafka broker for the test. Alternatively, mock `KafkaTemplate` with Mockito: `when(kafkaTemplate.send(any(), any(), any())).thenReturn(mockFuture)`. Verify it was called with the right arguments.

**Q59: The Kafka topic name `"testy"` is hardcoded. How would you externalize it?**
> Add to `application.properties`:
> ```properties
> kafka.topic.events=testy
> ```
> Inject with `@Value("${kafka.topic.events}")`:
> ```java
> @Value("${kafka.topic.events}")
> private String topicName;
> ```
> Or use a `@ConfigurationProperties` class for type-safe config binding.

**Q60: How does Kafka guarantee message ordering?**
> Within a single partition: strictly ordered (append-only log). Across partitions: no ordering guarantee. Since this project uses `event.getType()` as key, all events of the same type go to the same partition, guaranteeing per-type ordering. If you need global ordering of all events, use a single partition (sacrifices parallelism).

**Q61: What is the impact of `setAllowCredentials(true)` with `addAllowedHeader("*")`?**
> The CORS spec forbids wildcard headers when credentials are allowed. Spring's `CorsFilter` handles this by either throwing an error or converting `*` to the list of requested headers. In production: enumerate specific headers (`Content-Type`, `Authorization`) and specific allowed origins.

**Q62: What is Kafka's log compaction and would it be useful here?**
> Log compaction retains only the latest record per key, discarding older ones for the same key. For this event stream: NO. Events are facts, not state updates — every `checkout` is meaningful, not just the latest one. Log compaction is useful for changelog topics (like a database table's CDC stream).

**Q63: How would you add a new event type without deploying code changes?**
> The system is already event-type agnostic on the backend — the producer accepts any non-blank `type` string and the consumer tags metrics dynamically with whatever `event_type` arrives. Only the React UI hardcodes the 4 event types. Adding a new type requires changing `LandingPage.js`'s `EVENTS` array and redeploying the frontend.

**Q64: What is the difference between `management.endpoint.prometheus.enabled=true` and `management.metrics.export.prometheus.enabled=true`?**
> - `management.endpoint.prometheus.enabled=true`: Enables the `/actuator/prometheus` HTTP endpoint (the scrape target).
> - `management.metrics.export.prometheus.enabled=true`: Enables the `PrometheusMeterRegistry` (the in-memory Prometheus metric store). Both are needed. Without the registry, there's nothing to expose. Without the endpoint, the registry data can't be scraped.

**Q65: How would you handle backpressure if the React UI floods the producer?**
> Kafka producer has an internal buffer (`buffer.memory`, default 32MB). If the buffer fills (broker slow/unreachable), `kafkaTemplate.send()` blocks for `max.block.ms` (default 60s), then throws `TimeoutException`. At the HTTP level: configure Spring MVC thread pool limits, add rate limiting (Bucket4j or Spring Cloud Gateway's `RequestRateLimiter`).

---

## System Design Questions (20)

**Q66: Design the full observability stack for this system.**
> 1. Metrics: Micrometer → Prometheus → Grafana (already in place). 2. Logging: SLF4J + Logback → Elasticsearch/Loki → Grafana. 3. Tracing: Micrometer Tracing + Zipkin/Jaeger → Grafana Tempo. 4. Alerting: Prometheus AlertManager → PagerDuty/Slack. 5. Dashboards: Grafana with SLA panels (error rate, consumer lag, p99 latency).

**Q67: How would you redesign this to support 10 different event consumers (analytics, fraud detection, database writer)?**
> Each consumer gets its own consumer group. Kafka delivers each message to all consumer groups independently. Add: `analytics-consumer-group`, `fraud-consumer-group`, `db-writer-group`. Each runs as a separate service. The Kafka topic is the hub; new consumers can be added without changing the producer.

**Q68: Design a replay mechanism for failed events.**
> Store raw Kafka messages to an S3 bucket using Kafka Connect S3 Sink Connector. To replay: use Kafka's `--from-beginning` or a time-based seek (`consumer.offsetsForTimes()`). Alternatively, design the consumer to emit failed events to a `testy.DLT` topic which a separate replay service monitors and re-publishes to `testy`.

**Q69: How would you evolve from a monolithic Kafka topic to domain-driven topics?**
> Create separate topics per domain: `user-events`, `cart-events`, `purchase-events`. Producer routes based on event type. This allows: per-topic retention policies, per-topic consumer groups, different partition counts per load. Tradeoff: more operational complexity, harder cross-domain queries.

**Q70: How would you add a checkout flow that requires sequential state?**
> Use Kafka Streams or a state store. Create a stateful processor that: (1) on `addToCart` → store cart state keyed by session ID, (2) on `checkout` → retrieve cart state, validate, clear state. Use `KTable` for changelog storage backed by Kafka's internal topics. Alternatively, use a database (Redis for cart state, PostgreSQL for orders).

**Q71: How would you implement a rate limiter on `POST /producer/event`?**
> Use Bucket4j (token bucket algorithm) with a Spring MVC interceptor or `@RateLimiter`. For distributed rate limiting (multiple ProducerServer instances): use Bucket4j with Redis backend. Config: 100 requests per second per IP, burst of 200.

**Q72: How would you make this system GDPR-compliant (right to erasure)?**
> Personal data should not be in the Kafka event payload (currently only `"react-ui"` — fine). If user IDs are added: use Kafka's `delete` operation (set value to `null` for a key with log compaction), or encrypt each user's events with a per-user key and delete the key (crypto-shredding). Add a data retention policy to the Kafka topic (`retention.ms`).

**Q73: Design a multi-region version of this system.**
> Use Confluent MirrorMaker 2 or Kafka's built-in `MirrorMaker` to replicate the `testy` topic across regions. Producer in US-East writes to US-East Kafka. MirrorMaker replicates to EU-West. EU-West consumer reads from local Kafka. Caveat: this gives regional isolation, not active-active — consumers in EU-West are always behind US-East by replication lag.

**Q74: How would you monitor consumer lag?**
> Consumer lag = latest offset in partition − consumer's committed offset. Metrics: `kafka_consumer_fetch_manager_records_lag` (auto-exported by Micrometer Kafka binder). Or use Kafka's `kafka-consumer-groups.sh --describe`. Alert when lag > threshold (e.g., 10,000 messages). In Grafana: create a panel showing lag per partition.

**Q75: How would you add authentication to the Kafka cluster?**
> Option 1 — SASL/SCRAM: Username/password. Configure broker `listeners=SASL_PLAINTEXT://...`, `sasl.mechanism=SCRAM-SHA-512`. Add to `application.properties`:
> ```
> spring.kafka.properties.sasl.mechanism=SCRAM-SHA-512
> spring.kafka.properties.sasl.jaas.config=...
> ```
> Option 2 — mTLS: Client certificates. Set `spring.kafka.ssl.key-store-location`, `trust-store-location`. Option 3 — Confluent Cloud (managed, handles auth).

---

## Architecture Questions (20)

**Q76: Why separate the Producer and Consumer into different services?**
> Single Responsibility Principle. The producer handles HTTP ingestion and Kafka publishing; it can be scaled based on HTTP traffic. The consumer handles event processing; it can be scaled based on Kafka partition count. They can be deployed and updated independently. If the consumer crashes, the producer continues accepting events (Kafka buffers them).

**Q77: What would happen if you merged Producer and Consumer into one Spring Boot app?**
> It would work. The `@KafkaListener` would consume messages that `KafkaTemplate` produces (possibly even the same message before other subscribers). But: (1) you lose independent scaling, (2) a crash takes down both functions, (3) the codebase becomes harder to reason about.

**Q78: Why use Kafka instead of a direct HTTP call from Producer to Consumer?**
> HTTP call: synchronous, tight coupling — if consumer is down, the request fails. Kafka: asynchronous, decoupled — if consumer is down, messages accumulate in Kafka and are processed when consumer restarts. Kafka also gives you replay, multiple consumers, and retention out of the box.

**Q79: This project uses String serialization for Kafka messages. What are the tradeoffs?**
> Pros: Simplicity, human-readable (easy to debug with `kafka-console-consumer.sh`), no schema dependency.
> Cons: No schema enforcement (any string is valid), larger than binary formats (Avro/Protobuf are 10-100x smaller), no backward compatibility guarantees.

**Q80: How would you implement a fan-out pattern where one event triggers multiple consumers?**
> Already supported by Kafka's consumer group model. Add a new consumer group (`analytics-group`, `fraud-group`) each running its own service. All consumer groups receive all messages independently. No changes to producer or topic needed.

**Q81: What design pattern is the `@KafkaListener` implementing?**
> **Observer / Event-Driven**. The listener "observes" the Kafka topic and is notified when new messages arrive. It's also a **Template Method** pattern — Spring Kafka provides the polling loop and error handling infrastructure; you fill in `consume()` with the business logic.

**Q82: Why is the topic named `"testy"` instead of a domain-specific name?**
> This is a development/learning project. A production name would be `user-events`, `analytics-events`, or `course-platform.events`. The name has no operational impact but affects discoverability and maintainability.

**Q83: How does the project follow the 12-Factor App methodology?**
> Factor adherence:
> - **Config**: Partially — `application.properties` is in the repo (should be env vars). Kafka IP is hardcoded.
> - **Backing services**: Kafka is treated as an attached resource.
> - **Processes**: Both Spring Boot apps are stateless (no local state).
> - **Port binding**: Each app exports its service via a port.
> - Gaps: No log streams, no process management, no explicit dependency declaration (Gradle is close).

**Q84: What is the role of Micrometer as a metrics abstraction layer?**
> It's the SLF4J of metrics. You write `Counter.builder("events").register(meterRegistry)` once. Switch from Prometheus to Datadog by changing the registry implementation (dependency) and config — zero code changes. This is the **Adapter** design pattern.

**Q85: Could this architecture handle a checkout event that requires a database write, a payment API call, and an email send?**
> Not as currently designed. The consumer only increments a counter. To support that workflow: (1) Consumer listens to `checkout` events specifically, (2) calls payment API synchronously, (3) writes to database, (4) publishes an `order-confirmed` event for the email service. This is the **Saga pattern** — a sequence of local transactions coordinated through events.

---

## Follow-Up Questions (20)

**Q86: "You said the consumer uses `auto-offset-reset=earliest`. Won't that cause duplicate processing every restart?"**
> Only on the first run (before any offset is committed). After the first message is consumed, the consumer group's offset is committed to Kafka's `__consumer_offsets` internal topic. On subsequent restarts, Kafka reads the committed offset and resumes from there. Re-processing only happens if you deliberately reset the group offset or if `auto.offset.reset` is triggered (no committed offset exists).

**Q87: "Why not use WebSockets for real-time event streaming instead of REST + Kafka?"**
> WebSockets are browser ↔ server. Kafka is server ↔ server. They solve different problems. You could add WebSockets to push Kafka events to the browser in real-time (replacing the Grafana link), but Kafka would still be the right choice for server-side event processing and fan-out.

**Q88: "The CORS config is identical in both servers. Could you extract it to a shared library?"**
> Yes. Create a `shared-config` Gradle module with `CorsConfig.java` and publish it as a JAR. Both Producer and Consumer include it as a dependency. In a Spring Boot multi-module Gradle project or a separate published artifact. This removes duplication and ensures consistent CORS policy.

**Q89: "You mentioned Micrometer counters reset on JVM restart. How does Prometheus handle that?"**
> Prometheus stores time-series data independently. When a counter resets to 0, Prometheus detects the discontinuity and handles it correctly in `rate()` and `increase()` calculations. These functions account for counter resets (if the current value is less than the previous, Prometheus knows it reset). Raw counter queries (`producer_events_sent_total`) will show the new value from 0.

**Q90: "What would break first if you ran 3 ConsumerServer instances against 1 Kafka partition?"**
> Two of the three consumers would be idle. Kafka assigns at most one consumer per partition per consumer group. With 1 partition, only 1 consumer gets messages; the other 2 are in standby (failover mode). To parallelize: create the topic with 3 partitions.

**Q91: "How would you handle a Kafka broker going down in production?"**
> Kafka is designed for broker failures. With a 3-broker cluster and `replication-factor=3`, losing one broker is transparent — the partition leader election happens automatically. The producer may experience a brief pause during leader election. Configure `retries=Integer.MAX_VALUE` and `delivery.timeout.ms=120000` on the producer to handle transient failures.

**Q92: "The React app shows success even if Kafka publish fails. How would you fix the UX?"**
> On the backend: await the Kafka send future and return 202 Accepted only after Kafka confirms receipt. On the frontend: treat non-2xx responses as errors. Add retry with exponential backoff: `await fetch(..., { signal: AbortSignal.timeout(5000) })`.

**Q93: "How would you add a new metric: average event payload size?"**
> Use Micrometer `DistributionSummary` instead of `Counter`:
> ```java
> DistributionSummary.builder("event_payload_size_bytes")
>     .tag("event_type", event.getType())
>     .register(meterRegistry)
>     .record(payload.getBytes().length);
> ```
> Prometheus exposes `_count`, `_sum`, `_max`, and histogram buckets. PromQL: `event_payload_size_bytes_sum / event_payload_size_bytes_count` = average size.

**Q94: "Why does ConsumerServer need `spring-boot-starter-web`? It's just a Kafka consumer."**
> To expose `/actuator/prometheus`. Without the web starter, there's no HTTP server and Prometheus can't scrape metrics. Alternatively, you could use the Prometheus Pushgateway (consumer pushes metrics to Pushgateway, Prometheus scrapes Pushgateway) to remove the web dependency.

**Q95: "If you were presenting this project to a senior engineer, what would you say is the biggest design flaw?"**
> The biggest flaw is that the producer returns 200 OK to the client before confirming Kafka delivery. If Kafka is unreachable, the React UI shows "✅ Sent!" but no event was published. This violates the user's expectation. The fix is to await the `CompletableFuture` from `kafkaTemplate.send()` and return the response only after Kafka confirmation (or return 202 Accepted with a callback/webhook for true async).

**Q96: "Could you use Kafka Streams instead of a plain consumer?"**
> Yes. Kafka Streams is a client library for stream processing on top of Kafka. It would replace `EventConsumer` with a `StreamsBuilder` pipeline. Benefits: stateful operations (windowed counts), joins, aggregations, built-in fault tolerance with state stores. For this use case (simple counter), it's overkill — but for computing real-time rolling 5-minute event rates, Kafka Streams would be the right tool.

**Q97: "How would you implement an A/B test on this event pipeline?"**
> Route users (by user ID or session ID) to different producer variants. Emit events with a `variant` tag: `producer_events_sent_total{event_type="checkout",variant="A"}`. In Grafana, compare `checkout` conversion rates between variants A and B using `rate(...)` aggregated by variant.

**Q98: "Why is the Kafka message value a JSON string rather than bytes?"**
> Simplicity. `StringSerializer` is the easiest to configure and debug. The tradeoff: JSON is verbose (~50 bytes for this Event object vs ~10 bytes in Avro/Protobuf binary). For analytics at scale, switching to binary formats reduces broker storage costs and network bandwidth by 3-10x.

**Q99: "Can you explain what the Gradle version catalog (`libs.versions.toml`) does?"**
> It centralizes dependency versions in a TOML file. Instead of `'com.google.guava:guava:32.1.3-jre'` in every `build.gradle`, you declare it once in `libs.versions.toml` and reference it as `libs.guava`. Consistent versions across all modules without copy-paste errors. Gradle calls this a "type-safe dependency accessor."

**Q100: "How would you containerize this application with Docker?"**
> ```dockerfile
> FROM eclipse-temurin:21-jre-alpine
> WORKDIR /app
> COPY app/build/libs/app.jar app.jar
> EXPOSE 8080
> ENTRYPOINT ["java", "-jar", "app.jar"]
> ```
> Then `docker-compose.yml` linking kafka, producer, consumer, prometheus, grafana containers with environment variables for `KAFKA_BOOTSTRAP_SERVERS`, `SERVER_PORT`.

**Q101: "What is `spring-cloud-starter-bootstrap` doing and should you remove it?"**
> It loads a Spring Cloud Bootstrap context that reads `bootstrap.properties` before `application.properties`. It's used with Spring Cloud Config Server for centralized configuration. This project has no Config Server and no `bootstrap.properties`, so the dependency does nothing and should be removed — it adds startup overhead and unnecessary classpath complexity.

**Q102: "How would you write a meaningful integration test for the ProducerController?"**
> ```java
> @SpringBootTest
> @EmbeddedKafka(partitions = 1, topics = "testy")
> @AutoConfigureMockMvc
> class ProducerControllerTest {
>     @Autowired MockMvc mvc;
>     @Autowired KafkaListenerEndpointRegistry registry;
>
>     @Test
>     void sendEvent_publishesToKafka() throws Exception {
>         mvc.perform(post("/producer/event")
>             .contentType(APPLICATION_JSON)
>             .content("{\"type\":\"checkout\",\"payload\":\"test\"}"))
>             .andExpect(status().isOk())
>             .andExpect(content().string("Event sent: checkout"));
>         // Then verify KafkaTemplate was called (or use @EmbeddedKafka consumer to read the message)
>     }
> }
> ```

**Q103: "The React UI has no tests. How would you add them?"**
> Use React Testing Library (already a dev dependency). Test `emitEvent` by mocking `fetch` with `jest.fn()`. Assert button text changes to "Sending...", then "✅ Sent!", then resets. Assert `fetch` was called with the correct URL and body. Add an integration test with MSW (Mock Service Worker) to intercept fetch calls without mocking `fetch` directly.

**Q104: "Is this a push or pull architecture? Explain."**
> **Mixed**: 
> - React UI → ProducerServer: **push** (UI pushes events over HTTP).
> - Kafka: internally **pull** (consumers poll the broker).
> - Prometheus → Actuator: **pull** (Prometheus scrapes the HTTP endpoint).
> - Grafana → Prometheus: **pull** (Grafana queries Prometheus via PromQL HTTP API).
> The overall user-to-dashboard flow is **push-initiated** (user action) but **pull-scraped** (metrics are pulled periodically, not streamed in real-time to Grafana).

**Q105: "What would you add to make this project showcase-ready on your resume?"**
> 1. Docker Compose for one-command startup. 2. A pre-built Grafana dashboard (JSON provisioning). 3. GitHub Actions CI (build + test). 4. Fix the CORS bug. 5. Await Kafka future in producer. 6. Add `@RetryableTopic` for Dead Letter Queue. 7. Replace `System.out.println` with structured logging. 8. Remove dead dependencies. 9. Write integration tests with `@EmbeddedKafka`. 10. Add a README with architecture diagram.
