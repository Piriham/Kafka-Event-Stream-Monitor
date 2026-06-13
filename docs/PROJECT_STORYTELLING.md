# Project Storytelling Scripts

Use these scripts verbatim or adapt them for interviews, demos, and resume walkthroughs.
Speak in first person — you built this.

---

## 30-Second Elevator Pitch

> "I built a real-time event tracking pipeline for a course platform. Users click buttons on a React UI — things like 'Add to Cart' or 'Checkout' — and those events flow through Apache Kafka to a Spring Boot consumer that exposes Prometheus metrics. The whole thing is visible in a Grafana dashboard within seconds. The goal was to learn how event-driven systems work end-to-end, from the browser all the way to the observability stack."

---

## 2-Minute Summary

> "I built a full-stack event streaming system to understand how production analytics pipelines work in practice.
>
> The system has three main components. First, a React frontend that simulates a course platform landing page. Users can trigger four types of events — page views, clicks, add-to-cart, and checkout. Each button click sends a POST request to my Spring Boot producer server.
>
> The producer validates the incoming event, serializes it to JSON, and publishes it to a Kafka topic called 'testy', using the event type as the message key. Using the key ensures that all checkout events go to the same partition, which preserves ordering per event type.
>
> On the other side, a second Spring Boot service listens to that Kafka topic and increments Micrometer counters tagged by event type. Both services expose a Prometheus scrape endpoint at `/actuator/prometheus`. Prometheus scrapes these endpoints, and Grafana visualizes the data in real time.
>
> One thing I discovered while building this is that `kafkaTemplate.send()` is non-blocking — it returns a `CompletableFuture`. In my current implementation I don't await it, which means a Kafka failure would be invisible to the caller. In a production system I'd add a completion callback and handle delivery failures explicitly."

---

## 5-Minute Deep Dive

> "I built this project to get hands-on experience with an event-driven microservices architecture — the kind of stack you'd see in a real analytics or e-commerce platform.
>
> **The Problem I Was Solving**
> Modern platforms generate thousands of user events per second. You need a way to capture those events without blocking the user, process them independently, and make them visible to analysts in real time. Kafka solves this by acting as a durable, high-throughput buffer between event producers and consumers.
>
> **The Architecture**
> I have three services: a React UI, a Spring Boot producer server on port 8080, and a Spring Boot consumer server on port 8081. Kafka is the backbone connecting the producer and consumer.
>
> The React UI is intentionally simple — it simulates a course platform with four user actions: pageView, userClick, addToCart, and checkout. Each button click fires a `fetch` POST to the producer.
>
> The producer receives the event, validates that the event type is not blank — since the type is the Kafka message key and also the Prometheus metric tag — serializes the full Event object to JSON using Jackson, and calls `kafkaTemplate.send('testy', eventType, jsonPayload)`. I then immediately increment a Micrometer counter tagged with the event type and return 200 OK.
>
> The consumer uses `@KafkaListener` on the same topic. Spring Kafka handles all the polling, partition assignment, and offset management automatically. Inside `consume()`, I deserialize the JSON back to an Event POJO and increment a separate counter. If deserialization fails — say, if the JSON is malformed — I catch the exception, log it to stderr, and increment a failure counter. The trade-off here is that failed messages are silently dropped rather than retried, which in production I'd fix using `@RetryableTopic` with a Dead Letter Topic.
>
> **Observability**
> Both servers expose `/actuator/prometheus`. By adding `micrometer-registry-prometheus` as a dependency and two properties in `application.properties`, Spring Boot automatically formats all my Micrometer metrics in Prometheus exposition text. Prometheus scrapes both servers, and Grafana dashboards show me `producer_events_sent_total` and `consumer_events_received_total` broken down by event type in real time.
>
> **What I Learned**
> The biggest lesson was understanding Kafka's delivery guarantees. The producer's `kafkaTemplate.send()` is fire-and-forget by default — I had to learn that I need to handle the `CompletableFuture` to get true delivery confirmation. I also learned about consumer group rebalancing — if I start a second consumer instance, Kafka redistributes partitions between them automatically.
>
> **If I Were to Build This for Production**
> I'd add a Dead Letter Topic for failed consumer messages, await the Kafka future in the producer, add JWT authentication to the REST API, extract the topic name and Kafka broker address to environment variables, add Docker Compose for one-command startup, and write integration tests using `@EmbeddedKafka`."

---

## 10-Minute Architecture Discussion

> *(Deliver the 5-minute version above, then continue with:)*
>
> **Technology Choices and Tradeoffs**
>
> "I chose Kafka over RabbitMQ for a few reasons. First, Kafka retains messages on disk regardless of consumption — if I add a new consumer service six months from now, it can replay all historical events from the beginning. RabbitMQ deletes messages after they're acknowledged. Second, Kafka's consumer group model lets me scale consumers independently — I can run three consumer instances for three topic partitions, each handling a third of the load, just by starting more instances in the same consumer group. Third, Kafka's log structure makes it the natural choice for analytics workloads.
>
> For serialization I chose JSON strings over Avro or Protobuf. The tradeoff is verbosity — JSON is human-readable and easy to debug with the Kafka console consumer tool, but it's larger and has no schema enforcement. In a production system with high throughput, I'd use Avro with Confluent Schema Registry to get schema evolution guarantees and 10x better compression.
>
> **The CORS Bug I Found**
> One interesting thing I discovered: my CORS configuration whitelists `http://localhost:3001` (Grafana), but the React app runs on `http://localhost:3000`. Technically, browser fetch calls from the React UI should fail the CORS preflight check. In practice, development servers sometimes bypass CORS enforcement for localhost-to-localhost calls, but this is a real bug that would surface in a stricter browser environment. The fix is to add localhost:3000 to the allowed origins list.
>
> **Metrics Architecture**
> I used Micrometer rather than calling the Prometheus Java client directly. Micrometer is the vendor-neutral metrics facade — it's like SLF4J but for metrics. If I wanted to switch from Prometheus to Datadog tomorrow, I'd change one dependency and one config property, with zero code changes. The `Counter.builder().tag().register().increment()` pattern is the same regardless of backend.
>
> The metrics are tagged with `event_type`, which is the key design decision for Grafana dashboards. Without tags, I'd need a separate metric name per event type — with tags, one `producer_events_sent_total` metric gives me four independently queryable time series.
>
> **What a Scale-Up Would Look Like**
> If this platform grew to 10,000 events per second, here's what I'd change: (1) Increase topic partitions from 1 to 10-20. (2) Run 10-20 consumer instances in the same consumer group — each gets assigned a partition. (3) Run 3-5 producer instances behind a load balancer. (4) Enable Kafka producer batching with `linger.ms=5` to group small messages into larger batch sends for efficiency. (5) Add consumer lag monitoring — alert when consumer falls more than 10,000 messages behind the producer. (6) Consider moving to Avro serialization with Schema Registry.
>
> **What I'd Add Next**
> The system is missing a persistence layer. Right now metrics reset when services restart, and Prometheus stores the time-series data (which is fine for monitoring). But for a real platform, I'd add a ConsumerServer variant that writes events to PostgreSQL using Spring Data JPA — so you have a permanent event log for business intelligence queries. I'd also add a Kafka Streams application to compute real-time rolling 5-minute conversion rates (cart adds / checkout completions) using windowed aggregations."
