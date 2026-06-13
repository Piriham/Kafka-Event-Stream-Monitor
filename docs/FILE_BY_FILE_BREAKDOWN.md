# File-by-File Breakdown

---

## ProducerServer

### `ProducerServer/app/src/main/java/org/example/App.java`

```java
@SpringBootApplication
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}
```

**Purpose:** Entry point. `@SpringBootApplication` triggers:
1. Component scan of the `org.example` package and all sub-packages.
2. Auto-configuration based on classpath.
3. Embedded Tomcat startup on port 8080.

**Interview note:** This single annotation replaces ~100 lines of XML Spring configuration.

---

### `ProducerServer/app/src/main/java/org/example/config/CorsConfig.java`

**Purpose:** Defines a Spring `CorsFilter` bean to handle browser CORS preflight requests.

**Key logic:**
- `UrlBasedCorsConfigurationSource`: maps CORS rules to URL patterns.
- `/**`: applies to all endpoints.
- `allowedOrigin("http://localhost:3001")`: only requests from this origin bypass CORS (should also include `:3000`).
- `setAllowCredentials(true)`: allows cookies/auth headers to be sent cross-origin.
- `addAllowedMethod("*")`: GET, POST, PUT, DELETE all allowed.

**Why a `CorsFilter` bean instead of `@CrossOrigin`?**  
`@CrossOrigin` is per-controller. A `CorsFilter` bean applies globally before Spring MVC processes the request, which is more consistent and works with Spring Security too.

---

### `ProducerServer/app/src/main/java/org/example/controller/ProducerController.java`

**Purpose:** The only business REST endpoint in the system. Receives events from the React UI and publishes them to Kafka.

**Constructor injection:**
```java
public ProducerController(KafkaTemplate<String, String> kafkaTemplate,
                          MeterRegistry meterRegistry) {
```
- `KafkaTemplate<String, String>`: Spring auto-creates this bean from `application.properties` kafka producer config.
- `MeterRegistry`: Spring auto-creates this from Micrometer + Prometheus registry on classpath.
- `ObjectMapper`: Created manually with `new ObjectMapper()` — a minor anti-pattern (should be `@Autowired`).

**`sendEvent(Event event)` method — full walkthrough:**

| Line | Code | What it does |
|---|---|---|
| 31 | `event.getType() == null \|\| isBlank()` | Validates required field |
| 32 | `return 400` | Fast fail with client error |
| 35 | `objectMapper.writeValueAsString(event)` | Serialize POJO → JSON |
| 36 | `kafkaTemplate.send(TOPIC_NAME, event.getType(), payload)` | Publish to Kafka asynchronously |
| 38-42 | `Counter.builder(...).register(meterRegistry).increment()` | Update Micrometer counter |
| 44 | `return 200` | Success response |
| 46-48 | `catch (Exception e)` | Generic error handler → 500 |

**TOPIC_NAME = "testy"**: A constant (line 15) — should be externalized to `application.properties` in production.

---

### `ProducerServer/app/src/main/java/org/example/model/Event.java`

**Purpose:** POJO (Plain Old Java Object) representing a user event. Used as:
1. HTTP request body (deserialized by Spring MVC + Jackson).
2. Kafka message value (serialized by Jackson manually).

**Fields:**
- `type`: Identifies the event category (e.g., `"checkout"`). Used as Kafka message key.
- `payload`: Arbitrary extra data. React hardcodes `"react-ui"`.

**Design note:** This is a "shared model" — the same class exists identically in both ProducerServer and ConsumerServer. In a real microservices architecture, this would be extracted to a shared library or defined as an Avro/Protobuf schema in a Schema Registry.

---

### `ProducerServer/app/src/main/resources/application.properties`

```properties
server.port=8080
spring.kafka.bootstrap-servers=172.24.236.246:9092
spring.kafka.producer.key-serializer=...StringSerializer
spring.kafka.producer.value-serializer=...StringSerializer
management.endpoints.web.exposure.include=*
management.endpoint.prometheus.enabled=true
management.metrics.export.prometheus.enabled=true
```

**Key decisions:**
- Port 8080 (standard Spring Boot default).
- `StringSerializer` for both key and value → Kafka message is plain string.
- `management.endpoints.web.exposure.include=*` → exposes all actuator endpoints (dev-only setting).

---

### `ProducerServer/app/build.gradle`

**Notable dependencies:**
- `spring-boot-starter-web`: Embeds Tomcat, Jackson, Spring MVC.
- `spring-kafka`: Provides `KafkaTemplate`, `@KafkaListener`, auto-configuration.
- `spring-boot-starter-actuator`: Provides `/actuator/*` endpoints.
- `micrometer-registry-prometheus`: Formats metrics as Prometheus exposition text.
- `spring-cloud-starter-bootstrap 4.1.1`: Unused — bootstrap context loading for Spring Cloud Config. No `bootstrap.properties` exists.
- `mysql-connector-java`: Unused — no datasource configured.
- `lombok`: Unused — no `@Data` or similar annotations on any class.
- `guava`: Unused — Gradle version catalog artifact, likely copied from scaffold.

---

## ConsumerServer

### `ConsumerServer/src/main/java/org/example/consumer/EventConsumer.java`

**Purpose:** The core of the ConsumerServer. Listens to Kafka topic `testy` and emits Micrometer metrics.

**`@KafkaListener(topics = "testy", groupId = "metrics-consumer-group")`:**
- Spring Kafka creates a `ConcurrentMessageListenerContainer` on startup.
- Container runs a background thread that polls the broker every 5 seconds (default `max.poll.interval.ms`).
- When records arrive, `consume(String message)` is called for each record.

**`consume(String message)` method — full walkthrough:**

| Line | Code | What it does |
|---|---|---|
| 23 | `objectMapper.readValue(message, Event.class)` | Deserialize JSON string → Event POJO |
| 25 | `System.out.println(...)` | Log to stdout (no SLF4J) |
| 27-31 | `Counter...consumer_events_received_total...increment()` | Increment success counter with event_type tag |
| 33-39 | `catch (Exception e)` | On failure: log to stderr, increment failure counter |

**Critical design flaw:** The `catch` block does NOT re-throw the exception. Spring Kafka will then commit the offset for this message, permanently losing it. A production-grade consumer would either re-throw (triggering retry/DLQ) or use manual offset acknowledgement.

---

### `ConsumerServer/src/main/resources/application.properties`

```properties
server.port=8081
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.group-id=metrics-consumer-group
```

`auto-offset-reset=earliest`: When the consumer group `metrics-consumer-group` has no committed offset (e.g., first startup or reset), start reading from the earliest available message. This means replaying all historical events on first run.

---

## Frontend

### `landing-page/src/LandingPage.js`

**Purpose:** The entire UI. A single functional component with local state.

**State:**
```javascript
const [counts, setCounts] = useState({});     // {addToCart: 3, checkout: 1, ...}
const [statuses, setStatuses] = useState({}); // {addToCart: 'loading'|'success'|'error'|null}
```

**`emitEvent(eventType)` — the core function:**

1. Set button to loading state.
2. `fetch('http://localhost:8080/producer/event', POST)` with JSON body.
3. On success: increment count, set status to 'success'.
4. On failure: set status to 'error'.
5. After 1500ms: clear status (button resets).

**EVENTS constant:**
```javascript
const EVENTS = [
  { type: 'pageView',   label: '👁 Page View', ... },
  { type: 'userClick',  label: '🖱 User Click', ... },
  { type: 'addToCart',  label: '🛒 Add to Cart', ... },
  { type: 'checkout',   label: '💳 Checkout', ... },
];
```
These map directly to the `type` field sent to the backend and used as the Kafka message key and Micrometer tag.

**Footer links:**
- `http://localhost:3001` → Grafana
- `http://localhost:9090` → Prometheus UI

**Design pattern:** The component is a pure **presentation + local-state** component with no external state management (no Redux/Context). Appropriate for this scope.

---

### `landing-page/package.json`

- `react-scripts 5.0.1` = Create React App 5, uses Webpack 5.
- No backend proxy configured (would need `"proxy": "http://localhost:8080"` in `package.json` to avoid CORS issues in development).
- No environment variable for the API base URL — it's hardcoded in `LandingPage.js:18`.

---

## Test Files

### `ProducerServer/app/src/test/java/org/example/AppTest.java`

```java
@Test public void appHasAGreeting() {
    App classUnderTest = new App();
    assertNotNull("app should have a greeting", classUnderTest.getGreeting());
}
```

**Status: BROKEN.** `App.java` has no `getGreeting()` method. This is the unmodified Gradle scaffold test. Running `./gradlew test` will fail with a compilation error.

**Fix:** Delete this file and write a meaningful integration test using `@SpringBootTest` and `EmbeddedKafka`.
