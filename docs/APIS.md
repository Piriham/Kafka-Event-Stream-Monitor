# APIs

## ProducerServer REST API (Base URL: http://localhost:8080)

### POST /producer/event

**Purpose:** Accept a user-action event from the frontend, validate it, publish it to Kafka, and increment the producer metric counter.

**Defined in:** `ProducerServer/app/src/main/java/org/example/controller/ProducerController.java`

**Annotations:**
- `@RestController` — marks class as REST controller, all methods return response body directly
- `@RequestMapping("/producer")` — base path prefix
- `@PostMapping("/event")` — HTTP POST on /producer/event

---

#### Request

```http
POST /producer/event
Content-Type: application/json
```

```json
{
  "type": "addToCart",
  "payload": "react-ui"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | String | **Yes** | Event category. Examples: `pageView`, `userClick`, `addToCart`, `checkout` |
| `payload` | String | No | Arbitrary metadata. React UI hardcodes `"react-ui"` |

---

#### Responses

**200 OK — Event published successfully**
```
Event sent: addToCart
```

**400 Bad Request — Missing or blank event type**
```
Event type is required
```

**500 Internal Server Error — Kafka or serialization failure**
```
Failed to send event: <exception message>
```

---

#### Internal Logic (line-by-line)

```
ProducerController.java:29  @PostMapping("/event")
ProducerController.java:29  sendEvent(@RequestBody Event event)
ProducerController.java:31    if (event.getType() == null || event.getType().isBlank())
ProducerController.java:32        return 400
ProducerController.java:35    payload = objectMapper.writeValueAsString(event)
ProducerController.java:36    kafkaTemplate.send("testy", event.getType(), payload)
ProducerController.java:38-42  Counter increment (producer_events_sent_total[event_type])
ProducerController.java:44    return 200 "Event sent: <type>"
ProducerController.java:45-48  catch → return 500
```

---

#### Kafka Effect

After a successful call, a message arrives on topic `testy`:
- **Key:** `event.getType()` (e.g., `"addToCart"`)
- **Value:** JSON string of the full Event object

The key is used by Kafka for partition routing — all events of the same type go to the same partition, preserving ordering per event type.

---

### Spring Actuator Endpoints (both servers)

Both ProducerServer (:8080) and ConsumerServer (:8081) expose:

| Endpoint | Description |
|---|---|
| `GET /actuator` | Lists all enabled endpoints |
| `GET /actuator/health` | Application health status |
| `GET /actuator/metrics` | All registered Micrometer metric names |
| `GET /actuator/prometheus` | **Primary** — OpenMetrics scrape target for Prometheus |
| `GET /actuator/info` | Application info |

Configured in both `application.properties` files:
```properties
management.endpoints.web.exposure.include=*
management.endpoint.prometheus.enabled=true
management.metrics.export.prometheus.enabled=true
```

---

## ConsumerServer (Base URL: http://localhost:8081)

The ConsumerServer exposes **no business REST endpoints**. It is a pure Kafka consumer + metrics server. The only HTTP surface is the Spring Actuator endpoints above.

---

## CORS Policy

Both servers apply identical CORS configuration via `CorsConfig.java`:

```java
config.addAllowedOrigin("http://localhost:3001");  // Grafana
config.addAllowedHeader("*");
config.addAllowedMethod("*");
config.setAllowCredentials(true);
```

**Implication:** Only requests from `http://localhost:3001` (Grafana) pass CORS pre-flight. The React app at `http://localhost:3000` is **not** in the CORS whitelist. This is a bug — the React app successfully calls the producer because browsers send CORS headers with the `Origin` of the calling page, and `localhost:3000` is not whitelisted. In practice this would cause CORS errors in strict browser environments. (See PRODUCTION_READINESS.md.)

---

## Interview Questions on the API

**Q: Why does the Kafka message use `event.getType()` as the key?**
A: Kafka uses the key to determine which partition a message goes to (consistent hashing). Using the event type as the key guarantees all `checkout` events land on the same partition, preserving ordering per event type. This matters for sequential processing of checkout steps.

**Q: Why serialize the Event object to a JSON string rather than sending the object directly?**
A: `KafkaTemplate<String, String>` is configured with `StringSerializer`. The system chose simplicity — one serialization format for both key and value. An alternative would be `KafkaTemplate<String, Event>` with a `JsonSerializer`, which moves serialization concern into the Kafka config.

**Q: What happens if Kafka is down when a request arrives?**
A: `kafkaTemplate.send()` returns a `CompletableFuture`. In the current code the future is not awaited, so Kafka failures are silently swallowed unless they throw synchronously. The error would only surface if the KafkaProducer buffer overflows or the connection cannot be established within the configured `max.block.ms` timeout. The response to the client could be `200 OK` even if the message was not delivered.
