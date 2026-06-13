# Security

## Current Security Posture: Minimal / Development-Grade

This project has no authentication, no authorization, and no secrets management. It is appropriate for local development only.

---

## 1. CORS Configuration

**Files:** `CorsConfig.java` in both ProducerServer and ConsumerServer.

```java
config.setAllowCredentials(true);
config.addAllowedOrigin("http://localhost:3001");  // Grafana only
config.addAllowedHeader("*");
config.addAllowedMethod("*");
```

### Issue: Wrong Origin Whitelisted

The React UI runs on `http://localhost:3000`, but only `http://localhost:3001` (Grafana) is whitelisted. This means browser `fetch()` calls from the React UI should fail with a CORS policy error in compliant browsers.

In practice, the React development server may proxy the request, or the browser may not enforce CORS for `localhost` → `localhost` calls depending on the browser version.

**Fix:**
```java
config.addAllowedOrigin("http://localhost:3000");  // React UI
config.addAllowedOrigin("http://localhost:3001");  // Grafana
```

Or use `config.addAllowedOriginPattern("http://localhost:*")` for all localhost ports during development.

### Issue: `addAllowedHeader("*")` + `setAllowCredentials(true)`

Per the CORS spec, wildcard headers are not allowed when `allowCredentials=true`. Spring handles this gracefully, but it is technically non-conformant. Production code should enumerate specific headers.

---

## 2. No Authentication or Authorization

**Risk:** `POST /producer/event` is completely open. Any client on the network can publish arbitrary events.

**Attack scenarios:**
- Metric poisoning: Send thousands of fake `checkout` events → inflate conversion metrics.
- Denial of service: Flood the Kafka topic, causing consumer lag.

**Production Fix Options:**
- **API Key**: Add `X-API-Key` header validation in a Spring `HandlerInterceptor`.
- **JWT / OAuth 2.0**: Use `spring-security-oauth2-resource-server` to validate Bearer tokens.
- **mTLS**: Client certificates for service-to-service communication.

---

## 3. No Input Sanitization Beyond Null Check

**File:** `ProducerController.java:31`

```java
if (event.getType() == null || event.getType().isBlank()) {
    return ResponseEntity.badRequest().body("Event type is required");
}
```

Only `type` is validated. The `payload` field is completely unchecked. A malicious actor could send:
- Extremely large payloads (memory pressure on Kafka broker).
- Script injection strings in `payload` (irrelevant here since payload is never rendered to HTML, but worth noting).
- SQL injection strings (irrelevant — no database — but shows pattern of missing validation).

**Production Fix:** Add `@Valid` with Bean Validation constraints:
```java
public class Event {
    @NotBlank
    @Size(max = 50)
    private String type;

    @Size(max = 1000)
    private String payload;
}
```

---

## 4. Hardcoded Kafka Broker IP

**Files:** Both `application.properties` files.

```properties
spring.kafka.bootstrap-servers=172.24.236.246:9092
```

This is a hardcoded WSL2/VM IP address. Problems:
- Changes on reboot.
- Cannot be promoted to staging/production without code changes.

**Fix:** Externalize via environment variables:
```properties
spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
```

---

## 5. No TLS / Encryption in Transit

Kafka communication is plain text (`PLAINTEXT://` listener). All Kafka messages and HTTP traffic are unencrypted.

**Production Fix:**
- Kafka: Configure `SSL` or `SASL_SSL` listener on broker; add keystore/truststore config to `application.properties`.
- HTTP: Put a reverse proxy (nginx, traefik) with TLS termination in front of the Spring Boot apps.

---

## 6. Actuator Endpoints Fully Exposed

```properties
management.endpoints.web.exposure.include=*
```

This exposes all actuator endpoints publicly, including sensitive ones:
- `/actuator/env` — shows all environment variables (may contain secrets)
- `/actuator/threaddump` — thread dump
- `/actuator/heapdump` — full JVM heap dump

**Fix:** Limit exposure in production:
```properties
management.endpoints.web.exposure.include=health,prometheus
management.endpoints.web.exposure.exclude=env,heapdump,threaddump
```

---

## 7. Error Message Leakage

```java
// ProducerController.java:47
return ResponseEntity.internalServerError()
    .body("Failed to send event: " + e.getMessage());
```

`e.getMessage()` may expose internal stack details (Kafka broker address, class names) to the client.

**Fix:**
```java
log.error("Failed to send event", e);
return ResponseEntity.internalServerError().body("Internal server error");
```

---

## Security Risk Summary

| Risk | Severity | Present? | Fix |
|---|---|---|---|
| No auth on API | High | Yes | Add JWT or API key |
| CORS wrong origin | Medium | Yes | Add localhost:3000 |
| Hardcoded broker IP | Medium | Yes | Use env var |
| No input size limits | Medium | Yes | Bean Validation |
| Actuator fully open | Medium | Yes | Limit exposure |
| No TLS | Medium | Yes | nginx/TLS termination |
| Error message leakage | Low | Yes | Generic error responses |
| No Kafka auth | High | Yes | SASL + SSL |
