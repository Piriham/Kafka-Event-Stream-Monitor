# Kafka Event Stream Monitor

A full-stack event tracking pipeline built to learn Kafka, Spring Boot, and observability.

A React UI fires user events → a Spring Boot producer publishes them to Kafka → a consumer picks them up and emits metrics → Prometheus scrapes both services → Grafana visualises it all in real time.

---

## Services

| Service | Port |
| --- | --- |
| React UI | 3000 |
| Producer (Spring Boot) | 8080 |
| Consumer (Spring Boot) | 8081 |
| Kafka | 9092 |
| Prometheus | 9090 |
| Grafana | 3001 |

---

## Stack

React · Spring Boot 3 · Apache Kafka · Micrometer · Prometheus · Grafana · Java 21 · Gradle

---

## Running Locally

```bash
# 1. Start Kafka
docker run -d -p 9092:9092 bitnami/kafka:latest

# 2. Producer
cd ProducerServer && ./gradlew :app:bootRun

# 3. Consumer
cd ConsumerServer && ./gradlew bootRun

# 4. React UI
cd landing-page && npm install && npm start
```
