# Project Overview

## Purpose

A real-time **event-tracking pipeline** modelled after the analytics infrastructure used by e-learning or e-commerce platforms. When a user clicks a button on a course-platform landing page the action is immediately streamed through Apache Kafka, consumed by a metrics service, and made visible in Grafana dashboards — all within milliseconds.

The system answers the question: *"How do I wire a React UI → REST API → Kafka → metrics scraping → dashboard in the fewest moving parts?"*

---

## Users

| Actor | Role |
|---|---|
| End-user | Clicks buttons on the landing page (pageView, userClick, addToCart, checkout) |
| Operations / Analyst | Views Grafana dashboards to see event counts by type in real time |
| Developer | Learns how to build an event-driven pipeline with Spring Boot + Kafka + Prometheus + Grafana |

---

## Features

1. **Event Emission UI** — React single-page app with four event-type buttons; tracks local send counts and shows loading / success / error states.
2. **REST Producer API** — `POST /producer/event` on ProducerServer validates the event and publishes it to Kafka topic `testy` with the event type as the message key.
3. **Kafka Messaging** — Topic `testy` with string key/value serialization; consumer group `metrics-consumer-group`.
4. **Metrics Collection** — Both servers expose `/actuator/prometheus` endpoints. Micrometer counters `producer_events_sent_total` and `consumer_events_received_total` / `consumer_events_failed_total` are tagged by `event_type`.
5. **Observability Stack** — Prometheus scrapes both servers; Grafana visualizes the counters.
6. **CORS Configuration** — Both Spring Boot servers whitelist `http://localhost:3001` (Grafana) to allow browser-initiated requests.

---

## Architecture Style

**Event-Driven Microservices** — three independent processes communicate asynchronously via Kafka. The React UI talks to the producer over HTTP (synchronous); everything downstream is asynchronous.

---

## Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Frontend | React (Create React App) | 18.2 |
| Producer backend | Spring Boot | 3.2.2 |
| Consumer backend | Spring Boot | 3.2.2 |
| Message broker | Apache Kafka | (external, addr 172.24.236.246:9092) |
| Metrics library | Micrometer + Prometheus registry | bundled with Spring Boot 3.2.2 |
| Metrics scraper | Prometheus | port 9090 |
| Dashboard | Grafana | port 3001 |
| Build tool | Gradle | 8.6 (wrapper) |
| Language | Java | 21 |
| JSON | Jackson Databind | 2.16.1 |

---

## Deployment Model

Local / developer workstation. All components run on `localhost`:

| Service | Port |
|---|---|
| React UI | 3000 (default CRA) |
| ProducerServer | 8080 |
| ConsumerServer | 8081 |
| Kafka broker | 172.24.236.246:9092 (WSL2 or remote VM) |
| Prometheus | 9090 |
| Grafana | 3001 |

No Docker Compose, Kubernetes, or CI/CD pipeline is present — this is a local learning project.
