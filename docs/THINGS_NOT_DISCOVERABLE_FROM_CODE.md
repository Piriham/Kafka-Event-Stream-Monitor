# Things Not Discoverable From Code

These are questions, assumptions, and missing context that cannot be answered by reading the source code alone.

---

## Operational Details Unknown From Code

### Kafka Broker Setup
- **How is the Kafka broker at `172.24.236.246` configured?**
  - Is it running in WSL2, a Docker container, or a remote VM?
  - What Kafka version is it? (Affects KRaft vs Zookeeper, feature availability)
  - How many partitions does the `testy` topic have? (Created manually? Default 1?)
  - What is the message retention policy? (Default 7 days, but was it changed?)
  - Is it a single-broker setup? (No replication factor information available)
- **Is Kafka configured with authentication?** (Code uses plain `PLAINTEXT://` but the broker may require SASL)

### Prometheus Configuration
- No `prometheus.yml` file exists in the repo. Unknown:
  - What scrape interval is configured?
  - What labels/relabeling rules are applied to the targets?
  - Whether both `:8080` and `:8081` are actually configured as scrape targets?
  - Is Prometheus running locally, in Docker, or in WSL2?

### Grafana Configuration
- No dashboard JSON files are in the repo. Unknown:
  - What dashboards exist?
  - What PromQL queries are used?
  - What alert thresholds (if any) are configured?
  - Why port 3001 instead of the default 3000? (Port conflict with React? Deliberate choice?)

---

## Business Context Questions

1. **Is this a learning project or the foundation of a real product?**
   - If a real product: what is the eventual consistency model for missing events?
   - If learning: what course/tutorial was this following?

2. **What does "course platform" mean in this context?**
   - Is there a real e-commerce flow planned? (The event types suggest checkout, cart — are these stubs for a real purchase flow?)

3. **Who are the intended analysts?**
   - Is Grafana for the developer only, or for a product team?
   - Are there SLA requirements (e.g., "checkout events must appear in Grafana within 30 seconds")?

4. **Why `testy` as the topic name?**
   - This suggests it was a test topic that was never renamed. In a real project, what would the correct domain name be?

---

## Decisions Made Outside the Codebase

1. **Why WSL2 IP for Kafka (`172.24.236.246`) instead of `localhost`?**
   - Likely because Kafka advertised listeners on the WSL2 network interface, not localhost. This is a common WSL2 gotcha.

2. **Why are there two separate build systems?**
   - ProducerServer uses multi-module Gradle (with `settings.gradle` + `libs.versions.toml`).
   - ConsumerServer uses single-module Gradle.
   - Were they developed at different times? Did a refactoring happen for the ProducerServer?

3. **Why is `spring-cloud-starter-bootstrap` in the ProducerServer?**
   - Was there a plan to use Spring Cloud Config Server? Was it removed but the dependency left behind?

4. **Why does the ProducerServer have a MySQL dependency that's never used?**
   - Was there a plan to persist events to MySQL? Was it abandoned?

5. **Why does ConsumerServer have `CorsConfig` if it has no REST endpoints?**
   - Was there a planned REST endpoint (e.g., GET /events) that was never implemented?

---

## Missing Operational Context

- **How is the system started?** No Makefile, no start script, no docker-compose.yml. Was there a manual startup sequence?
- **Is Grafana pre-configured or set up manually each time?** (No provisioning files in the repo)
- **What OS was this developed on?** The `.DS_Store` files in `ProducerServer` and `landing-page` indicate macOS development. The broker IP `172.24.236.246` suggests a Windows WSL2 environment. This may mean the developer switches between machines.
- **Has this system ever run end-to-end?** The presence of `.gradle` build cache files suggests Gradle has been run. The `.idea` workspace files suggest IntelliJ IDEA was used.

---

## Questions for the Original Developer

1. What tutorial or resource inspired this project structure?
2. Was the Kafka broker ever successfully connected to and were events visible in Grafana?
3. What is the next planned feature (database persistence, user authentication, real cart flow)?
4. Why use React (CRA) rather than a simpler HTML+JS page? Was a richer UI planned?
5. Were the dead dependencies (Lombok, MySQL, Guava, spring-cloud-bootstrap) intentional for future use?
6. What is the significance of the event payload being hardcoded to `"react-ui"` — was this meant to identify the source system?

---

## Assumptions Made in This Documentation

1. The system was built as a **learning project** demonstrating Kafka integration with Spring Boot.
2. The broker at `172.24.236.246:9092` is a **local WSL2** Kafka instance.
3. The topic `testy` has **1 partition** (the default when created without specifying partitions).
4. **No authentication** is configured on either the Kafka broker or the Spring Boot services.
5. Prometheus and Grafana are run **externally** (via Docker or local install), not managed by this repo.
6. The `AppTest.java` broken test was **never fixed** — it's a discarded Gradle scaffold.
