# Commands

## Prerequisites

- Java 21 JDK installed
- Node.js 18+ and npm installed
- Apache Kafka running at `172.24.236.246:9092` (WSL2, Docker, or remote VM)
- Prometheus configured to scrape `:8080/actuator/prometheus` and `:8081/actuator/prometheus`
- Grafana running on port 3001 with Prometheus as datasource

---

## 1. Start Kafka (if running locally via Docker)

```bash
# Start Zookeeper
docker run -d --name zookeeper -p 2181:2181 confluentinc/cp-zookeeper:latest

# Start Kafka broker
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=localhost:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:latest
```

Or using KRaft mode (Kafka without Zookeeper, Kafka 3.x+):
```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  bitnami/kafka:latest
```

---

## 2. Create the Kafka Topic

```bash
# Using Kafka CLI (inside container or with kafka-topics.sh on PATH)
kafka-topics.sh --create \
  --bootstrap-server 172.24.236.246:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic testy

# Verify topic exists
kafka-topics.sh --list --bootstrap-server 172.24.236.246:9092

# Inspect topic
kafka-topics.sh --describe --topic testy --bootstrap-server 172.24.236.246:9092
```

---

## 3. Build the ProducerServer

```bash
cd "D:\Java\Kafka Mini Proj\ProducerServer"

# Build (compiles + runs tests + packages JAR)
./gradlew build

# Build skipping tests
./gradlew build -x test

# Show dependency tree
./gradlew dependencies
```

---

## 4. Run the ProducerServer

```bash
cd "D:\Java\Kafka Mini Proj\ProducerServer"

# Run via Gradle (development)
./gradlew :app:bootRun

# Run compiled JAR (production-style)
java -jar app/build/libs/app.jar

# Run on a different port
java -jar app/build/libs/app.jar --server.port=9080

# Override Kafka broker at runtime
java -jar app/build/libs/app.jar --spring.kafka.bootstrap-servers=localhost:9092
```

ProducerServer starts on **http://localhost:8080**

---

## 5. Build the ConsumerServer

```bash
cd "D:\Java\Kafka Mini Proj\ConsumerServer"

./gradlew build
./gradlew build -x test
```

---

## 6. Run the ConsumerServer

```bash
cd "D:\Java\Kafka Mini Proj\ConsumerServer"

./gradlew bootRun

# Or via JAR
java -jar build/libs/ConsumerServer.jar
```

ConsumerServer starts on **http://localhost:8081**

---

## 7. Start the React Frontend

```bash
cd "D:\Java\Kafka Mini Proj\landing-page"

# Install dependencies (first time)
npm install

# Start development server (port 3000 by default)
npm start

# Build production bundle
npm run build

# Serve production build locally
npx serve -s build
```

React UI available at **http://localhost:3000**

---

## 8. Start Prometheus

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
# Run Prometheus
docker run -d --name prometheus \
  -p 9090:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus

# Or binary
./prometheus --config.file=prometheus.yml
```

Prometheus UI: **http://localhost:9090**

---

## 9. Start Grafana

```bash
docker run -d --name grafana \
  -p 3001:3000 \
  grafana/grafana
```

Grafana UI: **http://localhost:3001** (default login: admin/admin)

Add Prometheus datasource: `http://localhost:9090`

---

## 10. Test the API Manually

```bash
# Send a checkout event
curl -X POST http://localhost:8080/producer/event \
  -H "Content-Type: application/json" \
  -d '{"type":"checkout","payload":"curl-test"}'

# Send a pageView event
curl -X POST http://localhost:8080/producer/event \
  -H "Content-Type: application/json" \
  -d '{"type":"pageView","payload":"manual-test"}'

# Check Prometheus metrics on ProducerServer
curl http://localhost:8080/actuator/prometheus | grep producer_events

# Check Prometheus metrics on ConsumerServer
curl http://localhost:8081/actuator/prometheus | grep consumer_events

# Health checks
curl http://localhost:8080/actuator/health
curl http://localhost:8081/actuator/health
```

---

## 11. Consume Kafka Messages Directly (Debug)

```bash
# Watch messages on the testy topic from the beginning
kafka-console-consumer.sh \
  --bootstrap-server 172.24.236.246:9092 \
  --topic testy \
  --from-beginning \
  --property print.key=true \
  --property key.separator=": "

# Output example:
# addToCart: {"type":"addToCart","payload":"react-ui"}
# checkout: {"type":"checkout","payload":"react-ui"}
```

---

## 12. Run Tests

```bash
# ProducerServer unit tests
cd "D:\Java\Kafka Mini Proj\ProducerServer"
./gradlew test

# React tests
cd "D:\Java\Kafka Mini Proj\landing-page"
npm test
```

> Note: The only existing test (`AppTest.java`) calls `classUnderTest.getGreeting()` which doesn't exist on the `App` class — this is a broken scaffold test that will fail.
