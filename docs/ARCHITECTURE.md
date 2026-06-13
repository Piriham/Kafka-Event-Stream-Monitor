# Architecture

## High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Developer Machine                          │
│                                                                     │
│  ┌──────────────┐   HTTP POST    ┌──────────────────┐              │
│  │  React UI    │ ─────────────► │  ProducerServer  │              │
│  │ (port 3000)  │                │  (port 8080)      │              │
│  └──────────────┘                └────────┬─────────┘              │
│                                           │ KafkaTemplate.send()   │
│                                           ▼                        │
│                                  ┌──────────────────┐              │
│                                  │   Apache Kafka   │              │
│                                  │  topic: "testy"  │              │
│                                  │ 172.24.236.246   │              │
│                                  │    port 9092     │              │
│                                  └────────┬─────────┘              │
│                                           │ @KafkaListener         │
│                                           ▼                        │
│                                  ┌──────────────────┐              │
│                                  │  ConsumerServer  │              │
│                                  │  (port 8081)     │              │
│                                  └──────────────────┘              │
│                                                                     │
│  ┌────────────────┐   scrape /actuator/prometheus                  │
│  │   Prometheus   │ ──────────────────────────────► :8080 & :8081  │
│  │  (port 9090)   │                                                 │
│  └───────┬────────┘                                                 │
│          │ datasource                                               │
│          ▼                                                          │
│  ┌────────────────┐                                                 │
│  │    Grafana     │                                                 │
│  │  (port 3001)   │◄── browser navigates here from React footer    │
│  └────────────────┘                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Mermaid — High-Level Architecture

```mermaid
flowchart LR
    UI["React UI\n(port 3000)"]
    PS["ProducerServer\n(port 8080)"]
    K["Kafka Broker\ntopic: testy"]
    CS["ConsumerServer\n(port 8081)"]
    P["Prometheus\n(port 9090)"]
    G["Grafana\n(port 3001)"]

    UI -->|"POST /producer/event"| PS
    PS -->|"KafkaTemplate.send()"| K
    K -->|"@KafkaListener"| CS
    P -->|"scrape /actuator/prometheus"| PS
    P -->|"scrape /actuator/prometheus"| CS
    P -->|"datasource"| G
    UI -->|"browser link"| G
    UI -->|"browser link"| P
```

---

## Mermaid — Component Diagram

```mermaid
graph TD
    subgraph landing-page ["landing-page (React)"]
        LP[LandingPage.js]
        APP[App.js]
        APP --> LP
    end

    subgraph ProducerServer ["ProducerServer (Spring Boot :8080)"]
        PA[App.java]
        PC[ProducerController.java]
        PCfg[CorsConfig.java]
        PM[Event.java model]
        PA --> PC
        PA --> PCfg
        PC --> PM
    end

    subgraph ConsumerServer ["ConsumerServer (Spring Boot :8081)"]
        CA[App.java]
        CE[EventConsumer.java]
        CCfg[CorsConfig.java]
        CM[Event.java model]
        CA --> CE
        CA --> CCfg
        CE --> CM
    end

    LP -->|"fetch POST"| PC
    PC -->|"KafkaTemplate"| Kafka[(Kafka topic: testy)]
    CE -->|"@KafkaListener"| Kafka
    PC -->|"Micrometer Counter"| MR1[MeterRegistry]
    CE -->|"Micrometer Counter"| MR2[MeterRegistry]
    MR1 -->|"/actuator/prometheus"| Prometheus
    MR2 -->|"/actuator/prometheus"| Prometheus
```

---

## Mermaid — Event Flow

```mermaid
sequenceDiagram
    participant UI as React UI
    participant PS as ProducerServer :8080
    participant K as Kafka (topic: testy)
    participant CS as ConsumerServer :8081
    participant P as Prometheus
    participant G as Grafana

    UI->>PS: POST /producer/event {type, payload}
    PS->>PS: Validate event.type not blank
    PS->>PS: Jackson serialize → JSON string
    PS->>K: send(topic="testy", key=event.type, value=JSON)
    PS->>PS: Increment counter producer_events_sent_total[event_type]
    PS-->>UI: 200 OK "Event sent: <type>"

    K-->>CS: @KafkaListener delivers message
    CS->>CS: Jackson deserialize JSON → Event
    CS->>CS: Increment counter consumer_events_received_total[event_type]
    CS->>CS: println "Consumed event: ..."

    P->>PS: GET /actuator/prometheus (scrape interval)
    P->>CS: GET /actuator/prometheus (scrape interval)
    P->>G: serve metrics as datasource
    G-->>G: Update dashboard panels
```

---

## Architectural Patterns

| Pattern | Where Used | Why |
|---|---|---|
| **Event-Driven Architecture** | Kafka producer/consumer | Decouples event creation from processing; consumer can be scaled or replaced without changing producer |
| **Microservices** | Two separate Spring Boot apps | Each service owns its responsibility; can be deployed, scaled, and updated independently |
| **Dependency Injection** | Spring `@Autowired` via constructor | Testability; Spring manages object lifecycle |
| **MVC (partial)** | ProducerServer: Controller + Model | Standard REST controller pattern |
| **Singleton** | MeterRegistry, KafkaTemplate | Spring manages these as beans; one instance per application context |
| **Observer (via Kafka)** | ConsumerServer @KafkaListener | Consumer is notified when producer publishes; no polling |
| **CORS Filter** | CorsConfig in both servers | Security boundary: restricts cross-origin HTTP |

---

## Folder Structure

```
Kafka Mini Proj/
├── ProducerServer/                  # Spring Boot REST + Kafka producer
│   ├── app/
│   │   ├── build.gradle             # dependencies: spring-web, kafka, actuator, micrometer-prometheus
│   │   └── src/main/java/org/example/
│   │       ├── App.java             # @SpringBootApplication entry point
│   │       ├── config/
│   │       │   └── CorsConfig.java  # CORS filter bean
│   │       ├── controller/
│   │       │   └── ProducerController.java  # POST /producer/event
│   │       └── model/
│   │           └── Event.java       # POJO: type + payload
│   │   └── src/main/resources/
│   │       └── application.properties   # port 8080, kafka config, actuator
│   ├── gradle/libs.versions.toml    # version catalog (guava, junit)
│   └── settings.gradle              # root project name, includes 'app'
│
├── ConsumerServer/                  # Spring Boot Kafka consumer + metrics
│   ├── build.gradle                 # dependencies: spring-web, kafka, actuator, micrometer
│   └── src/main/java/org/example/
│       ├── App.java                 # @SpringBootApplication entry point
│       ├── config/
│       │   └── CorsConfig.java      # identical CORS filter
│       ├── consumer/
│       │   └── EventConsumer.java   # @KafkaListener on topic "testy"
│       └── model/
│           └── Event.java           # identical POJO
│       └── src/main/resources/
│           └── application.properties   # port 8081, kafka config, consumer group
│
└── landing-page/                    # React CRA frontend
    ├── package.json                 # React 18, react-scripts 5
    └── src/
        ├── App.js                   # root component → LandingPage
        ├── LandingPage.js           # event buttons, fetch calls, counters
        └── LandingPage.css          # dark-theme grid layout
```
