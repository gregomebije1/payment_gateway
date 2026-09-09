# ISO 20022 Financial Payment Gateway (Cash-In / Settlement Engine)

An event-driven, high-throughput payment engine designed to process incoming financial transfers using the modern ISO 20022 messaging standard. This gateway ensures exactly-once processing semantics through a distributed idempotency layer and guarantees atomicity using the Transactional Outbox pattern.

## 🚀 Overview

This engine ingests financial messages, strictly validates them against official XML schemas (XSD), manages distributed processing locks, and streams transactional events reliably to downstream systems.

### Tech Stack
*   **Runtime:** Java 21 (utilising Virtual Threads for high-concurrency I/O optimization)
*   **Framework:** Spring Boot
*   **Message Broker:** Apache Kafka
*   **Caching & Locking:** Redis
*   **Database:** PostgreSQL / Cassandra

---

## 🏗️ Architecture & Core Mechanics

The gateway is built around a resilient, fault-tolerant architecture designed to maintain data integrity under heavy load or infrastructure failure.


```
[Client Request] ──> [HTTP/gRPC Endpoint]
                            │
                            ▼
              [Distributed Idempotency Layer] (Redis Lua Script)
                            │
                            ▼
                [Strict ISO 20022 XSD Validation]
                            │
             ┌──────────────┴──────────────┐
             │  Local ACID Transaction     │
             │                             │
             │  ├──> Save Payment Status   │
             │  └──> Write Outbox Event    │
             └──────────────┬──────────────┘
                            │
                            ▼
               [Debezium CDC / CDC Poller]
                            │
                            ▼
              [Kafka Topic: iso20022.payments.v1]

```
### 1. Distributed Idempotency Layer
To eliminate duplicate transaction processing, incoming requests are intercepted using an `Idempotency-Key` HTTP header or the `MsgId` extracted from the `pacs.008` header.
*   **Atomic Locking:** A Redis Lua script uses a `SETNX` + `TTL` strategy to safely acquire an atomic lock before processing begins.
*   **In-Flight Handling:** If a duplicate message arrives while processing is active, the system rejects it with an HTTP `409 Conflict` (or initiates polling).
*   **Completed Handling:** Once processing concludes, response payloads (including generated `pacs.002` payment status reports) are cached in the distributed cache to serve immediate, idempotent HTTP `200` responses for subsequent duplicate executions.

### 2. Transactional Outbox Pattern
Atomicity is guaranteed across the database and message broker by decoupling storage from event emission.
*   **Local ACID Boundary:** When a payment is validated, the service updates the payment entity status (e.g., `PENDING`) and writes a corresponding event record into an `outbox_events` table within a single local database transaction.
*   **Asynchronous Dispatch:** A background polling mechanism or a Change Data Capture (CDC) engine like Debezium streams events from the outbox table directly to Apache Kafka (`iso20022.payments.v1`), utilising manual offset commits for reliable delivery.

---

## 🛠️ Core Requirements

### Payload Ingestion
*   Exposes asynchronous HTTP/gRPC endpoints consuming **ISO 20022 pacs.008** (Financial Customer Credit Transfer) XML structures.
*   Enforces zero-tolerance schema validation against official ISO XSD specification files.

---

## 🧪 Testing Strategy

The repository includes a comprehensive testing suite verifying strict operational correctness, concurrent resilience, and fault tolerance.

### 1. Unit Testing
*   **Mocks:** Isolation of core components using mocked Redis locking mechanics and outbox processing workers.
*   **Edge Cases:** Boundary verification on XML parsing, covering malformed XSD formatting and missing mandatory fields (e.g., `SettlementInformation`, `InstructedAmount`).

### 2. Integration Testing
*   Utilises **Testcontainers** to spin up lightweight, ephemeral instances of PostgreSQL and Redis.
*   Validates that both the main payment entities and the `outbox_events` tables persist data within the identical transaction boundary.

### 3. Idempotency Verification (Concurrent Load Testing)
*   **Execution:** Simulated stress tests via automated tools (Gatling/JMeter) firing **100 concurrent HTTP POST requests** sharing an identical `MsgId` inside a narrow **10ms window**.
*   **Success Criteria:** 
    *   Exactly **1** payload is committed to the relational database.
    *   Exactly **1** message is successfully produced to the Kafka cluster.
    *   The remaining **99** requests receive a cached copy of the initial response or are safely rejected via an in-flight conflict error.

### 4. Resilience & Chaos Engineering
*   **Scenario:** Programmatic termination of the active Kafka broker container mid-transaction using chaos infrastructure.
*   **Success Criteria:** Financial payments remain safely committed inside PostgreSQL with zero data loss. The outbox processor enters an automated retry backoff loop, resuming seamless publication as soon as the Kafka cluster recovers.

