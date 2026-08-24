# Event-Driven Order System (Kafka)

A hands-on journey learning Apache Kafka with Java + Spring Boot, built step by step —
starting from a single producer/consumer pair and growing into a 4-service **Saga** with a
real compensating transaction. This document explains **what Kafka is**, **what we built**,
**how to run it**, and **how to test it (including the failure paths)** — in plain language.

---

## 1. What is Kafka? (the 30-second version)

Kafka is a **post office for programs**. One program drops off a message ("an order was placed"); other programs pick it up when they're ready. Kafka stores the messages safely, keeps them in order, and doesn't lose them if something crashes.

**Why use it?** It lets services talk **without knowing about each other**. The order service just announces "order placed!" — it doesn't care who listens. You can add new listeners later without changing the sender. This is called **decoupling**.

**When to use it:** many systems need the same stream of events, high volume, or you want events kept and replayable.
**When NOT to:** simple request/response ("give me an answer now") — a normal REST call is better.

---

## 2. Core vocabulary

| Term | Plain meaning | Supermarket analogy |
|------|---------------|---------------------|
| **Broker** | One Kafka server (stores & serves messages) | The whole store |
| **Topic** | A named channel for messages | A product category |
| **Partition** | An ordered sub-lane of a topic; the unit of parallelism | A checkout lane |
| **Offset** | A message's position number in a partition (0, 1, 2, …) | Your place in line |
| **Producer** | A program that writes messages | A customer joining a lane |
| **Consumer** | A program that reads messages | A cashier |
| **Consumer Group** | A team of consumers sharing a topic's partitions | A team of cashiers on one bank of lanes |
| **Replication** | Copies of a partition on other brokers for safety | A backup cashier who mirrors the till |

### Two rules that explain almost everything

1. **Order is guaranteed only *within* a partition**, never across partitions.
   Many lanes run in parallel, so there's no single "overall" order.
   → Need related events in order? Give them the **same key** — same key always goes to the same partition.

2. **Each partition is owned by exactly one consumer in a group.**
   → More partitions = more consumers can work in parallel.
   → If consumers > partitions, the extras sit **idle** (as hot standbys).

---

## 3. What we built: a Saga with a compensating transaction

The system started as a simple producer → consumer pair. It has grown into a **choreographed
Saga**: four independent services that only ever talk through Kafka events, each reacting to
the previous stage's outcome — including a genuine **rollback** when a later step fails.

```
                         ┌──────────────────────────┐
                         │   docker-compose.yml     │
                         │      KAFKA BROKER         │
                         │      localhost:9092      │
                         └────────────┬─────────────┘
                                      │ everyone connects here
        ┌──────────────┬─────────────┴─────────────┬──────────────┐
        │              │                           │              │
 ┌──────┴──────┐ ┌─────┴───────┐            ┌──────┴──────┐ ┌─────┴───────┐
 │order-service│ │inventory-   │            │payment-     │ │shipping-    │
 │(producer)   │ │service      │            │service      │ │service      │
 │POST /orders │ │             │            │             │ │             │
 └──────┬──────┘ └──┬───────┬──┘            └──┬──────┬───┘ └──────┬──────┘
        │           │       │                  │      │            │
        │  orders   │       │ stock-events     │      │payment-    │
        └──────────►│       └─────────────────►│      │events      │
                     │RESERVED / REJECTED       │      └───────────►│
                     │                          │COMPLETED / FAILED │(only on
                     │◄─────────────────────────┘                   │ COMPLETED)
                     │  payment-events (FAILED only)
                     │  → releases the stock it reserved
                     │    (the compensating transaction)
```

| Component | Folder | Role | Port |
|-----------|--------|------|------|
| **Kafka broker** | `docker-compose.yml` | Runs Kafka itself (KRaft mode, no ZooKeeper) | 9092 |
| **order-service** | `order-service/` | Producer — turns an HTTP request into an `Order` event | 8086 |
| **inventory-service** | `inventory-service/` | Reserves/rejects stock; releases it on payment failure | 8087 |
| **payment-service** | `payment-service/` | Charges the order once stock is reserved | 8088 |
| **shipping-service** | `shipping-service/` | Ships the order once payment has completed | 8085 |

Every service keeps its own copy of the event shape (`OrderEvent` / `Order`) — they agree on
**JSON**, never on shared Java code. That's deliberate: it's the real-world constraint that
makes this a multi-service system rather than one app split into files.

### The saga, step by step

| # | Topic | Producer | Consumer(s) | What happens |
|---|-------|----------|-------------|---------------|
| 1 | `orders` | order-service | inventory-service | An order is placed. |
| 2 | `stock-events` | inventory-service | payment-service | Stock is reserved (`RESERVED`) or the order is rejected (`REJECTED`, insufficient stock) — payment only reacts to `RESERVED`. |
| 3 | `payment-events` | payment-service | shipping-service, inventory-service | Payment completes (`COMPLETED`) or fails (`FAILED`). Shipping only reacts to `COMPLETED`. |
| 4 | *(compensation)* | inventory-service | — | On `FAILED`, inventory-service **releases the stock it reserved in step 2** — the saga's compensating transaction. Nothing was double-charged or left inconsistent. |

**Why choreography (events reacting to events) instead of orchestration (one service calling
the others)?** No single service needs to know the whole flow — inventory-service doesn't know
shipping exists, and shipping doesn't know inventory exists. Each just reacts to what it's
subscribed to. The trade-off: the *overall* flow only exists implicitly, spread across services
— which is exactly why this README documents it explicitly.

**Idempotency:** every consumer (`inventory-service`, `payment-service`, `shipping-service`)
tracks which order IDs it has already processed and ignores redelivered duplicates. Kafka is
at-least-once, so without this, a redelivered message would double-decrement stock, double-charge,
or double-ship.

**Retries + dead-letter queue:** every consumer retries a failing message twice (1s apart), then
routes it to a `<topic>.DLT` dead-letter topic instead of blocking the partition forever on one
poison message.

---

## 4. Prerequisites

- **Docker Desktop** (runs the broker) — must be started.
- **Java 17** and **Maven** (to build/run the services).

Check:
```bash
docker --version
java -version
mvn -version
```

---

## 5. How to run everything

### Step 1 — Start the broker (once)
```bash
cd C:/workspace/personal-project/kafka-demo
docker compose up -d
```
Stop later with `docker compose stop`, start again with `docker compose start`.

### Step 2 — Start all four services (each in its own terminal)
```bash
cd C:/workspace/personal-project/kafka-demo/order-service      && mvn spring-boot:run   # :8086
cd C:/workspace/personal-project/kafka-demo/inventory-service  && mvn spring-boot:run   # :8087
cd C:/workspace/personal-project/kafka-demo/payment-service    && mvn spring-boot:run   # :8088
cd C:/workspace/personal-project/kafka-demo/shipping-service   && mvn spring-boot:run   # :8085
```
Wait for each to log `Started ...Application`. Order doesn't matter much — Kafka buffers events
until a consumer is ready — but starting order-service last makes the logs easiest to follow.

### Step 3 — Place an order (PowerShell)
```powershell
Invoke-RestMethod -Uri "http://localhost:8086/api/orders" -Method Post -ContentType "application/json" -Body (@{ product = "Laptop"; quantity = 2; price = 999.0 } | ConvertTo-Json)
```

Watch the four terminals: order-service publishes, inventory-service reserves stock and
publishes, payment-service charges and publishes, shipping-service ships.

> **Note (PowerShell):** use `Invoke-RestMethod` with `ConvertTo-Json` as shown.
> `curl.exe -d "{\"...\"}"` fails with 400 because PowerShell mangles the quotes.

---

## 6. Testing the three paths

### Path A — happy path
```powershell
Invoke-RestMethod -Uri "http://localhost:8086/api/orders" -Method Post -ContentType "application/json" -Body (@{ product = "Laptop"; quantity = 1; price = 500.0 } | ConvertTo-Json)
```
Check each stage:
```powershell
Invoke-RestMethod http://localhost:8087/api/inventory   # stock decremented
Invoke-RestMethod http://localhost:8088/api/payments    # COMPLETED
Invoke-RestMethod http://localhost:8085/api/shipments    # shipped
```

### Path B — insufficient stock (rejected before payment is ever attempted)
Order more than the seeded stock (100) of a **new** product name:
```powershell
Invoke-RestMethod -Uri "http://localhost:8086/api/orders" -Method Post -ContentType "application/json" -Body (@{ product = "RareItem"; quantity = 500; price = 50.0 } | ConvertTo-Json)
```
`inventory-service` logs a `REJECTED` and publishes to `stock-events` — `payment-service`
ignores it (filters for `RESERVED` only), so nothing is charged and nothing ships.

### Path C — payment fails → compensating transaction (the interesting one)
Any product name containing "fail" forces a deterministic payment failure:
```powershell
Invoke-RestMethod -Uri "http://localhost:8086/api/orders" -Method Post -ContentType "application/json" -Body (@{ product = "FailProduct"; quantity = 3; price = 20.0 } | ConvertTo-Json)
```
1. `inventory-service` reserves 3 units (stock goes down) — check `GET :8087/api/inventory`.
2. `payment-service` fails the charge — check `GET :8088/api/payments` → `status: FAILED`.
3. `inventory-service` consumes the `FAILED` event and **releases the 3 units back** —
   check `GET :8087/api/inventory` again: stock is back to what it was before step 1.
4. `shipping-service` never sees this order — check `GET :8085/api/shipments`.

That round trip — reserve, fail, release — is the saga's compensating transaction working end to end.

---

## 7. Testing scaling (the fun part)

**Scaling = running a consumer more than once.** The instances automatically form one group and split the work.

1. Every topic here has 3 partitions:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
   ```
2. Start 2–3 instances of any downstream service, each on a different port, e.g. for shipping-service:
   ```bash
   mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8091"
   ```
3. Send a burst of orders and watch the instances split the work by partition (uneven split is
   expected — partition is chosen by `hash(orderId)`, not round-robin).

---

## 8. Useful Kafka CLI commands

Run inside the broker container (`docker exec kafka ...`).
On Windows Git Bash, prefix with `export MSYS_NO_PATHCONV=1` so paths aren't mangled.

**List topics**
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```
You should see `orders`, `stock-events`, `payment-events`, and their `.DLT` counterparts.

**Describe a topic (partitions, replicas)**
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic stock-events --bootstrap-server localhost:9092
```

**Inspect a consumer group — members, assignment, and LAG**
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --describe --group payment-group --bootstrap-server localhost:9092
```

> **LAG** is the key health number: `end offset − current offset` = unread backlog.
> `0` = consumers are keeping up. Consistently rising lag = add partitions + consumers.

---

## 9. Key things learned (and gotchas hit)

- **Saga / choreography:** services react to each other's events with no central orchestrator;
  the overall flow only exists implicitly, spread across independent consumers.
- **Compensating transaction:** the "undo" for a step that already happened (releasing stock
  after a failed payment) — not a database rollback, since the reservation already committed
  and other services may have already reacted to it.
- **One event shape, multiple stages:** `OrderEvent` carries optional `status`/`reason` fields
  that are simply `null` on messages from a stage that doesn't set them. That let every
  service keep a single `spring.json.value.default.type`, even `inventory-service`, which
  consumes two different topics.
- **Idempotent consumers:** every consumer dedupes by `orderId` before acting, so Kafka's
  at-least-once delivery can never cause a double-decrement, double-charge, or double-ship.
- **KRaft mode**: modern Kafka needs no ZooKeeper — the broker manages itself.
- **`listeners` vs `advertised.listeners`**: `listeners` are the ports Kafka opens; `advertised.listeners` is the address handed back to clients and must be routable (never `0.0.0.0`).
- **JSON across services**: the producer stamps its own class name in a header by default; consumers ignore it (`spring.json.use.type.headers=false`) and rely on their own default type instead.
- **PowerShell quoting**: POST JSON with `Invoke-RestMethod` + `ConvertTo-Json`, not escaped `curl.exe`.

---

## 10. Progress & what's next

| Phase | Topic | Status |
|-------|-------|--------|
| 1 | Run Kafka in Docker; send/read from the CLI | ✅ Done |
| 2 | Core concepts (topic, partition, offset, groups) | ✅ Done |
| 3 | First Spring Boot producer + consumer | ✅ Done |
| 4 | Two microservices exchanging JSON `Order` events | ✅ Done |
| 5 | Partitions & scaling (multiple instances, rebalancing) | ✅ Done |
| 6 | Error handling: retries & dead-letter topics | ✅ Done |
| 7 | 4-service Saga with a compensating transaction, idempotent consumers | ✅ Done |
| 8 | Advanced: Kafka Streams, exactly-once, schema registry | ⏳ Planned |
