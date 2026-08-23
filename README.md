# Kafka Learning Project

A hands-on journey learning Apache Kafka with Java + Spring Boot, built step by step.
This document explains **what Kafka is**, **what we built**, **how to run it**, and **how to test it** — in plain language.

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

## 3. What we built

```
                    ┌──────────────────────────┐
                    │   docker-compose.yml     │
                    │      KAFKA BROKER         │   ← the "post office" (infrastructure)
                    │      localhost:9092      │
                    └────────────┬─────────────┘
                                 │  everyone connects here
                 ┌───────────────┴───────────────┐
                 │                               │
         ┌───────┴────────┐             ┌────────┴────────┐
         │ order-service  │  topic:     │ shipping-service │
         │  (producer)    │  "orders"   │  (consumer)      │
         │  POST /api/... │ ──────────▶ │  @KafkaListener  │
         └────────────────┘             └─────────────────┘
```

Three parts, each independent:

| Component | Folder | Role | Port |
|-----------|--------|------|------|
| **Kafka broker** | `docker-compose.yml` | Runs Kafka itself (KRaft mode, no ZooKeeper) | 9092 |
| **order-service** | `order-service/` | Producer — turns an HTTP request into an `Order` event (JSON) | 8086 |
| **shipping-service** | `shipping-service/` | Consumer — receives the `Order` and "ships" it | 8085 (or override) |

They only agree on the **JSON shape** of an `Order`, not on shared Java code — that's real microservice thinking (shipping-service could be rewritten in another language and nothing breaks).

### How an order flows

```
curl POST {"product":"Laptop","quantity":2,"price":999}
      │
      ▼
OrderController          → assigns a UUID as orderId
      │
      ▼
OrderProducer            → kafkaTemplate.send("orders", orderId, order)   [JSON serialized]
      │
      ▼
   KAFKA topic "orders"  → stored on a partition (chosen by hash of the key)
      │
      ▼
OrderConsumer            → @KafkaListener rebuilds the Order from JSON
      │
      ▼
log: "Received order ... shipping 2 x 'Laptop' (total $1998)"
```

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

### Step 2 — Start the producer (its own terminal)
```bash
cd C:/workspace/personal-project/kafka-demo/order-service
mvn spring-boot:run
```
Wait for `Started OrderServiceApplication`.

### Step 3 — Start a consumer (its own terminal)
```bash
cd C:/workspace/personal-project/kafka-demo/shipping-service
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8091"
```
Wait for `Started ShippingServiceApplication`.

### Step 4 — Send an order (PowerShell)
```powershell
Invoke-RestMethod -Uri "http://localhost:8086/api/orders" -Method Post -ContentType "application/json" -Body (@{ product = "Laptop"; quantity = 2; price = 999.0 } | ConvertTo-Json)
```
You should get back `Order placed & published: <id>`, and see it printed in the consumer's terminal.

> **Note (PowerShell):** use `Invoke-RestMethod` with `ConvertTo-Json` as above.
> Using `curl.exe -d "{\"...\"}"` fails with 400 because PowerShell mangles the quotes.

---

## 6. How to test scaling (the fun part)

**Scaling = running the consumer more than once.** The instances automatically form one group and split the work.

1. Make sure the topic has multiple partitions (we set 3):
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
   ```
2. Start **2–3 consumer instances**, each on a different port (repeat Step 3 with `--server.port=8091`, `8092`, `8093`).
3. Send a burst of orders (PowerShell):
   ```powershell
   for ($i=1; $i -le 9; $i++) {
     $body = @{ product = "Item-$i"; quantity = $i; price = 10.0 } | ConvertTo-Json
     Invoke-RestMethod -Uri "http://localhost:8086/api/orders" -Method Post -ContentType "application/json" -Body $body
   }
   ```
4. **Watch:** each consumer terminal prints a *different subset* of the orders. The work was divided across the group.

### Things you'll observe

- **Split is uneven** (e.g. 2 / 2 / 5) because the partition is chosen by `hash(orderId) % partitions`, not round-robin.
- **Add a 4th instance** → one consumer sits **idle** (only 3 partitions to go around). It's a hot standby.
- **Rebalance**: every time an instance joins or leaves, Kafka re-divides the partitions among the current members (you'll see "rebalance" / "partitions assigned" in the logs). There's a brief pause while this happens.

---

## 7. Useful Kafka CLI commands

Run inside the broker container (`docker exec kafka ...`).
On Windows Git Bash, prefix with `export MSYS_NO_PATHCONV=1` so paths aren't mangled.

**List topics**
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Describe a topic (partitions, replicas)**
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

**Increase partitions (can only go up, never down)**
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --alter --topic orders --partitions 3 --bootstrap-server localhost:9092
```

**Inspect a consumer group — members, assignment, and LAG**
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --describe --group shipping-group --bootstrap-server localhost:9092
```

**Just the group members (count the rows = group size)**
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --describe --group shipping-group --members --bootstrap-server localhost:9092
```

> **LAG** is the key health number: `end offset − current offset` = unread backlog.
> `0` = consumers are keeping up. Consistently rising lag = add partitions + consumers.

---

## 8. Key things learned (and gotchas hit)

- **KRaft mode**: modern Kafka needs no ZooKeeper — the broker manages itself.
- **`listeners` vs `advertised.listeners`**: `listeners` are the ports Kafka opens; `advertised.listeners` is the address handed back to clients and must be routable (never `0.0.0.0`). Getting this wrong crashed the broker on first boot.
- **JSON across services**: the producer stamps its own class name in a header. The consumer (different package) must ignore it: `spring.json.use.type.headers=false` + `spring.json.value.default.type`.
- **Topic creation**: the `NewTopic` bean creates a topic *only if it doesn't exist*, with the partitions/replicas you choose. It doesn't reconfigure an existing topic (except it can *increase* partitions).
- **Single broker** = everything has replication factor 1 (only one copy possible).
- **PowerShell quoting**: POST JSON with `Invoke-RestMethod` + `ConvertTo-Json`, not escaped `curl.exe`.

---

## 9. Progress & what's next

| Phase | Topic | Status |
|-------|-------|--------|
| 1 | Run Kafka in Docker; send/read from the CLI | ✅ Done |
| 2 | Core concepts (topic, partition, offset, groups) | ✅ Done |
| 3 | First Spring Boot producer + consumer | ✅ Done |
| 4 | Two microservices exchanging JSON `Order` events | ✅ Done |
| 5 | Partitions & scaling (multiple instances, rebalancing) | ✅ Done |
| 6 | Error handling: retries & dead-letter topics | ⏭️ Next |
| 7 | Advanced: Kafka Streams, exactly-once, schema registry | ⏳ Planned |
