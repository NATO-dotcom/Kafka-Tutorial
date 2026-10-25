# 🚀 Kafka Clients — A Beginner's Guide to Apache Kafka

A hands-on Python project that demonstrates Apache Kafka's core concepts using a **Producer** and **Consumer** built with [`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python). Kafka runs locally via Docker in **KRaft mode** (no ZooKeeper required).

---

## 📖 Table of Contents

1. [What is Apache Kafka?](#what-is-apache-kafka)
2. [Why Kafka?](#why-kafka)
3. [Core Concepts & Terminology](#core-concepts--terminology)
   - [Broker](#broker)
   - [Topic](#topic)
   - [Partition](#partition)
   - [Producer](#producer)
   - [Consumer](#consumer)
   - [Consumer Group](#consumer-group)
   - [Offset](#offset)
   - [KRaft Mode](#kraft-mode)
4. [How Kafka Works — The Big Picture](#how-kafka-works--the-big-picture)
5. [Project Structure](#project-structure)
6. [Prerequisites](#prerequisites)
7. [Getting Started](#getting-started)
   - [1. Start Kafka with Docker](#1-start-kafka-with-docker)
   - [2. Install Python Dependencies](#2-install-python-dependencies)
   - [3. Run the Producer](#3-run-the-producer)
   - [4. Run the Consumer](#4-run-the-consumer)
8. [Code Walkthrough](#code-walkthrough)
   - [Producer Explained](#producer-explained)
   - [Consumer Explained](#consumer-explained)
   - [Docker Compose Explained](#docker-compose-explained)
9. [Key Configuration Properties](#key-configuration-properties)
10. [Common Beginner Mistakes](#common-beginner-mistakes)
11. [Useful Kafka CLI Commands](#useful-kafka-cli-commands)
12. [Next Steps & Further Learning](#next-steps--further-learning)

---

## What is Apache Kafka?

**Apache Kafka** is an open-source **distributed event streaming platform**. Think of it as a very fast, very reliable **messaging system** that lets different applications talk to each other by reading and writing **streams of events** (messages).

Originally developed at **LinkedIn** and later donated to the **Apache Software Foundation**, Kafka is now used by thousands of companies (Netflix, Uber, Airbnb, etc.) for:

- **Real-time data pipelines** — moving data between systems instantly
- **Event-driven architectures** — reacting to things as they happen
- **Log aggregation** — collecting logs from many services into one place
- **Stream processing** — transforming or analysing data on the fly

> **Analogy:** Imagine Kafka is a post office. **Producers** drop off letters (messages). The post office organises them into **mailboxes** (topics/partitions). **Consumers** come and pick up their letters whenever they're ready. The post office never loses a letter and keeps a record of every delivery.

---

## Why Kafka?

| Feature | Description |
|---|---|
| **High Throughput** | Can handle millions of messages per second |
| **Durability** | Messages are written to disk and replicated across brokers — data is safe |
| **Scalability** | Add more brokers/partitions to scale horizontally |
| **Fault Tolerance** | If a broker goes down, replicas take over automatically |
| **Decoupling** | Producers and consumers are independent — they don't need to know about each other |
| **Replay** | Consumers can re-read old messages by resetting their offset |

---

## Core Concepts & Terminology

### Broker

A **broker** is a single Kafka server. It receives messages from producers, stores them on disk, and serves them to consumers. In production, you run a **cluster** of multiple brokers for reliability. In this project we run a **single broker** for simplicity.

### Topic

A **topic** is a named category or feed to which messages are published. Topics are like database tables — you organise your data by creating different topics for different types of events.

In this project, we use a topic called **`orders`**.

```
                ┌─────────────────────────┐
                │       Topic: orders     │
                │  ┌────┬────┬────┬────┐  │
                │  │ m1 │ m2 │ m3 │ m4 │  │  ← messages (in order)
                │  └────┴────┴────┴────┘  │
                └─────────────────────────┘
```

### Partition

Each topic is split into one or more **partitions**. Partitions allow Kafka to:

- **Parallelise** reads and writes across multiple brokers
- **Guarantee ordering** within a single partition

Messages within a partition are assigned a sequential **offset** (an integer ID).

```
Topic: orders
  Partition 0:  [ msg0 | msg1 | msg2 | msg3 ]
  Partition 1:  [ msg0 | msg1 ]
  Partition 2:  [ msg0 | msg1 | msg2 ]
```

### Producer

A **producer** is any application that **writes (publishes)** messages to a Kafka topic. Producers decide which topic (and optionally which partition) to send a message to.

In this project → [`producer.py`](producer.py)

### Consumer

A **consumer** is any application that **reads (subscribes to)** messages from a Kafka topic. Consumers pull messages at their own pace.

In this project → [`consumer.py`](consumer.py)

### Consumer Group

A **consumer group** is a set of consumers that work together to consume a topic. Each partition is assigned to **exactly one** consumer in the group, which means:

- Multiple consumers in the same group **share the workload** (each reads different partitions)
- Multiple consumer groups can **each read all messages independently** (like multiple subscribers)

In this project, the consumer group is called **`order-tracker`**.

```
                 Topic: orders (3 partitions)
            ┌──────────┬──────────┬──────────┐
            │  Part 0  │  Part 1  │  Part 2  │
            └────┬─────┴────┬─────┴────┬─────┘
                 │          │          │
     Consumer Group: order-tracker
        ┌────────┴──┐  ┌───┴────┐  ┌──┴────────┐
        │ Consumer A │  │ Cons B │  │ Consumer C │
        └───────────┘  └────────┘  └───────────┘
```

### Offset

An **offset** is a unique integer that identifies each message within a partition. Kafka uses offsets to track which messages a consumer has already read.

| Offset Term | Meaning |
|---|---|
| `earliest` | Start reading from the very first message in the partition |
| `latest` | Start reading only new messages that arrive after the consumer starts |

In this project we use `auto.offset.reset: earliest` so the consumer reads **all existing messages** on first startup.

### KRaft Mode

Historically, Kafka relied on **Apache ZooKeeper** to manage cluster metadata (which brokers are alive, topic configs, leader elections, etc.). Starting with Kafka 3.3+, Kafka introduced **KRaft (Kafka Raft)** mode which removes the ZooKeeper dependency entirely.

In KRaft mode, Kafka brokers manage their own metadata using the **Raft consensus protocol**. This is:

- ✅ Simpler to deploy (fewer moving parts)
- ✅ Easier to operate (no ZooKeeper cluster to maintain)
- ✅ The future of Kafka (ZooKeeper support is being phased out)

This project runs Kafka in **KRaft mode** — notice there's no ZooKeeper service in the `docker-compose.yml`.

---

## How Kafka Works — The Big Picture

```
  ┌──────────┐         ┌──────────────────────────────────┐         ┌──────────┐
  │          │  write   │         KAFKA BROKER             │  read   │          │
  │ Producer ├────────► │  Topic: "orders"                 ├────────►│ Consumer │
  │          │         │  ┌────────────────────────────┐  │         │          │
  └──────────┘         │  │ Partition 0: [m1][m2][m3]  │  │         └──────────┘
                       │  └────────────────────────────┘  │
                       │  Messages persisted to disk       │
                       └──────────────────────────────────┘
```

1. The **Producer** creates a message (a JSON-encoded order) and sends it to the `orders` topic.
2. The **Kafka Broker** receives the message, appends it to the appropriate partition, and persists it to disk.
3. The **Consumer** (subscribed to `orders` as part of the `order-tracker` group) polls the broker and reads new messages.
4. Kafka tracks the consumer's **offset** so it knows where to resume if the consumer restarts.

---

## Project Structure

```
kafka-clients/
├── docker-compose.yml       # Runs Kafka broker in KRaft mode
├── producer.py              # Sends order messages to the "orders" topic
├── consumer.py              # Reads and prints order messages from the "orders" topic
├── pyproject.toml           # Python project config (uses uv package manager)
├── uv.lock                  # Locked dependency versions
├── .python-version          # Python 3.14
├── .gitignore               # Ignores venv, pycache, build artifacts
├── src/
│   └── kafka_clients/
│       └── __init__.py      # Package entry point
└── README.md                # ← You are here
```

---

## Prerequisites

| Tool | Purpose | Install Guide |
|---|---|---|
| **Docker** & **Docker Compose** | Run the Kafka broker | [docs.docker.com/get-docker](https://docs.docker.com/get-docker/) |
| **Python 3.14+** | Run the producer and consumer scripts | [python.org/downloads](https://www.python.org/downloads/) |
| **uv** (recommended) | Fast Python package manager | [docs.astral.sh/uv](https://docs.astral.sh/uv/getting-started/installation/) |

---

## Getting Started

### 1. Start Kafka with Docker

```bash
cd kafka-clients
docker compose up -d
```

This pulls the `apache/kafka:latest` image and starts a single Kafka broker in KRaft mode on **port 9092**.

To verify it's running:

```bash
docker compose ps
```

You should see the `kafka` service running and healthy.

### 2. Install Python Dependencies

Using **uv** (recommended):

```bash
uv sync
```

Or using **pip**:

```bash
pip install confluent-kafka
```

### 3. Run the Producer

```bash
uv run python producer.py
```

**Expected output:**

```
Delivery succeeded: orders [0]
```

This sends a single order to the `orders` topic. Each time you run it, a new order with a unique UUID is created.

### 4. Run the Consumer

```bash
uv run python consumer.py
```

**Expected output:**

```
Consumer is running and subscribed to orders topic
Received order: 10 x frozen yoghurt from lara
```

The consumer continuously polls for new messages. Press **Ctrl+C** to stop it gracefully.

> 💡 **Tip:** Run the consumer first in one terminal, then run the producer in another terminal to see messages arrive in real time.

---

## Code Walkthrough

### Producer Explained

> **File:** [`producer.py`](producer.py)

```python
from confluent_kafka import Producer
import json
import uuid
```

**Step 1 — Configure the producer.** The only required config is `bootstrap.servers` — the address of the Kafka broker(s) to connect to.

```python
producer_config = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_config)
```

**Step 2 — Define a delivery callback.** This function is called once Kafka confirms (or fails) the message delivery. This is how Kafka provides **at-least-once** delivery guarantees.

```python
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivery succeeded: {msg.topic()} [{msg.partition()}]")
```

**Step 3 — Create and send a message.** We create a JSON order object, serialise it to bytes, and send it to the `orders` topic.

```python
order = {
    "order_id": str(uuid.uuid4()),
    "user": "lara",
    "item": "frozen yoghurt",
    "quantity": 10
}
value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)
```

**Step 4 — Flush.** `producer.flush()` blocks until all buffered messages are delivered. Without this, the script might exit before the message is actually sent.

```python
producer.flush()
```

---

### Consumer Explained

> **File:** [`consumer.py`](consumer.py)

**Step 1 — Configure the consumer.** Three key settings:

```python
consumer_config = {
    "bootstrap.servers": "localhost:9092",   # Kafka broker address
    "group.id": "order-tracker",             # Consumer group name
    "auto.offset.reset": "earliest"          # Where to start if no offset is stored
}
consumer = Consumer(consumer_config)
```

| Config Property | What It Does |
|---|---|
| `bootstrap.servers` | Address of the Kafka broker(s) |
| `group.id` | Identifies which consumer group this consumer belongs to |
| `auto.offset.reset` | `"earliest"` = read from the beginning; `"latest"` = read only new messages |

**Step 2 — Subscribe to the topic.**

```python
consumer.subscribe(["orders"])
```

You can subscribe to **multiple topics** by adding more names to the list.

**Step 3 — Poll for messages in a loop.**

```python
while True:
    msg = consumer.poll(1.0)        # Wait up to 1 second for a message
    if msg is None:                  # No message available
        continue
    if msg.error():                  # Kafka-level error
        print("Error:", msg.error())
        continue

    value = msg.value().decode("utf-8")
    order = json.loads(value)
    print(f"Received order: {order['quantity']} x {order['item']} from {order['user']}")
```

`consumer.poll(timeout)` is the core loop — it fetches messages from Kafka. The timeout (in seconds) controls how long to wait before returning `None`.

**Step 4 — Graceful shutdown.**

```python
except KeyboardInterrupt:
    print("\n Stopping consumer")
finally:
    consumer.close()    # Commits offsets and leaves the consumer group cleanly
```

Always call `consumer.close()` to ensure the consumer:
- Commits its current offsets
- Leaves the consumer group (so partitions can be reassigned)

---

### Docker Compose Explained

> **File:** [`docker-compose.yml`](docker-compose.yml)

```yaml
services:
  kafka:
    image: apache/kafka:latest
    ports:
      - "9092:9092"                      # Expose Kafka to the host machine
```

**Key environment variables:**

| Variable | Value | Purpose |
|---|---|---|
| `KAFKA_NODE_ID` | `1` | Unique ID for this broker node |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | This node acts as **both** a broker and a KRaft controller |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@localhost:9093` | The controller quorum — which nodes vote on metadata |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` | Replication factor for the internal offsets topic (set to `1` because we have only 1 broker; default is `3`) |
| `KAFKA_LISTENERS` | `PLAINTEXT://:9092,CONTROLLER://:9093` | Network listeners — `9092` for clients, `9093` for controller-to-controller |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://localhost:9092` | The address clients should use to connect |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `CONTROLLER` | Which listener name is used for KRaft controller traffic |

**Volume:**

```yaml
volumes:
  - kafka_kraft:/var/lib/kafka/data    # Persist Kafka data across container restarts
```

---

## Key Configuration Properties

### Producer Config

| Property | Description | Example |
|---|---|---|
| `bootstrap.servers` | Comma-separated list of broker addresses | `"localhost:9092"` |
| `acks` | Acknowledgement level: `0`, `1`, or `all` | `"all"` (strongest guarantee) |
| `retries` | Number of retries on transient failures | `3` |
| `linger.ms` | How long to batch messages before sending | `5` |
| `batch.size` | Max batch size in bytes | `16384` |

### Consumer Config

| Property | Description | Example |
|---|---|---|
| `bootstrap.servers` | Broker addresses | `"localhost:9092"` |
| `group.id` | Consumer group identifier | `"order-tracker"` |
| `auto.offset.reset` | Where to start reading: `earliest` or `latest` | `"earliest"` |
| `enable.auto.commit` | Auto-commit offsets periodically | `true` (default) |
| `auto.commit.interval.ms` | How often to auto-commit | `5000` (5 seconds) |

---

## Common Beginner Mistakes

| Mistake | Fix |
|---|---|
| Using `bootstrap.server` (singular) | ✅ Use `bootstrap.servers` (plural) |
| Forgetting `producer.flush()` | ✅ Always flush before exiting — messages are buffered |
| Not calling `consumer.close()` | ✅ Always close in a `finally` block |
| Setting `auto.offset.reset: latest` and wondering why you see no messages | ✅ Use `earliest` to read existing messages |
| Kafka broker not ready when producer/consumer connects | ✅ Wait a few seconds after `docker compose up` |
| Running multiple consumers in the same group with fewer partitions than consumers | ✅ Some consumers will be idle — increase partitions or use different groups |

---

## Useful Kafka CLI Commands

You can exec into the running Kafka container to use Kafka's built-in CLI tools:

```bash
# Enter the Kafka container
docker exec -it kafka-clients-kafka-1 bash
```

Then run these commands inside the container:

```bash
# List all topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create a topic manually
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic my-topic --partitions 3 --replication-factor 1

# Describe a topic (see partitions, replicas, ISR)
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders

# Read messages from a topic (console consumer)
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders --from-beginning

# Produce messages from the terminal (console producer)
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders

# Check consumer group offsets and lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group order-tracker --describe
```

---

## Next Steps & Further Learning

Once you're comfortable with the basics, explore these topics:

- **🔑 Message Keys** — Route related messages to the same partition for ordering guarantees
- **📦 Serialisation** — Use Avro, Protobuf, or JSON Schema with a **Schema Registry**
- **⚙️ Multiple Partitions & Consumer Scaling** — Add partitions and multiple consumers to parallelise processing
- **🔄 Exactly-Once Semantics (EOS)** — Use transactional producers and idempotent delivery
- **📊 Kafka Streams / Faust** — Process and transform streams of data in real time
- **🔗 Kafka Connect** — Move data between Kafka and external systems (databases, S3, Elasticsearch)
- **📈 Monitoring** — Use JMX, Prometheus, and Grafana to monitor broker and consumer health
- **☁️ Managed Kafka** — Try Confluent Cloud, AWS MSK, or Redpanda for production deployments

### Recommended Resources

| Resource | Link |
|---|---|
| Apache Kafka Official Docs | [kafka.apache.org/documentation](https://kafka.apache.org/documentation/) |
| Confluent Kafka Python Client Docs | [docs.confluent.io/kafka-clients/python](https://docs.confluent.io/kafka-clients/python/current/overview.html) |
| Kafka: The Definitive Guide (Book) | [O'Reilly](https://www.oreilly.com/library/view/kafka-the-definitive/9781492043072/) |
| Confluent Developer Tutorials | [developer.confluent.io](https://developer.confluent.io/) |

---

## 🧹 Cleanup

To stop and remove the Kafka container and its data:

```bash
# Stop the services
docker compose down

# Stop and remove persistent volumes (deletes all Kafka data)
docker compose down -v
```

---

## License

This project is for educational purposes. Feel free to use, modify, and share.

---

**Happy streaming! 🎉**
