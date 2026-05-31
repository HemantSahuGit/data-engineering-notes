# Kafka Sizing Guide — Partitions, Brokers & Replication Factor

> A practical, interview-ready guide to Kafka capacity planning with a worked example.

---

## Table of Contents

1. [Key Terminologies](#1-key-terminologies)
2. [Ingress vs Egress vs Producer vs Consumer](#2-ingress-vs-egress-vs-producer-vs-consumer)
3. [Worked Example — Given Parameters](#3-worked-example--given-parameters)
4. [Step 1 — Calculate Raw Throughput](#4-step-1--calculate-raw-throughput)
5. [Step 2 — Calculate Partitions from Producer Side](#5-step-2--calculate-partitions-from-producer-side)
6. [Step 3 — Calculate Partitions from Consumer Side](#6-step-3--calculate-partitions-from-consumer-side)
7. [Step 4 — Final Partition Count](#7-step-4--final-partition-count)
8. [Step 5 — Replication Factor](#8-step-5--replication-factor)
9. [Step 6 — Broker Sizing](#9-step-6--broker-sizing)
10. [Summary Table](#10-summary-table)
11. [Common Mistakes to Avoid](#11-common-mistakes-to-avoid)
12. [Cross Questions & Answers](#12-cross-questions--answers)

---

## 1. Key Terminologies

### Event / Message
A single unit of data published to Kafka. In this example, one pipeline event of average size 500 bytes.

### Topic
A logical channel to which producers write and from which consumers read. A topic is divided into one or more **partitions**.

### Partition
The fundamental unit of parallelism in Kafka. A topic is split into N partitions. Each partition is an ordered, immutable sequence of records. More partitions = more parallelism for both producers and consumers.

### Producer Throughput
The rate at which a single producer instance can push data into a Kafka partition. Measured in MB/s or events/second. Measured from the **application's perspective** — how fast the app can produce and send.

### Consumer Throughput
The rate at which a single consumer instance can read and process data from one partition. Measured from the **application's perspective** — how fast the app can consume, deserialise, and process. Real-world processing (DB writes, business logic) limits this far below raw network speed.

### Consumer Group
A logical grouping of consumer instances that together consume a topic. Each partition is consumed by exactly **one consumer within a group** at a time. Multiple consumer groups can independently read the same topic — each group maintains its own offset.

### Replication Factor
The number of copies of each partition maintained across brokers. A replication factor of 3 means the data exists on 3 different brokers — one **leader** and two **followers** (ISR — In-Sync Replicas).

### ISR (In-Sync Replicas)
The set of replicas that are fully caught up with the leader. Kafka only acknowledges a write when all ISR replicas have received it (when `acks=all`). If a replica falls behind, it is removed from the ISR.

### Broker
A single Kafka server node. It stores partitions, serves producer writes, and serves consumer reads. A Kafka cluster has multiple brokers for fault tolerance and scalability.

### Offset
A unique sequential ID assigned to every message within a partition. Consumers track their position using offsets — this is how Kafka guarantees ordered, at-least-once or exactly-once delivery.

### Lag
The difference between the latest offset produced and the offset last consumed by a consumer. Lag growing continuously = consumer is slower than producer — a critical production alert signal.

### Retention Period
How long Kafka retains messages on disk regardless of whether they have been consumed. Default is 7 days. After this window, messages are deleted permanently.

---

## 2. Ingress vs Egress vs Producer vs Consumer

This is one of the most commonly confused concepts in Kafka interviews. The key distinction is **whose perspective** you are measuring from.

### Producer and Consumer — Application Perspective

| Term | Perspective | What it measures |
|---|---|---|
| **Producer throughput** | Application | How fast your app produces and sends messages to Kafka |
| **Consumer throughput** | Application | How fast your app reads and processes messages from Kafka |

These are the numbers you control and tune in your application code — batch size, compression, poll intervals, processing logic.

### Ingress and Egress — Broker Perspective

| Term | Perspective | What it measures |
|---|---|---|
| **Ingress** | Broker (network IN) | Data flowing INTO the Kafka broker from producers |
| **Egress** | Broker (network OUT) | Data flowing OUT OF the Kafka broker to consumers and replicas |

These are the numbers you monitor on the broker — used for network capacity planning, broker sizing, and infrastructure cost estimation.

### Why Egress is Always Larger Than Ingress

This is the critical insight. A producer writes data once (ingress). But that same data goes out multiple times:
- Once to each consumer group (egress to consumers)
- Once to each follower replica (egress for replication)

```
Producer writes 2.4 MB/s
        ↓
Broker receives 2.4 MB/s        ← INGRESS

Broker sends out:
  → Consumer Group 1: 2.4 MB/s  ┐
  → Consumer Group 2: 2.4 MB/s  ├─ Consumer egress = 2.4 × 3 = 7.2 MB/s
  → Consumer Group 3: 2.4 MB/s  ┘
  → Follower Replica 1: 2.4 MB/s ┐
  → Follower Replica 2: 2.4 MB/s ┘─ Replication egress = 2.4 × 2 = 4.8 MB/s
                                                          ──────────────────
Total broker egress                                     = 7.2 + 4.8 = 12 MB/s
Total broker network traffic    = ingress + egress = 2.4 + 12 = 14.4 MB/s
```

> So while your producer writes at 2.4 MB/s, each broker is handling ~4.8 MB/s of total network traffic (14.4 / 3 brokers). This is why broker network sizing cannot be based on producer throughput alone.

### Analogy to Remember

Think of a TV broadcast:
- The broadcaster (producer) transmits a signal once → **ingress to the tower**
- The tower (broker) retransmits to thousands of households (consumers) → **egress from the tower**
- The tower also sends a backup signal to redundant towers (replicas) → **replication egress**

The tower's outbound traffic is always a multiple of the inbound signal.

### Summary Comparison

```
┌─────────────────────────────────────────────────────────────────┐
│                     PERSPECTIVE COMPARISON                      │
├──────────────────┬──────────────────┬───────────────────────────┤
│ Term             │ Perspective      │ In Our Example            │
├──────────────────┼──────────────────┼───────────────────────────┤
│ Producer         │ Application OUT  │ 2.4 MB/s (writes to Kafka)│
│ Consumer         │ Application IN   │ 0.5 MB/s per partition    │
│ Broker Ingress   │ Broker Network IN│ 2.4 MB/s                  │
│ Broker Egress    │ Broker Network   │ 12 MB/s (consumers +      │
│                  │ OUT              │ replication)              │
└──────────────────┴──────────────────┴───────────────────────────┘
```

---

## 3. Worked Example — Given Parameters

### Consistent values — both MB/s and events/s are aligned

| Parameter | Value | Consistency check |
|---|---|---|
| Events per second | 5,000 | Given |
| Average event size | 500 bytes | Given |
| Raw throughput | **2.4 MB/s** | 5,000 × 500 / 1024 / 1024 = 2.38 ≈ 2.4 MB/s |
| Producer throughput per partition | **2 MB/s** | Industry standard for well-tuned producer |
| Producer events/s per partition | **4,000 events/s** | 2 MB/s / 500 bytes = 4,096 ≈ 4,000 ✅ consistent |
| Consumer throughput per partition | **0.5 MB/s** | Conservative — includes deserialisation + DB writes |
| Consumer events/s per partition | **1,000 events/s** | 0.5 MB/s / 500 bytes = 1,024 ≈ 1,000 ✅ consistent |
| Number of consumer groups | 3 | Given |

> **Why 0.5 MB/s for consumer?** At the network level a consumer can read 50–100 MB/s. But real-world processing — deserialisation, validation, writing to a database or Snowflake — drops effective throughput to 0.5–2 MB/s. Always use the application-level throughput, not raw network speed.

> **Consistency rule:** Producer: 2 MB/s = 4,000 events/s at 500 bytes/event → 4,000 × 500 = 2,000,000 bytes = 1.9 MB/s ✅ approximately consistent. Consumer: 0.5 MB/s = 1,000 events/s at 500 bytes/event → 1,000 × 500 = 500,000 bytes = 0.48 MB/s ✅ consistent. Both methods now agree.

---

## 4. Step 1 — Calculate Raw Throughput

Convert ingress rate to MB/s — the common unit for all calculations.

```
Raw throughput = events/sec × avg event size (bytes)
              = 5,000 × 500
              = 2,500,000 bytes/second
              = 2,500,000 / 1024 / 1024
              = 2.38 MB/s
              ≈ 2.4 MB/s (rounded up for planning)
```

> **Rule:** Always round up in capacity planning. Infrastructure running at 99% capacity has no headroom for traffic spikes.

---

## 5. Step 2 — Calculate Partitions from Producer Side

Each partition can handle at most the per-partition producer throughput. We need enough partitions so total capacity ≥ raw throughput.

**Using MB/s:**
```
Partitions (producer, MB/s)     = Total throughput / Producer throughput per partition
                                = 2.4 MB/s / 2 MB/s
                                = 1.2
                                → 2 partitions (round up)
```

**Using events/second:**
```
Partitions (producer, events/s) = Total events/s / Producer events/s per partition
                                = 5,000 / 4,000
                                = 1.25
                                → 2 partitions (round up)
```

✅ Both methods agree → **Producer side requires: 2 partitions**

---

## 6. Step 3 — Calculate Partitions from Consumer Side

Each partition is consumed by exactly one consumer instance per consumer group. Consumer throughput per partition is the bottleneck. Calculate how many partitions one consumer group needs to keep up.

**Using MB/s:**
```
Partitions (consumer, MB/s)     = Total throughput / Consumer throughput per partition
                                = 2.4 MB/s / 0.5 MB/s
                                = 4.8
                                → 5 partitions (round up)
```

**Using events/second:**
```
Partitions (consumer, events/s) = Total events/s / Consumer events/s per partition
                                = 5,000 / 1,000
                                = 5.0
                                → 5 partitions
```

✅ Both methods agree → **Consumer side requires: 5 partitions**

> Notice — with consistent values, both MB/s and events/s give the same answer. This is the sign that your assumptions are internally consistent. In an interview, always verify both methods agree before proceeding.

---

## 7. Step 4 — Final Partition Count

### Rule: Take the maximum across producer and consumer

```
Base partitions = max(producer partitions, consumer partitions)
                = max(2, 5)
                = 5 partitions
```

### Adjust for multiple consumer groups

All 3 consumer groups read the same partitions independently. More consumer groups do not require more partitions. However, to ensure partitions are evenly distributable across consumer instances in each group, and to give each group enough parallelism headroom, a conservative approach is:

```
Adjusted partitions = base partitions × number of consumer groups
                    = 5 × 3
                    = 15 partitions
```

> This is a conservative choice — each consumer group gets 5 partitions worth of dedicated parallelism. If consumer groups are lightweight readers, rounding 5 up to the nearest multiple of 3 (= 6) is sufficient instead.

### Add future growth buffer (20%)

```
Partitions with buffer = 15 × 1.20
                       = 18 partitions
```

> **Final partition count: 18 partitions** ✅

---

## 8. Step 5 — Replication Factor

### What is it?

Replication factor (RF) determines how many broker copies of each partition exist. RF = 3 means:
- 1 **leader** partition — handles all reads and writes
- 2 **follower** replicas — stay in sync, ready to take over on leader failure

### Rule of thumb

| Scenario | Recommended RF |
|---|---|
| Development / testing | 1 (no replication) |
| Standard production | 3 |
| Mission-critical / financial / healthcare | 3–5 |

### Why RF = 3?

```
With RF = 3:
  - Tolerates 1 broker failure (2 replicas remain)
  - Kafka continues serving reads and writes
  - With min.insync.replicas = 2, writes acknowledged
    only when at least 2 replicas confirm receipt

With RF = 2:
  - Tolerates 0 broker failures without data loss risk
  - Not recommended for production
```

### Minimum broker count

```
Minimum brokers = Replication Factor = 3
```

You need at least as many brokers as your replication factor. A replication factor of 3 on a 2-broker cluster is impossible.

---

## 9. Step 6 — Broker Sizing

### Partition replica distribution

```
Total partition replicas = partitions × RF
                        = 18 × 3
                        = 54 partition replicas across the cluster

Per broker (3 brokers)  = 54 / 3
                        = 18 replicas per broker
```

Kafka brokers comfortably handle hundreds of partitions each — 18 per broker is very light.

### Disk sizing per broker

```
Retention period         = 7 days (default)
Raw throughput           = 2.4 MB/s

Data per day             = 2.4 × 86,400 = 207,360 MB ≈ 202 GB/day
Data for 7 days          = 202 × 7      ≈ 1.4 TB (uncompressed, single copy)

With RF = 3 (3 copies):
Total cluster storage    = 1.4 TB × 3   = 4.2 TB
Per broker (3 brokers)   = 4.2 / 3      = 1.4 TB per broker

With Snappy compression (~50% reduction):
Per broker (compressed)  = 1.4 / 2      ≈ 700 GB per broker

Add 20–30% headroom:
Recommended disk         ≈ 1 TB per broker
```

### Network bandwidth per broker — Ingress and Egress breakdown

```
── CLUSTER LEVEL ──────────────────────────────────────────────

Ingress (producer writes)         =  2.4 MB/s
                                                               
Egress to consumers:
  Consumer Group 1                =  2.4 MB/s
  Consumer Group 2                =  2.4 MB/s
  Consumer Group 3                =  2.4 MB/s
  Total consumer egress           =  2.4 × 3  =  7.2 MB/s

Egress for replication:
  Follower Replica 1              =  2.4 MB/s
  Follower Replica 2              =  2.4 MB/s
  Total replication egress        =  2.4 × 2  =  4.8 MB/s
                                              ──────────────
Total cluster network traffic     =  2.4 + 7.2 + 4.8 = 14.4 MB/s

── PER BROKER (3 brokers) ─────────────────────────────────────

Network per broker                =  14.4 / 3 =  4.8 MB/s ≈ 38 Mbps
```

A standard 1 Gbps NIC handles this comfortably. For high-throughput production (100s of MB/s), 10 Gbps NICs are standard.

---

## 10. Summary Table

| Parameter | Calculated Value |
|---|---|
| Raw throughput (ingress) | **2.4 MB/s** |
| Producer throughput per partition | 2 MB/s = 4,000 events/s |
| Consumer throughput per partition | 0.5 MB/s = 1,000 events/s |
| Partitions — producer side | 2 |
| Partitions — consumer side | 5 |
| Base partitions (max of above) | 5 |
| Adjusted for 3 consumer groups (×3) | 15 |
| With 20% future buffer | **18 partitions** |
| Replication factor | **3** |
| Minimum brokers | **3** |
| Total partition replicas | 54 (18 per broker) |
| Disk per broker (compressed + headroom) | **~1 TB** |
| Broker egress (consumers + replication) | **12 MB/s** |
| Total cluster network traffic | **14.4 MB/s** |
| Network per broker | **~38 Mbps** |

---

## 11. Common Mistakes to Avoid

1. **Using inconsistent throughput units** — If you say producer handles 2 MB/s and also 4,000 events/s, verify: 4,000 × 500 bytes = 1.9 MB/s ✅. If the numbers don't reconcile, pick one and derive the other. Never use both independently.

2. **Not rounding up** — Always round up partition counts. A partition running at 100% has no spike headroom.

3. **Confusing ingress with producer throughput** — Ingress = broker perspective (network IN). Producer throughput = application perspective (how fast app sends). They are equal in value but different in meaning and where they are measured.

4. **Forgetting egress is a multiple of ingress** — With 3 consumer groups and RF=3, egress is 5× ingress. Always calculate egress separately for broker network sizing.

5. **Forgetting replication multiplies storage** — 1.4 TB of raw data × RF=3 = 4.2 TB total disk across the cluster.

6. **Using network-level consumer throughput** — A consumer can read 50+ MB/s at the network level. But application-level processing (DB writes, deserialisation) drops this to 0.5–2 MB/s. Always use the application-level number.

7. **Setting partitions too high** — More partitions = more file handles, more memory, longer leader election. A single broker should not exceed 2,000–4,000 partitions (including replicas).

8. **Ignoring compression** — Snappy or LZ4 reduces storage and network by 40–60%. Always mention this in sizing discussions.

---

## 12. Cross Questions & Answers

**Q: What is the difference between ingress and producer throughput?**

> Producer throughput is measured from the application's perspective — how fast your application produces and sends messages. Ingress is measured from the broker's perspective — how much data is arriving at the broker over the network. In value they are the same (2.4 MB/s), but the distinction matters when sizing: you tune producer throughput in your application, and you provision network bandwidth based on broker ingress/egress.

---

**Q: Why is egress always higher than ingress in Kafka?**

> Because the same data is sent out multiple times. A producer writes data once (ingress). The broker then sends that data out to every consumer group independently, plus to every follower replica for replication. With 3 consumer groups and RF=3, egress = ingress × (3 consumers + 2 replicas) = ingress × 5. This is why broker network capacity must always be planned based on egress, not ingress.

---

**Q: Why do you take the max of producer and consumer partition counts?**

> Because partitions are a hard constraint on parallelism. If you have 2 partitions but consumers need 5 to keep up, you have permanent consumer lag that cannot be resolved without repartitioning. We plan for the binding constraint upfront. Partitions can be increased later but at the cost of breaking key-based ordering.

---

**Q: Can you increase partitions after a topic is created?**

> Yes, using `kafka-topics.sh --alter`. However this breaks ordering guarantees for keyed messages — messages with the same key may land on different partitions after the change, breaking CDC-style ordering. For keyed topics, partition count should be finalised before go-live.

---

**Q: Why multiply by the number of consumer groups?**

> Each consumer group independently consumes all partitions at its own throughput. If all 3 groups need to max out at 5 partitions worth of parallelism simultaneously, the topic needs enough partitions to support all groups without any one group starving another. The ×3 approach is conservative. If consumer groups are lightweight, rounding base partitions to the nearest multiple of 3 is sufficient.

---

**Q: What happens if you have more consumers in a group than partitions?**

> Extra consumers sit idle. Kafka assigns exactly one consumer per partition within a group. If you have 18 partitions and 20 consumers in a group, 2 consumers receive no partition and do nothing — wasted compute.

---

**Q: What is `min.insync.replicas` and how does it relate to replication factor?**

> `min.insync.replicas` defines the minimum number of replicas that must acknowledge a write before Kafka considers it successful (when `acks=all`). Standard production setting:
> - RF = 3, min.insync.replicas = 2
>
> This means: tolerate 1 broker going down without losing data or halting writes. If only 1 ISR remains and it goes down, Kafka stops accepting writes rather than risking data loss.

---

**Q: What is the impact of too many partitions on a broker?**

> Each partition is a directory on disk with its own log files. Too many partitions increase file handle usage (OS limit ~100K), memory for index caching, leader election time, and replication overhead. A practical ceiling: 2,000–4,000 partitions per broker including replicas.

---

**Q: Why is RF=2 not recommended for production?**

> With RF=2 and `min.insync.replicas=2`, losing one broker makes the topic unavailable for writes — no ISR remains. With RF=3 and `min.insync.replicas=2`, losing one broker still leaves 2 ISR replicas and the topic stays writable. RF=2 gives fault tolerance without availability, which is counterproductive.

---

**Q: How does compression affect your sizing?**

> Snappy or LZ4 compression on the producer reduces payload size by 40–60% for structured data like JSON or Avro. This lowers broker ingress, egress, disk usage, and network bandwidth proportionally. However, partition count driven by events/second is unaffected — you still need partitions to handle 5,000 events/second regardless of payload size. Enable compression by default for all production Kafka pipelines.

---

*Last updated: 2026 | Author: Hemant Sahu*
