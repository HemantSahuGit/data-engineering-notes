# Kafka Sizing Guide — Partitions, Brokers & Replication Factor

> A practical, interview-ready guide to Kafka capacity planning with a worked example.

---

## Table of Contents

1. [Key Terminologies](#1-key-terminologies)
2. [Worked Example — Given Parameters](#2-worked-example--given-parameters)
3. [Step 1 — Calculate Raw Throughput](#3-step-1--calculate-raw-throughput)
4. [Step 2 — Calculate Partitions from Producer Side](#4-step-2--calculate-partitions-from-producer-side)
5. [Step 3 — Calculate Partitions from Consumer Side](#5-step-3--calculate-partitions-from-consumer-side)
6. [Step 4 — Final Partition Count](#6-step-4--final-partition-count)
7. [Step 5 — Replication Factor](#7-step-5--replication-factor)
8. [Step 6 — Broker Sizing](#8-step-6--broker-sizing)
9. [Summary Table](#9-summary-table)
10. [Common Mistakes to Avoid](#10-common-mistakes-to-avoid)
11. [Cross Questions & Answers](#11-cross-questions--answers)

---

## 1. Key Terminologies

### Event / Message
A single unit of data published to Kafka. In your case, one healthcare or pipeline event of average size 500 bytes.

### Topic
A logical channel to which producers write and from which consumers read. A topic is divided into one or more **partitions**.

### Partition
The fundamental unit of parallelism in Kafka. A topic is split into N partitions. Each partition is an ordered, immutable sequence of records. More partitions = more parallelism for both producers and consumers.

### Producer Throughput
The rate at which a single producer (or producer instance) can push data into a Kafka partition. Typically measured in MB/s or events/second. A standard producer can push **~10–100 MB/s** depending on hardware, batch size, compression, and network.

### Consumer Throughput
The rate at which a single consumer instance can read and process data from one partition. Consumers within the same consumer group each own one or more partitions exclusively. A standard consumer processes **~50–100 MB/s** at the network level, but real-world application processing (deserialisation, business logic, DB writes) often limits this to **1–5 MB/s**.

### Consumer Group
A logical grouping of consumer instances that together consume a topic. Each partition is consumed by exactly **one consumer within a group** at a time. Multiple consumer groups can independently read the same topic — each group maintains its own offset.

### Replication Factor
The number of copies of each partition maintained across brokers. A replication factor of 3 means the data exists on 3 different brokers — one **leader** and two **followers** (ISR — In-Sync Replicas).

### ISR (In-Sync Replicas)
The set of replicas that are fully caught up with the leader. Kafka only acknowledges a write when all ISR replicas have received it (when `acks=all`). If a replica falls behind, it is removed from the ISR.

### Broker
A single Kafka server node. It stores partitions, serves producer writes, and serves consumer reads. A Kafka cluster has multiple brokers for fault tolerance and scalability.

### Offset
A unique sequential ID assigned to every message within a partition. Consumers track their position using offsets — this is how Kafka guarantees ordered, exactly-once or at-least-once delivery.

### Lag
The difference between the latest offset produced and the offset last consumed by a consumer. Lag growing continuously = consumer is slower than producer — a critical production signal.

### Retention Period
How long Kafka retains messages on disk regardless of whether they have been consumed. Default is 7 days. After this window, messages are deleted permanently.

---

## 2. Worked Example — Given Parameters

| Parameter | Value |
|---|---|
| Ingress rate | 5,000 events/second |
| Average event size | 500 bytes |
| Number of consumer groups | 3 |
| Producer throughput per partition | 2 MB/s OR 4,000 events/second |
| Consumer throughput per partition | 1 MB/s OR 1,000 events/second |

---

## 3. Step 1 — Calculate Raw Throughput

First, convert ingress rate to MB/s so we have a single consistent unit for all calculations.

```
Raw throughput = events/sec × avg event size (bytes)
              = 5,000 × 500
              = 2,500,000 bytes/second
              = 2,500,000 / 1024 / 1024
              ≈ 2.38 MB/s
              ~ 2.5 MB/s (rounded for planning purposes)
```

> **Note:** Always round up in capacity planning — never down. Infrastructure that runs at 99% capacity has no headroom for spikes.

---

## 4. Step 2 — Calculate Partitions from Producer Side

A producer distributes messages across partitions. Each partition can handle at most the producer's per-partition throughput. We need enough partitions so the total capacity meets or exceeds raw throughput.

**Using MB/s:**
```
Partitions (producer, MB/s) = Total throughput / Producer throughput per partition
                             = 2.5 MB/s / 2 MB/s
                             = 1.25
                             → 2 partitions (round up)
```

**Using events/second:**
```
Partitions (producer, events/s) = Total events/s / Producer events/s per partition
                                 = 5,000 / 4,000
                                 = 1.25
                                 → 2 partitions (round up)
```

Both methods agree → **Producer side requires: 2 partitions**

---

## 5. Step 3 — Calculate Partitions from Consumer Side

Each partition is consumed by exactly one consumer instance per consumer group. So consumer throughput per partition is the bottleneck. We calculate how many partitions are needed for one consumer group to keep up.

**Using MB/s:**
```
Partitions (consumer, MB/s) = Total throughput / Consumer throughput per partition
                             = 2.5 MB/s / 1 MB/s
                             = 2.5
                             → 3 partitions (round up)
```

**Using events/second:**
```
Partitions (consumer, events/s) = Total events/s / Consumer events/s per partition
                                 = 5,000 / 1,000
                                 = 5
                                 → 5 partitions
```

> **Important:** There is a discrepancy between the MB/s calculation (3 partitions) and the events/s calculation (5 partitions). This happens because the two throughput metrics are not internally consistent — 1 MB/s ≠ 1,000 events/second at 500 bytes/event (1,000 × 500 = 500,000 bytes = 0.48 MB/s, not 1 MB/s). In a real interview, flag this inconsistency and use the **more conservative (higher) number** — always plan for the worst case.

**Consumer side requires: 5 partitions** (taking the higher, more conservative figure)

---

## 6. Step 4 — Final Partition Count

### Rule: Take the maximum across producer and consumer

```
Base partitions = max(producer partitions, consumer partitions)
                = max(2, 5)
                = 5 partitions
```

### Adjust for multiple consumer groups

Each consumer group independently consumes all partitions. Having 3 consumer groups does **not** require multiplying partitions — all groups read the same partitions. However, since each consumer group needs at least as many consumer instances as partitions to achieve maximum parallelism, and each group needs to keep up independently, a common practice is to ensure partitions are a **multiple of the number of consumer groups**. This makes partition assignment evenly distributable.

```
Adjusted partitions = 5 rounded up to nearest multiple of 3
                    = 6 partitions

Or conservatively:
Adjusted partitions = consumer groups × consumer partitions
                    = 3 × 5
                    = 15 partitions
```

> The choice between 6 and 15 depends on how isolated each consumer group's processing is. If all 3 consumer groups need to independently max out their own consumer throughput without interfering, 15 is the safer choice. If they are light consumers, 6 is sufficient.

For this exercise, using **15 partitions** as a safe baseline.

### Add future growth buffer (20%)

```
Partitions with buffer = 15 × 1.20
                       = 18 partitions
```

> **Final partition count: 18 partitions** ✅

> **Note on your calculation:** Your approach is correct. The only nuance is that the consumer group multiplication (×3) is a conservative safety choice — not a hard rule. Make sure to explain *why* you multiply by 3 in an interview, not just that you did it.

---

## 7. Step 5 — Replication Factor

### What is it?

Replication factor (RF) determines how many broker copies of each partition exist. A replication factor of 3 means:
- 1 **leader** partition (handles all reads and writes)
- 2 **follower** replicas (stay in sync, ready to take over)

### Rule of thumb

| Scenario | Recommended RF |
|---|---|
| Development / testing | 1 (no replication) |
| Standard production | 3 |
| Mission-critical / financial / healthcare | 3–5 |

### Why RF = 3?

```
With RF = 3:
- Can tolerate 1 broker failure (2 replicas remain)
- Kafka can continue serving reads and writes
- With min.insync.replicas = 2, writes are acknowledged
  only when at least 2 replicas have the data
```

```
With RF = 2:
- Can only tolerate 0 broker failures without data loss risk
- Not recommended for production
```

> **Your answer:** RF = 3 is correct for this healthcare/production use case. ✅

### Minimum broker count for RF = 3

You need **at least as many brokers as your replication factor**. With RF = 3, you need a minimum of 3 brokers.

```
Minimum brokers = Replication Factor = 3
```

---

## 8. Step 6 — Broker Sizing

### How many brokers do you need?

Each broker stores and serves partitions. With 18 partitions and RF = 3, the total number of partition replicas across the cluster is:

```
Total partition replicas = partitions × RF
                        = 18 × 3
                        = 54 partition replicas to distribute across brokers
```

With 3 brokers:
```
Partition replicas per broker = 54 / 3 = 18 replicas per broker
```

This is well within broker capacity. Kafka brokers can comfortably handle hundreds of partitions each.

### Disk sizing per broker

```
Retention period      = 7 days (default)
Total throughput      = 2.5 MB/s
Total data per day    = 2.5 × 86,400 = 216,000 MB/day ≈ 211 GB/day
Total data (7 days)   = 211 × 7 ≈ 1.5 TB (uncompressed)

With RF = 3:
Total storage across cluster = 1.5 TB × 3 = 4.5 TB
Per broker (3 brokers)       = 4.5 TB / 3 = 1.5 TB per broker
```

With compression (Snappy or LZ4, ~50% reduction):
```
Per broker (compressed) ≈ 750 GB per broker
```

Add 20–30% headroom:
```
Recommended disk per broker ≈ 1 TB per broker
```

### Network bandwidth per broker

Each broker handles both producer writes (incoming) and consumer reads (outgoing) for the partitions it leads, plus replication traffic.

```
Producer write traffic  = 2.5 MB/s (total, shared across brokers)
Consumer read traffic   = 2.5 MB/s × 3 consumer groups = 7.5 MB/s
Replication traffic     = 2.5 MB/s × (RF - 1) = 2.5 × 2 = 5 MB/s
                                                             ─────────
Total cluster traffic   = 2.5 + 7.5 + 5 = 15 MB/s
Per broker (3 brokers)  = 15 / 3 = 5 MB/s = 40 Mbps
```

A standard 1 Gbps NIC is more than sufficient. For high-throughput production, 10 Gbps NICs are recommended.

---

## 9. Summary Table

| Parameter | Calculated Value |
|---|---|
| Raw throughput | ~2.5 MB/s |
| Partitions (producer side) | 2 |
| Partitions (consumer side) | 5 |
| Base partitions (max) | 5 |
| Adjusted for 3 consumer groups | 15 |
| With 20% future buffer | **18 partitions** |
| Replication factor | **3** |
| Minimum brokers | **3** |
| Disk per broker (with compression) | **~1 TB** |
| Network per broker | **~40 Mbps** |

---

## 10. Common Mistakes to Avoid

1. **Not rounding up** — Always round up partition counts. Running a partition at 100% capacity leaves no room for spikes.

2. **Forgetting replication multiplies storage** — 1.5 TB of data with RF=3 means 4.5 TB of total disk across the cluster. A common interview mistake is ignoring this.

3. **Confusing consumer groups with consumers** — More consumer groups do NOT mean more partitions are needed. All groups read the same partitions independently.

4. **Ignoring replication traffic in network sizing** — Replication is a significant portion of broker network usage, especially with RF=3.

5. **Setting partitions too high** — More partitions = more overhead (leader election time, memory per broker, file handles). Don't add partitions unnecessarily.

6. **Not accounting for compression** — Snappy or LZ4 compression can reduce storage and network costs by 40–60%. Always mention this in sizing discussions.

7. **Inconsistent throughput units** — Make sure your MB/s and events/s assumptions are internally consistent. If they diverge, use the more conservative result.

---

## 11. Cross Questions & Answers

**Q: Why do you take the max of producer and consumer partition counts, not the average?**

> Because partitions are a hard constraint on parallelism. If you have 2 partitions but your consumers need 5 to keep up, you have a consumer lag problem that cannot be fixed without increasing partitions. Partitions can only be increased, never decreased — so we plan for the binding constraint from the start.

---

**Q: Can you increase partitions after a topic is created?**

> Yes, Kafka allows increasing partition count using `kafka-topics.sh --alter`. However, this breaks the ordering guarantee for keyed messages — messages with the same key may end up on different partitions after the change. For use cases that depend on key-based ordering (like CDC pipelines), increasing partitions requires careful planning and consumer restart coordination.

---

**Q: Why multiply by the number of consumer groups?**

> Each consumer group needs its own dedicated consumer instances. For a consumer group to achieve maximum parallelism, it needs at least as many consumer instances as there are partitions. If you have 3 consumer groups each needing 5 partitions worth of parallelism, the partition count needs to support all of them simultaneously without bottlenecking any one group. The ×3 approach is conservative — in practice, if consumer groups are light readers, fewer partitions suffice.

---

**Q: What happens if you have more consumers in a group than partitions?**

> Extra consumers sit idle. Kafka assigns exactly one consumer per partition within a group. If you have 18 partitions and 20 consumers in a group, 2 consumers receive no partition assignment and do nothing. This is wasted resource.

---

**Q: What is `min.insync.replicas` and how does it relate to replication factor?**

> `min.insync.replicas` (ISR) defines the minimum number of replicas that must acknowledge a write before Kafka considers it successful (when `acks=all`). The standard production setting is:
> - RF = 3
> - min.insync.replicas = 2
>
> This means: tolerate 1 broker going down without losing data. If only 1 replica is in sync and it goes down, Kafka stops accepting writes rather than risking data loss.

---

**Q: What is the impact of too many partitions on a broker?**

> Each partition is a directory on disk with its own log files. Too many partitions increase:
> - File handle usage (OS limit: typically 100K open files)
> - Memory usage (each partition uses RAM for index caching)
> - Leader election time (Zookeeper/KRaft must manage more state)
> - End-to-end latency (replication overhead grows linearly with partition count)
>
> A practical rule: a single broker should not exceed 2,000–4,000 partitions (including replicas).

---

**Q: Why is replication factor 2 not recommended for production?**

> With RF=2 and `min.insync.replicas=2`, losing even one broker makes the topic unavailable for writes — you have no remaining ISR. With RF=3 and `min.insync.replicas=2`, you can lose one broker and still have 2 ISR replicas, keeping the topic writable. RF=2 offers fault tolerance without availability, which defeats the purpose in production.

---

**Q: How would your sizing change if compression was enabled?**

> Enabling Snappy or LZ4 compression on the producer reduces the effective payload size by 40–60% for structured data like JSON or Avro. This means:
> - Throughput in MB/s drops significantly (fewer bytes on the wire)
> - Partition count driven by MB/s would decrease
> - Disk sizing per broker drops by the same ratio
> - Network usage drops
>
> However, partition count driven by events/second is unaffected by compression — you still need enough partitions to handle 5,000 events/second. In practice, enable compression always for production Kafka pipelines.

---

*Last updated: 2026 | Author: Hemant Sahu*
