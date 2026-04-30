# Databricks & PySpark — Scenario-Based Interview Notes

> Comprehensive study guide covering executor sizing, memory internals, small file problem,
> streaming schema evolution, compute strategy, alerting, and Delta merge patterns.
> Includes architecture diagrams, production code, and cross-questions for each topic.

---

## Table of Contents

1. [Q1 — Executor Sizing for a 10 GB Spark Job](#q1----executor-sizing-for-a-10-gb-spark-job)
2. [Q2 — Types of Memory in an Executor Container](#q2----types-of-memory-in-an-executor-container)
3. [Q3 — Unified Memory Manager: Dynamic Reallocation & Cache Eviction](#q3----unified-memory-manager-dynamic-reallocation--cache-eviction)
4. [Q4 — Streaming Pipeline with Schema Evolution (Auto Loader)](#q4----streaming-pipeline-with-schema-evolution-auto-loader)
5. [Q5 — Serverless vs All-Purpose Compute Architecture](#q5----serverless-vs-all-purpose-compute-architecture)
6. [Q6 — Databricks Alerting for Revenue KPI Drops](#q6----databricks-alerting-for-revenue-kpi-drops)
7. [Q7 — PySpark Delta Merge Patterns (All Variants)](#q7----pyspark-delta-merge-patterns-all-variants)
8. [Bonus — Executor Memory Config in Databricks UI vs Code](#bonus----executor-memory-config-in-databricks-ui-vs-code)
9. [Cross-Questions Master List](#cross-questions-master-list)
10. [Quick-Reference Cheat Sheet](#quick-reference-cheat-sheet)

---

## Q1 — Executor Sizing for a 10 GB Spark Job

### The Question
> You have a Spark job that needs to process 10 GB of data. How would you decide the number of cores and memory per executor? Explain your reasoning.

---

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│              CLUSTER  (6 nodes × 16 cores × 64 GB RAM)          │
│                                                                  │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────┐ │
│  │   Driver    │  │ Executor 1   │  │ Executor 2   │  │  ... │ │
│  │ Schedules   │→ │ 5 cores·20GB │→ │ 5 cores·20GB │  │      │ │
│  │   tasks     │  │              │  │              │  │      │ │
│  └─────────────┘  └──────────────┘  └──────────────┘  └──────┘ │
│                                                                  │
│  Partitions assigned to cores (1 partition per core per task)   │
│  [P1] [P2] [P3] [P4] [P5] [P6] [P7] [P8] ... [P80]            │
└─────────────────────────────────────────────────────────────────┘
```

---

### The Golden Rule — 5 Cores Per Executor

| Cores | Problem | Verdict |
|-------|---------|---------|
| 1 core per executor | Too many JVMs, HDFS underutilized, high overhead | ❌ Avoid |
| 5 cores per executor | Sweet spot — HDFS serves 5 threads efficiently, low GC | ✅ Use this |
| 15+ cores per executor | GC pauses on large heap, no task isolation | ❌ Avoid |

---

### Step-by-Step Calculation (6 nodes, 16 cores, 64 GB RAM)

| Parameter | Calculation | Result |
|-----------|-------------|--------|
| Executors per node | 16 cores ÷ 5 = 3 (1 core reserved for OS) | 3 executors/node |
| Memory per executor | 64 GB ÷ 3 ≈ 21 GB → reserve ~1 GB for OS | **20 GB** |
| Memory overhead | max(384 MB, 10% × 20 GB) | **2 GB** |
| Total executors | 3 × 6 nodes | **18 executors** |
| Total cores | 18 × 5 | **90 cores** |
| Partitions (10 GB) | 10,240 MB ÷ 128 MB per partition | **~80 partitions** |
| `default.parallelism` | 90 × 2 | **180** |

---

### Production Code

```python
spark = SparkSession.builder \
    .config("spark.executor.cores", "5") \
    .config("spark.executor.memory", "20g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.executor.instances", "18") \
    .config("spark.default.parallelism", "180") \
    .config("spark.sql.shuffle.partitions", "180") \
    .getOrCreate()
```

---

### Memory Breakdown Inside an Executor (20 GB)

```
┌─────────────────────────────────────────┐
│         JVM Heap (20 GB)                │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │  Reserved memory  — 300 MB fixed  │  │
│  ├───────────────────────────────────┤  │
│  │  Unified Managed Pool (60%)       │  │
│  │  ┌───────────────┬─────────────┐  │  │
│  │  │ Execution mem │ Storage mem │  │  │
│  │  │   ~5.91 GB    │  ~5.91 GB  │  │  │
│  │  │ Shuffle/Sort  │ Cache/RDDs  │  │  │
│  │  └───────────────┴─────────────┘  │  │
│  ├───────────────────────────────────┤  │
│  │  User memory (40%) — ~7.88 GB    │  │
│  │  UDFs, 3rd-party libs            │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
│  Memory Overhead (2 GB) — outside JVM   │
│  OS, Python UDFs, Netty buffers         │
└─────────────────────────────────────────┘
Total container ask = 20 GB + 2 GB = 22 GB
```

---

### Anti-Patterns

| Anti-pattern | Problem |
|---|---|
| 1 core per executor | Too many JVMs, HDFS underutilized |
| 1 fat executor per node | GC hell, no task isolation, SPOF |
| No memoryOverhead set | Container killed by YARN/K8s before Java even complains |
| shuffle partitions = 200 (default) | Causes tiny tasks for 10 GB — tune to total_cores × 2 |

---

### Interview One-Liner
> *"For a 10 GB job, I use 5 cores per executor as the baseline, calculate memory by dividing available node RAM by executors-per-node with a 10% OS reserve, set memory overhead to 10% of executor memory, and tune shuffle partitions to total cores × 2."*

---

### Cross-Questions for Q1

1. **What happens if you set 1 executor with all 16 cores on a node?**
   - Massive GC pauses. The heap shared across 16 concurrent tasks leads to frequent full GC. Also a single point of failure — if the executor dies, you lose the whole node's work.

2. **What is the difference between `spark.executor.instances` and autoscaling?**
   - `executor.instances` is a fixed count. Autoscaling lets the cluster grow/shrink between `minWorkers` and `maxWorkers` based on pending tasks. Use autoscaling for variable workloads, fixed instances for predictable batch jobs.

3. **Why do you leave 1 core for the OS per node?**
   - The OS, Databricks agent, and YARN NodeManager all need CPU. Running executors on all 16 cores starves system processes and causes task timeouts and heartbeat failures.

4. **How does `spark.sql.shuffle.partitions` differ from `spark.default.parallelism`?**
   - `default.parallelism` controls RDD operations and initial read parallelism. `shuffle.partitions` controls the number of partitions after a shuffle (groupBy, join, etc.). For SQL/DataFrame jobs tune `shuffle.partitions`; for RDD jobs tune `default.parallelism`.

5. **If the data grows to 100 GB, how do you scale your executor config?**
   - Scale out (more nodes/executors) rather than making each executor bigger. Target remains 128 MB per partition → ~800 partitions → set `shuffle.partitions = 800`. Add more nodes, keep the 5-core-per-executor rule.

6. **What is `spark.executor.memoryOverhead` and what uses it?**
   - Off-heap memory outside the JVM. Used by: OS threads, Python UDF worker processes (PySpark), Netty shuffle server buffers, Arrow-based Pandas UDFs. Default = max(384 MB, 10% of executor memory). Increase to 3–4 GB for PySpark heavy Pandas UDF workloads.

---

## Q2 — Types of Memory in an Executor Container

### The Question
> What are the types of memory in a Spark executor container? Explain with a diagram.

---

### Full Memory Layout Diagram

```
┌──────────────────────────────────────────────────────┐
│         YARN / K8s Container  (22 GB total ask)      │
│                                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │  Memory Overhead  (2 GB)  — OFF HEAP           │  │
│  │  OS · Python UDF processes · Netty buffers     │  │
│  │  native libs · NOT managed by JVM GC           │  │
│  └────────────────────────────────────────────────┘  │
│                                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │         JVM Heap  (20 GB = executor.memory)    │  │
│  │                                                │  │
│  │  ┌──────────────────────────────────────────┐ │  │
│  │  │  Reserved Memory  — 300 MB (fixed)        │ │  │
│  │  │  Spark internal objects & metadata        │ │  │
│  │  └──────────────────────────────────────────┘ │  │
│  │                                                │  │
│  │  Usable = (20 GB - 300 MB) = 19.7 GB          │  │
│  │                                                │  │
│  │  ┌──────────────────────────────────────────┐ │  │
│  │  │  Unified Managed Pool  (60% of usable)   │ │  │
│  │  │  = 11.82 GB  [spark.memory.fraction=0.6] │ │  │
│  │  │                                          │ │  │
│  │  │  ┌─────────────────┐ ← can borrow →      │ │  │
│  │  │  │ Execution Mem   │ ┌─────────────────┐ │ │  │
│  │  │  │   ~5.91 GB      │ │  Storage Mem    │ │ │  │
│  │  │  │ Shuffle, Sort   │ │   ~5.91 GB      │ │ │  │
│  │  │  │ Hash Joins, Agg │ │ Cache RDDs/DFs  │ │ │  │
│  │  │  │ Spills to disk  │ │ Broadcast vars  │ │ │  │
│  │  │  │ CAN evict store │ │ LRU eviction    │ │ │  │
│  │  │  └─────────────────┘ └─────────────────┘ │ │  │
│  │  └──────────────────────────────────────────┘ │  │
│  │                                                │  │
│  │  ┌──────────────────────────────────────────┐ │  │
│  │  │  User Memory  (40% of usable = 7.88 GB)  │ │  │
│  │  │  UDF internals · custom data structures   │ │  │
│  │  │  3rd-party libs · NOT managed by Spark    │ │  │
│  │  │  Overflow = OOM with no warning           │ │  │
│  │  └──────────────────────────────────────────┘ │  │
│  └────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

---

### Full Calculation Example (20 GB executor)

```
Usable memory     = (20 GB − 300 MB) × 0.6   =  11.82 GB  ← managed pool
Execution memory  =  11.82 GB × 0.5           =   5.91 GB
Storage memory    =  11.82 GB × 0.5           =   5.91 GB
User memory       = (20 GB − 300 MB) × 0.4   =   7.88 GB
Memory overhead   =  max(384 MB, 20 GB × 0.1) =   2.00 GB  ← outside JVM
─────────────────────────────────────────────────────────
Total container   =  20 GB + 2 GB             =  22.00 GB  ← what YARN allocates
```

---

### Unified Memory Pool — Borrowing Scenarios

```
Scenario A — Normal (no pressure):
[══════ Execution 50% ══════][══════ Storage 50% ══════]

Scenario B — Execution borrows idle storage:
[════════════ Execution 70% ════════════][═ Storage 30%]
                                          ↑ evicted LRU blocks

Scenario C — Heavy caching (ML iterative):
[═ Execution ═][═══════════ Storage 70% ════════════════]
  (idle)         fills execution's unused quota
```

---

### Key Configs

```python
spark.executor.memory            = "20g"   # JVM heap
spark.executor.memoryOverhead    = "2g"    # off-heap
spark.memory.fraction            = 0.6     # managed pool share
spark.memory.storageFraction     = 0.5     # storage share within managed pool
```

---

### The Asymmetry Rule (Most Important Interview Point)
> **Execution memory CAN evict storage memory. Storage memory CANNOT evict execution memory.**
>
> Reason: evicting data mid-shuffle would corrupt results. Storage blocks are just cache — they can always be recomputed.

---

### Interview One-Liner
> *"An executor container has four memory regions: off-heap overhead for OS and native buffers, a fixed 300 MB reserved region for Spark internals, a unified managed pool that dynamically splits between execution and storage memory, and an unmanaged user region. Total YARN ask = executor.memory + memoryOverhead."*

---

### Cross-Questions for Q2

1. **What happens when User Memory runs out?**
   - You get `java.lang.OutOfMemoryError` with no warning. Spark cannot spill or evict on your behalf since it doesn't manage that region. Fix: reduce complexity of UDFs, increase `executor.memory`, or reduce `memory.fraction` to give user memory a larger slice.

2. **Can you disable the Reserved Memory region?**
   - No. It is always 300 MB and hardcoded in `UnifiedMemoryManager.scala`. It cannot be configured.

3. **What is the difference between `spark.memory.storageFraction` and `spark.memory.fraction`?**
   - `memory.fraction` controls what share of usable heap goes to the managed pool (execution + storage combined). `storageFraction` controls the split *within* that pool between execution and storage. Default: 0.6 and 0.5 respectively.

4. **What is off-heap memory and when would you enable it?**
   - Off-heap memory bypasses JVM GC entirely and is managed manually. Enable with `spark.memory.offHeap.enabled = true` and set size with `spark.memory.offHeap.size`. Use for: reducing GC pauses on very large heaps, caching large DataFrames without GC pressure.

5. **How does Tungsten memory management relate to these regions?**
   - Tungsten uses sun.misc.Unsafe to manage memory in binary format, bypassing Java object overhead. It operates within the execution memory region and can use both on-heap and off-heap. It enables cache-friendly data structures and avoids Java serialization overhead.

---

## Q3 — Unified Memory Manager: Dynamic Reallocation & Cache Eviction

### The Question
> How does Spark's Unified Memory Manager allocate memory dynamically when execution needs more memory? What happens to cached data when jobs are slowing down and cached DataFrames are being evicted?

---

### The 4 Phases of Memory Pressure

```
Phase 1 — Normal Operation
━━━━━━━━━━━━━━━━━━━━━━━━━
[═══ Execution (50%) ═══][═══ Storage (50%) ═══]
Both pools healthy. No pressure. No eviction.

Phase 2 — Execution Borrows Idle Storage Quota
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[════════ Execution (70%) ════════][═ Storage ═]
Execution crossed the 50% line.
Borrows unused storage quota. No blocks evicted yet.
Storage region had free space — no conflict.

Phase 3 — LRU Eviction Triggered
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[══════════ Execution (85%) ══════════][Storage↓]
Execution needs space occupied by cached blocks.
Spark evicts LRU (least-recently-used) blocks from storage.
Cached DataFrame blocks are DROPPED from memory.
Next access of that DataFrame = FULL RECOMPUTE from source.

Phase 4 — Spill to Disk
━━━━━━━━━━━━━━━━━━━━━━━
[══════════ Execution (100%) — FULL ══════════]
No more storage to evict. Execution still needs more.
Intermediate shuffle/sort data serialized to local disk.
Job slows 10–100×. Disk I/O becomes the bottleneck.

Phase 5 — OutOfMemoryError
━━━━━━━━━━━━━━━━━━━━━━━━━
Spill also fails (no disk space / non-spillable operation).
Task throws OOM. Executor may be killed by YARN/K8s.
```

---

### `acquireExecutionMemory()` — Decision Flowchart

```
Task calls acquireExecutionMemory(N bytes)
           │
           ▼
┌─────────────────────────────────────┐
│ Free space in execution region?     │ ──YES──→ GRANT ✓
│ execFree = execLimit − execUsed     │
└─────────────────────────────────────┘
           │ NO
           ▼
┌─────────────────────────────────────┐
│ Idle (unused) storage quota?        │ ──YES──→ BORROW silently ✓
│ storUsed < storLimit                │
└─────────────────────────────────────┘
           │ NO
           ▼
┌─────────────────────────────────────┐
│ Evictable cached blocks in storage? │ ──YES──→ LRU EVICT → GRANT ✓
│ memoryStore.blocks (LRU order)      │           cached DF blocks dropped
└─────────────────────────────────────┘
           │ NO
           ▼
┌─────────────────────────────────────┐
│ Can task spill to disk?             │ ──YES──→ SPILL to disk → continue (slow)
│ Sort/shuffle spill supported        │
└─────────────────────────────────────┘
           │ NO
           ▼
     OutOfMemoryError thrown
     Task fails — executor may be killed
```

---

### Why Jobs Slow Down When Cache is Evicted

When a cached DataFrame block is evicted:
1. The data is **gone** from memory — no disk fallback unless `MEMORY_AND_DISK`
2. The next action on that DataFrame re-executes the **full lineage** from source
3. For iterative ML (e.g. MLlib training loops), this happens every iteration → catastrophic

---

### Simplified Spark Source Reference

```scala
// UnifiedMemoryManager.scala (simplified)
def acquireExecutionMemory(numBytes: Long, taskAttemptId: Long): Long = {
  def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
    if (extraMemoryNeeded > 0) {
      val reclaimable = math.max(
        storagePool.memoryFree,
        storagePool.poolSize - storageRegionSize)
      // Evict cached blocks LRU-first to free space
      storagePool.shrinkPoolToFreeSpace(math.min(extraMemoryNeeded, reclaimable))
    }
  }
  executionPool.acquireMemory(numBytes, taskAttemptId, maybeGrowExecutionPool)
}
```

---

### Fixes in Practice

```python
# Option 1 — Increase managed pool fraction
spark.conf.set("spark.memory.fraction", "0.8")          # default 0.6

# Option 2 — Give more of the managed pool to storage
spark.conf.set("spark.memory.storageFraction", "0.6")   # default 0.5

# Option 3 — Use MEMORY_AND_DISK to avoid total cache loss
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)
# Evicted blocks go to disk — slower but no full recompute

# Option 4 — Off-heap storage bypasses JVM GC
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")

# Option 5 — Increase overall executor memory
# --executor-memory 28g  (instead of 20g)
```

---

### Interview One-Liner
> *"The Unified Memory Manager treats execution and storage as a single fluid pool with a soft 50% boundary. When a task needs more memory, it first borrows idle storage quota, then evicts cached blocks LRU-first via `acquireExecutionMemory`. Storage can never evict execution memory mid-task because that would corrupt in-flight computation. When everything fails, Spark spills to disk. Job slowdowns happen because evicted cache blocks must be fully recomputed from source on next access."*

---

### Cross-Questions for Q3

1. **What is the difference between the old StaticMemoryManager and UnifiedMemoryManager?**
   - `StaticMemoryManager` (pre Spark 1.6) had fixed, non-borrowable pools: execution = 80% of heap, storage = 20%. If execution needed more it spilled immediately even if storage was idle. `UnifiedMemoryManager` allows dynamic borrowing — a major efficiency improvement.

2. **What happens to execution memory when a task finishes?**
   - It is released back to the pool via `releaseExecutionMemory()`. Storage can then reclaim it on the next `cache()` call. There is no automatic re-population of evicted cache blocks.

3. **Can you prevent Spark from evicting cached DataFrames?**
   - Not directly — you cannot pin a block. You can: (a) increase `storage.fraction` to give storage more space, (b) use `MEMORY_ONLY_SER` for compressed caching, (c) use off-heap storage, or (d) increase executor memory so there is enough room for both.

4. **What is the difference between `df.cache()` and `df.persist()`?**
   - `cache()` is equivalent to `persist(StorageLevel.MEMORY_AND_DISK)` for DataFrames. `persist()` lets you specify the `StorageLevel` explicitly: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`, `MEMORY_ONLY_SER`, `OFF_HEAP`, etc.

5. **When would spill to disk cause a job to fail rather than just slow down?**
   - When the spill directory (`spark.local.dir`) runs out of disk space. Also, some operations are non-spillable (e.g. certain hash aggregations with very large state). In those cases an OOM is thrown even though spill is conceptually possible.

6. **How does AQE (Adaptive Query Execution) help with memory pressure?**
   - AQE coalesces small shuffle partitions at runtime, reducing the number of tasks and the total shuffle data that needs to fit in execution memory. It also switches join strategies (e.g., sort-merge → broadcast) when it detects a small table, eliminating the shuffle entirely.

---

## Q4 — Streaming Pipeline with Schema Evolution (Auto Loader)

### The Question
> Your team receives event data as files landing continuously in cloud storage. The source team sometimes adds new columns or changes the schema without prior notice. You need to process these files in near-real time and load them into a Delta table without breaking the pipeline. How would you design this streaming solution to handle schema changes safely?

---

### End-to-End Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  SOURCE LAYER                                                    │
│  S3 / ADLS / GCS ──→ Event files land (JSON/Parquet/CSV)        │
│                       Source team adds new columns anytime       │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER — Auto Loader (cloudFiles)                      │
│  ┌──────────────────────────┐  ┌────────────────────────────┐   │
│  │  File detection          │  │  Schema inference          │   │
│  │  Incremental listing /   │  │  cloudFiles.inferColumn    │   │
│  │  event notifications     │  │  Types = true              │   │
│  │  Exactly-once processing │  │  Samples incoming files    │   │
│  └──────────────────────────┘  └────────────────────────────┘   │
│                                   Schema evolution auto-detect   │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  TRANSFORM LAYER — Structured Streaming                          │
│  mergeSchema=true │ Schema hints │ _rescued_data column          │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  SINK LAYER — Delta Lake                                         │
│  mergeSchema on write │ ACID transaction log │ Time travel       │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  DOWNSTREAM — BI · ML pipelines · downstream Delta tables        │
└──────────────────────────────────────────────────────────────────┘
```

---

### Schema Change Decision Flow (Per Micro-Batch)

```
New file lands in cloud storage
           │
           ▼
Auto Loader reads file + infers schema
schemaLocation stores current schema
           │
           ▼
┌──────────────────────────────────┐
│ Does incoming schema match       │ ──YES──→ Process normally ✓
│ stored schema?                   │
└──────────────────────────────────┘
           │ NO — schema changed
           ▼
What type of change?
     │              │              │
     ▼              ▼              ▼
New column      Type change    Column removed
added           int → string   from source
     │              │              │
     ▼              ▼              ▼
mergeSchema     Safe widening   Delta keeps
adds it to      auto-promoted   the column
Delta schema    Unsafe type →   New rows get
Old rows get    _rescued_data   NULL for that
NULL for new    saves the row   column
column          raw JSON
     │              │              │
     └──────┬────────┘──────────────┘
            │
            ▼
Delta write with mergeSchema = true
Schema update written atomically to transaction log
Readers see new schema on next query
            │
            ▼
Auto Loader updates schemaLocation checkpoint
Stream continues — no restart needed
```

---

### mergeSchema — Before and After

```
BATCH 1 — Original schema (3 columns):
┌──────────┬────────────┬────────────┐
│ event_id │ event_type │ timestamp  │
├──────────┼────────────┼────────────┤
│ evt_001  │ click      │ 2024-01-10 │
│ evt_002  │ view       │ 2024-01-10 │
│ evt_003  │ purchase   │ 2024-01-10 │
└──────────┴────────────┴────────────┘
Delta schema v1 in transaction log

New file arrives with extra column: user_country
      ↓ mergeSchema = true
      ↓ atomic schema update in transaction log

AFTER MERGE — Schema v2 (4 columns):
┌──────────┬────────────┬────────────┬──────────────┐
│ event_id │ event_type │ timestamp  │ user_country │
├──────────┼────────────┼────────────┼──────────────┤
│ evt_001  │ click      │ 2024-01-10 │ NULL         │ ← old rows
│ evt_002  │ view       │ 2024-01-10 │ NULL         │ ← old rows
│ evt_003  │ purchase   │ 2024-01-10 │ NULL         │ ← old rows
│ evt_004  │ click      │ 2024-01-11 │ IN           │ ← new row with value
└──────────┴────────────┴────────────┴──────────────┘
```

---

### schemaEvolutionMode Options

| Mode | Behaviour | Use When |
|------|-----------|----------|
| `addNewColumns` | New columns added automatically, old rows → NULL | Most common — safe default |
| `rescue` | New/incompatible columns → `_rescued_data` only | Review before committing |
| `failOnNewColumns` | Stream fails if schema changes | Strict contracts |
| `none` | Ignore schema changes, parse with original schema | Legacy pipelines |

---

### Production Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# ── READ STREAM ──
raw_stream = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation",    "/mnt/schema/events/")
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
  .option("cloudFiles.inferColumnTypes",  "true")
  .option("cloudFiles.rescuedDataColumn", "_rescued_data")
  .load("/mnt/landing/events/")
  .withColumn("_ingest_time", current_timestamp())
  .withColumn("_source_file", input_file_name())
)

# ── WRITE TO DELTA ──
query = (raw_stream.writeStream
  .format("delta")
  .option("checkpointLocation", "/mnt/checkpoints/events/")
  .option("mergeSchema", "true")
  .outputMode("append")
  .trigger(processingTime="30 seconds")
  .start("/mnt/delta/events/")
)

query.awaitTermination()
```

---

### Handling Incompatible Type Changes

```python
# Option A — Schema hints lock critical column types
.option("cloudFiles.schemaHints", "amount INT, user_id STRING")

# Option B — Safe cast defensively in transform layer
from pyspark.sql.functions import col, try_cast
df = raw_stream.withColumn("amount", try_cast(col("amount"), "INT"))

# Option C — Monitor rescued data and alert on non-empty
rescued_df = spark.read.format("delta").load("/mnt/delta/events/") \
    .filter("_rescued_data IS NOT NULL")
if rescued_df.count() > 0:
    send_alert(f"Schema incompatibility detected: {rescued_df.count()} rows rescued")
```

---

### Schema Audit with Delta Time Travel

```sql
-- See full schema history
DESCRIBE HISTORY delta.`/mnt/delta/events/`

-- Compare schemas across versions
DESCRIBE delta.`/mnt/delta/events/` VERSION AS OF 5
DESCRIBE delta.`/mnt/delta/events/` VERSION AS OF 10
```

---

### Interview One-Liner
> *"I'd use Auto Loader with `schemaEvolutionMode=addNewColumns` on the read side, backed by a persistent `schemaLocation` checkpoint, and write to Delta with `mergeSchema=true` so the transaction log absorbs new columns atomically. The `_rescued_data` column catches incompatible type changes without failing the stream. I'd monitor that column with alerts. The stream runs with 30-second micro-batches and never needs a manual restart on schema change."*

---

### Cross-Questions for Q4

1. **What is the difference between directory listing mode and file notification mode in Auto Loader?**
   - Directory listing: Auto Loader scans the directory on every trigger — works everywhere but slower at scale. File notification mode: uses cloud-native events (S3 SQS, Azure Event Grid) to detect new files instantly without scanning — zero-overhead at any scale. Notification mode is preferred in production.

2. **What is `cloudFiles.schemaLocation` and what happens if you delete it?**
   - It's a checkpoint path where Auto Loader persists the inferred schema between runs. If deleted, Auto Loader re-infers schema from scratch on the next run. This could cause issues if the current files have a different schema than what the Delta table expects — you may get schema mismatch errors on write.

3. **How does Auto Loader guarantee exactly-once file processing?**
   - It uses the checkpoint location to track which files have been processed. Even if the stream restarts, it reads the checkpoint and skips already-processed files. Combined with Delta's ACID writes, this gives end-to-end exactly-once semantics.

4. **What is the difference between `mergeSchema` and `overwriteSchema` in Delta?**
   - `mergeSchema=true` adds new columns to the existing schema — non-destructive, old data preserved. `overwriteSchema=true` replaces the entire schema — destructive, use only when doing a full table rewrite. Never use `overwriteSchema` in a streaming sink.

5. **Can you handle late-arriving data in this streaming pipeline?**
   - Yes. Add watermarking in the stream: `df.withWatermark("event_timestamp", "2 hours")`. Events arriving more than 2 hours late after the watermark will be dropped. For late data that must be included, use `MERGE` instead of append mode — write to a staging area, then merge into the Delta table periodically.

6. **What happens to the stream if the Delta table is OPTIMIZED while the stream is writing?**
   - Delta's ACID transaction log handles concurrent reads and writes safely. The stream will continue writing; OPTIMIZE runs in a separate transaction. The transaction log ensures readers and writers see a consistent view.

---

## Q5 — Serverless vs All-Purpose Compute Architecture

### The Question
> Daily ETL jobs run once a day (20–30 min). Ad-hoc analysis is done throughout the day. Cost optimization and cluster stability are important. How does the architecture differ between Serverless and All-Purpose compute? Which workloads go on each?

---

### Platform Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│         Shared Data Layer — Delta Lake on Cloud Storage         │
│              Bronze · Silver · Gold tables                      │
└────────────────────────┬────────────────────────────────────────┘
              ┌──────────┴──────────┐
              ▼                     ▼
┌─────────────────────┐   ┌──────────────────────┐
│  SERVERLESS         │   │  ALL-PURPOSE          │
│  COMPUTE            │   │  COMPUTE              │
│                     │   │                       │
│ • Daily ETL jobs    │   │ • DE notebook sessions│
│ • Analyst SQL       │   │ • ML model training   │
│ • DLT pipelines     │   │ • Complex Spark tuning│
│ • Scheduled tasks   │   │ • GPU workloads       │
│                     │   │                       │
│ Instant start ~2s   │   │ Cold start 4-8 min    │
│ Zero idle cost      │   │ Idle cost accrues     │
│ No cluster config   │   │ Full config control   │
│ Auto-scales 0→N     │   │ Autoscale (slower)    │
└─────────────────────┘   └──────────────────────┘
```

---

### Daily Cost Pattern — Why Serverless Wins for ETL

```
Cost / Utilisation Over 24 Hours
│
│   All-Purpose (persistent cluster):
│   ████████████████████████████████████   ← paying all day
│        ↑ ETL spike          ↑ analyst use
│   ░░░░░░░░ idle ░░░░░░░░░░░░░░░ idle ░░  ← wasted cost
│
│   Serverless:
│                ████                       ← ETL job (30 min only)
│                       ██ ███ ██ █         ← analyst queries (pay per query)
│   ▁▁▁▁▁▁▁▁▁▁▁▁    ▁▁▁▁  ▁  ▁▁  ▁▁▁▁▁▁  ← zero cost when idle
│
└──────────────────────────────────────────→ time (24h)
  00:00   04:00   08:00   12:00   16:00  20:00
```

---

### Side-by-Side Comparison

| Dimension | Serverless | All-Purpose |
|---|---|---|
| Startup time | ~2–5 seconds | 4–8 minutes cold start |
| Cluster management | Zero — fully managed | You own sizing & config |
| Idle cost | None — pay per second | Accrues when cluster is up |
| DBU rate | Higher per-DBU cost | Lower per-DBU cost |
| Config control | Limited — no custom Spark conf | Full Spark / JVM tuning |
| Scaling | Auto-scales 0 → N instantly | Autoscaling (minutes) |
| Session state | No — cold per job run | Yes — warm cache across cells |
| GPU support | Not available | Full GPU cluster support |
| Best workloads | ETL jobs, SQL, DLT, scheduled tasks | Interactive notebooks, ML, Spark tuning |
| Avoid for | Long iterative ML, fine-tuned Spark, GPU | Short scheduled jobs, analyst SQL |

---

### Cost Break-Even Calculation

```
All-Purpose cluster (running 24h):
  $2/DBU × 8 DBU × 24 hours = $384/day
  Productive use: 30 min ETL + 4h analyst = 4.5h
  Effective cost per productive hour = $384 ÷ 4.5h = $85/hour

Serverless (pay only when running):
  $3/DBU × 8 DBU × 4.5h active = $108/day
  Effective cost per productive hour = $108 ÷ 4.5h = $24/hour

Serverless is ~3.5× cheaper despite higher DBU rate.
Break-even: ~50% cluster utilisation.
Below 50% utilisation → Serverless wins on cost.
```

---

### Recommended Platform Topology

```
Platform topology
├── Serverless SQL Warehouse       ← all analyst SQL (Databricks SQL UI)
├── Serverless Job cluster         ← daily ETL (Workflows, 02:00 AM)
├── Serverless DLT pipeline        ← continuous/triggered Delta Live Tables
├── All-Purpose shared DE cluster  ← data engineer notebooks
│     autoscale 2→8 · auto-term 2h · spot instances
└── All-Purpose GPU cluster        ← ML training
      on-demand · auto-term 30min
```

---

### All-Purpose Cost Controls (Must Mention in Interview)

```python
# All-purpose cluster config (cluster.json)
cluster_config = {
    "cluster_name": "de-shared-cluster",
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "autoscale": {"min_workers": 2, "max_workers": 8},
    "autotermination_minutes": 120,           # KEY — kills idle cluster
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK"  # 60-70% cost saving
    },
    "spark_conf": {
        "spark.sql.shuffle.partitions": "200",
        "spark.executor.memory": "20g",
        "spark.databricks.delta.optimizeWrite.enabled": "true"
    }
}
```

---

### Interview One-Liner
> *"I'd route all scheduled and short-burst workloads — daily ETL and analyst SQL — to Serverless because they have low utilisation and the zero-idle-cost model wins decisively. Interactive engineering notebooks and ML training go on All-Purpose because engineers need warm session state and Spark tuning control, and I'd protect those clusters with auto-terminate and spot instances. The break-even is roughly 50% utilisation — below that Serverless is cheaper despite the higher DBU rate."*

---

### Cross-Questions for Q5

1. **What is a Serverless SQL Warehouse and how does it differ from a Pro or Classic warehouse?**
   - Classic: customer-managed VMs, you provision size. Pro: same VMs but adds Photon engine and query federation. Serverless: Databricks-managed VMs in Databricks' account — instant start, auto-scales to zero, no cluster config. Serverless is the most cost-efficient for bursty analyst workloads.

2. **Can you run MLflow experiments on Serverless compute?**
   - Yes — MLflow tracking works on Serverless. However, Serverless does not support GPU nodes, so you cannot do GPU-accelerated training. For CPU-based ML experiments, Serverless works fine.

3. **How do you handle library installation on Serverless compute?**
   - Serverless supports cluster libraries defined in the Workflow job config or via `%pip install` in notebooks. However, you cannot use init scripts or cluster-scoped library installs. Use `requirements.txt` in the Workflow environment config.

4. **What Spark configs can you still set on Serverless?**
   - Query-level configs only: `spark.sql.shuffle.partitions`, `spark.sql.adaptive.*`, `spark.databricks.delta.*`, `spark.sql.autoBroadcastJoinThreshold`, `spark.sql.files.maxRecordsPerFile`. You cannot set: `executor.memory`, `executor.cores`, `executor.memoryOverhead`, `memory.fraction`.

5. **When would you choose All-Purpose over Serverless even for a simple ETL?**
   - When the ETL requires custom Spark configs (e.g. specific memory tuning), uses custom JVM libraries, needs init scripts for system-level setup, or is part of a development workflow where engineers iterate on the notebook code interactively before scheduling it.

---

## Q6 — Databricks Alerting for Revenue KPI Drops

### The Question
> The business team tracks a daily revenue KPI in Databricks using a Delta table. If daily revenue drops by more than 10% compared to the previous day, business stakeholders should get an alert immediately on email or Slack. How would you design and implement this?

---

### End-to-End Alerting Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  DATA LAYER                                                  │
│  Delta table: gold.daily_revenue                             │
│  ETL writes daily row at 00:30 AM                           │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  DETECTION LAYER — 3 approaches                              │
│  ┌─────────────────┐ ┌─────────────────┐ ┌───────────────┐  │
│  │  Approach A     │ │  Approach B     │ │  Approach C   │  │
│  │  Databricks     │ │  Workflow +     │ │  DLT          │  │
│  │  SQL Alerts     │ │  Notebook       │ │  Expectations │  │
│  │  (no-code)      │ │  (Python)       │ │  (pipeline)   │  │
│  └─────────────────┘ └─────────────────┘ └───────────────┘  │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  ALERT LOGIC                                                 │
│  pct_change = (today_rev - yesterday_rev) / yesterday × 100  │
│  Alert fires when pct_change < -10                           │
└────────────────────┬─────────────────────────────────────────┘
                     │
          ┌──────────┼──────────┬─────────────┐
          ▼          ▼          ▼             ▼
       Email       Slack     PagerDuty    MS Teams
          └──────────┴──────────┴─────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  STAKEHOLDERS — receive alert with full context              │
│  Revenue today: $82,000 · Yesterday: $100,000 · Drop: -18%   │
└──────────────────────────────────────────────────────────────┘

Orchestration:  ETL job (00:30) → Alert check (01:00)
                  both tasks in same Databricks Workflow
```

---

### Workflow Task Chain

```
Databricks Workflow: daily-revenue-alert
cron: 0 30 0 * * ?  (00:30 AM daily)
│
├── Task 1: etl_job
│     Notebook: /ETL/daily_revenue_etl
│     Writes today's revenue row to Delta
│
└── Task 2: alert_check  [depends on Task 1]
      Notebook: /Alerts/revenue_drop_alert
      Reads Delta → computes pct_change → fires if < -10%
```

---

### Delta Table Schema

```sql
CREATE TABLE gold.daily_revenue (
    date        DATE,
    revenue     DECIMAL(18,2),
    order_count BIGINT,
    region      STRING
) USING DELTA
PARTITIONED BY (date);
```

---

### Approach A — Databricks SQL Alerts (No-Code)

```sql
-- Save as named query: "revenue_drop_check"
WITH today AS (
    SELECT SUM(revenue) AS rev
    FROM gold.daily_revenue
    WHERE date = CURRENT_DATE
),
yesterday AS (
    SELECT SUM(revenue) AS rev
    FROM gold.daily_revenue
    WHERE date = CURRENT_DATE - INTERVAL 1 DAY
)
SELECT
    t.rev                                        AS today_revenue,
    y.rev                                        AS yesterday_revenue,
    ROUND((t.rev - y.rev) / y.rev * 100, 2)     AS pct_change
FROM today t, yesterday y
```

**Setup steps in UI:**
1. Databricks SQL → Queries → New Query → paste above → Save as `revenue_drop_check`
2. Databricks SQL → Alerts → Create Alert
3. Query: `revenue_drop_check` | Column: `pct_change` | Operator: `<` | Threshold: `-10`
4. Add notification destinations: Email, Slack webhook
5. Schedule: Every day at 01:00 AM

---

### Approach B — Workflow + Notebook (Production-Grade)

```python
# Notebook: /Alerts/revenue_drop_alert
from pyspark.sql import functions as F
import requests, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── 1. Read Delta and compute pct change ──
df = (spark.read.format("delta")
      .table("gold.daily_revenue")
      .groupBy("date")
      .agg(F.sum("revenue").alias("daily_revenue"))
      .orderBy("date", ascending=False)
      .limit(2))

rows          = df.collect()
today_rev     = float(rows[0]["daily_revenue"])
yesterday_rev = float(rows[1]["daily_revenue"])
pct_change    = ((today_rev - yesterday_rev) / yesterday_rev) * 100
today_date    = rows[0]["date"]

print(f"Today: ${today_rev:,.0f}  |  Yesterday: ${yesterday_rev:,.0f}  |  Change: {pct_change:.1f}%")

# ── 2. Severity tiering ──
THRESHOLD = -10.0

if pct_change < -30:
    severity = "P1 - CRITICAL"
    channels = ["slack", "email", "pagerduty"]
elif pct_change < -20:
    severity = "P2 - HIGH"
    channels = ["slack", "email"]
elif pct_change < THRESHOLD:
    severity = "P3 - WARNING"
    channels = ["slack"]
else:
    channels = []
    print(f"Revenue healthy ({pct_change:.1f}%) — no alert needed")

# ── 3a. Slack notification ──
if "slack" in channels:
    slack_url = dbutils.secrets.get(scope="revenue-alerts", key="slack-webhook-url")

    message = {
        "text": f":rotating_light: *Revenue Alert — {severity}*",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"Revenue Alert — {severity}"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Date:*\n{today_date}"},
                    {"type": "mrkdwn", "text": f"*Drop:*\n{pct_change:.1f}%"},
                    {"type": "mrkdwn", "text": f"*Today:*\n${today_rev:,.0f}"},
                    {"type": "mrkdwn", "text": f"*Yesterday:*\n${yesterday_rev:,.0f}"},
                    {"type": "mrkdwn", "text": f"*Threshold:*\n{THRESHOLD}%"},
                    {"type": "mrkdwn", "text": f"*Action:*\nInvestigate orders pipeline"}
                ]
            }
        ]
    }

    resp = requests.post(slack_url, json=message)
    print(f"Slack alert sent: {resp.status_code}")

# ── 3b. Email via SMTP ──
if "email" in channels:
    smtp_password = dbutils.secrets.get(scope="revenue-alerts", key="smtp-password")
    recipients    = ["cfo@company.com", "data-eng@company.com", "revenue-ops@company.com"]

    msg             = MIMEMultipart("alternative")
    msg["Subject"]  = f"[{severity}] Revenue dropped {pct_change:.1f}% on {today_date}"
    msg["From"]     = "databricks-alerts@company.com"
    msg["To"]       = ", ".join(recipients)

    html = f"""
    <html><body>
    <h2 style="color:{'#d9534f' if 'CRITICAL' in severity else '#f0ad4e'}">
        Revenue Alert — {severity}
    </h2>
    <table border="1" cellpadding="8" style="border-collapse:collapse;">
        <tr><td><b>Date</b></td><td>{today_date}</td></tr>
        <tr><td><b>Today revenue</b></td><td>${today_rev:,.0f}</td></tr>
        <tr><td><b>Yesterday revenue</b></td><td>${yesterday_rev:,.0f}</td></tr>
        <tr><td><b>% change</b></td>
            <td style="color:#d9534f"><b>{pct_change:.1f}%</b></td></tr>
        <tr><td><b>Alert threshold</b></td><td>{THRESHOLD}%</td></tr>
    </table>
    <p>Please investigate the orders pipeline immediately.</p>
    </body></html>
    """

    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login("databricks-alerts@company.com", smtp_password)
        server.sendmail(msg["From"], recipients, msg.as_string())

    print("Email alert sent to:", recipients)
```

---

### Storing Secrets Securely

```bash
# One-time CLI setup
databricks secrets create-scope revenue-alerts
databricks secrets put --scope revenue-alerts --key slack-webhook-url
databricks secrets put --scope revenue-alerts --key smtp-password
```

```python
# In notebook — output always shows [REDACTED], never exposed
slack_url = dbutils.secrets.get(scope="revenue-alerts", key="slack-webhook-url")
```

---

### Approach C — DLT Expectations (Pipeline-Native)

```python
import dlt

@dlt.table(name="daily_revenue_validated")
@dlt.expect_or_warn(
    "revenue_not_dropped_10pct",
    """
    (SELECT ROUND((today.rev - yest.rev) / yest.rev * 100, 2)
     FROM (SELECT SUM(revenue) AS rev FROM LIVE.daily_revenue
           WHERE date = CURRENT_DATE) today,
          (SELECT SUM(revenue) AS rev FROM LIVE.daily_revenue
           WHERE date = CURRENT_DATE - 1) yest) >= -10
    """
)
def daily_revenue_validated():
    return spark.read.table("LIVE.daily_revenue")
```

---

### Workflow JSON (Orchestration)

```json
{
  "name": "daily-revenue-alert",
  "schedule": {
    "quartz_cron_expression": "0 0 1 * * ?",
    "timezone_id": "Asia/Kolkata"
  },
  "tasks": [
    {
      "task_key": "etl_job",
      "notebook_task": {"notebook_path": "/ETL/daily_revenue_etl"}
    },
    {
      "task_key": "alert_check",
      "depends_on": [{"task_key": "etl_job"}],
      "notebook_task": {"notebook_path": "/Alerts/revenue_drop_alert"}
    }
  ],
  "email_notifications": {
    "on_failure": ["data-eng@company.com"]
  }
}
```

---

### Severity Tiering (Interview Differentiator)

```python
if pct_change < -30:
    severity = "P1 - CRITICAL"
    channels = ["slack", "email", "pagerduty"]   # wake someone up
elif pct_change < -20:
    severity = "P2 - HIGH"
    channels = ["slack", "email"]
elif pct_change < -10:
    severity = "P3 - WARNING"
    channels = ["slack"]                          # Slack only, monitor
else:
    channels = []                                 # no alert
```

---

### Interview One-Liner
> *"Two complementary layers: for business team, Databricks SQL Alerts — write the pct_change query, set threshold -10, add Email and Slack destinations, schedule at 01:00. For engineering reliability, a Databricks Workflow where ETL runs first, then an alert notebook reads Delta, computes the LAG-based drop, and fires rich Slack blocks and HTML emails via secrets-backed webhooks. Severity tiers: -10% = Slack warning, -20% = email, -30% = PagerDuty on-call."*

---

### Cross-Questions for Q6

1. **How would you avoid duplicate alerts if the job runs multiple times?**
   - Add a deduplication check: store last alert timestamp in a Delta table or Databricks secret. Only fire if the last alert for the same date was more than N hours ago.

2. **What if the ETL job itself fails and writes no data — would a false alert fire?**
   - Yes, if today's row is missing, the query returns NULL and the comparison fails silently. Add a data freshness check: assert that `max(date) = CURRENT_DATE` before computing the drop. If today's data is absent, fire a separate "ETL data missing" alert.

3. **How do you make the alert self-healing — i.e., send a recovery notification when revenue is normal again?**
   - Track alert state: write alert status (FIRED/RESOLVED) to a Delta table. On each run, if status was FIRED and revenue is now healthy, send a RESOLVED notification and update the state table.

4. **How would you extend this to track multiple KPIs?**
   - Parameterise the notebook with Databricks Widgets: `dbutils.widgets.text("kpi_column", "revenue")`. Run separate Workflow tasks per KPI, each passing a different widget value. Or use a config Delta table with all KPI definitions and loop over them in a single notebook.

5. **What is the difference between Databricks SQL Alert and Workflow-based alerting?**
   - SQL Alert: no-code, built-in UI, limited message formatting, tied to SQL Warehouse availability. Workflow-based: full Python control, rich message formatting, retry logic, secret management, dependency chains, better audit trail. SQL Alert is faster to set up; Workflow is more robust for production.

---

## Q7 — PySpark Delta Merge Patterns (All Variants)

### The Question
> How do you write merge (upsert) code in PySpark for Delta Lake?

---

### Setup

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
```

---

### Pattern 1 — Basic Upsert (Insert + Update)

```python
delta_table = DeltaTable.forName(spark, "gold.daily_revenue")
source_df   = spark.read.format("delta").table("staging.daily_revenue_staging")

delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.date = source.date AND target.region = source.region"
).whenMatchedUpdate(set={
    "revenue":     "source.revenue",
    "order_count": "source.order_count",
    "updated_at":  "current_timestamp()"
}).whenNotMatchedInsert(values={
    "date":        "source.date",
    "region":      "source.region",
    "revenue":     "source.revenue",
    "order_count": "source.order_count",
    "updated_at":  "current_timestamp()"
}).execute()
```

---

### Pattern 2 — Insert + Update + Delete

```python
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedDelete(
    condition="source.is_deleted = true"
).whenMatchedUpdate(
    condition="source.is_deleted = false",
    set={
        "name":       "source.name",
        "email":      "source.email",
        "updated_at": "current_timestamp()"
    }
).whenNotMatchedInsert(
    condition="source.is_deleted = false",
    values={
        "customer_id": "source.customer_id",
        "name":        "source.name",
        "email":       "source.email",
        "created_at":  "current_timestamp()",
        "updated_at":  "current_timestamp()"
    }
).execute()
```

> **Note:** `whenMatchedDelete` must always come BEFORE `whenMatchedUpdate`.

---

### Pattern 3 — Update Only When Data Has Actually Changed (No-Op Guard)

```python
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdate(
    condition="""
        target.name    != source.name  OR
        target.email   != source.email OR
        target.address != source.address
    """,
    set={
        "name":       "source.name",
        "email":      "source.email",
        "address":    "source.address",
        "updated_at": "current_timestamp()"
    }
).whenNotMatchedInsert(values={
    "customer_id": "source.customer_id",
    "name":        "source.name",
    "email":       "source.email",
    "address":     "source.address",
    "created_at":  "current_timestamp()",
    "updated_at":  "current_timestamp()"
}).execute()
```

---

### Pattern 4 — SCD Type 2 (Slowly Changing Dimensions)

```python
# Step 1 — expire old rows that have changed
delta_table = DeltaTable.forName(spark, "gold.dim_customers")

delta_table.alias("target").merge(
    source_df.alias("source"),
    """
        target.customer_id = source.customer_id AND
        target.is_current   = true              AND
        (target.name != source.name OR target.email != source.email)
    """
).whenMatchedUpdate(set={
    "is_current": "false",
    "end_date":   "current_date()"
}).execute()

# Step 2 — insert new current rows for changed + net-new records
new_rows = source_df.alias("source").join(
    delta_table.toDF().filter("is_current = true").alias("target"),
    on="customer_id",
    how="left_anti"          # net-new customers
).unionByName(
    source_df.alias("source").join(
        delta_table.toDF()
            .filter("is_current = false AND end_date = current_date()")
            .alias("target"),
        on="customer_id",
        how="inner"          # changed customers whose old row was just expired
    ).select("source.*")
).withColumn("is_current", F.lit(True)) \
 .withColumn("start_date", F.current_date()) \
 .withColumn("end_date",   F.lit(None).cast("date"))

new_rows.write.format("delta").mode("append").saveAsTable("gold.dim_customers")
```

---

### Pattern 5 — Merge Using SQL Syntax

```python
spark.sql("""
    MERGE INTO gold.daily_revenue AS target
    USING staging.daily_revenue_staging AS source
    ON target.date = source.date AND target.region = source.region
    WHEN MATCHED THEN UPDATE SET
        target.revenue     = source.revenue,
        target.order_count = source.order_count,
        target.updated_at  = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        date, region, revenue, order_count, updated_at
    ) VALUES (
        source.date, source.region, source.revenue,
        source.order_count, current_timestamp()
    )
    WHEN NOT MATCHED BY SOURCE THEN DELETE
""")
```

---

### Pattern 6 — Partition-Filtered Merge (Performance Critical for Large Tables)

```python
from datetime import date, timedelta

yesterday = str(date.today() - timedelta(days=1))
today     = str(date.today())

source_df = spark.read.format("delta") \
    .table("staging.events") \
    .filter(F.col("event_date").isin([yesterday, today]))

delta_table = DeltaTable.forName(spark, "gold.events")

delta_table.alias("target").merge(
    source_df.alias("source"),
    # Partition predicate on target drastically reduces files scanned
    f"target.event_date >= '{yesterday}' AND "
    f"target.event_id = source.event_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

### Pattern 7 — `whenMatchedUpdateAll` / `whenNotMatchedInsertAll` Shorthand

```python
# Use when source and target have identical column names
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll()    \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

### Merge by Path vs by Name

```python
# By Unity Catalog table name
delta_table = DeltaTable.forName(spark, "catalog.schema.table")

# By storage path
delta_table = DeltaTable.forPath(spark, "/mnt/delta/gold/daily_revenue")
```

---

### Check Merge Metrics After Execution

```python
# View number of rows inserted, updated, deleted
history = delta_table.history(1)
history.select("operationMetrics").show(truncate=False)

# Sample output:
# {numTargetRowsInserted -> 150, numTargetRowsUpdated -> 320,
#  numTargetRowsDeleted -> 5, numSourceRows -> 475}
```

---

### Key Rules to Remember

```
1. Always alias both sides — "target" and "source"
2. Match condition should use the business/primary key
3. Add partition predicate in match condition for large tables
4. whenMatchedDelete must come BEFORE whenMatchedUpdate
5. DeltaTable.forName works with Unity Catalog (catalog.schema.table)
6. Check operationMetrics after merge to verify correctness
7. Use no-op guard (condition on whenMatchedUpdate) to avoid unnecessary writes
```

---

### Cross-Questions for Q7

1. **What is the difference between `whenMatchedUpdateAll()` and `whenMatchedUpdate(set={...})`?**
   - `updateAll()` updates every column in the target to match the source — equivalent to `SET *`. Use when source and target schemas are identical. `update(set={...})` lets you explicitly map each column — use when schemas differ or you need expressions like `current_timestamp()`.

2. **What happens if your merge condition matches multiple source rows to one target row?**
   - Delta raises a `DeltaUnsupportedOperationException`: "cannot perform MERGE as multiple source rows matched and attempted to modify the same target row." Deduplicate your source before merging: `source_df.dropDuplicates(["customer_id"])`.

3. **How does a Delta merge handle schema evolution (new columns in source)?**
   - By default, merge rejects new source columns not in the target schema. To absorb them, set `spark.databricks.delta.schema.autoMerge.enabled = true`. Then `whenNotMatchedInsertAll()` and `whenMatchedUpdateAll()` will include new columns.

4. **What is `WHEN NOT MATCHED BY SOURCE THEN DELETE`?**
   - Available in Delta Lake (SQL syntax). Deletes target rows that have no matching row in the source — effectively syncing the target to be a replica of the source. Useful for full-refresh patterns where deletions in source must propagate to target.

5. **How do you optimise a merge on a very large table (1 TB+)?**
   - (a) Add a partition column predicate in the match condition so Delta scans only relevant files. (b) Z-ORDER the target table on the join key so data skipping skips most files. (c) Deduplicate and filter source before the merge. (d) Run `OPTIMIZE` after the merge to re-compact files written by the merge.

6. **What is the difference between SCD Type 1 and SCD Type 2 and how does Delta handle each?**
   - SCD Type 1: simply overwrite old values — a standard `whenMatchedUpdate` handles this. SCD Type 2: preserve history by expiring the old row (`is_current = false`) and inserting a new row with the new values. Delta handles Type 2 as shown in Pattern 4 above.

---

## Bonus — Executor Memory Config in Databricks UI vs Code

### The 4 Ways to Configure Executor Memory

```
Method 1 — Cluster UI (no code)
  Compute → Create Cluster → Advanced Options → Spark tab
  Type key=value pairs in the Spark Config text area
  Applies before cluster starts. Cannot change at runtime.

Method 2 — Cluster JSON / REST API
  Use spark_conf block in cluster spec JSON
  Used by Terraform, REST API, CI/CD pipelines

Method 3 — spark.conf.set() in notebook
  ONLY works for query-level configs
  CANNOT change executor.memory, cores, overhead

Method 4 — Init scripts
  Shell script runs before Spark starts
  Advanced use — for JVM opts, env vars, system packages
```

---

### Databricks Cluster UI — Spark Config Section

```
# In Databricks UI: Compute → Create Cluster
# Advanced Options → Spark tab → Spark Config box:

spark.executor.memory 20g
spark.executor.cores 5
spark.executor.memoryOverhead 2g
spark.driver.memory 8g
spark.memory.fraction 0.6
spark.memory.storageFraction 0.5
spark.sql.shuffle.partitions 200
spark.sql.adaptive.enabled true
```

---

### Cluster JSON (Terraform / REST API)

```json
{
  "cluster_name": "etl-cluster",
  "node_type_id": "i3.xlarge",
  "num_workers": 4,
  "spark_conf": {
    "spark.executor.memory": "20g",
    "spark.executor.cores": "5",
    "spark.executor.memoryOverhead": "2g",
    "spark.driver.memory": "8g",
    "spark.memory.fraction": "0.6",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true"
  },
  "autotermination_minutes": 120
}
```

---

### Runtime vs Startup Config — The Critical Rule

| Config | Set at runtime? | Why |
|--------|----------------|-----|
| `spark.executor.memory` | ❌ No | JVM heap allocated at startup |
| `spark.executor.cores` | ❌ No | Thread pool fixed at startup |
| `spark.executor.memoryOverhead` | ❌ No | Container size fixed at startup |
| `spark.driver.memory` | ❌ No | Driver JVM fixed at startup |
| `spark.memory.fraction` | ❌ No | Unified Memory Manager init at startup |
| `spark.sql.shuffle.partitions` | ✅ Yes | Planner config, read per query |
| `spark.sql.adaptive.enabled` | ✅ Yes | AQE flag, per-query |
| `spark.databricks.delta.*` | ✅ Yes | Delta table properties |
| `spark.sql.autoBroadcastJoinThreshold` | ✅ Yes | Execution parameter |

---

### Serverless — What Databricks Manages for You

On Serverless compute, Databricks owns all infrastructure configs:
- `spark.executor.memory` — hidden, auto-set by Databricks
- `spark.executor.cores` — not configurable
- `spark.executor.memoryOverhead` — auto-tuned
- `spark.memory.fraction` — fixed internally

You can only set query-level configs via `spark.conf.set()` in the notebook.

---

### Cross-Questions for Bonus

1. **What does Databricks auto-set for `executor.memory` if you don't configure it?**
   - Databricks calculates it from the node type: `executor.memory ≈ node RAM - OS reserve - Databricks agent`. For `i3.xlarge` (30.5 GB RAM), it auto-sets ~27 GB. You only override when implementing the 5-core multi-executor-per-node strategy.

2. **What is the difference between cluster-level Spark config and table properties in Delta?**
   - Cluster Spark config applies to all queries on the cluster session. Table properties (set via `ALTER TABLE SET TBLPROPERTIES`) apply to that table regardless of which cluster reads it. Example: `delta.autoOptimize.autoCompact = true` as a table property runs auto-compaction on every cluster that writes to it.

---

## Cross-Questions Master List

### Spark Internals Cross-Questions

| Question | Key Answer |
|----------|-----------|
| What is a stage in Spark? | A set of tasks that can run without a shuffle. Stage boundaries are drawn at wide transformations (groupBy, join, repartition). |
| What is a task in Spark? | The unit of work — one task processes one partition. Tasks run on one core. |
| What is the difference between a narrow and wide transformation? | Narrow: data stays on same partition (map, filter, select). Wide: data shuffled across partitions (groupBy, join, distinct). Wide = stage boundary. |
| What is the DAG in Spark? | Directed Acyclic Graph of transformations. Spark builds it lazily and optimises before execution. The Catalyst optimizer rewrites it for efficiency. |
| What is Catalyst optimizer? | Spark SQL's query optimizer. Applies rule-based (constant folding, predicate pushdown) and cost-based optimisation. Generates physical plans. |
| What is Tungsten? | Spark's binary memory management layer. Stores data in native binary format using sun.misc.Unsafe, avoiding Java serialization overhead. Enables vectorised execution. |
| What is AQE? | Adaptive Query Execution (Spark 3.x+). Optimises the plan at runtime using actual shuffle statistics: coalesces small partitions, switches join strategies, handles skew. |

---

### Delta Lake Cross-Questions

| Question | Key Answer |
|----------|-----------|
| What is the Delta transaction log? | A JSON-based log at `_delta_log/`. Every write (insert, update, delete, OPTIMIZE) adds an entry. Used for ACID compliance, time travel, schema enforcement. |
| What is Delta time travel? | Read any historical version: `df.read.format("delta").option("versionAsOf", 5).load(path)` or `option("timestampAsOf", "2024-01-01")`. |
| What is Delta ACID? | Atomicity: all-or-nothing writes. Consistency: schema enforced. Isolation: optimistic concurrency with conflict detection. Durability: transaction log on durable storage. |
| What is OPTIMIZE in Delta? | Compacts small files into larger ~1 GB files (bin-packing). Reduces file count, improves read performance. Does not delete old files — use VACUUM for that. |
| What is VACUUM? | Physically deletes old files no longer referenced by the transaction log. Default retention = 7 days (for time travel safety). Run after OPTIMIZE. |
| What is Z-ORDER? | OPTIMIZE + ZORDER BY (col) co-locates rows with similar values in the same files. Enables the Delta engine to skip entire files during reads on filtered queries. |
| What is data skipping? | Delta reads file-level min/max statistics from the transaction log and skips files that cannot contain rows matching your WHERE clause. ZORDER amplifies this. |
| What is schema enforcement? | Delta rejects writes whose schema doesn't match the table schema. Protects data quality. Override with `mergeSchema=true` or `overwriteSchema=true`. |

---

### Databricks-Specific Cross-Questions

| Question | Key Answer |
|----------|-----------|
| What is Unity Catalog? | Databricks' unified governance layer. One metastore for all workspaces. Three-level namespace: catalog.schema.table. Handles column-level security, lineage, auditing. |
| What is Delta Live Tables (DLT)? | Declarative ETL framework. You define tables with `@dlt.table` decorator; Databricks manages DAG execution, data quality (expectations), and auto-scaling. |
| What is Photon? | Databricks' native vectorised C++ query engine. Replaces the JVM-based Spark SQL engine for SQL and DataFrame operations. 2–4× faster on analytical queries. |
| What is the difference between Jobs cluster and All-Purpose cluster? | Jobs cluster: spun up and torn down for a single Workflow run — cheaper, isolated. All-Purpose: persistent, shared across notebooks, interactive. |
| What are Databricks Widgets? | `dbutils.widgets` lets you parameterise notebooks with text, dropdown, or multi-select inputs. Used to pass parameters from Workflow tasks to notebooks. |

---

## Quick-Reference Cheat Sheet

### Executor Sizing Rules

```
5 cores per executor          — always
memory per executor           = (node RAM / executors_per_node) × 0.9
executors per node            = floor(total_cores / 5)  leaving 1 core for OS
memory overhead               = max(384MB, executor_memory × 0.1)
shuffle partitions            = total_cores × 2
target partition size         = 128–256 MB
```

### Memory Fractions

```
managed pool   = (executor.memory - 300MB) × memory.fraction (0.6)
execution mem  = managed pool × (1 - storageFraction) (0.5)
storage mem    = managed pool × storageFraction (0.5)
user memory    = (executor.memory - 300MB) × (1 - memory.fraction) (0.4)
overhead       = max(384MB, executor.memory × 0.1)  ← outside JVM
```

### Delta Commands

```sql
OPTIMIZE delta.`/path/`                          -- compact files
OPTIMIZE delta.`/path/` WHERE date = '2024-01-01' -- compact one partition
OPTIMIZE delta.`/path/` ZORDER BY (customer_id)  -- compact + co-locate
VACUUM delta.`/path/` RETAIN 168 HOURS           -- delete old files (7 days)
DESCRIBE HISTORY delta.`/path/`                  -- audit trail
```

### Auto Loader Key Options

```python
.option("cloudFiles.format", "json")
.option("cloudFiles.schemaLocation", "/mnt/schema/")
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.option("cloudFiles.inferColumnTypes", "true")
.option("cloudFiles.rescuedDataColumn", "_rescued_data")
```

### Merge Rules

```
1. Always alias target and source
2. Use business key in match condition
3. Add partition predicate for large tables
4. whenMatchedDelete BEFORE whenMatchedUpdate
5. Deduplicate source before merge to avoid multi-match error
6. Check operationMetrics after merge
```

### Serverless vs All-Purpose Quick Pick

```
ETL job runs once/day          → Serverless
Analyst SQL queries            → Serverless SQL Warehouse
Scheduled DLT pipeline         → Serverless
Interactive DE notebooks       → All-Purpose (auto-term 2h, spot)
ML training, GPU workloads     → All-Purpose
Complex Spark tuning needed    → All-Purpose
```

---

*Notes compiled from Databricks + PySpark scenario-based interview preparation sessions.*
*Topics: executor sizing, memory internals, small file problem, streaming schema evolution,*
*Serverless vs All-Purpose compute, KPI alerting, Delta merge patterns.*
