# Databricks & PySpark — Interview Notes (Part 2)
### Remaining Topics — Comprehensive Study Guide

---

## 📑 Table of Contents

| # | Topic |
|---|-------|
| 1 | [Spark DAG, Stages & Tasks Internals](#1-spark-dag-stages--tasks-internals) |
| 2 | [Shuffle Operations & Optimisation](#2-shuffle-operations--optimisation) |
| 3 | [Data Skew — Detection & Handling](#3-data-skew--detection--handling) |
| 4 | [Delta Lake Internals — Transaction Log & Checkpoints](#4-delta-lake-internals--transaction-log--checkpoints) |
| 5 | [Spark Join Strategies — Deep Dive](#5-spark-join-strategies--deep-dive) |
| 6 | [Structured Streaming Internals — Watermarking, Triggers, Output Modes](#6-structured-streaming-internals) |
| 7 | [Delta Live Tables (DLT)](#7-delta-live-tables-dlt) |
| 8 | [Unity Catalog — Governance & Access Control](#8-unity-catalog) |
| 9 | [Spark UI — Debugging & Performance Tuning](#9-spark-ui--debugging--performance-tuning) |
| 10 | [Adaptive Query Execution (AQE) — Deep Dive](#10-adaptive-query-execution-aqe--deep-dive) |
| 11 | [Partitioning Strategies — Deep Dive](#11-partitioning-strategies--deep-dive) |
| 12 | [Caching & Persistence Strategies](#12-caching--persistence-strategies) |
| 13 | [Databricks Workflows — Advanced Patterns](#13-databricks-workflows--advanced-patterns) |
| 14 | [PySpark DataFrame vs RDD vs SQL](#14-pyspark-dataframe-vs-rdd-vs-sql) |
| 15 | [MLflow Integration with Databricks](#15-mlflow-integration-with-databricks) |
| 16 | [Common Interview Scenario Patterns](#16-common-interview-scenario-patterns) |

---

---

# 1. Spark DAG, Stages & Tasks Internals

## Scenario
> Explain how Spark breaks a job into DAG, stages, and tasks. What causes a stage boundary?

---

## Architecture Diagram

```
USER CODE (df.groupBy().agg().write())
          │
          ▼
    LOGICAL PLAN          ← Catalyst Analyser validates column names, types
          │
          ▼
    OPTIMISED PLAN        ← Catalyst Optimiser applies rules (predicate pushdown, etc.)
          │
          ▼
    PHYSICAL PLAN         ← Planner picks strategies (BroadcastHashJoin vs SortMergeJoin)
          │
          ▼
         DAG              ← Directed Acyclic Graph of RDD transformations
          │
          ├── STAGE 1 ────────────────────────────────────────────────
          │   Task 1 (partition 0)   Task 2 (partition 1)   Task N...
          │   read + filter + map    read + filter + map
          │                   │
          │           SHUFFLE WRITE  ← Stage boundary! Data written to disk
          │
          ├── STAGE 2 ────────────────────────────────────────────────
          │   Task 1                 Task 2
          │   shuffle read + agg     shuffle read + agg
          │                   │
          │           SHUFFLE WRITE  ← Another boundary
          │
          └── STAGE 3 ────────────────────────────────────────────────
              Task 1
              final write to Delta
```

---

## What Causes a Stage Boundary?

A new stage is created whenever Spark needs a **shuffle** — i.e., data must move between executors.

```
Narrow transformations (SAME stage, no shuffle):
  map()         filter()       flatMap()
  select()      withColumn()   union()
  coalesce()    sample()       limit()

Wide transformations (NEW stage, requires shuffle):
  groupBy()     agg()          join() (non-broadcast)
  distinct()    repartition()  sortBy()
  reduceByKey() window()       orderBy()
```

---

## Task = Partition

```
1 partition = 1 task = 1 core used

Stage 1 has 80 partitions → 80 tasks run in parallel
(up to the number of available cores)

If you have 90 cores and 80 tasks:
  → All 80 run in parallel in one wave ✅

If you have 20 cores and 80 tasks:
  → 4 waves of 20 tasks each ⏳
```

---

## Catalyst Optimiser Rules

```python
# What YOU write:
df.filter(col("country") == "IN") \
  .join(orders_df, "customer_id") \
  .select("customer_id", "revenue")

# What Catalyst ACTUALLY executes (after optimisation):
# 1. Predicate pushdown: filter applied BEFORE join (scans less data)
# 2. Column pruning: only reads customer_id, revenue, country columns
# 3. Join reordering: smaller filtered table goes to build side
```

---

## Key Configs

```python
# Number of shuffle partitions (default 200 — tune for data size)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# See the physical plan
df.explain(mode="formatted")  # mode: simple, extended, codegen, cost, formatted

# See DAG in Spark UI: http://<driver>:4040 → Jobs → Stages
```

---

## ❓ Cross-Questions

**Q: What is the difference between a transformation and an action in Spark?**
> Transformations are lazy — they build the DAG but don't execute (e.g., `filter`, `groupBy`, `join`). Actions trigger execution by submitting a job to the scheduler (e.g., `count()`, `collect()`, `write()`, `show()`). This laziness allows Catalyst to optimise the full plan before any work starts.

**Q: What is pipelining in Spark?**
> Within a stage, Spark pipelines multiple narrow transformations together — they execute in a single pass over the data without writing intermediate results to disk. For example, `read → filter → select → map` all happen in one pass per partition, in memory.

**Q: What is the difference between a Job, Stage, and Task in Spark UI?**
> A **Job** is triggered by an action and contains one or more Stages. A **Stage** is a set of tasks that can run without shuffling. A **Task** is the smallest unit of work — one task processes one partition on one executor core.

**Q: How does `explain()` help in performance tuning?**
> `df.explain("formatted")` shows the physical plan Spark will execute. Look for: (1) `FileScan` with `PushedFilters` — confirms predicate pushdown is working. (2) `BroadcastHashJoin` vs `SortMergeJoin` — tells you if broadcast optimisation kicked in. (3) `Exchange` nodes — each Exchange = a shuffle = a stage boundary.

---

---

# 2. Shuffle Operations & Optimisation

## Scenario
> Explain how Spark shuffle works. How do you optimise shuffle-heavy jobs?

---

## How Shuffle Works Internally

```
STAGE 1 — MAP SIDE (Shuffle Write)
─────────────────────────────────────────────────────
Each task:
  1. Applies map/filter/transform
  2. Partitions output rows by hash(key) % num_reduce_partitions
  3. Writes each partition's data to local disk (shuffle files)
  4. Reports file locations to driver (MapOutputTracker)

             Executor 1              Executor 2
             [Task 1 output]         [Task 2 output]
             Part 0 | Part 1 | Part 2 | Part 0 | Part 1 | Part 2
               ↓         ↓       ↓       ↓         ↓       ↓
           (written to local disk on each executor)

STAGE 2 — REDUCE SIDE (Shuffle Read)
─────────────────────────────────────────────────────
Each reduce task:
  1. Fetches its assigned partition from ALL executors (HTTP)
  2. Merges / sorts / aggregates fetched data
  3. Produces final output

  Reducer Task 0 fetches: Part 0 from Exe1 + Part 0 from Exe2 + ...
  Reducer Task 1 fetches: Part 1 from Exe1 + Part 1 from Exe2 + ...
```

---

## Shuffle Types

| Shuffle Implementation | Used for | When |
|---|---|---|
| Sort Shuffle | Most operations | Default — Spark 1.2+ |
| Tungsten Sort | Optimised binary sort | When serialisation is CPU-bound |
| Hash Shuffle (legacy) | Small datasets | Spark < 1.2, rarely used now |

---

## Shuffle Optimisation Techniques

### 1. Tune shuffle partition count

```python
# Too few → each partition is huge → spills, slow reducers
# Too many → tiny tasks → scheduler overhead

# Rule of thumb:
# spark.sql.shuffle.partitions = total_data_size / 128 MB
# For 10 GB: 10,240 / 128 = 80 → set to 100–200

spark.conf.set("spark.sql.shuffle.partitions", "200")

# Better: Let AQE handle it dynamically
spark.conf.set("spark.sql.adaptive.enabled",                    "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes","134217728")  # 128 MB
```

### 2. Avoid unnecessary shuffles

```python
# BAD — triggers shuffle
df.groupBy("key").agg(F.count("*"))  # shuffle if not already partitioned by key

# GOOD — repartition once, then multiple operations on same partition key
df_partitioned = df.repartition(200, "key")
result1 = df_partitioned.groupBy("key").agg(F.sum("amount"))
result2 = df_partitioned.groupBy("key").agg(F.count("*"))
# Both reuse the same partition layout — no second shuffle
```

### 3. Use broadcast join for small tables

```python
from pyspark.sql.functions import broadcast

# Avoids shuffle entirely for the small side
result = large_df.join(broadcast(small_lookup_df), "customer_id")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")  # default 10 MB
```

### 4. Pre-aggregate before shuffle (map-side combine)

```python
# BAD — shuffles all raw rows then aggregates
df.groupBy("category").agg(F.sum("revenue"))

# GOOD for very large groups — pre-aggregate per partition first
df_pre = df.groupBy("partition_key", "category").agg(F.sum("revenue").alias("rev"))
df_pre.groupBy("category").agg(F.sum("rev"))
```

### 5. Persist before multiple shuffle operations

```python
# If DF is used in multiple shuffles, cache it first
df.cache()
df.groupBy("a").count()
df.groupBy("b").sum("c")
df.unpersist()
```

---

## ❓ Cross-Questions

**Q: What causes shuffle spill and how do you fix it?**
> Shuffle spill happens when a reduce task has more data than fits in its execution memory. Data spills to disk as serialised bytes. Fix: increase `spark.executor.memory`, reduce `spark.sql.shuffle.partitions` (so each reducer gets less data), or use AQE to auto-size partitions.

**Q: What is the difference between shuffle spill (memory) and shuffle spill (disk)?**
> Spill (memory) = amount of in-memory data that needed to be serialised before writing to disk. Spill (disk) = amount of data actually written to disk. Both are visible in Spark UI → Stages. High spill = memory pressure.

**Q: How does `spark.shuffle.compress` work?**
> When `spark.shuffle.compress = true` (default), shuffle files are compressed with the codec set by `spark.io.compression.codec` (default `lz4`). This reduces I/O but adds CPU overhead. For CPU-bound jobs, disable it.

---

---

# 3. Data Skew — Detection & Handling

## Scenario
> Your Spark job has one task taking 10× longer than others. How do you detect and fix data skew?

---

## What is Data Skew?

```
BALANCED (good):
  Partition 0: 1,000,000 rows  ████████████
  Partition 1: 1,100,000 rows  █████████████
  Partition 2:   950,000 rows  ███████████
  All tasks finish in ~same time ✅

SKEWED (bad):
  Partition 0:    50,000 rows  █
  Partition 1: 9,800,000 rows  ████████████████████████████████████████
  Partition 2:    80,000 rows  █
  One task takes 100× longer — entire job waits for it ❌
```

---

## Detecting Skew

```
1. Spark UI → Stages → Task Metrics:
   - Sort by "Duration" descending
   - If max task duration >> median task duration → skew

2. Spark UI → Stages → "Data Read" column:
   - If one task reads 10 GB and others read 100 MB → skew

3. In code:
```

```python
# Check partition sizes
from pyspark.sql.functions import spark_partition_id

df.withColumn("partition_id", spark_partition_id()) \
  .groupBy("partition_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(20)

# Check key distribution for join keys
df.groupBy("customer_id").count() \
  .orderBy("count", ascending=False) \
  .show(20)
# If top key has millions of rows → join skew
```

---

## Fixing Skew — Techniques

### Technique 1 — AQE Skew Join (Spark 3.x, automatic)

```python
# AQE automatically detects and splits skewed partitions
spark.conf.set("spark.sql.adaptive.enabled",           "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",  "true")
# Splits partitions > skewedPartitionFactor × median AND > skewedPartitionThresholdInBytes
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",           "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
```

### Technique 2 — Salting (for groupBy / agg skew)

```python
import random
from pyspark.sql import functions as F

SALT_BUCKETS = 10

# Step 1 — Add random salt key to distribute data
df_salted = df.withColumn(
    "salted_key",
    F.concat(F.col("skewed_key"), F.lit("_"), (F.rand() * SALT_BUCKETS).cast("int"))
)

# Step 2 — Aggregate with salted key
partial_agg = df_salted.groupBy("salted_key") \
    .agg(F.sum("revenue").alias("revenue_partial"))

# Step 3 — Strip salt and do final aggregation
final_agg = partial_agg \
    .withColumn("original_key", F.split(F.col("salted_key"), "_")[0]) \
    .groupBy("original_key") \
    .agg(F.sum("revenue_partial").alias("total_revenue"))
```

### Technique 3 — Salting for join skew

```python
SALT_BUCKETS = 10

# Salt the large (skewed) DataFrame
large_df_salted = large_df.withColumn(
    "salt", (F.rand() * SALT_BUCKETS).cast("int")
).withColumn(
    "salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt"))
)

# Replicate the small (lookup) DataFrame for each salt bucket
from pyspark.sql.functions import array, explode, lit

small_df_replicated = small_df.withColumn(
    "salt", explode(array([lit(i) for i in range(SALT_BUCKETS)]))
).withColumn(
    "salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt"))
)

# Join on salted key
result = large_df_salted.join(small_df_replicated, "salted_key")
```

### Technique 4 — Separate skewed keys

```python
# Split into skewed and non-skewed portions, process separately
hot_keys = ["customer_A", "customer_B"]  # known hot keys

skewed_df     = df.filter(F.col("customer_id").isin(hot_keys))
non_skewed_df = df.filter(~F.col("customer_id").isin(hot_keys))

# Use broadcast join for hot keys (small result set)
result_hot     = skewed_df.join(broadcast(lookup_df), "customer_id")

# Use regular join for normal keys
result_normal  = non_skewed_df.join(lookup_df, "customer_id")

# Union results
final = result_hot.union(result_normal)
```

### Technique 5 — Repartition by skewed key with more buckets

```python
# Increase partition count to spread the skewed key across more tasks
df.repartition(500, "skewed_key").groupBy("skewed_key").agg(...)
```

---

## ❓ Cross-Questions

**Q: How is data skew different from partition imbalance?**
> Partition imbalance is a broader term — partitions have unequal row counts. Data skew specifically refers to the case where a small number of keys have disproportionately large data volumes, typically caused by natural data distribution (e.g., a few popular customers, a null key dominating).

**Q: What is null key skew and how do you handle it?**
> If a join key column has many NULL values, all NULL rows end up in the same partition (hash(NULL) = constant). Filter out NULLs before joining: `df.filter(col("key").isNotNull())`, or handle NULLs separately and union them back.

**Q: What is the AQE skew join threshold and how do you tune it?**
> A partition is considered skewed if: its size > `skewedPartitionFactor` × median partition size AND > `skewedPartitionThresholdInBytes`. Default factor is 5, default threshold is 256 MB. Lower the factor to catch milder skew, lower the threshold to catch smaller skewed partitions.

---

---

# 4. Delta Lake Internals — Transaction Log & Checkpoints

## Scenario
> Explain how Delta Lake's transaction log works. How does time travel work internally?

---

## Delta Lake Architecture

```
Delta Table Directory
├── _delta_log/                     ← Transaction log directory
│   ├── 00000000000000000000.json   ← Version 0 (CREATE TABLE)
│   ├── 00000000000000000001.json   ← Version 1 (first INSERT)
│   ├── 00000000000000000002.json   ← Version 2 (UPDATE)
│   ├── ...
│   ├── 00000000000000000010.checkpoint.parquet  ← Checkpoint (every 10 commits)
│   └── _last_checkpoint             ← Points to latest checkpoint
│
├── part-00000-abc123.parquet       ← Actual data files
├── part-00001-def456.parquet
└── ...
```

---

## Transaction Log — JSON Commit File Structure

```json
// 00000000000000000001.json — example commit
{
  "commitInfo": {
    "timestamp": 1704067200000,
    "operation": "WRITE",
    "operationParameters": {"mode": "Append"},
    "operationMetrics": {
      "numFiles": "3",
      "numOutputRows": "150000",
      "numOutputBytes": "52428800"
    }
  },
  "add": {
    "path": "part-00000-abc123.parquet",
    "size": 17476266,
    "modificationTime": 1704067200000,
    "dataChange": true,
    "stats": "{\"numRecords\":50000,\"minValues\":{\"date\":\"2024-01-01\"},\"maxValues\":{\"date\":\"2024-01-01\"},\"nullCount\":{\"date\":0}}"
  },
  "remove": {
    "path": "part-00000-old456.parquet",
    "deletionTimestamp": 1704067200000,
    "dataChange": true
  }
}
```

---

## How Time Travel Works

```
CURRENT STATE (Version 5):
  Active files = union of all ADD actions − all REMOVE actions
  across versions 0–5

TIME TRAVEL TO VERSION 2:
  Active files = union of all ADD actions − all REMOVE actions
  across versions 0–2 ONLY
  → Files removed in v3, v4, v5 are still on disk (until VACUUM)
  → Those files are still readable → time travel works ✅

AFTER VACUUM (RETAIN 0 HOURS):
  Physical files for old versions deleted from disk
  → Time travel to those versions no longer possible ❌
```

---

## Checkpoint Files

```python
# Every 10 commits (configurable), Delta writes a checkpoint
# Checkpoint = Parquet snapshot of the full transaction log state
# Avoids reading all JSON files from the beginning on each read

# Configure checkpoint frequency
spark.conf.set("spark.databricks.delta.checkpointInterval", "10")

# Manual checkpoint (rarely needed)
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "gold.events")
dt.vacuum()         # triggers checkpoint internally
```

---

## ACID Guarantees

```
ATOMICITY:    A commit either fully succeeds or doesn't appear in the log
CONSISTENCY:  Schema enforcement prevents invalid writes
ISOLATION:    Optimistic concurrency — readers never blocked by writers
DURABILITY:   Log files are written to cloud storage (S3/ADLS/GCS)
```

---

## Delta Lake Key Operations — SQL Cheat Sheet

```sql
-- Check history (all versions, operations, metrics)
DESCRIBE HISTORY gold.events

-- Time travel — query by version
SELECT * FROM gold.events VERSION AS OF 5
SELECT * FROM gold.events TIMESTAMP AS OF '2024-01-15 00:00:00'

-- Restore table to previous version
RESTORE TABLE gold.events TO VERSION AS OF 5
RESTORE TABLE gold.events TO TIMESTAMP AS OF '2024-01-15'

-- Table details (size, num files, partitions)
DESCRIBE DETAIL gold.events

-- Show table schema and properties
DESCRIBE EXTENDED gold.events

-- Generate manifest (for Presto/Athena/Redshift Spectrum compatibility)
GENERATE symlink_format_manifest FOR TABLE gold.events
```

---

## Delta Table Properties

```sql
ALTER TABLE gold.events
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.logRetentionDuration'        = 'interval 30 days',
    'delta.deletedFileRetentionDuration'= 'interval 7 days',
    'delta.checkpointInterval'          = '10',
    'delta.minReaderVersion'            = '1',
    'delta.minWriterVersion'            = '2'
);
```

---

## ❓ Cross-Questions

**Q: How does Delta Lake ensure concurrent write safety?**
> Delta uses optimistic concurrency control. Each writer reads the current log version, performs its operation, and then attempts to write the next version. If another writer has already written that version, the first writer checks for conflicts. If the files they modified don't overlap, both commits succeed (one after the other). If they do overlap, one writer must retry.

**Q: What happens to `_delta_log` if a write fails midway?**
> The JSON commit file for that version is never written (or is incomplete and ignored). Delta only considers a version committed when its complete JSON file exists. Partial Parquet data files that were written are simply orphaned — they're not referenced by any commit and will be cleaned up by VACUUM.

**Q: What is the difference between `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration`?**
> `logRetentionDuration` controls how long JSON commit log files are kept (for `DESCRIBE HISTORY`). `deletedFileRetentionDuration` controls how long physically removed data files are retained (for time travel). VACUUM uses `deletedFileRetentionDuration` to determine which files to delete.

**Q: Can two Delta writers write to the same table concurrently?**
> Yes — Delta supports concurrent writes with ACID guarantees. Appends (INSERT) are always compatible with each other. Merges (MERGE/UPDATE/DELETE) on non-overlapping partition ranges are also compatible. Concurrent writes to the same partition range result in one writer retrying.

---

---

# 5. Spark Join Strategies — Deep Dive

## Scenario
> Explain Spark's join strategies. When does Spark choose each one?

---

## Join Strategy Decision Tree

```
Spark checks join type and table sizes:
                    │
                    ▼
       Is one table ≤ autoBroadcastJoinThreshold (10 MB default)?
                    │
            YES ────┼────▶  BROADCAST HASH JOIN
                    │         Small table sent to all executors as HashMap
                    │         No shuffle, fastest join ✅
                    │
            NO      ▼
       Can Spark sort the two sides with existing partitioning?
                    │
            YES ────┼────▶  SORT MERGE JOIN (default for large-large)
                    │         Both sides sorted and merged partition by partition
                    │         Requires shuffle on both sides
                    │
            NO      ▼
       Is one side much smaller and can fit in memory per task?
                    │
            YES ────┼────▶  SHUFFLE HASH JOIN
                    │         Smaller side shuffled and built into HashMap per partition
                    │
                    └──────▶  CARTESIAN JOIN (only for cross joins — dangerous!)
```

---

## 1. Broadcast Hash Join

```python
from pyspark.sql.functions import broadcast

# Explicit broadcast hint
result = large_df.join(broadcast(small_df), "customer_id")

# Configure threshold (default 10 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")

# Disable broadcast (force sort merge)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

```
How it works:
  1. Driver collects small table → serialises to byte array
  2. Broadcasts to ALL executors (stored as HashMap in memory)
  3. Each executor probes the HashMap for each row of the large table
  4. ZERO shuffle of the large table ✅
  5. Ideal for dimension tables < 50 MB
```

---

## 2. Sort Merge Join (Default for Large-Large)

```
How it works:
  Stage 1 (both sides):
    - Shuffle both DataFrames by join key hash
    - Sort each partition by join key
  Stage 2 (merge):
    - Merge-sort both sorted streams → O(n) merge
    - Both sides processed in a single pass
  Cost: 2 shuffles + 2 sorts
  Best for: large-large equi-joins where keys have low-medium cardinality
```

```python
# Hint to force sort merge join
from pyspark.sql.functions import merge_join_hint  # not needed — it's default
# Or use SQL hint:
spark.sql("""
    SELECT /*+ MERGE(orders, customers) */ *
    FROM orders JOIN customers ON orders.customer_id = customers.customer_id
""")
```

---

## 3. Shuffle Hash Join

```
How it works:
  1. Shuffle BOTH sides by join key
  2. For the smaller side per partition: build in-memory HashMap
  3. Probe HashMap with larger side rows
  Cost: 2 shuffles, 1 hash build per partition
  When: Spark prefers this when one side is 3× smaller than the other
        and sort is more expensive than hash build
```

---

## 4. All Join Types — Code Reference

```python
# Inner join — only matching rows
df1.join(df2, "customer_id", "inner")

# Left outer — all from left, NULL from right if no match
df1.join(df2, "customer_id", "left")

# Right outer — all from right, NULL from left if no match
df1.join(df2, "customer_id", "right")

# Full outer — all from both, NULL where no match
df1.join(df2, "customer_id", "full")

# Left semi — rows from left WHERE key EXISTS in right (no right columns)
df1.join(df2, "customer_id", "left_semi")
# Equivalent to: SELECT * FROM df1 WHERE customer_id IN (SELECT customer_id FROM df2)

# Left anti — rows from left WHERE key NOT IN right
df1.join(df2, "customer_id", "left_anti")
# Equivalent to: SELECT * FROM df1 WHERE customer_id NOT IN (SELECT customer_id FROM df2)

# Cross join — cartesian product (DANGEROUS on large data)
df1.crossJoin(df2)

# Multiple join conditions
df1.join(df2,
    (df1.customer_id == df2.customer_id) &
    (df1.date == df2.date),
    "inner")
```

---

## Join Performance Tips

```python
# 1. Broadcast small dimension tables
result = fact_df.join(broadcast(dim_df), "dim_key")

# 2. Filter BEFORE join (reduce data shuffled)
filtered = large_df.filter(col("date") >= "2024-01-01")
result = filtered.join(other_df, "key")

# 3. Select only needed columns BEFORE join
small_large = large_df.select("key", "needed_col1", "needed_col2")
result = small_large.join(other_df, "key")

# 4. Repartition on join key before multiple joins
df_partitioned = df.repartition(200, "customer_id")
r1 = df_partitioned.join(df2, "customer_id")
r2 = df_partitioned.join(df3, "customer_id")
# Second join reuses same partitioning → no re-shuffle

# 5. Use SQL hints for join strategy control
spark.sql("""
    SELECT /*+ BROADCAST(d) */ f.*, d.name
    FROM fact f JOIN dim d ON f.dim_id = d.id
""")
```

---

## ❓ Cross-Questions

**Q: Why does Spark prefer Sort Merge Join over Shuffle Hash Join for large tables?**
> Sort Merge Join is more memory-efficient — it doesn't need to build a full HashMap per partition. It processes both sides in a single sorted merge pass. Hash Join requires the smaller side to fit in memory as a HashMap per partition, which can cause OOM on large partitions.

**Q: What is a bucketed join and how does it eliminate shuffle?**
> If both tables are pre-bucketed (written with `bucketBy`) on the join key with the same number of buckets, Spark can join them without any shuffle — both tables' data for the same key bucket are already co-located. Use for very frequent joins on the same key in a pipeline.

```python
df.write.bucketBy(32, "customer_id").sortBy("customer_id") \
    .format("parquet").saveAsTable("bucketed_orders")
```

**Q: What is a skewed join and how does AQE handle it automatically?**
> A skewed join occurs when one partition in a join has far more rows than others (due to data skew). AQE detects this at runtime and automatically splits the skewed partition into smaller sub-partitions, duplicating the corresponding partition of the other side to maintain correctness.

---

---

# 6. Structured Streaming Internals

## Scenario
> Explain watermarking, triggers, and output modes in Spark Structured Streaming.

---

## Streaming Architecture

```
SOURCE                   SPARK STREAMING ENGINE               SINK
──────                   ─────────────────────────           ──────
Kafka topic    ────▶     Trigger fires (every 30s)   ────▶  Delta table
S3 landing     ────▶     Read new micro-batch data   ────▶  Kafka topic
ADLS files     ────▶     Apply transformations       ────▶  Console
                         Check watermark
                         Write to sink
                         Update checkpoint
                              ▲
                              │
                    Checkpoint location
                    (maintains state across restarts)
```

---

## Output Modes

| Mode | Behaviour | Use for |
|---|---|---|
| `append` | Only new rows added since last trigger | Stateless transforms, window agg after watermark |
| `complete` | Entire result table rewritten each trigger | Aggregations without watermark (small result sets) |
| `update` | Only rows that changed since last trigger | Aggregations with watermark |

```python
# Append mode (most common for Delta sink)
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/events/") \
    .trigger(processingTime="30 seconds") \
    .start("/mnt/delta/events/")

# Update mode (for aggregations)
query = df.groupBy("customer_id") \
    .agg(F.sum("amount").alias("total")) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
```

---

## Triggers

```python
from pyspark.sql.streaming import DataStreamWriter

# Fixed interval micro-batch (most common)
.trigger(processingTime="30 seconds")

# Process all available data then stop (for backfill)
.trigger(once=True)

# Process one micro-batch then stop (Spark 3.3+)
.trigger(availableNow=True)

# Continuous processing (low-latency, experimental)
.trigger(continuous="1 second")
```

---

## Watermarking — Handling Late Data

```
Event timeline (events arrive out of order):
                          ┌──────────────┐
                          │ Watermark    │
                          │ = max(event  │
                          │   time) -    │
                          │   threshold  │
                          └──────────────┘

Time ────────────────────────────────────────────────────────▶
     12:00  12:05  12:10  12:15  12:20
              │              │
              │              └── Event arrives at 12:15
              │                  but has event_time = 12:03
              │                  (12 min late)
              └── Watermark at 12:20 - 10min = 12:10
                  Event at 12:03 < 12:10 → DROPPED (too late)
                  Event at 12:08 < 12:10 → DROPPED
                  Event at 12:12 > 12:10 → ACCEPTED ✅
```

```python
from pyspark.sql import functions as F

query = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .load()
    .select(
        F.col("value").cast("string"),
        F.col("timestamp").alias("event_time")
    )
    # Define watermark — drop events > 10 min late
    .withWatermark("event_time", "10 minutes")
    # Window aggregation
    .groupBy(
        F.window("event_time", "5 minutes"),   # 5-min tumbling window
        F.col("region")
    )
    .agg(F.sum("revenue").alias("revenue"))
    .writeStream
    .outputMode("update")                      # update mode for windowed agg
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/revenue_windows/")
    .trigger(processingTime="1 minute")
    .start("/mnt/delta/revenue_windows/")
)
```

---

## Window Types

```python
# Tumbling window — fixed, non-overlapping
F.window("event_time", "5 minutes")
# 12:00-12:05, 12:05-12:10, 12:10-12:15

# Sliding window — overlapping
F.window("event_time", "10 minutes", "5 minutes")
# 12:00-12:10, 12:05-12:15, 12:10-12:20

# Session window — gap-based (Spark 3.2+)
F.session_window("event_time", "5 minutes")
# Groups events within 5-min inactivity gap
```

---

## Streaming + Delta — Exactly-Once Semantics

```
Checkpoint location stores:
  - Offset of last processed batch (Kafka offset / file list)
  - State of stateful operations
  - Transaction ID for idempotent Delta writes

On restart:
  1. Read checkpoint → know exactly where to resume
  2. Re-process only new data
  3. Delta ACID write with same transaction ID → idempotent
  Result: exactly-once delivery even after failure ✅
```

---

## ❓ Cross-Questions

**Q: What is the difference between `trigger(once=True)` and `trigger(availableNow=True)`?**
> `once=True` processes all available data in a single micro-batch — useful for testing but can create one very large batch. `availableNow=True` (Spark 3.3+) processes all available data in multiple optimally-sized micro-batches, then stops — better for scheduled incremental loads.

**Q: Can you join two streaming DataFrames?**
> Yes — Spark supports stream-stream joins with watermarks. Both streams must define watermarks on the join key's timestamp to bound the state size. Without watermarks, state grows infinitely and will eventually OOM.

**Q: What happens to a Structured Streaming job when it crashes?**
> On restart, Spark reads the checkpoint location to determine the last successfully committed batch offset and the last Delta transaction. It resumes from exactly that point — no data is reprocessed or lost, provided the source data is still available (Kafka retention, files still in landing zone).

**Q: What is stateful vs stateless streaming?**
> Stateless: each micro-batch is processed independently — `filter`, `select`, `withColumn`. These are simple and fast. Stateful: operations that require memory across batches — `groupBy + agg`, `window`, `deduplication`, `stream-stream join`. Stateful operations need a watermark and checkpoint to manage state size.

---

---

# 7. Delta Live Tables (DLT)

## Scenario
> When would you use Delta Live Tables and how does it differ from standard Spark Structured Streaming?

---

## DLT vs Standard Streaming

| Feature | Standard Structured Streaming | Delta Live Tables |
|---|---|---|
| Pipeline definition | Python/Scala code + manual orchestration | Declarative Python/SQL with `@dlt.table` |
| Error handling | Manual try/catch | Built-in expectations with quarantine |
| Monitoring | Spark UI | DLT Event Log + Pipeline UI |
| Dependency management | Manual | Auto-inferred from table references |
| Cluster lifecycle | You manage | DLT manages |
| Data quality | Manual validation | Native `@dlt.expect` |
| Schema evolution | Manual `mergeSchema` | Auto-handled |

---

## DLT Pipeline Architecture

```
BRONZE (raw)       SILVER (cleaned)      GOLD (aggregated)
─────────────────  ────────────────────  ─────────────────
@dlt.table         @dlt.table            @dlt.table
raw_events    ──▶  cleaned_events   ──▶  daily_revenue_kpi
              ↑                    ↑
        Auto Loader          @dlt.expect
        reads from S3        data quality
                             checks here
```

---

## DLT Code Patterns

```python
import dlt
from pyspark.sql import functions as F

# ── BRONZE — Raw ingestion ──
@dlt.table(
    name="raw_events",
    comment="Raw events from cloud storage",
    table_properties={"quality": "bronze"}
)
def raw_events():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schema/events")
        .load("/mnt/landing/events/")
    )

# ── SILVER — Cleaned with data quality expectations ──
@dlt.table(
    name="cleaned_events",
    comment="Validated and cleaned events",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_event_id",   "event_id IS NOT NULL")
@dlt.expect("valid_event_type", "event_type IN ('click','view','purchase')")
@dlt.expect_or_drop("valid_revenue", "revenue >= 0")   # drop bad rows
@dlt.expect_or_fail("no_future_dates", "event_date <= current_date()")  # fail pipeline
def cleaned_events():
    return (dlt.read_stream("raw_events")
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("revenue",    F.col("revenue").cast("decimal(18,2)"))
        .dropDuplicates(["event_id"])
    )

# ── GOLD — Business aggregate ──
@dlt.table(
    name="daily_revenue_kpi",
    comment="Daily revenue KPI for business reporting"
)
def daily_revenue_kpi():
    return (dlt.read("cleaned_events")  # batch read from silver
        .groupBy("event_date", "region")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("event_id").alias("event_count")
        )
    )
```

---

## DLT Expectation Actions

```python
# Log violation but keep the row — monitoring only
@dlt.expect("name", "condition")

# Drop rows that violate the expectation
@dlt.expect_or_drop("name", "condition")

# Fail the entire pipeline on any violation
@dlt.expect_or_fail("name", "condition")

# Multiple expectations at once
@dlt.expect_all({
    "valid_id":      "id IS NOT NULL",
    "valid_amount":  "amount > 0",
    "valid_country": "country_code IN ('IN','US','UK')"
})

@dlt.expect_all_or_drop({...})
@dlt.expect_all_or_fail({...})
```

---

## ❓ Cross-Questions

**Q: What is the difference between `dlt.read()` and `dlt.read_stream()`?**
> `dlt.read_stream()` creates a streaming source from a DLT table — used in SILVER/BRONZE tables that process data incrementally. `dlt.read()` creates a batch/snapshot source — used in GOLD tables that aggregate over the full dataset.

**Q: What is a DLT pipeline update mode — Triggered vs Continuous?**
> Triggered: pipeline runs once (processes all available data) and stops — like `trigger(availableNow=True)`. Cost-effective for hourly/daily pipelines. Continuous: pipeline runs 24/7, processing data as it arrives — low-latency but higher cost.

**Q: Where do quarantined rows (from `expect_or_drop`) go?**
> Dropped rows are not written to the target table. They are logged in the DLT event log with the expectation name and row data. You can query the event log (`delta.`/pipelines/<id>/system/events`) to inspect dropped rows.

---

---

# 8. Unity Catalog

## Scenario
> What is Unity Catalog and how does it improve governance in Databricks?

---

## Unity Catalog Architecture

```
UNITY CATALOG METASTORE (one per region)
│
├── CATALOG: prod
│   ├── SCHEMA: bronze
│   │   ├── TABLE: raw_events
│   │   └── TABLE: raw_orders
│   ├── SCHEMA: silver
│   │   └── TABLE: cleaned_events
│   └── SCHEMA: gold
│       └── TABLE: daily_revenue_kpi
│
├── CATALOG: dev
│   └── SCHEMA: sandbox
│       └── TABLE: test_events
│
└── EXTERNAL LOCATIONS (S3/ADLS paths with IAM bindings)
    ├── s3://prod-datalake/  → mapped to prod catalog storage
    └── s3://dev-datalake/   → mapped to dev catalog storage
```

---

## Three-Level Namespace

```sql
-- Unity Catalog uses: catalog.schema.table
SELECT * FROM prod.gold.daily_revenue_kpi

-- Grant table access
GRANT SELECT ON TABLE prod.gold.daily_revenue_kpi TO `analyst_group`;

-- Grant schema access
GRANT USE SCHEMA ON SCHEMA prod.gold TO `data_engineers`;

-- Grant catalog access
GRANT USE CATALOG ON CATALOG prod TO `data_team`;

-- Row-level security via dynamic views
CREATE VIEW prod.gold.revenue_masked AS
SELECT
    customer_id,
    CASE WHEN is_account_admin() THEN revenue ELSE 'REDACTED' END AS revenue,
    region
FROM prod.gold.daily_revenue;
```

---

## Data Lineage & Audit

```sql
-- Column-level lineage tracked automatically
-- View in Databricks UI: Data → Table → Lineage

-- Audit log — who accessed what and when
SELECT * FROM system.access.audit
WHERE action_name = 'READ'
AND object_type  = 'TABLE'
AND object_name  = 'prod.gold.daily_revenue_kpi'
ORDER BY event_time DESC
```

---

## ❓ Cross-Questions

**Q: What is the difference between Unity Catalog and the legacy Hive Metastore?**
> Hive Metastore is workspace-scoped — each Databricks workspace has its own Hive Metastore. Unity Catalog is account-scoped — one metastore for all workspaces in a region, enabling cross-workspace data sharing, centralised governance, and column-level lineage tracking. Unity Catalog also integrates with cloud IAM for fine-grained access control.

**Q: What are External Tables vs Managed Tables in Unity Catalog?**
> Managed tables: Delta files stored in Unity Catalog's managed storage location. Dropping the table drops the data. External tables: Delta files stored in a customer-managed location (e.g., `s3://your-bucket/`). Dropping the table removes the metadata but not the data.

**Q: What is Delta Sharing and how does it relate to Unity Catalog?**
> Delta Sharing is an open protocol for sharing live Delta tables with external organisations or different cloud platforms without copying data. Unity Catalog is the governance layer that manages who can share what. Together they enable secure cross-organisation data sharing.

---

---

# 9. Spark UI — Debugging & Performance Tuning

## Scenario
> How do you use the Spark UI to diagnose and fix performance problems?

---

## Spark UI Navigation Map

```
http://<driver-host>:4040
│
├── Jobs tab
│   ├── List of all jobs (triggered by actions)
│   ├── Duration, status, stages per job
│   └── Click job → see stages
│
├── Stages tab
│   ├── List of all stages
│   ├── Tasks per stage, shuffle read/write
│   ├── Task distribution (look for outliers)
│   └── Click stage → task-level metrics
│
├── Storage tab
│   ├── Cached RDDs/DataFrames
│   ├── Memory used per cached dataset
│   └── Fraction cached in memory vs disk
│
├── Executors tab
│   ├── Memory used/total per executor
│   ├── GC time per executor
│   ├── Shuffle read/write per executor
│   └── Task time distribution
│
├── SQL/DataFrame tab
│   ├── Physical plan for each query
│   ├── Metrics per operator
│   └── Click node → see actual row counts
│
└── Environment tab
    └── All Spark configs in effect
```

---

## Common Performance Problems & Diagnosis

### Problem 1 — Data Skew
```
Symptom in UI:
  Stages → Tasks → sort by Duration
  One task: 15 minutes
  All other tasks: 30 seconds
  → Classic skew

Fix: AQE skew join, salting (see Section 3)
```

### Problem 2 — Disk Spill
```
Symptom in UI:
  Stages → Shuffle Spill (Memory) > 0
  Stages → Shuffle Spill (Disk) > 0

Fix:
  - Increase spark.executor.memory
  - Reduce spark.sql.shuffle.partitions
  - Enable AQE
```

### Problem 3 — GC Pressure
```
Symptom in UI:
  Executors tab → GC Time > 5-10% of task time
  Tasks take long but CPU usage is low

Fix:
  - Increase executor memory
  - Reduce data cached in memory
  - Use G1GC: -XX:+UseG1GC
  - Use off-heap: spark.memory.offHeap.enabled=true
```

### Problem 4 — Too Many / Too Few Partitions
```
Symptom in UI:
  Stages → Tasks = 20,000 but each runs < 100ms
  → Too many tiny tasks (partition count too high)

  OR

  Stages → Tasks = 5, each runs 30 minutes
  → Too few partitions (not enough parallelism)

Fix:
  - Tune spark.sql.shuffle.partitions
  - Enable AQE coalescePartitions
```

### Problem 5 — Missing Predicate Pushdown
```
Symptom in SQL tab:
  Physical plan shows FileScan without PushedFilters
  Full table scan even though you have a WHERE clause

Fix:
  df.explain("formatted")  # check for PushedFilters
  - Use partition columns in filter
  - Ensure filter is on a column Delta has stats for
  - ZORDER on frequently filtered columns
```

---

## Key Metrics to Always Check

```
In Stages tab for each stage:
  - Input Size / Records: how much data was read
  - Shuffle Write: data written for next stage (high = expensive)
  - Shuffle Read: data read from prev stage
  - Spill (disk): should be 0 ideally
  - Duration: should be similar across tasks (skew check)

In SQL tab for each query:
  - number of output rows per operator (check for data explosion)
  - Join type: BroadcastHashJoin ✅ vs SortMergeJoin (may be expensive)
  - Exchange: each Exchange node = a shuffle
```

---

## ❓ Cross-Questions

**Q: How do you read a Spark physical plan?**
> Read bottom-up — Spark executes from bottom to top. Look for: `FileScan` (data read), `Filter` (predicate), `Exchange` (shuffle boundary), `Sort`, `Aggregate`, `Join`. Each `Exchange` = a stage boundary. `BroadcastExchange` = small table being broadcast. `PushedFilters` next to `FileScan` = predicate pushdown working.

**Q: What does "Executor Deserialisation Time" mean in Spark UI?**
> Time spent deserialising the task closure (the serialised function + dependencies) on the executor before the task starts processing data. High deserialisation time can indicate large UDFs or too many broadcast variables being deserialised per task.

---

---

# 10. Adaptive Query Execution (AQE) — Deep Dive

## Scenario
> What is AQE and what are its three main features?

---

## AQE Overview

```
Without AQE (Spark 2.x):
  Plan is fixed at compile time based on statistics (often stale)
  → Wrong join strategy chosen
  → Too many shuffle partitions
  → Skew not detected

With AQE (Spark 3.x, enabled by default):
  Plan is re-optimised at runtime after each shuffle stage
  → Sees ACTUAL data sizes and distributions
  → Dynamically adjusts the plan mid-execution
```

---

## AQE Feature 1 — Dynamic Coalescing of Shuffle Partitions

```
Before AQE:
  shuffle.partitions = 200
  After shuffle, 190 partitions have ~1 MB each (too small)
  10 partitions have ~100 MB each
  → 190 wasted tiny tasks

After AQE:
  AQE sees actual partition sizes after shuffle
  Coalesces 190 tiny partitions into ~10 optimal ones
  → Fewer tasks, right size, faster execution
```

```python
spark.conf.set("spark.sql.adaptive.enabled",                       "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",    "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",  "134217728")  # 128 MB target
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
```

---

## AQE Feature 2 — Dynamic Switching of Join Strategies

```
Compile time estimate: Table B = 500 MB → SortMergeJoin planned

Runtime reality after filtering:
  Table B after filter = 8 MB → fits in broadcast threshold!

AQE switches: SortMergeJoin → BroadcastHashJoin at runtime ✅
  → Eliminates shuffle of Table B entirely
```

```python
spark.conf.set("spark.sql.adaptive.enabled",                              "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled",           "true")
# AQE will switch join strategy automatically when it sees actual sizes
```

---

## AQE Feature 3 — Dynamic Skew Join Handling

```
Runtime detection:
  Partition 42 has 8 GB of data
  Median partition size: 100 MB
  Factor: 8 GB / 100 MB = 80× → SKEWED

AQE response:
  Splits partition 42 into 80 sub-partitions
  Duplicates the corresponding partition of the other side
  Each sub-partition processed by a separate task
  → Skew handled automatically without code changes ✅
```

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                         "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",            "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",  "256mb")
```

---

## Full AQE Config Block

```python
# Recommended AQE config for production
spark.conf.set("spark.sql.adaptive.enabled",                               "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",            "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum",    "1")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes",          "134217728")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                      "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",        "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes","256mb")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled",            "true")
```

---

## ❓ Cross-Questions

**Q: Is AQE on by default in Databricks?**
> Yes — in Databricks Runtime 7.3+, AQE is enabled by default. In open-source Spark, it became the default in Spark 3.2.

**Q: Can AQE help if statistics are completely missing?**
> AQE works based on actual runtime metrics from completed shuffle stages, not pre-execution statistics. So yes — even if table statistics are stale or missing, AQE observes real data sizes after each stage and re-optimises the remaining plan.

**Q: What is the difference between AQE and Cost-Based Optimiser (CBO)?**
> CBO uses pre-computed table statistics (`ANALYZE TABLE`) to estimate costs at compile time. AQE re-optimises at runtime based on actual observed data. They are complementary — CBO gives a better starting plan, AQE corrects it mid-execution.

---

---

# 11. Partitioning Strategies — Deep Dive

## Scenario
> How do you choose the right partitioning strategy for a Delta table?

---

## Partition Strategy Decision Framework

```
Question 1: What is the cardinality of the partition column?
  LOW (< 10,000 unique values) → Good partition column
  HIGH (millions of unique values) → Bad partition column

Question 2: How are queries filtered?
  Most queries filter by date → partition by date
  Most queries filter by region → partition by region
  Mixed → partition by date, ZORDER by region

Question 3: How much data per partition?
  Target: 1 GB+ per partition directory
  Too little → small file problem
  Too big → single partition bottleneck

Question 4: How often does data change?
  Daily append → partition by date (only new partition written)
  Frequent updates across dates → fewer partitions (less MERGE overhead)
```

---

## Common Partition Patterns

```python
# Pattern 1 — Date partitioning (most common)
df.write.partitionBy("date").format("delta").save(path)
# Pros: great for time-range queries, daily ETL writes one new partition
# Cons: too many small partitions if daily data < 1 GB

# Pattern 2 — Year/Month/Day hierarchy
df.withColumn("year",  F.year("date")) \
  .withColumn("month", F.month("date")) \
  .write.partitionBy("year", "month", "day").format("delta").save(path)
# Pros: efficient for monthly/yearly range scans
# Cons: 3-level directory hierarchy (slower LIST operations)

# Pattern 3 — Region + Date (composite)
df.write.partitionBy("region", "date").format("delta").save(path)
# Good when: queries almost always filter by region AND date

# Pattern 4 — No partitioning + ZORDER
df.write.format("delta").save(path)
DeltaTable.forPath(spark, path).optimize().zorderBy("customer_id", "date")
# Good for: tables < 1 TB where Z-ordering provides enough data skipping
# Avoids small file problem from high-cardinality partitioning

# Pattern 5 — Liquid Clustering (Databricks Runtime 13.3+)
# Replaces static partitioning with dynamic clustering — no partition columns needed
spark.sql("""
    ALTER TABLE gold.events
    CLUSTER BY (customer_id, date)
""")
# Re-clusters automatically on OPTIMIZE
# Can change clustering columns without rewriting the table
```

---

## Anti-Patterns

```python
# BAD 1 — High cardinality partition key
df.write.partitionBy("user_id").format("delta").save(path)
# 10 million users → 10 million directories → each with tiny files ❌

# BAD 2 — Timestamp partition (too fine-grained)
df.write.partitionBy("event_timestamp").format("delta").save(path)
# One directory per millisecond → billions of directories ❌

# BAD 3 — Too many partition columns
df.write.partitionBy("year","month","day","hour","region","category").save(path)
# Creates astronomical number of directories ❌
# Rule: maximum 2-3 partition columns
```

---

## Liquid Clustering (Databricks 13.3+)

```sql
-- Create table with liquid clustering
CREATE TABLE gold.events
CLUSTER BY (customer_id, event_date)
AS SELECT * FROM staging.events;

-- Change clustering columns (without rewriting data)
ALTER TABLE gold.events CLUSTER BY (region, event_date);

-- Trigger clustering
OPTIMIZE gold.events;

-- Check clustering info
DESCRIBE DETAIL gold.events;
-- Shows: clusteringColumns, numFiles, sizeInBytes
```

---

## ❓ Cross-Questions

**Q: What is the difference between partitioning and bucketing?**
> Partitioning creates separate directories per partition value — used for partition pruning at read time (file skipping at directory level). Bucketing pre-sorts and distributes data into a fixed number of files per partition based on hash of the bucket column — used to eliminate shuffle in joins and aggregations on the bucket key.

**Q: When would you choose Liquid Clustering over traditional partitioning?**
> Liquid Clustering (Databricks 13.3+) is better when: clustering columns are high-cardinality, you need to change clustering strategy without full rewrites, or you're building new tables without knowing the optimal partition strategy upfront. It uses Z-ordering under the hood but manages it automatically.

**Q: What is partition pruning?**
> When a query has a filter on the partition column (`WHERE date = '2024-01-01'`), Spark skips all directories except the matching partition(s) — it never even lists or reads those files. This can reduce I/O by orders of magnitude. Partition pruning only works on exact equality or range filters on the partition column.

---

---

# 12. Caching & Persistence Strategies

## Scenario
> When should you cache a DataFrame and which storage level should you use?

---

## Storage Levels Comparison

| Storage Level | Memory | Disk | Serialised | Replicated | Use for |
|---|---|---|---|---|---|
| `MEMORY_ONLY` | ✅ | ❌ | ❌ | ❌ | Fast access, enough memory |
| `MEMORY_AND_DISK` | ✅ | ✅ (on eviction) | ❌ | ❌ | Safety net — evicted to disk not lost |
| `MEMORY_ONLY_SER` | ✅ | ❌ | ✅ | ❌ | Reduced memory footprint, more GC |
| `MEMORY_AND_DISK_SER` | ✅ | ✅ | ✅ | ❌ | Best balance for large DataFrames |
| `DISK_ONLY` | ❌ | ✅ | ✅ | ❌ | Very large data, slower access |
| `OFF_HEAP` | ✅ (off-heap) | ❌ | ✅ | ❌ | Bypass GC for large caches |

---

## When to Cache vs Not Cache

```python
# CACHE — when DF is reused multiple times in same job
df_joined = fact_df.join(broadcast(dim_df), "key")
df_joined.cache()                    # materialise + cache

result1 = df_joined.filter("region = 'IN'").groupBy("date").sum("revenue")
result2 = df_joined.filter("region = 'US'").groupBy("category").count()
# Both reuse df_joined from cache — no recompute ✅

df_joined.unpersist()                # release memory when done

# DO NOT CACHE — when DF is used only once
df.filter(...).groupBy(...).write...  # no cache needed
```

---

## Cache Code Patterns

```python
from pyspark import StorageLevel

# Simple cache (MEMORY_AND_DISK by default in DataFrames)
df.cache()

# Explicit storage level
df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Force materialisation (cache is lazy by default)
df.cache().count()     # count() triggers computation and caches result

# Release cache
df.unpersist()
df.unpersist(blocking=True)   # wait for cache to be freed before continuing

# Check cached DataFrames in current session
spark.catalog.listDatabases()   # not directly useful
# Better: Spark UI → Storage tab
```

---

## ❓ Cross-Questions

**Q: Is `df.cache()` the same as `df.persist()`?**
> `df.cache()` is shorthand for `df.persist(StorageLevel.MEMORY_AND_DISK)` for DataFrames. They're functionally equivalent for DataFrames. For RDDs, `rdd.cache()` = `rdd.persist(StorageLevel.MEMORY_ONLY)`.

**Q: Does caching a DataFrame affect its lineage?**
> No — caching doesn't break the lineage (DAG). If the cache is evicted (due to memory pressure), Spark recomputes the DataFrame from its lineage automatically (for `MEMORY_ONLY`) or reads it from disk (for `MEMORY_AND_DISK`). The lineage is always preserved.

**Q: Can you cache a streaming DataFrame?**
> No — streaming DataFrames are processed incrementally and cannot be cached as a whole. You can cache lookup/reference DataFrames that are joined with a streaming DataFrame.

---

---

# 13. Databricks Workflows — Advanced Patterns

## Scenario
> How do you design complex multi-task pipelines in Databricks Workflows?

---

## Workflow Architecture Patterns

```
PATTERN 1 — Linear dependency chain
  ETL (Task A) → Transform (Task B) → Alert (Task C)
  B starts only after A succeeds
  C starts only after B succeeds

PATTERN 2 — Fan-out (parallel tasks)
  Ingest (Task A) ──┬──▶ Silver_Region_IN (Task B)
                    ├──▶ Silver_Region_US (Task C)
                    └──▶ Silver_Region_EU (Task D)
  B, C, D run in parallel after A succeeds

PATTERN 3 — Fan-in (join after parallel)
  Task B ──┐
  Task C ──┼──▶ Gold aggregation (Task E)
  Task D ──┘
  E starts only after B, C, D all succeed

PATTERN 4 — Conditional branching
  Validation (Task A) ──▶ if success: Transform (Task B)
                       └─▶ if failure: Alert (Task C)
  Using: task.run_if conditions
```

---

## Workflow JSON Definition

```json
{
  "name": "daily-data-platform-pipeline",
  "schedule": {
    "quartz_cron_expression": "0 30 0 * * ?",
    "timezone_id": "Asia/Kolkata",
    "pause_status": "UNPAUSED"
  },
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "notebook_task": {"notebook_path": "/ETL/bronze_ingest"},
      "job_cluster_key": "etl_cluster",
      "timeout_seconds": 1800,
      "max_retries": 2,
      "retry_on_timeout": true
    },
    {
      "task_key": "silver_transform",
      "depends_on": [{"task_key": "bronze_ingest"}],
      "notebook_task": {"notebook_path": "/ETL/silver_transform"},
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "gold_revenue",
      "depends_on": [{"task_key": "silver_transform"}],
      "notebook_task": {"notebook_path": "/ETL/gold_revenue"},
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "alert_check",
      "depends_on": [{"task_key": "gold_revenue"}],
      "notebook_task": {"notebook_path": "/Alerts/revenue_drop_alert"},
      "environment_key": "serverless"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "etl_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id":  "i3.xlarge",
        "num_workers":   4,
        "spark_conf": {
          "spark.sql.adaptive.enabled":                    "true",
          "spark.databricks.delta.optimizeWrite.enabled":  "true"
        }
      }
    }
  ],
  "email_notifications": {
    "on_failure": ["data-eng@company.com"],
    "on_success": [],
    "no_alert_for_skipped_runs": true
  }
}
```

---

## Passing Data Between Tasks

```python
# Task A — pass value to downstream task
result_value = compute_something()
dbutils.jobs.taskValues.set(key="row_count", value=result_value)

# Task B — read value from upstream task
row_count = dbutils.jobs.taskValues.get(
    taskKey="bronze_ingest",
    key="row_count",
    default=0,
    debugValue=100
)
print(f"Upstream task processed {row_count} rows")
```

---

## ❓ Cross-Questions

**Q: What is the difference between a Workflow Job cluster and an All-Purpose cluster for tasks?**
> A Job cluster is created fresh for each Workflow run and terminated when the run completes — no idle cost. An All-Purpose cluster persists and can be used interactively in notebooks alongside the job. Use Job clusters for production scheduled pipelines; use All-Purpose for development.

**Q: How do you implement SLA monitoring in Databricks Workflows?**
> Set `timeout_seconds` at the task level and `max_retries` for automatic retry. Add email/Slack notifications on failure. For SLA alerts (e.g., pipeline must finish by 06:00 AM), use Databricks monitoring APIs or add an external monitoring job that checks pipeline run completion time.

---

---

# 14. PySpark DataFrame vs RDD vs SQL

## Scenario
> What is the difference between RDD, DataFrame, and Spark SQL? When do you use each?

---

## Comparison Table

| Feature | RDD | DataFrame | Spark SQL |
|---|---|---|---|
| Abstraction level | Low | High | High |
| Type safety | Yes (compile-time in Scala) | No (schema at runtime) | No |
| Catalyst optimisation | ❌ No | ✅ Yes | ✅ Yes |
| Tungsten execution | ❌ No | ✅ Yes | ✅ Yes |
| Ease of use | Complex | Easy | Easiest |
| Performance | Slowest | Fast | Same as DataFrame |
| Use for | Custom partitioning, ML ops | ETL, analytics | Ad-hoc, BI, SQL users |

---

## When to Use Each

```python
# RDD — use only when:
# 1. You need precise control over partitioning
# 2. Working with unstructured data (text, binary)
# 3. Calling Java/Scala libraries that return RDDs
rdd = sc.textFile("s3://bucket/logs/*.txt")
rdd.map(parse_log_line).filter(lambda x: x is not None)

# DataFrame — use for most ETL work
df = spark.read.format("delta").table("gold.events")
df.filter(col("date") >= "2024-01-01") \
  .groupBy("region").agg(F.sum("revenue"))

# SQL — use for ad-hoc queries, BI tools, readability
spark.sql("""
    SELECT region, SUM(revenue) as total_revenue
    FROM gold.events
    WHERE date >= '2024-01-01'
    GROUP BY region
    ORDER BY total_revenue DESC
""")

# Convert between them
df.createOrReplaceTempView("events_view")
result = spark.sql("SELECT * FROM events_view WHERE region = 'IN'")

rdd = df.rdd
df2 = spark.createDataFrame(rdd, schema=df.schema)
```

---

## ❓ Cross-Questions

**Q: Why is DataFrame faster than RDD?**
> DataFrames go through the Catalyst optimiser (rewrites the plan for efficiency) and Tungsten execution engine (binary memory format, SIMD CPU instructions, code generation). RDDs bypass both — every operation is a Python/Java lambda, interpreted at runtime with JVM object overhead.

**Q: When would you convert a DataFrame to RDD?**
> When applying complex custom Python logic that can't be expressed in DataFrame/SQL APIs, or when calling libraries that return RDDs. Always convert back to DataFrame as quickly as possible to regain Catalyst optimisation.

---

---

# 15. MLflow Integration with Databricks

## Scenario
> How do you use MLflow with Databricks for experiment tracking and model deployment?

---

## MLflow Architecture in Databricks

```
EXPERIMENT TRACKING             MODEL REGISTRY           DEPLOYMENT
─────────────────────          ─────────────────────    ─────────────────
mlflow.start_run()        ──▶  mlflow.register_model()  Model Serving
log_param("lr", 0.01)          Stage: Staging           (REST endpoint)
log_metric("rmse", 0.15)       Stage: Production        Batch inference
log_artifact("model.pkl")      Stage: Archived          Streaming scoring
mlflow.log_model(model)
```

---

## MLflow Tracking Code

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import numpy as np

# Auto-logging (captures params, metrics, model automatically)
mlflow.sklearn.autolog()

# Or manual logging
with mlflow.start_run(run_name="revenue_forecast_v1") as run:
    # Log parameters
    mlflow.log_param("n_estimators",  100)
    mlflow.log_param("max_depth",     10)
    mlflow.log_param("feature_set",   "v2")

    # Train model
    model = RandomForestRegressor(n_estimators=100, max_depth=10)
    model.fit(X_train, y_train)

    # Log metrics
    y_pred = model.predict(X_test)
    rmse   = np.sqrt(mean_squared_error(y_test, y_pred))
    mlflow.log_metric("rmse",    rmse)
    mlflow.log_metric("r2_score", model.score(X_test, y_test))

    # Log model
    mlflow.sklearn.log_model(
        model,
        artifact_path="random_forest_model",
        registered_model_name="RevenueForecaster"
    )

    print(f"Run ID: {run.info.run_id}")
    print(f"RMSE: {rmse:.4f}")
```

---

## Model Registry & Promotion

```python
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Transition model stage
client.transition_model_version_stage(
    name="RevenueForecaster",
    version=3,
    stage="Production",
    archive_existing_versions=True   # archive old Production version
)

# Load production model for inference
model = mlflow.sklearn.load_model("models:/RevenueForecaster/Production")
predictions = model.predict(X_new)
```

---

## ❓ Cross-Questions

**Q: What is the difference between MLflow run and MLflow experiment?**
> An experiment is a named collection of runs (e.g., "Revenue Forecasting"). A run is a single training execution within an experiment, containing params, metrics, and artifacts. Multiple engineers can log runs to the same experiment for comparison.

**Q: How does Databricks MLflow differ from open-source MLflow?**
> Databricks MLflow uses the Databricks tracking server (hosted, no setup needed), integrates with Unity Catalog for model governance, provides Model Serving (REST API deployment), and has Databricks-native UI. Open-source requires you to host your own tracking server.

---

---

# 16. Common Interview Scenario Patterns

## Quick-Reference Answers

### "Job is slow — how do you diagnose?"
```
1. Spark UI → Stages → look for:
   a. Skew: one task much longer than others
   b. Spill: shuffle spill > 0
   c. GC time: > 5% of task time
   d. Missing partitions: 200 tiny tasks under 1MB each

2. SQL tab → physical plan → look for:
   a. Missing PushedFilters (no predicate pushdown)
   b. SortMergeJoin where BroadcastHashJoin expected
   c. Full table scan on large tables

3. Code review → look for:
   a. .collect() on large DataFrames → OOM
   b. UDFs in Python → serialisation overhead
   c. withColumn() in a loop → creates deep plan
   d. join without filter → full shuffle of both sides
```

### "OOM error — how do you fix it?"
```
OutOfMemoryError: Java heap space
→ Increase spark.executor.memory
→ Reduce data processed per task (more partitions)
→ Check for data skew (one partition too large)
→ Replace Python UDFs with built-in functions
→ Unpersist cached DataFrames no longer needed

Container killed / exit code 143
→ executor.memory + memoryOverhead > node RAM
→ Increase spark.executor.memoryOverhead (especially for PySpark)
→ Reduce spark.executor.memory so total fits in node
```

### "Pipeline is failing on schema changes"
```
Root cause: source team added/changed columns without notice

Fix:
→ Switch to Auto Loader (cloudFiles format)
→ Set cloudFiles.schemaEvolutionMode = addNewColumns
→ Enable mergeSchema = true on Delta write
→ Add cloudFiles.rescuedDataColumn = "_rescued_data" for bad rows
→ Monitor _rescued_data column in production
```

### "Delta table query is slow"
```
1. Run ANALYZE TABLE gold.events COMPUTE STATISTICS
   → Updates table stats for Catalyst CBO

2. Run OPTIMIZE gold.events
   → Compact small files into ~1 GB files

3. Run OPTIMIZE gold.events ZORDER BY (customer_id)
   → Co-locate data for common filter columns

4. Check partition strategy:
   → Are queries filtering on partition column?
   → Is there a high-cardinality partition causing small files?

5. Enable AQE:
   → spark.sql.adaptive.enabled = true

6. Check predicate pushdown:
   → df.explain("formatted") → look for PushedFilters
```

---

## Must-Know PySpark Functions — Quick Reference

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# String
F.concat(col1, lit("_"), col2)
F.split(col, "_")[0]
F.regexp_replace(col, "[^a-zA-Z]", "")
F.upper(col) / F.lower(col) / F.trim(col)

# Date/Time
F.current_date() / F.current_timestamp()
F.to_date(col, "yyyy-MM-dd")
F.date_add(col, 7) / F.date_sub(col, 1)
F.datediff(end_date, start_date)
F.date_format(col, "yyyy-MM")
F.from_utc_timestamp(col, "Asia/Kolkata")

# Numeric
F.round(col, 2)
F.abs(col)
F.greatest(col1, col2)
F.least(col1, col2)

# Null handling
F.coalesce(col1, col2, lit(0))   # first non-null
F.isnull(col) / F.isnotnull(col)
F.when(col.isNull(), lit(0)).otherwise(col)
F.fillna({"col1": 0, "col2": "unknown"})

# Aggregation
F.sum / F.count / F.avg / F.min / F.max
F.countDistinct(col)
F.collect_list(col) / F.collect_set(col)
F.first(col, ignorenulls=True)
F.approx_count_distinct(col)      # faster than countDistinct

# Window
w = Window.partitionBy("region").orderBy("date")
F.lag(col, 1).over(w)
F.lead(col, 1).over(w)
F.rank().over(w)
F.dense_rank().over(w)
F.row_number().over(w)
F.sum(col).over(w)                # running total
F.sum(col).over(Window.partitionBy("region"))  # partition total

# Array/Map
F.explode(array_col)
F.array(col1, col2, col3)
F.array_contains(array_col, value)
F.size(array_col)
F.map_keys(map_col) / F.map_values(map_col)
F.from_json(col, schema)
F.to_json(struct_col)

# Conditional
F.when(condition, value).when(other, other_val).otherwise(default)
F.try_cast(col, "INT")            # returns NULL instead of error

# UDF (use sparingly — prefer built-ins)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def my_udf(value):
    return value.upper() if value else None

df.withColumn("upper_name", my_udf(col("name")))

# Pandas UDF (vectorised — much faster than regular UDF)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def revenue_with_tax(revenue: pd.Series) -> pd.Series:
    return revenue * 1.18

df.withColumn("revenue_with_tax", revenue_with_tax(col("revenue")))
```

---

## Common Interview One-Liners (Memorise These)

| Topic | One-liner |
|---|---|
| Executor sizing | "5 cores per executor, node RAM ÷ executors per node × 0.9, overhead = 10%" |
| Unified Memory | "Execution can evict storage; storage cannot evict execution — storage always loses in a memory fight" |
| Small files | "Prevent with optimizeWrite + coalesce; fix with OPTIMIZE + VACUUM + ZORDER" |
| Schema evolution | "Auto Loader + addNewColumns + mergeSchema + _rescued_data = zero-downtime schema changes" |
| Serverless | "You trade infra control for zero-ops — Databricks owns executor sizing, you own query configs" |
| Data skew | "Detect in Spark UI by sorted task duration; fix with AQE skewJoin or salting" |
| Delta internals | "Transaction log = audit trail of every add/remove; VACUUM deletes old files; time travel reads old log" |
| AQE | "Three features: coalesce partitions, switch join strategy, handle skew — all at runtime after each shuffle" |
| Broadcast join | "Small table → HashMap on all executors → zero shuffle of large table — fastest join type" |
| Caching | "Cache when DF used multiple times; use MEMORY_AND_DISK so eviction goes to disk not lost; unpersist when done" |

---

*Part 2 of Databricks & PySpark Interview Notes*
*Topics: DAG internals · Shuffle · Skew · Delta Lake internals · Join strategies · Streaming · DLT · Unity Catalog · Spark UI · AQE · Partitioning · Caching · Workflows · MLflow*
