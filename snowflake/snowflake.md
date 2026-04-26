# Snowflake Complete Interview Guide
### For Experienced Data Engineers (5+ YOE)

---

## TABLE OF CONTENTS

### PART 1 — CORE SNOWFLAKE
- [1.1 Architecture Overview](#11-architecture-overview)
- [1.2 Separation of Compute and Storage](#12-separation-of-compute-and-storage)
- [1.3 Virtual Warehouses](#13-virtual-warehouses)
- [1.4 Micro-Partitioning](#14-micro-partitioning)
- [1.5 Clustering Key](#15-clustering-key)
- [1.6 Partition Pruning](#16-partition-pruning)
- [1.7 Time Travel](#17-time-travel)
- [1.8 Fail-safe](#18-fail-safe)
- [1.9 Zero-Copy Cloning](#19-zero-copy-cloning)
- [1.10 Stages — Internal vs External](#110-stages--internal-vs-external)
- [1.11 File Formats](#111-file-formats)
- [1.12 Data Sharing](#112-data-sharing)
- [1.13 Table Types — Permanent vs Transient vs Temporary](#113-table-types--permanent-vs-transient-vs-temporary)
- [1.14 Caching — Three Layers](#114-caching--three-layers)
- [1.15 Resource Monitors](#115-resource-monitors)
- [1.16 ACCOUNT_USAGE vs INFORMATION_SCHEMA](#116-account_usage-vs-information_schema)

### PART 2 — SNOWPIPE & DATA INGESTION
- [2.1 What is Snowpipe?](#21-what-is-snowpipe)
- [2.2 How Snowpipe Works Internally](#22-how-snowpipe-works-internally)
- [2.3 Auto-Ingest](#23-auto-ingest)
- [2.4 COPY INTO vs Snowpipe](#24-copy-into-vs-snowpipe)
- [2.5 Monitoring Snowpipe](#25-monitoring-snowpipe)
- [2.6 What Happens if Snowpipe Fails?](#26-what-happens-if-snowpipe-fails)
- [2.7 Debugging Snowpipe Issues](#27-debugging-snowpipe-issues)
- [2.8 S3 Event Notifications — SNS and SQS](#28-s3-event-notifications--sns-and-sqs)

### PART 3 — SQL QUESTIONS
- [3.1 Second Highest Salary](#31-second-highest-salary)
- [3.2 Top N Records Per Group](#32-top-n-records-per-group)
- [3.3 Remove Duplicates](#33-remove-duplicates)
- [3.4 Find Nth Highest Salary](#34-find-nth-highest-salary)
- [3.5 Window Functions](#35-window-functions)
- [3.6 Running Total](#36-running-total)
- [3.7 Consecutive Records / Streaks](#37-consecutive-records--streaks)
- [3.8 All Join Types Explained](#38-all-join-types-explained)
- [3.9 QUALIFY Clause in Snowflake](#39-qualify-clause-in-snowflake)
- [3.10 PIVOT and UNPIVOT](#310-pivot-and-unpivot)

### PART 4 — PERFORMANCE OPTIMIZATION
- [4.1 Systematic Query Optimization Approach](#41-systematic-query-optimization-approach)
- [4.2 How to Read Query Profile](#42-how-to-read-query-profile)
- [4.3 What Causes Full Table Scan?](#43-what-causes-full-table-scan)
- [4.4 Disk Spillage](#44-disk-spillage)
- [4.5 When to Scale Warehouse?](#45-when-to-scale-warehouse)
- [4.6 How to Reduce Query Cost?](#46-how-to-reduce-query-cost)
- [4.7 Clustering Depth and SYSTEM$CLUSTERING_INFORMATION](#47-clustering-depth-and-systemclustering_information)

### PART 5 — STREAMS, TASKS & CDC
- [5.1 Streams](#51-streams)
- [5.2 Tasks](#52-tasks)
- [5.3 How Streams Work Internally](#53-how-streams-work-internally)
- [5.4 CDC Implementation in Snowflake](#54-cdc-implementation-in-snowflake)
- [5.5 Streams vs Traditional CDC Tools](#55-streams-vs-traditional-cdc-tools)
- [5.6 Building Incremental Pipelines](#56-building-incremental-pipelines)

### PART 6 — SCENARIO-BASED QUESTIONS
- [6.1 Query Taking Too Long](#61-query-taking-too-long)
- [6.2 Snowpipe Stopped Loading](#62-snowpipe-stopped-loading)
- [6.3 Duplicate Data Coming](#63-duplicate-data-coming)
- [6.4 Design Pipeline: S3 → Snowflake](#64-design-pipeline-s3--snowflake)
- [6.5 Handle Late Arriving Data](#65-handle-late-arriving-data)
- [6.6 Handle Schema Changes](#66-handle-schema-changes)
- [6.7 Large Table (TB Scale) Performance Issue](#67-large-table-tb-scale-performance-issue)
- [6.8 Debug Pipeline Failure](#68-debug-pipeline-failure)
- [6.9 Sudden Spike in Snowflake Credits](#69-sudden-spike-in-snowflake-credits)
- [6.10 Design Reusable SQL for Non-Technical Users](#610-design-reusable-sql-for-non-technical-users)

### PART 7 — CLOUD & INTEGRATION
- [7.1 Load Data from S3 to Snowflake](#71-load-data-from-s3-to-snowflake)
- [7.2 External Stage](#72-external-stage)
- [7.3 Storage Integration](#73-storage-integration)
- [7.4 Snowflake Integration with AWS and Azure](#74-snowflake-integration-with-aws-and-azure)
- [7.5 SNS and SQS Explained](#75-sns-and-sqs-explained)
- [7.6 Orchestrating Pipelines — Airflow and ADF](#76-orchestrating-pipelines--airflow-and-adf)

### PART 8 — SECURITY
- [8.1 RBAC in Snowflake](#81-rbac-in-snowflake)
- [8.2 How Roles Work](#82-how-roles-work)
- [8.3 Dynamic Data Masking Policy](#83-dynamic-data-masking-policy)
- [8.4 Row-Level Security (Row Access Policy)](#84-row-level-security-row-access-policy)
- [8.5 Secure Views](#85-secure-views)
- [8.6 How to Share Data Securely](#86-how-to-share-data-securely)
- [8.7 Network Policies](#87-network-policies)
- [8.8 Data Encryption in Snowflake](#88-data-encryption-in-snowflake)

### PART 9 — DATA MODELING
- [9.1 Fact and Dimension Tables](#91-fact-and-dimension-tables)
- [9.2 Star vs Snowflake Schema](#92-star-vs-snowflake-schema)
- [9.3 SCD — Slowly Changing Dimensions](#93-scd--slowly-changing-dimensions)
- [9.4 When to Use Materialized Views](#94-when-to-use-materialized-views)
- [9.5 Normalization vs Denormalization](#95-normalization-vs-denormalization)

### PART 10 — ADVANCED SNOWFLAKE
- [10.1 VARIANT Data Type](#101-variant-data-type)
- [10.2 Querying JSON in Snowflake](#102-querying-json-in-snowflake)
- [10.3 Snowpark](#103-snowpark)
- [10.4 UDFs and Stored Procedures](#104-udfs-and-stored-procedures)
- [10.5 Search Optimization Service](#105-search-optimization-service)
- [10.6 Dynamic Tables](#106-dynamic-tables)
- [10.7 Database Replication and Failover](#107-database-replication-and-failover)
- [10.8 Horizontal vs Vertical Scaling](#108-horizontal-vs-vertical-scaling)

### PART 11 — REAL PROJECT QUESTIONS
- [11.1 Explain Your Project End-to-End](#111-explain-your-project-end-to-end)
- [11.2 Challenges You Faced](#112-challenges-you-faced)
- [11.3 How Did You Optimize Performance?](#113-how-did-you-optimize-performance)
- [11.4 How Did You Handle Data Quality?](#114-how-did-you-handle-data-quality)
- [11.5 How Did You Design the Pipeline?](#115-how-did-you-design-the-pipeline)
- [11.6 How Do You Handle Failures?](#116-how-do-you-handle-failures)

### PART 12 — BEHAVIORAL QUESTIONS
- [12.1 Why Do You Want to Switch?](#121-why-do-you-want-to-switch)
- [12.2 Describe Your Current Role](#122-describe-your-current-role)
- [12.3 Your Contribution in the Project](#123-your-contribution-in-the-project)
- [12.4 Describe a Challenge You Solved](#124-describe-a-challenge-you-solved)

### PART 13 — TRICKY / DIFFERENTIATOR QUESTIONS
- [13.1 Why Snowflake over Redshift/BigQuery?](#131-why-snowflake-over-redshiftbigquery)
- [13.2 When NOT to Use Snowflake?](#132-when-not-to-use-snowflake)
- [13.3 Cost vs Performance Tradeoff](#133-cost-vs-performance-tradeoff)
- [13.4 Explain Your Design Decisions](#134-explain-your-design-decisions)
- [13.5 How Do You Handle Trade-offs in Architecture?](#135-how-do-you-handle-trade-offs-in-architecture)
- [13.6 ETL vs ELT in Snowflake](#136-etl-vs-elt-in-snowflake)

---

---

# PART 1 — CORE SNOWFLAKE

---

## 1.1 Architecture Overview

### Question: Explain Snowflake architecture

Snowflake has a **3-layer architecture** that is cloud-native, separating storage, compute, and coordination into distinct layers.

```
┌─────────────────────────────────────────────────────────────────┐
│                    CLOUD SERVICES LAYER                         │
│   (Authentication · Query Optimization · Metadata · Security)  │
└────────────────────────────┬────────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
  │  WAREHOUSE  │    │  WAREHOUSE  │    │  WAREHOUSE  │
  │   ETL_WH    │    │  REPORT_WH  │    │   ML_WH     │
  │  (Compute)  │    │  (Compute)  │    │  (Compute)  │
  └─────────────┘    └─────────────┘    └─────────────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                             │ (all read same data)
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                   STORAGE LAYER                             │
  │        (S3 / Azure Blob / GCS — Micro-Partitions)          │
  └─────────────────────────────────────────────────────────────┘
```

**Layer 1 — Storage Layer:**
All data is stored as columnar micro-partitions in cloud object storage (S3/Azure Blob/GCS). Storage is completely decoupled from compute. Data is automatically compressed, encrypted, and organized. You pay for storage regardless of whether compute is running.

**Layer 2 — Compute Layer (Virtual Warehouses):**
These are independent clusters of VMs that execute SQL queries. Each warehouse has its own CPU, memory, and local SSD cache. Multiple warehouses can run simultaneously on the same data without any contention or locking. You only pay for compute when the warehouse is running.

**Layer 3 — Cloud Services Layer:**
This is the brain of Snowflake. It handles: authentication and access control, query parsing and optimization, metadata management (min/max stats, partition info), transaction management, and security. It coordinates everything between storage and compute. This layer runs continuously and is billed at a small fraction of total usage.

> **Interview tip:** Emphasize the key insight — storage and compute are completely separated, enabling independent scaling. The Cloud Services layer is what makes multi-warehouse access to the same data possible without any coordination overhead at the data level.

---

## 1.2 Separation of Compute and Storage

### Question: What is separation of compute and storage?

In traditional databases like Redshift (classic) or on-premise systems like Teradata, compute and storage are tightly coupled on the same physical nodes. When you need more storage, you buy more compute nodes, and vice versa. This leads to over-provisioning and wasted resources.

```
TRADITIONAL (Coupled):                 SNOWFLAKE (Decoupled):

  ┌──────────────────┐                 ┌──────────────────┐
  │  Node 1          │                 │  Virtual WH 1    │
  │  CPU + RAM       │                 │  (ETL workload)  │
  │  Storage (disk)  │                 └────────┬─────────┘
  ├──────────────────┤                          │
  │  Node 2          │                 ┌────────┴─────────┐
  │  CPU + RAM       │                 │   SHARED STORAGE  │
  │  Storage (disk)  │                 │  (S3/Azure/GCS)  │
  └──────────────────┘                 └────────┬─────────┘
  Scale both together ↑                         │
                                       ┌────────┴─────────┐
                                       │  Virtual WH 2    │
                                       │  (BI workload)   │
                                       └──────────────────┘
                                       Scale independently ↑
```

**Benefits of separation:**
- Scale compute up/down without touching data
- Multiple warehouses can read the same data simultaneously — no locking
- Suspend compute when not in use — pay only for storage during idle
- Different teams get different warehouse sizes on the same data
- No resource contention between ETL, reporting, and ML workloads
- Warehouses can be created and destroyed in seconds

> **Say in interview:** "We can have an ETL warehouse doing heavy loads while an analytics warehouse serves dashboards — both reading the same tables, zero interference. That's impossible in a coupled architecture."

---

## 1.3 Virtual Warehouses

### Question: What are virtual warehouses?

A virtual warehouse is a cluster of compute resources (CPU, memory, SSD cache) that executes SQL queries in Snowflake. It's an MPP (Massively Parallel Processing) compute cluster that you can start, stop, resize, and multiply on demand.

**Warehouse Sizes:**
```
XS  → 1 server node   → 1 credit/hour
S   → 2 server nodes  → 2 credits/hour
M   → 4 server nodes  → 4 credits/hour
L   → 8 server nodes  → 8 credits/hour
XL  → 16 nodes        → 16 credits/hour
2XL → 32 nodes        → 32 credits/hour
... up to 6XL
```
Each size doubles both the nodes AND the credits consumed per hour. But if it runs twice as fast, the total cost is similar — you just get the result faster.

**Types of Warehouses:**
- **Standard:** General queries, ETL, reporting
- **Multi-cluster:** For high-concurrency — automatically adds/removes clusters (Enterprise edition)
- **Snowpark-optimized:** More memory per node for ML and Python workloads

**Key behaviors:**
- Auto-suspend: warehouse suspends after N seconds of inactivity (billing stops)
- Auto-resume: warehouse wakes up automatically when a query arrives
- Each warehouse maintains its own local SSD cache (warm cache)
- Billing is per-second with a minimum of 60 seconds

> **Say in interview:** "We separate workloads — LOAD_WH for Snowpipe/COPY, TRANSFORM_WH for dbt models, and REPORTING_WH for dashboards. Each is right-sized independently. REPORTING_WH is Small with 60-second auto-suspend because queries are small but frequent. TRANSFORM_WH is Large but only runs during the nightly batch window."

---

## 1.4 Micro-Partitioning

### Question: What is micro-partitioning?

Snowflake automatically divides all table data into **micro-partitions** — contiguous units of storage, each containing 50–500MB of uncompressed data (typically 16MB compressed on disk).

```
TABLE: orders (500GB)
│
├── Micro-partition 1 (16MB compressed)
│   ├── Columns: order_id, customer_id, order_date, amount, ...
│   ├── order_date range: 2023-01-01 to 2023-01-03
│   └── Metadata: min/max per column, null count, distinct count
│
├── Micro-partition 2 (16MB compressed)
│   ├── Columns: order_id, customer_id, order_date, amount, ...
│   ├── order_date range: 2023-01-03 to 2023-01-05
│   └── Metadata: min/max per column, null count, distinct count
│
└── ... (thousands more partitions)
```

**Key characteristics:**
- Partitioning happens **automatically** based on ingestion order — no manual configuration
- Data within each partition is stored **columnar** — each column stored together
- Snowflake records metadata for every column in every partition: min value, max value, number of distinct values, null count
- This metadata is stored in the Cloud Services layer and queried before any data is read
- Micro-partitions are **immutable** — they are never updated in-place, only replaced

**Why it matters:**
- Enables partition pruning (skip partitions that can't match your filter)
- Columnar storage enables compression and fast column scans
- Works with any data type — dates, strings, numbers
- No maintenance required — Snowflake manages it entirely

> **Compared to Hive partitioning:** In Hive, you manually create directory-level partitions like `/data/year=2024/month=01/`. You must specify the partition column upfront. In Snowflake, micro-partitioning is fully automatic and works on every column, not just one designated partition column.

---

## 1.5 Clustering Key

### Question: What is a clustering key?

A clustering key is one or more columns you explicitly define to physically co-locate data with similar values in the same micro-partitions. It's used when the natural ingestion order of data doesn't align with the query filter patterns.

**The problem it solves:**
If data is ingested in time order (natural for streaming), micro-partitions are organized by ingestion time. But if your reports always filter by REGION, every partition contains a mix of all regions — you must scan all partitions even for a single region query.

```
WITHOUT CLUSTERING KEY (data in ingestion order):
Partition 1: regions = [US, EU, APAC, US, EU]  ← must scan for any region
Partition 2: regions = [APAC, US, EU, US, APAC] ← must scan for any region
Partition 3: regions = [EU, APAC, EU, US, EU]   ← must scan for any region

WITH CLUSTERING KEY ON REGION:
Partition 1: regions = [US, US, US, US, US]    ← skip if not US
Partition 2: regions = [EU, EU, EU, EU, EU]    ← skip if not EU
Partition 3: regions = [APAC, APAC, APAC, ...]  ← skip if not APAC
```

**When to use clustering key:**
- Table is very large (multiple TB)
- Queries consistently filter on a specific column (e.g., `WHERE date_key = '2024-01-01'`)
- Query Profile shows partition pruning percentage is poor (high partitions scanned / total)
- The column has high cardinality with range-based filters (dates, region codes)

**When NOT to use:**
- Small/medium tables — natural pruning is sufficient
- Columns with very low cardinality (boolean, 3-value status)
- OLTP-style tables with point lookups — use Search Optimization Service instead
- When the maintenance credit cost exceeds the query savings

**Syntax:**
```sql
-- Define on existing table
ALTER TABLE orders CLUSTER BY (order_date);

-- Check clustering quality
SELECT SYSTEM$CLUSTERING_INFORMATION('orders', '(order_date)');

-- Check automatic clustering history
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;
```

> **Say in interview:** "I analyze query profiles first — if I see low partition pruning percentage, I evaluate clustering. I also check automatic clustering history to confirm the credit cost isn't exceeding the query savings."

---

## 1.6 Partition Pruning

### Question: What is partition pruning?

Partition pruning is Snowflake's ability to **skip micro-partitions entirely** that cannot contain rows matching a query's filter conditions — using the stored min/max metadata per partition.

**How it works step by step:**
```
Query: SELECT * FROM orders WHERE order_date = '2024-03-15'

Cloud Services layer checks metadata:
  Partition 1: order_date min=2024-01-01, max=2024-01-31 → SKIP (March not in range)
  Partition 2: order_date min=2024-02-01, max=2024-02-28 → SKIP (March not in range)
  Partition 3: order_date min=2024-03-01, max=2024-03-31 → SCAN (March in range)
  Partition 4: order_date min=2024-04-01, max=2024-04-30 → SKIP (March not in range)

Result: Only Partition 3 is sent to the warehouse for scanning.
```

**What blocks pruning (important interview topic):**

| Pattern | Problem | Fix |
|---|---|---|
| `YEAR(order_date) = 2024` | Function wraps column — min/max metadata unusable | `order_date BETWEEN '2024-01-01' AND '2024-12-31'` |
| `TO_DATE(created_at) = '2024-01-01'` | Same — function wrap | Use range filter on raw column |
| `WHERE name LIKE '%Smith'` | Leading wildcard — can't prune | Add leading filter if possible |
| Implicit type cast | `WHERE int_col = '123'` | Match data types explicitly |
| OR across columns | Optimizer may not prune | Rewrite as UNION or IN list |

**How to verify pruning is working:**
In Query Profile, click the TableScan node and look at:
- `Partitions scanned: 3`
- `Partitions total: 1,200`
- That's 0.25% — excellent pruning

> **Say in interview:** "A query was scanning 95% of partitions on a 2TB table. I traced it to a `YEAR(order_date) = 2024` filter. I rewrote it as `order_date >= '2024-01-01' AND order_date < '2025-01-01'`. Partition pruning jumped from 5% to 97%. Query time dropped from 4 minutes to 8 seconds."

---

## 1.7 Time Travel

### Question: What is Time Travel?

Time Travel allows you to **access historical data** — data that has been changed or deleted — for a defined retention period. It uses Snowflake's internal data versioning system built on top of the micro-partition immutability.

**Retention periods by edition:**
- Standard edition: 0–1 day (configurable per object)
- Enterprise edition: 0–90 days (configurable per object)

**Syntax options:**
```sql
-- Query data as of a specific timestamp
SELECT * FROM orders
AT (TIMESTAMP => '2024-01-15 10:00:00'::TIMESTAMP);

-- Query data before a specific statement executed
SELECT * FROM orders
BEFORE (STATEMENT => '<query_id_of_bad_update>');

-- Query data N seconds ago
SELECT * FROM orders
AT (OFFSET => -3600);  -- 1 hour ago

-- Clone a table as it was at a point in time
CREATE TABLE orders_backup
CLONE orders AT (TIMESTAMP => '2024-01-15 10:00:00'::TIMESTAMP);

-- Restore a dropped table within Time Travel period
UNDROP TABLE orders;
UNDROP SCHEMA my_schema;
UNDROP DATABASE my_db;
```

**Use cases:**
- Recover accidentally deleted or corrupted data
- Audit trail — what did the table look like last Tuesday?
- Clone a snapshot before a risky operation
- Debugging — compare current data vs yesterday's data

**Storage cost:**
Time Travel data counts toward storage billing. For large tables with high DML volume, keeping 90 days of history can significantly increase storage costs.

**Tip to reduce cost:**
```sql
-- Reduce time travel for large staging tables that don't need recovery
ALTER TABLE staging_events
SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- Transient tables have 0 or 1 day max (no Fail-safe)
CREATE TRANSIENT TABLE staging_data (...);
```

> **Say in interview:** "If a wrong COPY INTO corrupts a production table, I use: `CREATE TABLE orders_backup AS SELECT * FROM orders BEFORE (STATEMENT => 'bad_query_id')` — then truncate and reload from backup. The whole recovery takes 2 minutes, no DBA involvement."

---

## 1.8 Fail-safe

### Question: What is Fail-safe?

Fail-safe is a **7-day data recovery period** that begins automatically after the Time Travel retention period expires. It is a last-resort disaster recovery mechanism managed entirely by Snowflake — not accessible by users directly.

```
Data written → [TIME TRAVEL period: 0–90 days] → [FAIL-SAFE: 7 days] → Deleted forever
                     ↑ user can access                ↑ Snowflake Support only
```

**Key points:**
- Duration: Always exactly 7 days — not configurable
- Access: You **cannot** query or restore Fail-safe data yourself — you must contact Snowflake Support and they recover it for you
- Purpose: Protection against catastrophic failures, system errors, accidental data loss beyond Time Travel window
- Cost: Storage during Fail-safe period is billed to your account like normal storage
- Scope: Only permanent tables have 7-day Fail-safe. Transient tables have 0-day. Temporary tables have 0-day.

**Fail-safe vs Time Travel:**

| Feature | Time Travel | Fail-safe |
|---|---|---|
| Duration | 0–90 days (configurable) | 7 days (fixed) |
| Access | Self-service via SQL | Snowflake Support only |
| Purpose | Operational recovery | Disaster recovery |
| Cost control | Reduce retention to save | Cannot be avoided for permanent tables |

**Cost optimization tip:**
```sql
-- Use transient tables for staging/intermediate data
-- No Fail-safe = lower storage cost (no 7-day tail)
CREATE TRANSIENT TABLE raw_staging (...);
CREATE TRANSIENT SCHEMA staging_schema;
CREATE TRANSIENT DATABASE staging_db;
```

> **Say in interview:** "We use Transient tables for all staging and intermediate data — they have no Fail-safe storage overhead. Only production gold/mart tables use permanent tables where the 7-day Fail-safe protection is genuinely valuable."

---

## 1.9 Zero-Copy Cloning

### Question: What is Zero-copy cloning?

Zero-copy cloning creates a **copy of a database, schema, or table instantly, without duplicating the underlying data**. Both the original and the clone point to the same micro-partitions in storage. No data is physically copied — it's metadata-only.

```
BEFORE CLONE:
  Production DB ─────────── Micro-partitions [A][B][C][D][E]

AFTER CLONE:
  Production DB ─────────── Micro-partitions [A][B][C][D][E]
  Dev DB (clone) ──────────┘  (same physical data, shared)

AFTER CHANGES IN CLONE:
  Production DB ─────────── Micro-partitions [A][B][C][D][E]
  Dev DB (clone) ──────────── [A'][B][C][D][E]
                                ↑ new partition written (copy-on-write)
                                   B,C,D,E still shared
```

**How copy-on-write works:**
When data in the clone or original changes, only the **changed micro-partitions** are written as new partitions. The unchanged ones remain shared. Storage cost grows only as data diverges.

**Clone any object:**
```sql
-- Clone an entire database (instant, even for TB-scale)
CREATE DATABASE dev_db CLONE prod_db;

-- Clone a schema
CREATE SCHEMA dev_schema CLONE prod_db.prod_schema;

-- Clone a table
CREATE TABLE orders_backup CLONE orders;

-- Clone a table as it was at a specific time
CREATE TABLE orders_jan_snapshot
CLONE orders AT (TIMESTAMP => '2024-01-31 23:59:59'::TIMESTAMP);
```

**Use cases:**
- Instant dev/test environments: `CREATE DATABASE dev CLONE prod`
- Test schema changes before applying to prod
- Create backup snapshots before risky migrations
- CI/CD pipelines: each PR gets its own cloned database for testing
- Data science: clone prod data for experimentation without impacting production

> **Say in interview:** "In our CI/CD pipeline, every pull request triggers a Snowflake clone of production via Terraform. Our dbt tests run against the clone, and it's destroyed after the PR merges. Full production-fidelity testing with no storage cost beyond the changed rows, and zero risk to production."

---

## 1.10 Stages — Internal vs External

### Question: What are stages? Explain internal vs external.

A stage is a **named Snowflake object** pointing to a file storage location used for loading data into and unloading data out of Snowflake.

```
Data Files                    Stage                     Snowflake Table
(CSV/JSON/Parquet)  ──PUT──►  @stage  ──COPY INTO──►   MY_TABLE
                   ◄──GET──                ◄──COPY INTO──  (unload)
```

**Internal Stages (storage managed by Snowflake):**

| Type | Reference | Notes |
|---|---|---|
| User stage | `@~` | Every user gets one automatically. Personal use only. |
| Table stage | `@%table_name` | Each table has one automatically. Tied to that table. |
| Named internal stage | `@my_stage` | Explicitly created. Shareable across users/processes. |

```sql
-- Create a named internal stage
CREATE STAGE my_internal_stage;

-- Upload file from local machine
PUT file:///local/path/data.csv @my_internal_stage;

-- List files
LIST @my_internal_stage;

-- Load from stage
COPY INTO orders FROM @my_internal_stage/data.csv;
```

**External Stages (storage in your cloud account):**
```sql
-- Create external stage pointing to S3 (with Storage Integration)
CREATE STAGE s3_orders_stage
  URL = 's3://my-company-bucket/orders/'
  STORAGE_INTEGRATION = my_s3_integration
  FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

-- Load from external stage
COPY INTO orders FROM @s3_orders_stage
  PATTERN = '.*orders_2024.*\.csv';

-- List files in external stage
LIST @s3_orders_stage;
```

> **Say in interview:** "We use external stages pointing to our S3 data lake. The Storage Integration uses an IAM role — no access keys stored in Snowflake, which is the secure recommended approach. Named internal stages are used for ad-hoc loads when a developer uploads a one-off file."

---

## 1.11 File Formats

### Question: What are file formats?

A file format is a **named Snowflake object** that describes the structure and parsing options of files being loaded or unloaded. It encapsulates all parsing configuration so it doesn't need to be repeated in every COPY command.

**Supported types:** CSV, JSON, Avro, ORC, Parquet, XML

**Creating a CSV file format:**
```sql
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', 'N/A', '')
  EMPTY_FIELD_AS_NULL = TRUE
  DATE_FORMAT = 'YYYY-MM-DD'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
  COMPRESSION = AUTO;
```

**Creating a JSON file format:**
```sql
CREATE OR REPLACE FILE FORMAT my_json_format
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE     -- removes outer [ ] from array of records
  STRIP_NULL_VALUES = FALSE
  COMPRESSION = AUTO;
```

**Using in COPY INTO:**
```sql
COPY INTO orders
FROM @s3_orders_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
ON_ERROR = 'CONTINUE';
```

**Using inline (without named format):**
```sql
COPY INTO orders
FROM @s3_stage
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 1);
```

> **Say in interview:** "I create a separate file format per data source since different vendors send different CSV dialects — different delimiters, quote characters, null representations. Centralizing it as a named object means one change fixes all pipelines using that format, and it's version-controlled in our Terraform."

---

## 1.12 Data Sharing

### Question: What is Snowflake data sharing?

Data Sharing allows you to share **live, read-only access** to your data with other Snowflake accounts — without copying or moving any data. The consumer directly reads your micro-partitions with full real-time access.

```
PROVIDER ACCOUNT                        CONSUMER ACCOUNT
┌─────────────────────────┐             ┌─────────────────────────┐
│  prod_db.public         │             │  shared_db              │
│    secure_view          │──── SHARE ──│    (read-only view of   │
│    (filters sensitive   │             │     provider's data)    │
│     columns/rows)       │             │                         │
│                         │             │  Consumer pays for      │
│  Provider pays for      │             │  their compute          │
│  storage                │             └─────────────────────────┘
└─────────────────────────┘
         │
   Same physical
   micro-partitions
   (no data copy)
```

**Setting up data sharing:**
```sql
-- PROVIDER SIDE
-- Step 1: Create a share
CREATE SHARE customer_analytics_share;

-- Step 2: Grant access to objects
GRANT USAGE ON DATABASE prod_db TO SHARE customer_analytics_share;
GRANT USAGE ON SCHEMA prod_db.public TO SHARE customer_analytics_share;
GRANT SELECT ON VIEW prod_db.public.customer_secure_view TO SHARE customer_analytics_share;

-- Step 3: Add consumer account
ALTER SHARE customer_analytics_share
  ADD ACCOUNTS = partner_account_identifier;

-- CONSUMER SIDE
-- Create a database from the share
CREATE DATABASE shared_customer_data
  FROM SHARE provider_account.customer_analytics_share;

-- Query it like any database
SELECT * FROM shared_customer_data.public.customer_secure_view;
```

**Types of sharing:**
- **Direct share:** Account-to-account within the same cloud region
- **Data Exchange:** Private marketplace for a defined group of organizations
- **Snowflake Marketplace:** Public data products (weather data, financial data, demographic data, etc.)

> **Say in interview:** "We share processed financial data with our external auditors via secure views — they see only the columns and rows scoped to the audit period. No data movement, always current data, and we revoke access instantly when the audit is done."

---

## 1.13 Table Types — Permanent vs Transient vs Temporary

### Question: What is the difference between permanent, transient, and temporary tables?

| Feature | Permanent | Transient | Temporary |
|---|---|---|---|
| Time Travel | 0–90 days | 0–1 day max | 0–1 day max |
| Fail-safe | 7 days | 0 days | 0 days |
| Storage cost | Highest (data + TT + FS) | Medium (data + up to 1 day TT) | Session only |
| Persistence | Until explicitly dropped | Until explicitly dropped | Session ends = table dropped |
| Visible to | All users | All users | Creating session only |

**Use cases by type:**

```sql
-- PERMANENT: Production tables where full recovery is critical
CREATE TABLE fact_sales (...);  -- default is permanent

-- TRANSIENT: Staging, intermediate, scratch tables
CREATE TRANSIENT TABLE stg_raw_orders (...);
CREATE TRANSIENT TABLE temp_deduped (...);

-- TEMPORARY: Session-scoped CTEs or intermediate results
-- Exists only in your session, auto-dropped on disconnect
CREATE TEMPORARY TABLE my_session_work (...);
```

**Cost impact example:**
Imagine a 1TB staging table with 50GB/day of new data:
- Permanent with 90-day retention: You pay for ~4.5TB of Time Travel + 0.35TB of Fail-safe = ~5TB extra storage
- Transient with 1-day retention: ~50GB of Time Travel, no Fail-safe = ~50GB extra storage
- Savings: Dramatic for large, high-churn staging tables

> **Say in interview:** "Our architecture rule: every table in the raw/staging layer is TRANSIENT. Every table in the mart/gold layer is PERMANENT. This alone cut our storage bill by 30% because we had TB-scale staging tables holding 90 days of unnecessary Time Travel data."

---

## 1.14 Caching — Three Layers

### Question: Explain how caching works in Snowflake

Snowflake has three distinct caching layers, each serving a different purpose. Understanding all three is important for performance optimization.

```
Query arrives
     │
     ▼
┌─────────────────────────────────┐
│  1. METADATA CACHE               │ ← Cloud Services layer
│  (COUNT(*), MIN, MAX without    │   Always on, free
│   scanning actual data)         │   No warehouse needed
└────────────────┬────────────────┘
                 │ (if not metadata-only)
                 ▼
┌─────────────────────────────────┐
│  2. RESULT CACHE                 │ ← Cloud Services layer
│  (Exact same query within       │   24-hour window
│   24 hours, data unchanged)     │   Free, no warehouse needed
└────────────────┬────────────────┘
                 │ (if not cached)
                 ▼
┌─────────────────────────────────┐
│  3. WAREHOUSE (LOCAL SSD) CACHE  │ ← Compute layer
│  (Recently scanned micro-       │   Per-warehouse
│   partitions cached on SSD)     │   Cleared on suspend
└────────────────┬────────────────┘
                 │ (if not in SSD cache)
                 ▼
         Cloud Storage (S3/GCS/Azure)
         (Full read from remote — slowest)
```

**Layer 1 — Metadata Cache:**
- Answers queries that only need table metadata — no actual data scanning required
- Examples: `SELECT COUNT(*) FROM orders`, `SELECT MAX(order_date) FROM orders`
- Returns instantly even with the warehouse suspended
- Always active, cannot be disabled

**Layer 2 — Result Cache:**
- Stores the complete result of every executed query for 24 hours
- If the exact same query is run again and the underlying data hasn't changed — result is returned instantly, zero compute cost
- Requirements: Exact query text match (including whitespace), same database/schema context, no non-deterministic functions (`CURRENT_TIMESTAMP()`, `RANDOM()`, `UUID()`), underlying data unchanged
- Can be disabled: `ALTER SESSION SET USE_CACHED_RESULT = FALSE`
- Shared across all users on the same account

**Layer 3 — Warehouse (Local SSD) Cache:**
- When a warehouse scans micro-partitions from storage, it caches them on local SSD
- Subsequent queries that need the same partitions on the same warehouse read from SSD instead of cloud storage — much faster
- Cache is per-warehouse and per-cluster
- **Cleared when the warehouse is suspended** — this is why aggressive auto-suspend can hurt repeated workloads
- Best strategy: Keep warehouses running slightly longer (5–10 min suspend) for BI dashboards where the same data is queried repeatedly

**Checking cache effectiveness:**
```sql
-- Check percentage of data served from warehouse SSD cache
SELECT
    warehouse_name,
    COUNT(*) AS query_count,
    SUM(bytes_scanned) AS total_bytes_scanned,
    SUM(bytes_scanned * percentage_scanned_from_cache) AS bytes_from_cache,
    ROUND(SUM(bytes_scanned * percentage_scanned_from_cache) /
          NULLIF(SUM(bytes_scanned), 0) * 100, 2) AS pct_from_cache
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY total_bytes_scanned DESC;
```

> **Say in interview:** "The result cache is the most powerful and most often overlooked optimization. A BI dashboard with 100 users running the same daily sales query at 9am — only the first run costs compute. All 99 others hit the result cache. I make sure our BI tool doesn't append session timestamps to queries, which would bust the cache."

---

## 1.15 Resource Monitors

### Question: What are resource monitors?

Resource monitors are Snowflake objects that **track and control credit consumption** by virtual warehouses. They prevent budget overruns by triggering notifications or suspending warehouses when usage thresholds are hit.

```sql
-- Create a resource monitor for a team budget
CREATE OR REPLACE RESOURCE MONITOR analytics_team_monitor
  WITH CREDIT_QUOTA = 500         -- 500 credits per month
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY        -- alert when 75% used
    ON 90 PERCENT DO NOTIFY        -- alert when 90% used
    ON 100 PERCENT DO SUSPEND      -- suspend warehouse at 100%
    ON 110 PERCENT DO SUSPEND_IMMEDIATE;  -- hard stop at 110%

-- Assign to a warehouse
ALTER WAREHOUSE analytics_wh
  SET RESOURCE_MONITOR = analytics_team_monitor;
```

**Two levels of resource monitors:**
- **Account-level:** Monitors total credit consumption across the entire Snowflake account
- **Warehouse-level:** Monitors credits used by a specific warehouse or set of warehouses

**Actions available:**
- `NOTIFY`: Send email notification to account admins
- `SUSPEND`: Allow running queries to finish, then suspend warehouse
- `SUSPEND_IMMEDIATE`: Immediately cancel all queries and suspend warehouse

**Checking resource monitor status:**
```sql
SHOW RESOURCE MONITORS;
```

> **Say in interview:** "We assign resource monitors to every team's warehouse with a monthly credit budget. At 80% they get a Slack alert (we built a Lambda that polls the API). At 100% the warehouse suspends automatically. No finance surprises at month end."

---

## 1.16 ACCOUNT_USAGE vs INFORMATION_SCHEMA

### Question: What is the difference between ACCOUNT_USAGE and INFORMATION_SCHEMA?

Both provide metadata and monitoring data, but they serve different purposes and have different trade-offs.

| Feature | ACCOUNT_USAGE | INFORMATION_SCHEMA |
|---|---|---|
| Data latency | 45 minutes to 3 hours | Real-time / near-real-time |
| Data retention | Up to 1 year (365 days) | 7–14 days depending on view |
| Scope | Entire account history | Current database / session |
| Dropped objects | Included | Not included (only active) |
| Performance | Slower (more data) | Faster (less data) |
| Access required | ACCOUNTADMIN or SNOWFLAKE db granted | Any user with object access |

**Common ACCOUNT_USAGE views (for monitoring and governance):**
```sql
-- Credit consumption by warehouse
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY CREDITS_USED DESC;

-- Most expensive queries in last 7 days
SELECT QUERY_ID, QUERY_TEXT, WAREHOUSE_NAME, EXECUTION_TIME,
       BYTES_SCANNED, CREDITS_USED_CLOUD_SERVICES
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY EXECUTION_TIME DESC
LIMIT 20;

-- Login history and failed attempts
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE IS_SUCCESS = 'NO'
ORDER BY EVENT_TIMESTAMP DESC;

-- Access history (who queried what)
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE QUERY_START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP());
```

**Common INFORMATION_SCHEMA views (for real-time checks):**
```sql
-- Real-time COPY history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'ORDERS',
  START_TIME => DATEADD('hours', -1, CURRENT_TIMESTAMP())
));

-- Task execution history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('hour', -6, CURRENT_TIMESTAMP())
));

-- Pipe status
SELECT SYSTEM$PIPE_STATUS('my_pipe');
```

> **Say in interview:** "For operational monitoring — pipeline failures, running task status — I use INFORMATION_SCHEMA because it's real-time. For cost analysis, optimization decisions, and monthly reporting — I use ACCOUNT_USAGE because I need historical depth and the ability to see dropped objects."

---

---

# PART 2 — SNOWPIPE & DATA INGESTION

---

## 2.1 What is Snowpipe?

### Question: What is Snowpipe?

Snowpipe is Snowflake's **continuous, serverless data ingestion service** that automatically loads small files into Snowflake as soon as they arrive in a stage — without needing a running virtual warehouse.

**Key characteristics:**
- **Serverless:** Snowflake manages the underlying compute. You are billed per second of processing time — not by warehouse credits
- **Near real-time:** Files are typically loaded within 30–120 seconds of arriving in the stage
- **Event-driven:** Triggered by cloud event notifications (S3 → SQS → Snowpipe) or REST API calls
- **Designed for small files:** Optimal for many small files arriving continuously, not large batch files
- **14-day deduplication:** Same file path won't be loaded twice within 14 days

> **Say in interview:** "When a Kafka connector drops JSON micro-batches to S3 every 30 seconds, SQS notifies Snowpipe, and data is in Snowflake within 60 seconds — without any warehouse running or costing credits between files."

---

## 2.2 How Snowpipe Works Internally

### Question: How does Snowpipe work internally?

```
S3 Bucket
    │
    │ (file uploaded)
    │
    ▼
S3 Event Notification ──► SQS Queue (Snowflake-managed)
                                │
                                │ (Snowpipe polls queue continuously)
                                ▼
                         Snowpipe Service
                         (serverless compute)
                                │
                                │ (micro-batch COPY INTO)
                                ▼
                         Target Snowflake Table
                                │
                                │ (success/failure recorded)
                                ▼
                    INFORMATION_SCHEMA.COPY_HISTORY
```

**Step-by-step flow:**
1. File lands in S3 (or Azure Blob / GCS)
2. S3 Event Notification sends an "ObjectCreated" message to an SQS queue (Snowflake creates and manages this SQS queue)
3. Snowflake's Snowpipe service continuously polls the SQS queue
4. For each file notification, Snowpipe triggers a micro-batch COPY INTO operation
5. Data is loaded into the target Snowflake table
6. Load status (success/failure, rows loaded, errors) is recorded in COPY_HISTORY

**Important internals:**
- Snowpipe maintains a load history for **14 days** to prevent duplicate loads. If the same file path arrives again within 14 days, it is silently skipped.
- Uses **Snowflake-managed serverless compute** — not your virtual warehouse
- Processes files in micro-batches of ~100–500MB for efficiency
- Multiple files can be processed in parallel

> **Say in interview:** "The 14-day deduplication is critical to understand. If a file is accidentally PUT to S3 twice with the same path, Snowpipe loads it only once. After 14 days, it would reload if PUT again — so our file naming convention always includes a timestamp to ensure uniqueness."

---

## 2.3 Auto-Ingest

### Question: What is auto-ingest?

Auto-ingest is the mechanism where Snowpipe **automatically triggers** when a file arrives in a cloud stage, using cloud event notifications — without requiring manual API calls.

**Setup for AWS S3:**
```sql
-- Step 1: Create pipe with AUTO_INGEST = TRUE
CREATE PIPE orders_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO raw_orders
  FROM @s3_orders_stage
  FILE_FORMAT = (FORMAT_NAME = 'my_json_format');

-- Step 2: Get the SQS ARN that Snowflake created
SHOW PIPES LIKE 'orders_pipe';
-- Look for the notification_channel column — this is the SQS ARN

-- Step 3: Go to AWS Console → S3 bucket → Properties → Event Notifications
-- Create event notification:
--   Events: s3:ObjectCreated:*
--   Prefix: orders/    (optional, to filter path)
--   Suffix: .json      (optional, to filter extension)
--   Destination: SQS → paste the ARN from SHOW PIPES
```

**Setup for Azure Blob:**
Uses Azure Event Grid + Azure Storage Queue instead of SQS.

**Without auto-ingest (REST API approach):**
```python
# Call Snowpipe REST API to trigger load manually
import requests

url = f"https://{account}.snowflakecomputing.com/v1/data/pipes/{pipe_name}/insertFiles"
payload = {"files": [{"path": "orders/2024/01/15/data.json"}]}
response = requests.post(url, json=payload, headers=auth_headers)
```

This is used when you control file ingestion in application code and want to trigger Snowpipe programmatically.

> **Say in interview:** "The SQS queue acts as a reliable buffer between S3 and Snowpipe. Even if Snowpipe is temporarily down during a Snowflake maintenance window, S3 events queue up in SQS and are processed in order when Snowpipe resumes — no file notifications are lost."

---

## 2.4 COPY INTO vs Snowpipe

### Question: What is the difference between COPY INTO and Snowpipe?

| Factor | COPY INTO | Snowpipe |
|---|---|---|
| **Trigger** | Manual execution or scheduled Task | Event-driven (SQS) or REST API |
| **Latency** | Minutes (warehouse startup + execution) | Seconds to ~2 minutes |
| **Compute** | Your virtual warehouse (credits/hour) | Snowflake serverless (per-second billing) |
| **Cost model** | Warehouse credits | ~0.06 credits per 1000 files processed |
| **Best for** | Large batch files (GB+) | Small, frequent files (KB–MB) |
| **Deduplication** | 64-day file history | 14-day file history |
| **Error handling** | ON_ERROR options | ON_ERROR = CONTINUE default |
| **Monitoring** | COPY_HISTORY view | COPY_HISTORY + PIPE_STATUS |
| **Parallelism** | Controlled by warehouse size | Managed by Snowflake automatically |

**When to use which:**
- Use **COPY INTO** for: end-of-day full loads, large historical backfills, one-time migrations, files that are GB in size each
- Use **Snowpipe** for: streaming micro-batches from Kafka, real-time event ingestion, IoT data, any scenario where data freshness within minutes matters

**Cost comparison for 1TB of data:**
- COPY INTO with Large warehouse: ~1 hour run = 8 credits = ~$24
- Snowpipe on 1TB in many small files: serverless billing is typically slightly higher per GB but provides near-real-time freshness

> **Say in interview:** "We use COPY INTO for our end-of-day reconciliation loads from the ERP — it dumps a 5GB file at midnight. Snowpipe handles our real-time Kafka event stream that produces thousands of small files per hour. Right tool for each use case."

---

## 2.5 Monitoring Snowpipe

### Question: How do you monitor Snowpipe?

**1. Check pipe execution status (real-time):**
```sql
SELECT SYSTEM$PIPE_STATUS('orders_pipe');
-- Returns JSON:
-- {
--   "executionState": "RUNNING",
--   "pendingFileCount": 3,
--   "lastIngestedTimestamp": "2024-01-15T10:30:00Z",
--   "lastIngestedFileList": ["orders/data_001.json"]
-- }
```

**2. Check copy history for load results:**
```sql
SELECT
    file_name,
    status,
    rows_loaded,
    rows_parsed,
    error_count,
    first_error_message,
    last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_ORDERS',
    START_TIME => DATEADD('hours', -6, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;
```

**3. Check pipe usage and credit consumption:**
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
    PIPE_NAME => 'ORDERS_PIPE'
));
```

**4. Historical analysis (up to 365 days) via ACCOUNT_USAGE:**
```sql
SELECT pipe_name, credits_used, files_inserted, bytes_inserted
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;
```

**5. Build an automated alerting task:**
```sql
CREATE TASK monitor_snowpipe_failures
  WAREHOUSE = MONITORING_WH
  SCHEDULE = '5 MINUTE'
AS
  CALL alert_on_pipe_failure();  -- stored procedure that checks and alerts via SNS
```

> **Say in interview:** "I built a monitoring stored procedure that runs every 5 minutes as a Snowflake Task. It queries COPY_HISTORY for STATUS != 'LOADED', counts the failures, and calls AWS SNS if failures exceed a threshold. We catch pipeline issues within 5 minutes."

---

## 2.6 What Happens if Snowpipe Fails?

### Question: What happens if Snowpipe fails?

**Scenario 1 — File parsing errors (partial failure):**
- By default Snowpipe uses `ON_ERROR = CONTINUE` — it skips malformed rows and loads valid ones
- Error details captured in COPY_HISTORY: ERROR_COUNT, FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE
- You can change to `ON_ERROR = ABORT_STATEMENT` to fail the entire file on any error

**Scenario 2 — Snowflake infrastructure errors (transient failure):**
- Snowpipe will **automatically retry** the failed load
- The SQS message remains in the queue until Snowpipe successfully acknowledges it
- If the SQS message visibility timeout expires (default 4 days) before load — the message is moved to the Dead Letter Queue (DLQ) if configured, or permanently lost

**Scenario 3 — File deleted from S3 before load:**
- Snowpipe attempts to read the file and fails with a file not found error
- Load fails permanently — file is gone
- Prevention: Never delete files from S3 until they appear in COPY_HISTORY with STATUS = 'LOADED'

**Scenario 4 — Pipe is paused:**
- SQS messages continue accumulating in the queue
- When pipe is resumed, it processes the backlog
- After 14 days in the queue, SQS messages may expire depending on the queue's message retention setting

**Recovery steps:**
```sql
-- Resume a paused pipe
ALTER PIPE my_pipe SET PIPE_EXECUTION_PAUSED = FALSE;

-- Refresh a pipe to re-scan the stage for files that were missed
ALTER PIPE my_pipe REFRESH;

-- Refresh for a specific prefix (date folder)
ALTER PIPE my_pipe REFRESH PREFIX = '2024/01/15/';

-- Manual fallback: load missed files with COPY INTO
COPY INTO raw_orders
FROM @s3_orders_stage/2024/01/15/
FILE_FORMAT = (FORMAT_NAME = 'my_json_format')
ON_ERROR = CONTINUE;
```

> **Say in interview:** "Our production rule: S3 files are retained for 30 days in the hot tier before moving to Glacier. This gives us a 30-day window to retry any failed Snowpipe loads manually with COPY INTO. We never delete immediately after upload."

---

## 2.7 Debugging Snowpipe Issues

### Question: How do you debug Snowpipe issues?

**Systematic debugging checklist:**

**Step 1 — Verify pipe is running:**
```sql
SELECT SYSTEM$PIPE_STATUS('my_pipe');
-- Check: "executionState" should be "RUNNING"
-- If "STOPPED" or "PAUSED": ALTER PIPE my_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
```

**Step 2 — Check recent load history for errors:**
```sql
SELECT file_name, status, error_count, first_error_message, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MY_TABLE',
    START_TIME => DATEADD('hours', -6, CURRENT_TIMESTAMP())))
WHERE STATUS != 'LOADED'
ORDER BY LAST_LOAD_TIME DESC;
```

**Step 3 — Validate a specific file manually:**
```sql
COPY INTO my_table
FROM @my_stage/path/to/problematic_file.json
FILE_FORMAT = (FORMAT_NAME = 'my_json_format')
VALIDATION_MODE = 'RETURN_ERRORS';
-- Returns the exact rows that would fail with error details
```

**Step 4 — Check SQS queue in AWS Console:**
- Navigate to SQS in AWS Console
- Find the queue whose ARN matches your pipe's `notification_channel`
- Check: Are new messages arriving? Are messages stuck? Is there a Dead Letter Queue with unprocessed messages?

**Step 5 — Check S3 event notification configuration:**
- Navigate to S3 bucket → Properties → Event Notifications
- Verify: The event type includes "s3:ObjectCreated:*"
- Verify: The destination SQS ARN matches the one in SHOW PIPES
- Test: Manually upload a test file and check if the SQS message count increases

**Step 6 — Check storage integration permissions:**
```sql
DESC INTEGRATION my_s3_integration;
-- Check STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Verify these still match the trust policy in the IAM role
```

**Common root causes and fixes:**

| Symptom | Root Cause | Fix |
|---|---|---|
| Pipe STOPPED | Pipe was paused | `ALTER PIPE SET PIPE_EXECUTION_PAUSED = FALSE` |
| No messages in SQS | S3 event notification not configured | Set up ObjectCreated event in S3 |
| Files in SQS but not loading | Storage integration IAM role changed | Update IAM role trust policy |
| LOAD_FAILED in COPY_HISTORY | Schema mismatch or bad data | Use VALIDATION_MODE to find errors |
| Old files not loaded | Files existed before pipe was created | Use `ALTER PIPE REFRESH` |

---

## 2.8 S3 Event Notifications — SNS and SQS

### Question: What are S3 event notifications? Explain SNS and SQS.

**SNS (Simple Notification Service):**
A pub/sub messaging service where one published message can fan out to multiple different subscribers simultaneously.

```
S3 Bucket → SNS Topic → ┬── SQS Queue 1 (Snowpipe)
                         ├── Lambda Function (data validation)
                         ├── SQS Queue 2 (another consumer)
                         └── Email Notification
```

Used when multiple systems need to react to the same S3 file upload event.

**SQS (Simple Queue Service):**
A message queue where messages are stored durably until a consumer reads and deletes them. Guarantees at-least-once delivery.

```
S3 Bucket → SQS Queue → Snowpipe reads + deletes message
                ↑
         Messages wait here if Snowpipe is busy
         (up to message retention period: default 4 days)
```

For Snowpipe specifically:
- When `AUTO_INGEST = TRUE`, Snowflake creates an SQS queue in its own AWS account
- You configure S3 to send "ObjectCreated" events to this SQS queue's ARN
- Snowpipe continuously polls this queue for new file notifications
- After successfully loading a file, Snowpipe deletes the SQS message
- If loading fails, the message becomes visible again for retry after the visibility timeout

> **Say in interview:** "SQS is the reliability layer between S3 and Snowpipe. If Snowflake has a maintenance window for 30 minutes, S3 still sends events to SQS, they sit in the queue, and when Snowpipe comes back up it processes the backlog in order. Zero data loss."

---

---

# PART 3 — SQL QUESTIONS

---

## 3.1 Second Highest Salary

### Question: Write a query to find the second highest salary

**Method 1 — Subquery (simple, universally understood):**
```sql
SELECT MAX(salary) AS second_highest_salary
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);
```

**Method 2 — DENSE_RANK (recommended for interviews — handles ties):**
```sql
SELECT salary AS second_highest_salary
FROM (
    SELECT salary,
           DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employees
) ranked
WHERE rnk = 2;
```

**Method 3 — OFFSET (clean, readable):**
```sql
SELECT DISTINCT salary AS second_highest_salary
FROM employees
ORDER BY salary DESC
LIMIT 1 OFFSET 1;
```

**When there's no second highest (NULL handling):**
```sql
SELECT IFNULL(
    (SELECT DISTINCT salary FROM employees ORDER BY salary DESC LIMIT 1 OFFSET 1),
    NULL
) AS second_highest_salary;
```

> **Always use DENSE_RANK in interviews** — it handles ties correctly and generalizes to Nth highest. Know the difference: RANK skips numbers on ties (1,1,3), DENSE_RANK doesn't (1,1,2).

---

## 3.2 Top N Records Per Group

### Question: Write a query to get top 3 salaries per department

**Using ROW_NUMBER (one row per rank, no ties):**
```sql
SELECT department, employee_name, salary
FROM (
    SELECT
        department,
        employee_name,
        salary,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
    FROM employees
) ranked
WHERE rn <= 3;
```

**Using DENSE_RANK (include ties):**
```sql
SELECT department, employee_name, salary
FROM (
    SELECT
        department,
        employee_name,
        salary,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dr
    FROM employees
) ranked
WHERE dr <= 3;
```

**Snowflake QUALIFY shortcut (no subquery needed):**
```sql
SELECT department, employee_name, salary
FROM employees
QUALIFY ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) <= 3;
```

> **Follow-up question from interviewers:** "What if two employees tie for 3rd place?" — ROW_NUMBER would arbitrarily include only one of them. DENSE_RANK would include both. The right choice depends on business requirements.

---

## 3.3 Remove Duplicates

### Question: How do you remove duplicate rows in Snowflake?

**Step 1 — Identify duplicates:**
```sql
SELECT order_id, COUNT(*) AS duplicate_count
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```

**Method 1 — SELECT DISTINCT (simple but loses control):**
```sql
SELECT DISTINCT * FROM orders;
```

**Method 2 — ROW_NUMBER to keep most recent:**
```sql
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY updated_at DESC  -- keep the most recent version
           ) AS rn
    FROM orders
)
WHERE rn = 1;
```

**Method 3 — QUALIFY (Snowflake-specific, cleanest):**
```sql
SELECT * FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;
```

**Method 4 — Deduplicate in-place (recreate table):**
```sql
CREATE OR REPLACE TABLE orders AS
SELECT * FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;
```

> **Say in interview:** "In Snowflake I prefer the QUALIFY approach — it's cleaner than a subquery, avoids an extra level of nesting, and is a Snowflake-specific feature that shows you know the platform."

---

## 3.4 Find Nth Highest Salary

### Question: Find the Nth highest salary (generalized)

```sql
-- Replace N with any number (2nd, 3rd, 4th...)
SELECT salary AS nth_highest_salary
FROM (
    SELECT salary,
           DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employees
)
WHERE rnk = N;  -- e.g., N=3 for 3rd highest

-- Snowflake QUALIFY version:
SELECT DISTINCT salary
FROM employees
QUALIFY DENSE_RANK() OVER (ORDER BY salary DESC) = 3;
```

**Why DENSE_RANK over ROW_NUMBER:**
- If two employees have salary 100,000 (tied for 2nd), ROW_NUMBER would rank one as 2nd and one as 3rd — the 3rd highest would then be 3rd place which is actually tied for 2nd. This is misleading.
- DENSE_RANK correctly identifies both as rank 2, and the true 3rd distinct salary gets rank 3.

---

## 3.5 Window Functions

### Question: Explain window functions — ROW_NUMBER, RANK, DENSE_RANK

**Comparison with same data:**
```
Salaries: 100,000 | 100,000 | 85,000 | 70,000

ROW_NUMBER:  1 | 2 | 3 | 4  (always unique, ties get arbitrary order)
RANK:        1 | 1 | 3 | 4  (ties get same rank, next rank SKIPPED)
DENSE_RANK:  1 | 1 | 2 | 3  (ties get same rank, next rank NOT skipped)
```

**Syntax and examples:**
```sql
SELECT
    employee_name,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num,
    RANK()       OVER (PARTITION BY department ORDER BY salary DESC) AS rnk,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rnk,
    LAG(salary, 1)  OVER (PARTITION BY department ORDER BY hire_date) AS prev_salary,
    LEAD(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) AS next_salary,
    NTILE(4)        OVER (ORDER BY salary DESC) AS salary_quartile,
    PERCENT_RANK()  OVER (ORDER BY salary) AS pct_rank,
    CUME_DIST()     OVER (ORDER BY salary) AS cumulative_dist
FROM employees;
```

**Window frame specification:**
```sql
-- Running total using explicit frame
SUM(sales) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)

-- 7-day moving average
AVG(daily_revenue) OVER (
    ORDER BY report_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)

-- Compare each row to first row in group
FIRST_VALUE(salary) OVER (
    PARTITION BY department
    ORDER BY hire_date
    ROWS UNBOUNDED PRECEDING
) AS founding_member_salary
```

---

## 3.6 Running Total

### Question: Write a query for a running total

**Basic running total:**
```sql
SELECT
    order_date,
    amount,
    SUM(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM orders
ORDER BY order_date;
```

**Running total partitioned by customer:**
```sql
SELECT
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS customer_running_total
FROM orders;
```

**Running total with reset (per month):**
```sql
SELECT
    order_date,
    amount,
    DATE_TRUNC('month', order_date) AS month,
    SUM(amount) OVER (
        PARTITION BY DATE_TRUNC('month', order_date)
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS monthly_running_total
FROM orders;
```

**ROWS vs RANGE difference:**
```sql
-- ROWS: Physical row positions — current row is only that one row
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

-- RANGE: Logical value range — all rows with same ORDER BY value are included together
-- This matters when there are ties in the ORDER BY column
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

---

## 3.7 Consecutive Records / Streaks

### Question: Find users with login streaks of 3+ consecutive days

**The classic gaps-and-islands technique:**
```sql
-- Step 1: Subtract row_number from date to get a "group key"
-- Consecutive dates will have the same (date - row_number) value
WITH date_groups AS (
    SELECT
        user_id,
        login_date,
        DATEADD('day',
            -ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date),
            login_date
        ) AS streak_group
    FROM user_logins
    WHERE login_date IS NOT NULL
    GROUP BY user_id, login_date  -- one row per user per day
),

-- Step 2: Group by user + streak_group to find streaks
streaks AS (
    SELECT
        user_id,
        streak_group,
        MIN(login_date) AS streak_start,
        MAX(login_date) AS streak_end,
        COUNT(*) AS streak_length
    FROM date_groups
    GROUP BY user_id, streak_group
)

-- Step 3: Filter for desired streak length
SELECT user_id, streak_start, streak_end, streak_length
FROM streaks
WHERE streak_length >= 3
ORDER BY streak_length DESC;
```

**Why it works:**
If login dates are Jan 1, Jan 2, Jan 3 — row numbers are 1, 2, 3.
`Jan 1 - 1 = Dec 31`, `Jan 2 - 2 = Dec 31`, `Jan 3 - 3 = Dec 31` → all the same group key.
If there's a gap (Jan 1, Jan 2, Jan 5): `Jan 5 - 4 = Jan 1` → different group key. Gap detected.

---

## 3.8 All Join Types Explained

### Question: Explain all types of JOINs with examples

```sql
-- Sample tables:
-- employees: (id, name, dept_id)
-- departments: (id, dept_name)

-- INNER JOIN: Only rows with match in both tables
SELECT e.name, d.dept_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id;

-- LEFT JOIN: All employees + their department (NULL if no dept)
SELECT e.name, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id;

-- RIGHT JOIN: All departments + their employees (NULL if no employees)
SELECT e.name, d.dept_name
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id;

-- FULL OUTER JOIN: All rows from both, NULLs where no match
SELECT e.name, d.dept_name
FROM employees e
FULL OUTER JOIN departments d ON e.dept_id = d.id;

-- CROSS JOIN: Every employee paired with every department (n × m rows)
SELECT e.name, d.dept_name
FROM employees e
CROSS JOIN departments d;

-- SELF JOIN: Find employees and their managers (same table)
SELECT e.name AS employee, m.name AS manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id;

-- SEMI JOIN (EXISTS): Employees who have at least one order
SELECT * FROM employees e
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.employee_id = e.id);
-- Note: doesn't return duplicate rows even if multiple orders per employee

-- ANTI JOIN (NOT EXISTS): Employees with no orders
SELECT * FROM employees e
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.employee_id = e.id);
```

**Key distinction — SEMI vs INNER JOIN:**
- INNER JOIN can produce duplicate rows when one employee has multiple matching orders
- SEMI JOIN (EXISTS) returns each employee at most once — only checks existence

---

## 3.9 QUALIFY Clause in Snowflake

### Question: What is the QUALIFY clause in Snowflake?

QUALIFY is a **Snowflake-specific SQL clause** that filters the results of window functions, similar to how HAVING filters aggregate functions. It eliminates the need for an outer subquery just to filter on a window function result.

```sql
-- Without QUALIFY (requires subquery):
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
    FROM employees
) WHERE rn = 1;

-- With QUALIFY (cleaner, Snowflake-specific):
SELECT *
FROM employees
QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1;

-- More examples:
-- Top salary per department
SELECT * FROM employees
QUALIFY RANK() OVER (PARTITION BY department ORDER BY salary DESC) = 1;

-- Latest order per customer
SELECT * FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1;

-- Deduplicate keeping most recent
SELECT * FROM raw_events
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY processed_at DESC) = 1;
```

> **Mention QUALIFY in any deduplication or ranking question.** It's a Snowflake-specific feature that signals you know the platform well. Not available in standard SQL or most other databases.

---

## 3.10 PIVOT and UNPIVOT

### Question: How do you use PIVOT and UNPIVOT in Snowflake?

**PIVOT — Transform rows into columns:**
```sql
-- Source data:
-- | region | quarter | revenue |
-- | US     | Q1      | 100     |
-- | US     | Q2      | 150     |
-- | EU     | Q1      | 80      |

SELECT *
FROM quarterly_revenue
PIVOT (
    SUM(revenue)
    FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4')
) AS p;

-- Result:
-- | region | Q1  | Q2  | Q3   | Q4   |
-- | US     | 100 | 150 | NULL | NULL |
-- | EU     | 80  | NULL| NULL | NULL |
```

**UNPIVOT — Transform columns into rows:**
```sql
-- Source: one row per region with quarter columns
-- | region | Q1 | Q2 | Q3 | Q4 |

SELECT region, quarter, revenue
FROM quarterly_revenue
UNPIVOT (
    revenue FOR quarter IN (Q1, Q2, Q3, Q4)
) AS unpvt;

-- Result:
-- | region | quarter | revenue |
-- | US     | Q1      | 100     |
-- | US     | Q2      | 150     |
```

---

---

# PART 4 — PERFORMANCE OPTIMIZATION

---

## 4.1 Systematic Query Optimization Approach

### Question: How do you optimize query performance in Snowflake?

**My systematic 6-step approach:**

**Step 1 — Profile before optimizing (never guess):**
Open the Query Profile in Snowflake UI. Identify the most expensive node. Look at: partitions scanned, bytes spilled, join types, row counts at each step.

**Step 2 — Fix partition pruning first (highest ROI):**
```sql
-- BAD: Function on column prevents pruning
WHERE YEAR(order_date) = 2024
WHERE TO_DATE(created_at) = '2024-01-01'

-- GOOD: Range filter allows min/max metadata to work
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
WHERE created_at >= '2024-01-01' AND created_at < '2024-01-02'
```

**Step 3 — Fix joins (second most common issue):**
```sql
-- Filter BEFORE joining to reduce rows in the join
WITH filtered_orders AS (
    SELECT * FROM orders WHERE order_date >= '2024-01-01'  -- filter first
),
filtered_customers AS (
    SELECT * FROM customers WHERE country = 'India'  -- filter first
)
SELECT f.*, c.customer_name
FROM filtered_orders f
JOIN filtered_customers c ON f.customer_id = c.id;
-- Now the join operates on a much smaller dataset
```

**Step 4 — Fix warehouse sizing (if spill exists):**
```sql
-- Check for spill in query profile or history
SELECT query_id, bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE bytes_spilled_to_remote_storage > 0
AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY bytes_spilled_to_remote_storage DESC;
```
If remote spill exists → scale up warehouse.

**Step 5 — Add clustering key if needed:**
Only after confirming pruning is still poor even with correct filter syntax. Check clustering information:
```sql
SELECT SYSTEM$CLUSTERING_INFORMATION('orders', '(order_date)');
-- Look for "average_depth": < 2 is excellent, > 4 may need attention
```

**Step 6 — Leverage caching:**
- Ensure identical query text across BI tool sessions (no dynamic timestamps)
- For repeated heavy aggregations → materialized views or dynamic tables

---

## 4.2 How to Read Query Profile

### Question: How do you read a Query Profile?

Access: Snowflake UI → Activity → Query History → click any query → Query Profile tab.

**Key things to look at:**

**1. The execution tree (left panel):**
```
TableScan (orders)          ← where data enters
     │
     ▼
Filter                      ← WHERE clause applied
     │
     ▼
HashJoin                    ← joining with another table
     │
     ▼
Aggregate                   ← GROUP BY / aggregation
     │
     ▼
Sort                        ← ORDER BY
     │
     ▼
Result                      ← sent back to client
```

The **widest/most expensive node** (shown by node size or percentage) is where to focus.

**2. TableScan node details:**
- `Partitions scanned: 12 / 1,200` → 1% scanned = excellent pruning
- `Partitions scanned: 1,200 / 1,200` → 100% scanned = full table scan, investigate filter
- `Bytes scanned: 2.3 TB` → very large scan, check if pruning is possible

**3. Spillage indicators:**
- `Bytes spilled to local storage: 4.2 GB` → exceeded memory, scale up warehouse
- `Bytes spilled to remote storage: 12 GB` → severe — immediately scale up, this is 10–100x slower than memory

**4. Join type:**
- `Hash Join` → standard and expected for large table joins
- `Cartesian Join` unexpectedly → missing join condition! Check your ON clause.
- `NLJ (Nested Loop Join)` → acceptable for small tables, problematic for large ones

**5. Row count explosions:**
```
Filter: 10M rows → 10M rows   (expected)
HashJoin: 10M rows → 150M rows  (unexpected explosion = bad join condition / fan-out)
```
If output rows >> input rows from a join, check for missing join conditions or non-unique keys.

**6. Operator statistics panel (right panel):**
Shows: input/output rows, bytes consumed, execution time, memory used for selected node.

---

## 4.3 What Causes Full Table Scan?

### Question: What causes a full table scan in Snowflake?

A full table scan means Snowflake is reading all (or nearly all) micro-partitions of a table. This is visible in Query Profile as `Partitions scanned ≈ Partitions total`.

**Cause 1 — Function wrapping the filter column:**
```sql
-- CAUSES FULL SCAN (function prevents min/max metadata use):
WHERE YEAR(sale_date) = 2024
WHERE DATE_TRUNC('month', event_time) = '2024-01-01'
WHERE LOWER(status) = 'active'

-- FIX:
WHERE sale_date >= '2024-01-01' AND sale_date < '2025-01-01'
WHERE event_time >= '2024-01-01' AND event_time < '2024-02-01'
WHERE status = 'ACTIVE'  -- store data in consistent case
```

**Cause 2 — No WHERE clause:**
```sql
SELECT SUM(revenue) FROM fact_sales;  -- scans all data
```

**Cause 3 — Leading wildcard in LIKE:**
```sql
WHERE customer_name LIKE '%smith%'  -- can't use min/max, must scan everything
-- Use Search Optimization Service for substring searches instead
```

**Cause 4 — Implicit type conversion:**
```sql
WHERE numeric_id = '12345'  -- string vs number comparison causes casting
-- Fix: match data types
WHERE numeric_id = 12345
```

**Cause 5 — OR conditions across multiple columns:**
```sql
WHERE order_id = 100 OR customer_id = 200  -- optimizer may not prune effectively
-- Fix: use UNION ALL
SELECT * FROM orders WHERE order_id = 100
UNION ALL
SELECT * FROM orders WHERE customer_id = 200 AND order_id != 100
```

**Cause 6 — Table not clustered on filter column (for large tables):**
Natural ingestion order doesn't match query patterns. Fix: add clustering key on the frequently filtered column.

---

## 4.4 Disk Spillage

### Question: What is disk spillage and how do you fix it?

Disk spillage occurs when a virtual warehouse **runs out of in-memory space** during query execution and must write intermediate data to disk (first local SSD, then remote cloud storage if SSD is also full).

```
Query execution memory:
  RAM (fastest)         ← query tries here first
    │ (full)
    ▼
  Local SSD cache       ← spills here if RAM full
  (warehouse nodes)       "Bytes spilled to local storage"
    │ (full)
    ▼
  Remote storage (S3)   ← spills here if SSD also full
  (much slower)           "Bytes spilled to remote storage"
```

**What causes spillage:**
- Large hash joins (joining two large tables)
- Wide aggregations on large datasets
- Large DISTINCT operations
- Warehouse is too small for the data volume being processed

**How to fix:**

**Fix 1 — Scale up the warehouse:**
```sql
-- Temporarily scale up for heavy queries
ALTER WAREHOUSE my_wh SET WAREHOUSE_SIZE = 'X-LARGE';
-- Run the query
ALTER WAREHOUSE my_wh SET WAREHOUSE_SIZE = 'MEDIUM';  -- scale back down
```

**Fix 2 — Break the query into steps:**
```sql
-- Instead of one massive query:
CREATE TEMPORARY TABLE step1_filtered AS
SELECT customer_id, SUM(amount) AS total
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY customer_id;  -- intermediate result is smaller

-- Then join the smaller result:
SELECT c.*, s.total
FROM customers c
JOIN step1_filtered s ON c.id = s.customer_id;
```

**Fix 3 — Push filters down before joining:**
Filter each table to the minimum required rows before performing the join. This reduces the size of the hash table in memory.

---

## 4.5 When to Scale Warehouse?

### Question: When should you scale up vs scale out a warehouse?

**Scale UP (larger warehouse size) when:**
- Disk spillage is visible in Query Profile
- Complex queries with large joins, large window functions, or big sort operations
- Heavy batch transformation runs (dbt model rebuild, end-of-month reports)
- Single query needs more memory to complete efficiently

```sql
-- Scale up for a known heavy batch job
ALTER WAREHOUSE transform_wh SET WAREHOUSE_SIZE = 'X-LARGE';
CALL run_monthly_aggregation();  -- heavy stored procedure
ALTER WAREHOUSE transform_wh SET WAREHOUSE_SIZE = 'MEDIUM';
```

**Scale OUT (multi-cluster warehouse) when:**
- Multiple users are running queries simultaneously and experiencing queue time
- Concurrency is the bottleneck, not individual query complexity
- Workload has predictable business-hours peaks

```sql
-- Configure multi-cluster for BI dashboard workloads
ALTER WAREHOUSE analytics_wh
  SET MIN_CLUSTER_COUNT = 1
      MAX_CLUSTER_COUNT = 5
      SCALING_POLICY = 'ECONOMY';  -- or STANDARD for more aggressive scale-out
```

**Cost insight:**
A 2x larger warehouse costs 2x credits per hour but often runs the same query 2x faster. Total cost is similar — you just get the result faster (less wall-clock time). The benefit is user experience and pipeline speed, not necessarily cost.

**Right-sizing rule of thumb:**
- Start with Medium for most workloads
- Scale up if: queries take >5 minutes on Medium and you see spill
- Scale down if: queries finish in <10 seconds on Large (over-provisioned)

---

## 4.6 How to Reduce Query Cost?

### Question: How do you reduce Snowflake query/compute costs?

**1. Auto-suspend aggressively:**
```sql
ALTER WAREHOUSE analytics_wh SET AUTO_SUSPEND = 60;     -- 60 seconds for ad-hoc
ALTER WAREHOUSE etl_wh       SET AUTO_SUSPEND = 300;    -- 5 minutes for ETL
ALTER WAREHOUSE reporting_wh SET AUTO_SUSPEND = 120;    -- 2 minutes for BI
```

**2. Leverage result cache (free queries):**
Ensure BI tool queries don't include dynamic session variables or timestamps that bust the cache. Same exact query text within 24 hours = $0 compute.

**3. Right-size warehouses — don't over-provision:**
```sql
-- Identify overprovisioned queries (finish in <5 seconds but on Large warehouse)
SELECT warehouse_name, warehouse_size, AVG(execution_time/1000) AS avg_seconds
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY avg_seconds;
```

**4. Use transient tables for scratch/staging:**
```sql
CREATE TRANSIENT TABLE stg_daily_load (...);
-- No Fail-safe = lower storage cost
```

**5. Reduce Time Travel on large high-churn tables:**
```sql
ALTER TABLE fact_events SET DATA_RETENTION_TIME_IN_DAYS = 7;  -- was 90 days
-- Saves weeks of compressed historical storage
```

**6. Monitor top credit consumers weekly:**
```sql
SELECT
    query_text,
    warehouse_name,
    execution_time / 60000 AS execution_minutes,
    credits_used_cloud_services,
    bytes_scanned / POWER(1024, 3) AS gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY execution_time DESC
LIMIT 20;
```

**7. Use resource monitors to prevent surprise bills:**
Set monthly credit quotas per warehouse/team with auto-suspend at 100%.

---

## 4.7 Clustering Depth and SYSTEM$CLUSTERING_INFORMATION

### Question: How do you check if a table needs a clustering key?

```sql
-- Check clustering information for a table
SELECT SYSTEM$CLUSTERING_INFORMATION('my_database.my_schema.orders', '(order_date)');
```

**Output interpretation:**
```json
{
  "cluster_count": 4,
  "row_count": 500000000,
  "average_overlaps": 342.8,
  "average_depth": 24.3,
  "partition_depth_histogram": {
    "00000": 0,
    "00001": 0,
    "00002": 12,
    "00003": 48,
    ...
  }
}
```

**Key metrics:**
- `average_depth`: Average number of micro-partitions per distinct key value. Lower is better.
  - Depth < 2: Excellent clustering, no action needed
  - Depth 2–4: Good, marginal benefit from clustering key
  - Depth > 4: Poor clustering on this column, clustering key could help significantly
- `average_overlaps`: How many other partitions each partition overlaps with. Lower is better.

**Check automatic clustering credit consumption:**
```sql
SELECT
    table_name,
    SUM(credits_used) AS total_credits,
    SUM(num_bytes_reclustered) / POWER(1024, 3) AS gb_reclustered
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY total_credits DESC;
```

If the credits consumed by automatic clustering exceed the query savings you're getting from pruning, it may not be worth keeping the clustering key.

---

---

# PART 5 — STREAMS, TASKS & CDC

---

## 5.1 Streams

### Question: What are Streams in Snowflake?

A Stream is a Snowflake object that **tracks all DML changes** (INSERT, UPDATE, DELETE) to a table since the last time the stream was consumed. It's Snowflake's native Change Data Capture (CDC) mechanism.

**How to create and use streams:**
```sql
-- Create a standard stream (captures INSERT, UPDATE, DELETE)
CREATE STREAM orders_stream ON TABLE raw_orders;

-- Create an append-only stream (only captures INSERTs — cheaper)
CREATE STREAM orders_stream_append ON TABLE raw_orders
  APPEND_ONLY = TRUE;

-- Query the stream to see pending changes
SELECT * FROM orders_stream;
-- Returns changed rows plus metadata columns:
--   METADATA$ACTION      = 'INSERT' or 'DELETE'
--   METADATA$ISUPDATE    = TRUE/FALSE (TRUE means this DELETE/INSERT pair is an UPDATE)
--   METADATA$ROW_ID      = unique internal row identifier
```

**How UPDATEs appear in a standard stream:**
An UPDATE is represented as two rows:
- One DELETE row with the OLD values
- One INSERT row with the NEW values
- Both rows have `METADATA$ISUPDATE = TRUE`

```sql
-- Consume only inserts from stream:
INSERT INTO target_table
SELECT col1, col2, col3
FROM orders_stream
WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE;

-- Consume only true deletes:
DELETE FROM target_table
WHERE id IN (
    SELECT id FROM orders_stream
    WHERE METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = FALSE
);
```

**Stream types:**

| Type | Captures | Use Case |
|---|---|---|
| Standard | INSERT, UPDATE, DELETE | Full CDC on dimension tables |
| Append-only | INSERT only | Faster, cheaper for log/event tables |
| Insert-only | INSERT only (external tables) | Ingesting from external table stages |

**Important stream expiry:**
A stream stores an offset pointer into Snowflake's transaction log. This offset relies on Time Travel. If the stream is not consumed before the table's Time Travel retention period expires — the stream becomes stale and must be recreated.

---

## 5.2 Tasks

### Question: What are Tasks in Snowflake?

A Task is a Snowflake object that **executes a single SQL statement or stored procedure call** on a defined schedule or when triggered by a parent task in a dependency chain (DAG).

```sql
-- Create a basic scheduled task
CREATE OR REPLACE TASK refresh_daily_summary
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- every day at 6am UTC
AS
  INSERT INTO daily_summary
  SELECT DATE_TRUNC('day', order_date), SUM(amount)
  FROM orders
  WHERE order_date = CURRENT_DATE - 1
  GROUP BY 1;

-- IMPORTANT: Tasks are created in SUSPENDED state by default
-- You must explicitly resume them
ALTER TASK refresh_daily_summary RESUME;

-- Suspend a task
ALTER TASK refresh_daily_summary SUSPEND;

-- Manually trigger a task (for testing)
EXECUTE TASK refresh_daily_summary;
```

**Task with stream condition (only run if there's data):**
```sql
CREATE OR REPLACE TASK process_new_orders
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')  -- skip if stream is empty
AS
  MERGE INTO clean_orders c
  USING orders_stream s ON c.order_id = s.order_id
  WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
  WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
    INSERT (order_id, customer_id, amount) VALUES (s.order_id, s.customer_id, s.amount);
```

**Task DAG (dependency chain):**
```sql
-- Parent task
CREATE TASK task_extract SCHEDULE = '0 2 * * *' AS ...;

-- Child tasks (run after parent completes successfully)
CREATE TASK task_transform AFTER task_extract AS ...;
CREATE TASK task_load      AFTER task_transform AS ...;
CREATE TASK task_validate  AFTER task_load AS ...;

-- Resume in REVERSE order (children first, then parent)
ALTER TASK task_validate  RESUME;
ALTER TASK task_load      RESUME;
ALTER TASK task_transform RESUME;
ALTER TASK task_extract   RESUME;  -- parent last
```

**Monitor task history:**
```sql
SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    TASK_NAME => 'PROCESS_NEW_ORDERS'
))
ORDER BY scheduled_time DESC;
```

---

## 5.3 How Streams Work Internally

### Question: How do Streams work internally?

A stream doesn't physically copy changed data. Instead, it stores a **transaction offset** — a pointer into Snowflake's immutable micro-partition history.

```
Time ────────────────────────────────────────────────────────►

Table state:  [v1] ─► [v2] ─► [v3] ─► [v4] ─► [v5 = current]
                                ↑
                           Stream offset
                      (stream sees v4, v5 as "new")

When stream is consumed (DML commits successfully):
Table state:  [v1] ─► [v2] ─► [v3] ─► [v4] ─► [v5 = current]
                                                      ↑
                                               Offset advances to v5
                                          (v4 → v5 changes are consumed)
```

**Key internal behaviors:**
1. Stream stores an offset, not a copy of data. Very lightweight.
2. When you query a stream, Snowflake computes the diff between the offset version and current using Time Travel.
3. **Offset only advances when a DML using the stream commits in a transaction.** If you SELECT from the stream but don't INSERT/MERGE/DELETE using it, the offset stays.
4. If no DML commits for so long that the offset falls outside the table's Time Travel window — the stream becomes stale. You can't recover it; you must recreate it.

**Transactional consumption:**
```sql
-- This pattern ensures stream is only consumed if the MERGE succeeds
BEGIN TRANSACTION;
    MERGE INTO target ...
    USING orders_stream ...;
COMMIT;  -- stream offset advances only on commit
-- If ROLLBACK is called, stream offset stays at previous position
```

---

## 5.4 CDC Implementation in Snowflake

### Question: How do you implement CDC in Snowflake?

**Pattern 1 — Append-only (new records only):**
```sql
-- Stream
CREATE STREAM raw_events_stream ON TABLE raw_events APPEND_ONLY = TRUE;

-- Task
CREATE TASK load_new_events
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = '2 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw_events_stream')
AS
  INSERT INTO clean_events
  SELECT event_id, user_id, event_type, event_time
  FROM raw_events_stream;
```

**Pattern 2 — Full CDC with MERGE (INSERT/UPDATE/DELETE):**
```sql
CREATE TASK full_cdc_orders
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
  MERGE INTO clean_orders tgt
  USING (
      SELECT order_id, customer_id, amount, status, updated_at,
             METADATA$ACTION, METADATA$ISUPDATE
      FROM orders_stream
  ) src
  ON tgt.order_id = src.order_id
  WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' AND NOT src.METADATA$ISUPDATE THEN
      DELETE
  WHEN MATCHED AND src.METADATA$ACTION = 'INSERT' AND src.METADATA$ISUPDATE THEN
      UPDATE SET tgt.customer_id = src.customer_id,
                 tgt.amount = src.amount,
                 tgt.status = src.status,
                 tgt.updated_at = src.updated_at
  WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
      INSERT (order_id, customer_id, amount, status, updated_at)
      VALUES (src.order_id, src.customer_id, src.amount, src.status, src.updated_at);
```

**Pattern 3 — SCD Type 2 using streams:**
```sql
CREATE TASK scd2_customers
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = '10 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('customers_stream')
AS
  -- Expire old versions of changed records
  UPDATE dim_customer
  SET eff_end_date = CURRENT_DATE,
      is_current = FALSE
  WHERE customer_id IN (
      SELECT customer_id FROM customers_stream
      WHERE METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = TRUE
  )
  AND is_current = TRUE;

-- Then insert new versions (separate statement or in a stored procedure)
```

---

## 5.5 Streams vs Traditional CDC Tools

### Question: How does Snowflake Streams compare to traditional CDC tools?

| Aspect | Streams (native) | Traditional CDC (Debezium/Kafka) |
|---|---|---|
| Setup complexity | Low — just `CREATE STREAM` | High — connector config, Kafka cluster, schema registry |
| Infrastructure | Zero — fully managed | Kafka cluster, connector infrastructure to maintain |
| Latency | Seconds to minutes | Sub-second (true real-time) |
| Source types | Only Snowflake tables | Any database — Oracle, MySQL, Postgres, SQL Server |
| Cost | Minimal stream overhead | Kafka and connector infrastructure costs |
| Data movement | No movement (in-Snowflake) | Data travels through Kafka |
| Replayability | Via Time Travel offset | Via Kafka topic retention |

**When to use Streams:**
- Snowflake-to-Snowflake CDC (most common use case)
- Building incremental refresh pipelines within Snowflake
- When operational simplicity is a priority

**When traditional CDC is still needed:**
- Source is not Snowflake (Oracle, MySQL, Postgres → Snowflake)
- True sub-second latency required
- Data must fan out to multiple consumers beyond Snowflake

---

## 5.6 Building Incremental Pipelines

### Question: How do you build incremental pipelines in Snowflake?

**Full architecture — 3-layer incremental pipeline:**

```
Raw Layer (full fidelity, append-only)
    │
    │ [Stream 1: append-only]
    ▼
Cleaned Layer (validated, typed, deduplicated)
    │
    │ [Stream 2: standard, captures updates]
    ▼
Mart Layer (business-ready aggregations)
    ↑
    └── Task DAG drives all three layers
```

**Implementation:**
```sql
-- Layer 1: Landing (Snowpipe loads here)
CREATE TABLE raw_orders (raw VARIANT, _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP());

-- Stream on raw
CREATE STREAM raw_orders_stream ON TABLE raw_orders APPEND_ONLY = TRUE;

-- Layer 2: Cleaned table
CREATE TABLE clean_orders (
    order_id STRING PRIMARY KEY,
    customer_id STRING,
    amount FLOAT,
    order_date DATE,
    _processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Stream on cleaned
CREATE STREAM clean_orders_stream ON TABLE clean_orders;

-- Task: raw → clean
CREATE TASK raw_to_clean
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw_orders_stream')
AS
  INSERT INTO clean_orders
  SELECT
      raw:order_id::STRING,
      raw:customer_id::STRING,
      raw:amount::FLOAT,
      raw:order_date::DATE
  FROM raw_orders_stream
  QUALIFY ROW_NUMBER() OVER (PARTITION BY raw:order_id ORDER BY _loaded_at DESC) = 1;

-- Task: clean → mart (runs after clean task)
CREATE TASK clean_to_mart
  AFTER raw_to_clean
  WAREHOUSE = TRANSFORM_WH
  WHEN SYSTEM$STREAM_HAS_DATA('clean_orders_stream')
AS
  MERGE INTO mart_daily_orders m
  USING (
      SELECT DATE_TRUNC('day', order_date) AS day, SUM(amount) AS revenue, COUNT(*) AS orders
      FROM clean_orders_stream
      GROUP BY 1
  ) s ON m.day = s.day
  WHEN MATCHED THEN UPDATE SET m.revenue = m.revenue + s.revenue, m.orders = m.orders + s.orders
  WHEN NOT MATCHED THEN INSERT (day, revenue, orders) VALUES (s.day, s.revenue, s.orders);

-- Resume in order
ALTER TASK clean_to_mart RESUME;
ALTER TASK raw_to_clean  RESUME;
```

---

---

# PART 6 — SCENARIO-BASED QUESTIONS

---

## 6.1 Query Taking Too Long

### Scenario: A query is taking too long. How will you optimize it?

**Systematic approach (what interviewers want to hear):**

**Step 1 — Open Query Profile (data first, never guess):**
- What is the most expensive node?
- What percentage of partitions are being scanned?
- Is there any disk spillage?
- Is there an unexpected Cartesian join or row explosion?

**Step 2 — Fix partition pruning:**
```sql
-- Check: are most partitions being scanned?
-- If yes, look at the WHERE clause

-- Common fix: remove function wrappers
-- BAD: WHERE YEAR(sale_date) = 2024
-- GOOD: WHERE sale_date >= '2024-01-01' AND sale_date < '2025-01-01'
```

**Step 3 — Fix joins:**
```sql
-- Filter BEFORE the join, not after
-- BAD:
SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id
WHERE o.region = 'India';

-- GOOD: Push filter into CTE before joining
WITH india_orders AS (
    SELECT * FROM orders WHERE region = 'India'
)
SELECT * FROM india_orders o JOIN customers c ON o.cust_id = c.id;
```

**Step 4 — Address disk spillage:**
- Scale up warehouse temporarily
- Break the query into intermediate steps using CREATE TEMPORARY TABLE

**Step 5 — Consider clustering key if large table with poor pruning:**
```sql
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_sales', '(sale_date)');
-- If average_depth > 4, clustering may help significantly
```

**Step 6 — Leverage existing cache behavior:**
- Check if result cache can be used (same query repeated)
- For recurring aggregations → materialized view or dynamic table

---

## 6.2 Snowpipe Stopped Loading

### Scenario: Snowpipe stopped loading data. What will you do?

```
Investigation flowchart:

Check SYSTEM$PIPE_STATUS
          │
    ┌─────┴─────┐
  RUNNING    STOPPED/PAUSED
    │              │
    │         Resume pipe: ALTER PIPE SET PIPE_EXECUTION_PAUSED = FALSE
    │
Check COPY_HISTORY for errors
          │
    ┌─────┴──────┐
  ERRORS     NO RECENT LOADS
    │              │
Check file     Check SQS queue in AWS
(VALIDATION    Is it receiving messages?
 MODE)              │
              ┌─────┴─────┐
           YES (messages) NO messages
              │                │
         Storage integration   Check S3 event notification
         permissions OK?       Is it configured correctly?
```

**Full debug commands:**
```sql
-- Step 1
SELECT SYSTEM$PIPE_STATUS('my_pipe');

-- Step 2
SELECT file_name, status, error_count, first_error_message
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MY_TABLE',
    START_TIME => DATEADD('hours', -6, CURRENT_TIMESTAMP())))
WHERE STATUS != 'LOADED';

-- Step 3: Validate a specific file
COPY INTO my_table FROM @my_stage/suspect_file.csv
VALIDATION_MODE = 'RETURN_ERRORS';

-- Step 4: Refresh pipe for missed files
ALTER PIPE my_pipe REFRESH PREFIX = '2024/01/15/';

-- Step 5: Manual recovery
COPY INTO my_table FROM @my_stage/missed_file.csv;
```

**Common root causes and fixes:**

| Symptom | Root Cause | Fix |
|---|---|---|
| Pipe PAUSED | Set accidentally | `ALTER PIPE SET PIPE_EXECUTION_PAUSED = FALSE` |
| No SQS messages | S3 event notification missing | Configure S3 ObjectCreated → SQS |
| SQS messages but no loads | IAM role trust changed | Update IAM role trust policy |
| LOAD_FAILED | Schema mismatch / bad data | Fix file or schema, use VALIDATION_MODE |
| Old files not loading | Files existed before pipe creation | `ALTER PIPE REFRESH` |

---

## 6.3 Duplicate Data Coming

### Scenario: Duplicate data is appearing in your tables. How do you handle it?

**Prevent → Detect → Remediate:**

**Prevention (design phase):**
```sql
-- Use MERGE instead of INSERT (idempotent upsert)
MERGE INTO clean_orders tgt
USING new_orders src ON tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET tgt.amount = src.amount
WHEN NOT MATCHED THEN INSERT (order_id, amount) VALUES (src.order_id, src.amount);

-- File-level dedup: Snowpipe's 14-day dedup prevents file-level duplicates
-- Design pipelines to be idempotent — running twice = same result as once
```

**Detection:**
```sql
-- Find duplicates
SELECT order_id, COUNT(*) AS dup_count
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Quantify scope
SELECT COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_row_count
FROM orders;
```

**Remediation:**
```sql
-- Option 1: QUALIFY (cleanest in Snowflake)
CREATE OR REPLACE TABLE orders AS
SELECT * FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;

-- Option 2: CREATE new table + rename
CREATE TABLE orders_deduped AS
SELECT * FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;

ALTER TABLE orders RENAME TO orders_old;
ALTER TABLE orders_deduped RENAME TO orders;
-- Verify then drop orders_old
```

**Root cause investigation:**
- Was COPY INTO run twice without file tracking?
- Did a Task retry cause a double-process of a stream that already committed?
- Is there a fan-out in a JOIN producing extra rows (check row count explosion in Query Profile)?
- Is Snowpipe's 14-day window expired for files that were reloaded?

---

## 6.4 Design Pipeline: S3 → Snowflake

### Scenario: Design a production-grade S3 → Snowflake pipeline

**Architecture:**
```
S3 Bucket
    │
    │ (ObjectCreated event)
    ▼
SQS Queue (Snowflake-managed)
    │
    ▼
Snowpipe (serverless)
    │
    │ COPY INTO
    ▼
RAW / LANDING TABLE (VARIANT column, append-only)
  _loaded_at TIMESTAMP
  _source_file VARCHAR
    │
    │ (append-only Stream)
    ▼
Snowflake Task (every 5 min)
    │
    │ INSERT + QUALIFY dedup
    ▼
CLEAN TABLE (typed columns, deduplicated)
    │
    │ (standard Stream)
    ▼
Snowflake Task (runs after clean task)
    │
    │ MERGE
    ▼
MART TABLE (aggregations, business-ready)
    │
    ▼
BI Tool / Databricks / API
```

**Key components:**

```sql
-- 1. Storage Integration (IAM role, no access keys)
CREATE STORAGE INTEGRATION prod_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-company-bucket/');

-- 2. External Stage
CREATE STAGE prod_s3_stage
  URL = 's3://my-company-bucket/orders/'
  STORAGE_INTEGRATION = prod_s3_integration
  FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE);

-- 3. Raw landing table
CREATE TABLE raw_orders (
    raw           VARIANT,
    _loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source_file  VARCHAR
);

-- 4. Snowpipe
CREATE PIPE orders_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO raw_orders (raw, _source_file)
  FROM (SELECT $1, METADATA$FILENAME FROM @prod_s3_stage)
  FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE)
  ON_ERROR = CONTINUE;

ALTER PIPE orders_pipe RESUME;
```

**Best practices built in:**
- Never load directly to production table — raw → curated
- Keep raw table indefinitely for reprocessing capability
- Include `_source_file` column for debugging which file caused issues
- Use VARIANT for raw to survive schema changes in source
- ON_ERROR = CONTINUE with monitoring so bad records don't block valid records

---

## 6.5 Handle Late Arriving Data

### Scenario: How do you handle late-arriving data in your pipelines?

**The core principle:** Always use `event_time` (when the event actually happened) not `load_time` (when it arrived in Snowflake) for all business logic and aggregations.

**Strategy 1 — Rolling reprocess window:**
```sql
-- Rebuild aggregations for last 7 days on every run
-- This handles data arriving up to 7 days late

CREATE TASK rebuild_daily_aggregations
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = 'USING CRON 0 3 * * * UTC'  -- 3am daily
AS
  -- Delete last 7 days of aggregations
  DELETE FROM mart_daily_sales
  WHERE sale_date >= CURRENT_DATE - 7;

  -- Recompute from source including any late-arriving data
  INSERT INTO mart_daily_sales
  SELECT
      DATE_TRUNC('day', event_time) AS sale_date,
      region,
      SUM(amount) AS total_revenue,
      COUNT(*) AS order_count
  FROM clean_orders
  WHERE event_time >= CURRENT_DATE - 7
  GROUP BY 1, 2;
```

**Strategy 2 — Watermark-based late detection:**
```sql
-- Track the latest event_time seen per source
CREATE TABLE pipeline_watermarks (
    source_name VARCHAR,
    last_event_time TIMESTAMP,
    last_checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Alert if event_time is far behind current time
SELECT source_name,
       DATEDIFF('hour', last_event_time, CURRENT_TIMESTAMP()) AS hours_behind
FROM pipeline_watermarks
WHERE hours_behind > 6;  -- alert if more than 6 hours behind
```

**Strategy 3 — Idempotent partition replacement:**
```sql
-- For each date partition: DELETE then INSERT (always correct, never accumulates)
DELETE FROM fact_orders WHERE order_date = :run_date;
INSERT INTO fact_orders
SELECT * FROM clean_orders WHERE order_date = :run_date;
```

Running this twice produces the same result as once — fully idempotent.

---

## 6.6 Handle Schema Changes

### Scenario: How do you handle schema changes from source systems?

**The safest approach — VARIANT as a schema firewall:**
```sql
-- Raw table stores JSON as VARIANT — schema-agnostic
CREATE TABLE raw_api_events (
    raw        VARIANT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Source adds a new field? No pipeline change needed.
-- raw:new_field is simply accessible immediately.

-- Transform layer extracts explicitly
CREATE OR REPLACE VIEW clean_events AS
SELECT
    raw:event_id::STRING       AS event_id,
    raw:user_id::STRING        AS user_id,
    raw:event_type::STRING     AS event_type,
    raw:new_field::STRING      AS new_field,  -- add this when ready
    _loaded_at
FROM raw_api_events;
```

**Handling additive changes (new columns):**
```sql
-- For typed tables: ALTER TABLE adds the column with NULLs for old rows
ALTER TABLE clean_orders ADD COLUMN discount_amount FLOAT;

-- Backfill if needed
UPDATE clean_orders SET discount_amount = 0 WHERE discount_amount IS NULL;
```

**Handling breaking changes (renamed/deleted columns):**
```sql
-- Version the schema in the raw table
-- Add schema_version column to raw table
-- Branch transform logic by version

SELECT
    CASE
        WHEN raw:schema_version::INT >= 2
             THEN raw:customer_identifier::STRING
        ELSE raw:customer_id::STRING  -- old field name
    END AS customer_id
FROM raw_events;
```

**Using dbt for schema drift detection:**
- Add `not_null` and `relationships` tests on all key columns
- CI/CD pipeline runs dbt test on a cloned environment before deploying
- Schema drift is caught before it reaches production

> **Say in interview:** "Our core pattern: land everything as VARIANT in the raw layer. Source schema changes never break our ingestion. We only deal with changes in the transform layer, which we control and can deploy safely via CI/CD."

---

## 6.7 Large Table (TB Scale) Performance Issue

### Scenario: You have a performance issue on a TB-scale table. How do you approach it?

**Step-by-step approach:**

**Step 1 — Baseline measurement:**
```sql
-- Run the problematic query and note Query ID
-- Check in Query Profile:
-- - Partitions scanned vs total
-- - Any spillage
-- - Join types and row counts
```

**Step 2 — Check and fix partition pruning:**
```sql
-- Get current clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_events', '(event_date)');

-- If average_depth > 4:
ALTER TABLE fact_events CLUSTER BY (event_date);
-- Wait for clustering to complete (check AUTOMATIC_CLUSTERING_HISTORY)
-- Re-run query and compare Query Profile
```

**Step 3 — Fix query patterns:**
```sql
-- Ensure filters allow pruning:
-- Rewrite function-wrapped date filters
-- Add partition column to all queries on this table
-- Document as team coding standard
```

**Step 4 — Address disk spillage:**
```sql
-- For heavy analytical queries: temporarily scale up
ALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE = '2X-LARGE';
-- Run query
ALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE = 'LARGE';
```

**Step 5 — Pre-aggregation for common patterns:**
```sql
-- If the same aggregation is run many times daily → dynamic table
CREATE DYNAMIC TABLE fact_events_daily_summary
  TARGET_LAG = '1 hour'
  WAREHOUSE = TRANSFORM_WH
AS
  SELECT
      DATE_TRUNC('day', event_date) AS day,
      region,
      event_type,
      COUNT(*) AS event_count,
      SUM(value) AS total_value
  FROM fact_events
  GROUP BY 1, 2, 3;
```

**Step 6 — Search Optimization for point lookups:**
```sql
-- For queries like WHERE user_id = '12345' on a non-clustered column
ALTER TABLE fact_events ADD SEARCH OPTIMIZATION;
-- Build time: varies, check INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY
```

---

## 6.8 Debug Pipeline Failure

### Scenario: Your data pipeline failed. Walk me through debugging it.

**Step 1 — Identify the failure layer:**
```sql
-- Check Task execution history
SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())))
WHERE STATE = 'FAILED'
ORDER BY SCHEDULED_TIME DESC;
```

**Step 2 — Read the error message:**
```sql
-- Get full error details
SELECT error_message, error_code
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(...))
WHERE state = 'FAILED';
```

**Step 3 — Reproduce manually:**
```sql
-- Run the failing SQL manually (or call the stored procedure)
-- Use a small sample first to isolate the issue
```

**Step 4 — Check upstream data:**
```sql
-- Was there bad data in the source?
SELECT * FROM raw_orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1
WHERE _loaded_at >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
LIMIT 100;

-- Check for NULL in critical columns
SELECT COUNT(*) AS total, COUNT(order_id) AS non_null_order_id
FROM raw_orders
WHERE _loaded_at >= DATEADD('hour', -2, CURRENT_TIMESTAMP());
```

**Step 5 — Check permissions:**
```sql
-- Did a role or warehouse get modified?
SHOW GRANTS TO ROLE pipeline_role;
SHOW WAREHOUSES LIKE 'TRANSFORM_WH';
```

**Step 6 — Fix and backfill:**
```sql
-- After fixing root cause, identify the data gap
SELECT MIN(_loaded_at), MAX(_loaded_at) FROM raw_orders
WHERE order_date BETWEEN '2024-01-15' AND '2024-01-16';

-- Manually run the failed transform for the gap period
INSERT INTO clean_orders
SELECT ... FROM raw_orders
WHERE _loaded_at BETWEEN '2024-01-15 02:00:00' AND '2024-01-15 08:00:00';
```

---

## 6.9 Sudden Spike in Snowflake Credits

### Scenario: Snowflake credits spiked unexpectedly. How do you investigate?

**Step 1 — Identify which warehouse consumed the credits:**
```sql
SELECT
    warehouse_name,
    DATE_TRUNC('day', start_time) AS day,
    SUM(credits_used) AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -14, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY credits_used DESC;
```

**Step 2 — Find the expensive queries in that warehouse:**
```sql
SELECT
    query_id,
    LEFT(query_text, 200) AS query_preview,
    execution_time / 60000 AS execution_minutes,
    bytes_scanned / POWER(1024, 3) AS gb_scanned,
    bytes_spilled_to_local_storage / POWER(1024, 3) AS gb_spilled_local,
    bytes_spilled_to_remote_storage / POWER(1024, 3) AS gb_spilled_remote,
    user_name,
    warehouse_name
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    AND warehouse_name = 'MY_WAREHOUSE'
ORDER BY execution_time DESC
LIMIT 20;
```

**Step 3 — Identify if a new job or user caused it:**
```sql
-- New users running expensive queries?
SELECT user_name, COUNT(*) AS query_count, SUM(execution_time)/60000 AS total_minutes
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY total_minutes DESC;
```

**Step 4 — Set up resource monitors to prevent future overruns:**
```sql
CREATE RESOURCE MONITOR spend_guard
  WITH CREDIT_QUOTA = 1000
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 80 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE expensive_wh SET RESOURCE_MONITOR = spend_guard;
```

---

## 6.10 Design Reusable SQL for Non-Technical Users

### Scenario: Business/finance teams keep asking you to re-run SQL with different dates and filters. How do you design reusable SQL so they can run it themselves?

This is a systems design question. The answer shows you think beyond SQL to usability and self-service.

**The core principle:** Abstract the SQL logic from the inputs. Users interact with parameters — never the raw SQL.

---

**Solution 1 — Stored Procedure with Parameters (best for automation):**
```sql
CREATE OR REPLACE PROCEDURE get_sales_report(
    start_date DATE,
    end_date DATE,
    country VARCHAR,
    product_name VARCHAR
)
RETURNS TABLE (region STRING, product STRING, total_sales FLOAT, order_count INT)
LANGUAGE SQL
AS
$$
    SELECT
        region,
        product_name,
        SUM(revenue)   AS total_sales,
        COUNT(*)       AS order_count
    FROM fact_sales
    WHERE order_date  BETWEEN start_date AND end_date
      AND country_name = country
      AND product       = product_name
    GROUP BY region, product_name
    ORDER BY total_sales DESC;
$$;
```

Users call it with:
```sql
CALL get_sales_report('2024-01-01', '2024-03-31', 'India', 'Laptop');
```
They never see the SQL. One line. Document this for them.

---

**Solution 2 — Config/Parameter Table + View (best for always-on dashboards):**
```sql
-- Step 1: Parameter table users (or you on their behalf) update
CREATE TABLE report_parameters (
    param_id      INT PRIMARY KEY,
    start_date    DATE,
    end_date      DATE,
    country       VARCHAR,
    product_name  VARCHAR,
    updated_by    VARCHAR,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Step 2: View reads from parameter table
CREATE OR REPLACE VIEW parameterized_sales_report AS
SELECT
    f.region,
    f.product_name,
    SUM(f.revenue)  AS total_sales,
    COUNT(*)        AS order_count
FROM fact_sales f
JOIN report_parameters p ON p.param_id = 1
WHERE f.order_date    BETWEEN p.start_date AND p.end_date
  AND f.country_name  = p.country
  AND f.product       = p.product_name
GROUP BY f.region, f.product_name;
```

User updates the parameter row (one simple UPDATE), then runs `SELECT * FROM parameterized_sales_report`. The view does the rest.

---

**Solution 3 — Scheduled Task for Recurring Reports:**
```sql
CREATE TASK weekly_india_laptop_report
  WAREHOUSE = REPORTING_WH
  SCHEDULE = 'USING CRON 0 7 * * MON Asia/Kolkata'
AS
  INSERT INTO sales_report_output
  SELECT
      CURRENT_DATE                        AS report_run_date,
      region,
      product_name,
      SUM(revenue)                        AS total_sales,
      COUNT(*)                            AS order_count
  FROM fact_sales
  WHERE order_date BETWEEN DATEADD('day', -7, CURRENT_DATE) AND CURRENT_DATE
  GROUP BY region, product_name;

ALTER TASK weekly_india_laptop_report RESUME;
```

Every Monday at 7am, data is ready. Users just query `sales_report_output`. No manual trigger needed.

---

**Solution 4 — Databricks Notebook Widgets (for Databricks-native teams):**
```python
# ---- USER INPUTS (only edit this cell) ----
dbutils.widgets.text("start_date",   "2024-01-01", "Start Date")
dbutils.widgets.text("end_date",     "2024-03-31", "End Date")
dbutils.widgets.dropdown("country",  "India", ["India", "USA", "UK", "Germany"])
dbutils.widgets.text("product_name", "Laptop", "Product Name")
# -------------------------------------------

start_date   = dbutils.widgets.get("start_date")
end_date     = dbutils.widgets.get("end_date")
country      = dbutils.widgets.get("country")
product_name = dbutils.widgets.get("product_name")

# Core SQL (users never see or touch this cell)
query = f"""
    SELECT region, product_name, SUM(revenue) AS total_sales, COUNT(*) AS order_count
    FROM fact_sales
    WHERE order_date   BETWEEN '{start_date}' AND '{end_date}'
      AND country_name  = '{country}'
      AND product        = '{product_name}'
    GROUP BY region, product_name
    ORDER BY total_sales DESC
"""

df = spark.read.format("snowflake").options(**snowflake_options).option("query", query).load()
display(df)
```

Users see input boxes at the top of the notebook. They fill in the values and click Run All. Zero SQL interaction.

---

**Solution 5 — Streamlit in Snowflake (most user-friendly — web form UI):**
```python
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()

st.title("Sales Analysis Report")
st.markdown("Fill in the filters below and click Run Report")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=pd.Timestamp("2024-01-01"))
with col2:
    end_date = st.date_input("End Date", value=pd.Timestamp("2024-03-31"))

country      = st.selectbox("Country", ["India", "USA", "UK", "Germany", "All"])
product_name = st.text_input("Product Name", "Laptop")

if st.button("Run Report"):
    with st.spinner("Running analysis..."):
        df = session.call("get_sales_report", start_date, end_date, country, product_name)
        st.dataframe(df.to_pandas())
        st.success(f"Report complete — {len(df.to_pandas())} rows returned")
```

Users get a browser-based form with dropdowns and date pickers. No SQL, no notebooks, no Python. Lives entirely inside Snowflake.

---

**Choosing the right solution:**

| User type | Best solution |
|---|---|
| Somewhat technical, runs SQL | Stored Procedure |
| Wants an always-updated dashboard | Config table + View |
| Same report every Monday | Scheduled Task |
| Databricks notebook user | Widget-based notebook |
| Fully non-technical | Streamlit in Snowflake |

> **Say in interview:** "The core principle in all of these is the same: one place for the SQL logic, clean separation from the inputs, and no way for a user to accidentally break the core query. The right implementation depends on the user's technical comfort level."

---

---

# PART 7 — CLOUD & INTEGRATION

---

## 7.1 Load Data from S3 to Snowflake

### Question: How do you load data from S3 to Snowflake?

```sql
-- Step 1: Create Storage Integration (one-time setup)
CREATE STORAGE INTEGRATION my_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/');

-- Step 2: Get IAM details from Snowflake
DESC INTEGRATION my_s3_integration;
-- Note: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID

-- Step 3: Create IAM Role in AWS with S3 read permissions
-- Trust policy uses the above ARN and external ID

-- Step 4: Update integration with role ARN
ALTER STORAGE INTEGRATION my_s3_integration
  SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789:role/my-snowflake-role';

-- Step 5: Create External Stage
CREATE STAGE my_s3_stage
  URL = 's3://my-bucket/data/'
  STORAGE_INTEGRATION = my_s3_integration
  FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

-- Step 6: Load data
COPY INTO my_table
FROM @my_s3_stage
PATTERN = '.*orders_2024.*\.csv'
ON_ERROR = CONTINUE;

-- Step 7: Verify
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MY_TABLE',
    START_TIME => DATEADD('minutes', -10, CURRENT_TIMESTAMP())));
```

---

## 7.2 External Stage

### Question: What is an external stage?

An external stage is a named Snowflake object that **references a location in cloud storage you own** — S3, Azure Blob, or GCS. It stores the connection details (URL, credentials, file format) centrally so COPY INTO commands don't need to repeat them.

Key features:
- Supports glob patterns for file selection: `PATTERN = '.*2024.*\.csv'`
- Supports prefix-based loading: `COPY INTO t FROM @stage/2024/01/`
- Can be listed: `LIST @my_stage;`
- Files can be removed: `REMOVE @my_stage/old_file.csv;`
- Multiple pipes/tasks can reference the same stage

---

## 7.3 Storage Integration

### Question: What is a storage integration?

A Storage Integration is a Snowflake security object enabling access to cloud storage **without storing credentials** in Snowflake. Uses IAM role delegation (AWS), Service Principal (Azure), or Service Account (GCP).

The IAM trust relationship creates a security boundary where Snowflake's AWS account assumes your IAM role using a specific external ID — preventing confused deputy attacks where another AWS account could trick your role.

```sql
-- View integration details
DESC INTEGRATION my_s3_integration;

-- Key output fields:
-- STORAGE_AWS_IAM_USER_ARN  → put this in IAM role trust policy
-- STORAGE_AWS_EXTERNAL_ID   → use as condition in trust policy (security)
```

---

## 7.4 Snowflake Integration with AWS and Azure

### Question: How does Snowflake integrate with AWS and Azure?

**AWS integration points:**
- **S3:** External stages with Storage Integration (IAM roles)
- **SQS:** Auto-ingest event notifications for Snowpipe
- **KMS:** Customer-managed encryption keys (Tri-Secret Secure)
- **PrivateLink:** Private network connectivity — traffic never traverses public internet
- **Lambda/EventBridge:** Trigger external processes from Snowflake events
- **Glue/EMR:** ETL tools that load into Snowflake via JDBC/ODBC

**Azure integration points:**
- **Azure Blob Storage:** External stages with Storage Integration (Service Principals)
- **Azure Event Grid + Storage Queue:** Auto-ingest for Snowpipe
- **Azure Key Vault:** Customer-managed encryption keys
- **Private Link:** Private connectivity
- **Azure Data Factory:** Native Snowflake connector for pipeline orchestration
- **Azure Active Directory (Entra):** SSO and federated authentication
- **Synapse:** Snowflake can replace or complement Synapse Analytics

---

## 7.5 SNS and SQS Explained

See section 2.8 for detailed explanation. Summary:
- **SNS** = pub/sub, fan-out to multiple subscribers (used when multiple services need the same S3 event)
- **SQS** = queue, durable storage until consumed (used by Snowpipe as the buffer between S3 events and ingestion processing)

---

## 7.6 Orchestrating Pipelines — Airflow and ADF

### Question: How do you orchestrate Snowflake pipelines with Airflow or ADF?

**Apache Airflow:**
```python
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG('daily_sales_pipeline', start_date=datetime(2024,1,1), schedule_interval='@daily') as dag:

    extract = SnowflakeOperator(
        task_id='extract_to_staging',
        sql='CALL sp_extract_to_staging()',
        snowflake_conn_id='snowflake_default'
    )

    transform = SnowflakeOperator(
        task_id='run_transformations',
        sql='CALL sp_run_transforms()',
        snowflake_conn_id='snowflake_default'
    )

    validate = SnowflakeOperator(
        task_id='run_dbt_tests',
        sql='CALL sp_run_data_quality_checks()',
        snowflake_conn_id='snowflake_default'
    )

    extract >> transform >> validate  # dependency chain
```

**When to use each orchestrator:**

| Tool | Use When |
|---|---|
| **Airflow** | Cross-system pipelines (S3 + Salesforce + Snowflake), complex dependencies, existing Airflow investment |
| **ADF** | Azure-native stack, minimal code preference, simple extract-load from Azure sources |
| **Snowflake Tasks** | Snowflake-only pipelines, simplest option, no external scheduler needed |
| **dbt Cloud** | SQL transformation layer, version control, built-in testing and documentation |

> **Say in interview:** "We use Airflow for cross-system orchestration — it triggers Salesforce extract, waits for files to land in S3, then triggers Snowflake transforms. For Snowflake-only transform chains (raw → clean → mart), we use native Tasks — no need for an external scheduler."

---

---

# PART 8 — SECURITY

---

## 8.1 RBAC in Snowflake

### Question: What is RBAC in Snowflake?

RBAC (Role-Based Access Control) is Snowflake's security model where **permissions are granted to roles, and roles are assigned to users** — never permissions directly to users.

**Built-in role hierarchy:**
```
ACCOUNTADMIN
    │ (inherits)
SYSADMIN          SECURITYADMIN
    │                  │
    │              USERADMIN
    │
PUBLIC (auto-granted to all users)
```

**Role descriptions:**
- `ACCOUNTADMIN`: Highest level — full account control. Use only for initial setup. Never for daily work.
- `SYSADMIN`: Creates warehouses, databases, schemas, tables, stages
- `SECURITYADMIN`: Manages users, roles, and grants
- `USERADMIN`: Creates users and roles (can't grant privileges)
- `PUBLIC`: Auto-granted to every user — minimal default access

**Creating and using custom roles:**
```sql
-- Create functional roles
CREATE ROLE data_analyst_role;
CREATE ROLE data_engineer_role;
CREATE ROLE data_loader_role;

-- Grant object privileges to roles
GRANT USAGE ON DATABASE prod_db TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA prod_db.marts TO ROLE data_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA prod_db.marts TO ROLE data_analyst_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA prod_db.marts TO ROLE data_analyst_role;

-- Assign roles to users
GRANT ROLE data_analyst_role TO USER john.doe@company.com;

-- Service accounts get dedicated roles
GRANT ROLE data_loader_role TO USER snowpipe_service_account;
```

---

## 8.2 How Roles Work

### Question: How do roles work in Snowflake? Explain role hierarchy and FUTURE GRANTS.

**Role inheritance:**
When Role A is granted to Role B, Role B inherits all privileges of Role A.
```sql
GRANT ROLE read_only_role TO ROLE data_analyst_role;
-- data_analyst_role now has all permissions of read_only_role + its own
```

**Switching roles:**
```sql
USE ROLE data_engineer_role;  -- switch active role in session
SHOW GRANTS TO ROLE data_analyst_role;  -- see what a role can do
SHOW GRANTS OF ROLE data_analyst_role;  -- see who has this role
```

**FUTURE GRANTS (critical for automation):**
Without FUTURE GRANTS, you must manually grant permissions every time a new table/view is created.
```sql
-- Auto-grant SELECT on any new tables created in this schema
GRANT SELECT ON FUTURE TABLES IN SCHEMA prod_db.marts TO ROLE data_analyst_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA prod_db.marts TO ROLE data_analyst_role;

-- For the entire database
GRANT SELECT ON FUTURE TABLES IN DATABASE prod_db TO ROLE data_analyst_role;
```

---

## 8.3 Dynamic Data Masking Policy

### Question: What is a masking policy in Snowflake?

A Dynamic Data Masking policy **masks sensitive column values at query time** based on the querying user's role — without modifying the underlying stored data.

```sql
-- Create masking policy for PII (email)
CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ADMIN_ROLE', 'COMPLIANCE_ROLE') THEN val
        WHEN CURRENT_ROLE() IN ('DATA_ANALYST_ROLE') THEN
            REGEXP_REPLACE(val, '.+@', '****@')  -- show: ****@gmail.com
        ELSE '***MASKED***'
    END;

-- Apply to column
ALTER TABLE customers MODIFY COLUMN email
    SET MASKING POLICY email_mask;

-- Verify
DESCRIBE TABLE customers;  -- shows masking policy applied to column
```

**SSN masking with partial reveal:**
```sql
CREATE OR REPLACE MASKING POLICY ssn_mask AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ADMIN_ROLE') THEN val
        WHEN CURRENT_ROLE() IN ('ANALYST_ROLE') THEN '***-**-' || RIGHT(val, 4)
        ELSE '***-**-****'
    END;
```

Key points:
- Masking happens **at query time** — stored data is never changed
- Works on both tables and views
- Can use CURRENT_ROLE(), CURRENT_USER(), or custom mapping tables
- Can be combined with Row Access Policies for full column + row level security
- Centrally managed — one change applies everywhere the column is used

---

## 8.4 Row-Level Security (Row Access Policy)

### Question: What is row-level security in Snowflake?

Row Access Policies restrict which **rows** a user can see in a table — based on their role, username, or any custom logic stored in a mapping table.

```sql
-- Mapping table: which user can see which region
CREATE TABLE user_region_access (
    username VARCHAR,
    allowed_region VARCHAR
);

INSERT INTO user_region_access VALUES
    ('john.doe', 'India'),
    ('jane.smith', 'USA'),
    ('admin_user', 'ALL');

-- Create row access policy
CREATE OR REPLACE ROW ACCESS POLICY region_access_policy
AS (region_col VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('ADMIN_ROLE')
    OR 'ALL' IN (
        SELECT allowed_region FROM user_region_access WHERE username = CURRENT_USER()
    )
    OR region_col IN (
        SELECT allowed_region FROM user_region_access WHERE username = CURRENT_USER()
    );

-- Apply to table
ALTER TABLE fact_sales ADD ROW ACCESS POLICY region_access_policy ON (region);
```

Now:
- John Doe sees only India rows
- Jane Smith sees only USA rows
- Admin users see all rows
- All from the same physical table — no data duplication

**Use cases:** Multi-tenant data isolation, regional data access control, department-specific visibility.

---

## 8.5 Secure Views

### Question: What are secure views and when do you use them?

A Secure View **hides the view definition** (the SQL code) from users who don't own the view. Regular views expose their SQL definition to anyone who can query them.

```sql
-- Regular view: definition visible to all users with access
CREATE VIEW customer_summary AS
SELECT customer_id, name, email FROM customers;

-- Secure view: SQL hidden from consumers
CREATE SECURE VIEW customer_summary AS
SELECT customer_id, name, email FROM customers;

-- Check if a view is secure
SHOW VIEWS LIKE 'customer_summary';
-- is_secure column = YES
```

**When to use secure views:**
- Data Sharing: Required when sharing views across accounts — consumers can't see your SQL
- Hiding business logic from contractors/external parties
- Preventing column inference attacks (deducing base table structure from view SQL)

**Trade-off:**
Secure views **bypass query optimization** on the view definition — Snowflake can't push predicates through a secure view. This can make them slower than regular views. Use only when security requires it.

---

## 8.6 How to Share Data Securely

See section 1.12 for full data sharing explanation.

**Security layering for sharing:**
```sql
-- 1. Mask sensitive columns in the shared view
CREATE SECURE VIEW shared_customer_view AS
SELECT
    customer_id,
    name,
    REGEXP_REPLACE(email, '.+@', '****@') AS email_masked,  -- mask PII
    region,
    total_purchases
FROM customers
WHERE is_active = TRUE;  -- only share active customers

-- 2. Share only the secure view (not the base table)
GRANT SELECT ON VIEW shared_customer_view TO SHARE partner_share;
```

---

## 8.7 Network Policies

### Question: What are network policies in Snowflake?

Network policies restrict which IP addresses can connect to Snowflake — an additional layer of access control beyond authentication.

```sql
-- Create network policy allowing only office IPs and VPN
CREATE NETWORK POLICY corporate_access_policy
  ALLOWED_IP_LIST = (
      '203.0.113.0/24',   -- office IP range
      '198.51.100.5'      -- VPN IP
  )
  BLOCKED_IP_LIST = ();   -- explicitly block none

-- Apply to entire account
ALTER ACCOUNT SET NETWORK_POLICY = corporate_access_policy;

-- Apply to a specific user (overrides account policy for that user)
ALTER USER service_account_user SET NETWORK_POLICY = service_account_policy;
```

Use cases: Ensure production credentials can only be used from known IP ranges. Prevent compromised credentials from being used outside corporate network.

---

## 8.8 Data Encryption in Snowflake

### Question: How does Snowflake handle data encryption?

**Encryption by default:**
- All data at rest is encrypted using **AES-256**
- All data in transit uses **TLS 1.2+**
- Encryption is always on — cannot be disabled
- Snowflake manages all encryption keys by default at no extra cost

**Key management hierarchy:**
```
Root Key (Snowflake's Hardware Security Module)
    │
    ▼
Account Master Key (per Snowflake account)
    │
    ▼
Table Master Key (per table)
    │
    ▼
File Key (per micro-partition file)
```

Keys are automatically rotated periodically by Snowflake.

**Customer-managed keys (Tri-Secret Secure):**
For maximum security, Enterprise edition supports Tri-Secret Secure where the customer holds one of the encryption keys. Snowflake cannot decrypt your data without your key.
```sql
-- Uses AWS KMS, Azure Key Vault, or GCP Cloud KMS
-- Configured at the account level
```

---

---

# PART 9 — DATA MODELING

---

## 9.1 Fact and Dimension Tables

### Question: How do you design fact and dimension tables?

**Fact Tables:**
- Contain measurable, quantitative data — the "what happened"
- Have foreign keys pointing to dimension tables
- Have numeric measure columns (amount, quantity, duration, count)
- Have date/time keys for time intelligence
- Tend to be very wide (many columns) and very large (billions of rows)

```sql
CREATE TABLE fact_sales (
    -- Surrogate keys (join to dimensions)
    date_key        INT NOT NULL,
    customer_key    INT NOT NULL,
    product_key     INT NOT NULL,
    region_key      INT NOT NULL,
    -- Degenerate dimension (no separate dim table needed)
    order_id        STRING NOT NULL,
    -- Measures
    quantity        INT,
    unit_price      FLOAT,
    discount_amount FLOAT,
    revenue         FLOAT,
    cost            FLOAT,
    profit          FLOAT,
    -- Audit columns
    _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

**Dimension Tables:**
- Contain descriptive, contextual attributes — the "who, what, where"
- Used for filtering, grouping, and labeling in reports
- Have surrogate keys as primary keys
- Contain human-readable descriptions

```sql
CREATE TABLE dim_customer (
    customer_key     INT IDENTITY PRIMARY KEY,  -- surrogate key
    customer_id      STRING NOT NULL,           -- natural/business key
    name             STRING,
    email            STRING,
    segment          STRING,  -- Enterprise, SMB, Individual
    country          STRING,
    region           STRING,
    city             STRING,
    signup_date      DATE,
    -- SCD Type 2 columns
    eff_start_date   DATE DEFAULT CURRENT_DATE,
    eff_end_date     DATE,
    is_current       BOOLEAN DEFAULT TRUE,
    -- Audit
    _loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

**Date Dimension (always include this):**
```sql
CREATE TABLE dim_date (
    date_key         INT PRIMARY KEY,  -- 20240115 format
    date_actual      DATE,
    day_of_week      STRING,
    day_name         STRING,
    month_num        INT,
    month_name       STRING,
    quarter          INT,
    year             INT,
    is_weekend       BOOLEAN,
    is_holiday       BOOLEAN,
    fiscal_quarter   INT,
    fiscal_year      INT
);
```

---

## 9.2 Star vs Snowflake Schema

### Question: Explain Star schema vs Snowflake schema. Which is better for Snowflake?

**Star Schema:**
```
                    dim_customer
                         │
                         │ (FK)
dim_date ────────── fact_sales ──────── dim_product
                         │
                         │ (FK)
                    dim_region
```
- Flat, denormalized dimensions
- Fewer joins needed for queries
- Slight data redundancy in dimensions
- Simple — easy for analysts and BI tools

**Snowflake Schema:**
```
dim_product_subcategory ──► dim_product_category ──► dim_product ──► fact_sales
                                                                           │
                                                                      dim_customer
                                                                           │
                                                                      dim_city ──► dim_region ──► dim_country
```
- Normalized dimensions (dimension tables normalized further)
- More joins needed
- Less storage redundancy
- More complex — harder to query and understand

**Which to use in Snowflake (the database)?**

Ironically, use **Star schema** in Snowflake (the database). Here's why:
- Snowflake's columnar storage and micro-partitioning make joins cheap
- The storage redundancy in Star schema costs very little in cloud storage pricing
- Analysts make fewer mistakes with simpler schemas
- BI tools (Tableau, Power BI) work better with fewer join levels
- Query optimization in Snowflake handles denormalized schemas well

> **Say in interview:** "Despite the database being named Snowflake, I use Star schema for analytics. The join savings from normalization are negligible in Snowflake, but the simplicity benefit — for analysts and BI tools — is significant."

---

## 9.3 SCD — Slowly Changing Dimensions

### Question: How do you handle SCD in Snowflake?

**SCD Type 1 — Overwrite (no history):**
```sql
-- Just update the record. No history kept.
-- Use when: old value is wrong, or history doesn't matter
MERGE INTO dim_customer tgt
USING customer_updates src ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
    UPDATE SET tgt.email = src.email, tgt.phone = src.phone, tgt.city = src.city;
```

**SCD Type 2 — Preserve history (most common for analytics):**
```sql
-- expire old version
UPDATE dim_customer
SET eff_end_date = CURRENT_DATE - 1,
    is_current   = FALSE
WHERE customer_id IN (SELECT customer_id FROM customer_stream WHERE METADATA$ACTION = 'DELETE')
  AND is_current = TRUE;

-- insert new version
INSERT INTO dim_customer
    (customer_id, name, email, segment, country,
     eff_start_date, eff_end_date, is_current)
SELECT
    customer_id, name, email, segment, country,
    CURRENT_DATE, NULL, TRUE
FROM customer_stream
WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE;
```

**Query current state:**
```sql
SELECT * FROM dim_customer WHERE is_current = TRUE;
```

**Query historical state (as of a specific date):**
```sql
SELECT * FROM dim_customer
WHERE eff_start_date <= '2023-06-01'
  AND (eff_end_date > '2023-06-01' OR eff_end_date IS NULL);
```

**SCD Type 3 — Previous value column:**
```sql
-- Add a "previous" column for the changing attribute
ALTER TABLE dim_customer ADD COLUMN previous_email STRING;

-- On change: shift current → previous
UPDATE dim_customer
SET previous_email = email, email = new_email
WHERE customer_id = 'C001';
-- Only one level of history — simple but limited
```

---

## 9.4 When to Use Materialized Views

### Question: When should you use materialized views in Snowflake?

**Use materialized views when:**
- The same expensive query runs very frequently (many times per hour)
- The base table changes infrequently relative to query frequency
- The result set is much smaller than the base data (good compression ratio)
- The query is a simple aggregation (COUNT, SUM, MIN, MAX) on an append-only table

```sql
CREATE MATERIALIZED VIEW mv_daily_sales_summary AS
SELECT
    DATE_TRUNC('day', order_date) AS sale_day,
    region,
    COUNT(*) AS order_count,
    SUM(revenue) AS total_revenue
FROM fact_sales
GROUP BY 1, 2;
```

**Do NOT use when:**
- Base table changes very frequently — background maintenance cost exceeds query benefit
- Query involves JOINs (Snowflake MVs don't support joins — use Dynamic Tables instead)
- Query uses non-deterministic functions
- The query runs rarely — result cache is sufficient

**Modern alternative — Dynamic Tables (preferred):**
```sql
-- Dynamic Tables support JOINs, complex SQL, and automatic incremental refresh
CREATE DYNAMIC TABLE daily_sales_summary
  TARGET_LAG = '30 minutes'  -- refresh every 30 minutes
  WAREHOUSE = TRANSFORM_WH
AS
  SELECT
      DATE_TRUNC('day', f.order_date) AS sale_day,
      c.region,
      COUNT(*) AS order_count,
      SUM(f.revenue) AS total_revenue
  FROM fact_sales f
  JOIN dim_customer c ON f.customer_key = c.customer_key  -- JOINs work here!
  GROUP BY 1, 2;
```

---

## 9.5 Normalization vs Denormalization

### Question: Explain normalization vs denormalization. Which do you use in Snowflake?

**Normalization:**
Organizing data to minimize redundancy. Each piece of information exists in exactly one place.
- 1NF: Atomic values, no repeating groups
- 2NF: No partial dependencies on composite key
- 3NF: No transitive dependencies (every non-key column depends only on the primary key)
- Best for: OLTP systems (MySQL, PostgreSQL) with frequent updates

**Denormalization:**
Intentionally introducing redundancy to improve read performance. Pre-joining tables, duplicating attributes.
- Best for: OLAP/analytics (Snowflake, BigQuery) where reads dominate
- Reduces number of JOINs needed
- Increases storage slightly
- Updates require changing multiple places (but in analytics, you typically append, not update)

**My recommendation for Snowflake analytics layers:**
- Raw layer: normalized or semi-structured (VARIANT) — stay close to source
- Cleaned layer: lightly normalized — deduplicated, typed, validated
- Mart layer: **denormalized** — wide, flat tables with pre-joined attributes that analysts need

> **Justification:** "In Snowflake, cloud storage is cheap. The cost of denormalization (extra storage) is much lower than the benefit (simpler queries, fewer analyst errors, better BI tool performance)."

---

---

# PART 10 — ADVANCED SNOWFLAKE

---

## 10.1 VARIANT Data Type

### Question: What is the VARIANT data type in Snowflake?

VARIANT is a Snowflake data type that can store **any semi-structured data** — JSON, Avro, ORC, Parquet, XML — as a single column value. It can hold any value: string, number, boolean, array, nested object, or null.

**Loading JSON into VARIANT:**
```sql
-- Load JSON files directly
COPY INTO raw_events
FROM @s3_stage
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE);

-- The entire JSON document goes into the VARIANT column
```

**Snowflake stores VARIANT efficiently:**
Internally, Snowflake uses a binary columnar representation (Snowflake Internal Representation — SIR) for VARIANT. It extracts column statistics and metadata during ingestion, enabling predicate pushdown and selective parsing — making VARIANT surprisingly performant.

**Creating tables with VARIANT:**
```sql
CREATE TABLE raw_api_events (
    event_id    STRING,   -- often worth extracting high-cardinality join keys
    raw         VARIANT,  -- everything else in VARIANT
    _loaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

---

## 10.2 Querying JSON in Snowflake

### Question: How do you query JSON / VARIANT in Snowflake?

**Navigating nested structure — colon notation:**
```sql
SELECT
    raw:user_id::STRING          AS user_id,         -- top-level field
    raw:order:order_id::STRING   AS order_id,         -- nested field
    raw:order:amount::FLOAT      AS amount,            -- nested number
    raw:metadata:browser::STRING AS browser,           -- nested string
    raw:tags[0]::STRING          AS first_tag,         -- first array element
    raw:tags[1]::STRING          AS second_tag         -- second array element
FROM raw_events;
```

**Flattening arrays — LATERAL FLATTEN:**
```sql
-- Turn each array element into its own row
SELECT
    e.raw:order_id::STRING AS order_id,
    f.index                AS item_position,
    f.value:product_id::STRING AS product_id,
    f.value:quantity::INT      AS quantity,
    f.value:price::FLOAT       AS price
FROM raw_orders e,
LATERAL FLATTEN(input => e.raw:items) f;
```

**Checking if a key exists:**
```sql
WHERE raw:optional_field IS NOT NULL
```

**Parsing a JSON string into VARIANT:**
```sql
SELECT PARSE_JSON('{"key": "value"}'):key::STRING;
-- Returns: value
```

**Converting VARIANT back to JSON string:**
```sql
SELECT TO_JSON(my_variant_col) FROM my_table;
```

**Type casting reference:**
```sql
-- STRING
raw:field::STRING      or  raw:field::VARCHAR

-- NUMBERS
raw:field::INT
raw:field::FLOAT
raw:field::NUMBER(10,2)

-- DATE/TIME
raw:field::DATE
raw:field::TIMESTAMP
raw:field::TIMESTAMP_NTZ  -- no timezone

-- BOOLEAN
raw:field::BOOLEAN
```

---

## 10.3 Snowpark

### Question: What is Snowpark?

Snowpark is a **developer framework** allowing you to write data engineering, ML, and transformation code in Python, Java, or Scala — executed directly inside Snowflake's compute engine. Data never leaves Snowflake.

**Snowpark Python — DataFrame API:**
```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, avg, when

# Connect
session = Session.builder.configs({
    "account": "my_account",
    "user": "my_user",
    "password": "...",
    "warehouse": "SNOWPARK_WH",
    "database": "PROD_DB",
    "schema": "PUBLIC"
}).create()

# Build a DataFrame (lazy — doesn't execute yet)
df = session.table("fact_sales")

# Apply transformations (SQL generated under the hood)
result = (
    df
    .filter(col("region") == "India")
    .filter(col("order_date") >= "2024-01-01")
    .group_by("product_name")
    .agg(
        sum_("revenue").alias("total_revenue"),
        avg("discount_amount").alias("avg_discount")
    )
    .sort(col("total_revenue").desc())
)

# Actually execute and bring to Python
result.show()  # prints to console
pandas_df = result.to_pandas()  # convert to pandas
```

**Snowpark UDFs:**
```python
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType

@udf(return_type=StringType(), input_types=[StringType()])
def clean_phone(phone: str) -> str:
    import re
    return re.sub(r'[^0-9]', '', phone) if phone else None

# Use in DataFrame
df.select(clean_phone(col("phone_number"))).show()
```

**Snowpark for ML:**
```python
from snowflake.ml.modeling.linear_model import LogisticRegression

# Train model inside Snowflake (data stays in Snowflake)
model = LogisticRegression()
model.fit(training_df, input_cols=feature_cols, label_cols=["CHURN"])

# Score predictions (runs inside Snowflake warehouse)
predictions = model.predict(test_df)
```

**Snowpark vs External Python:**

| Aspect | Snowpark | External Python |
|---|---|---|
| Data location | Stays in Snowflake | Pulled out of Snowflake |
| Data egress cost | Zero | Potentially large |
| Latency | Low (compute near data) | Network + transfer overhead |
| Scale | Uses Snowflake warehouse | Depends on external cluster |
| Use case | Data engineering + ML | Complex non-SQL logic |

---

## 10.4 UDFs and Stored Procedures

### Question: What are UDFs and stored procedures? When do you use each?

**UDFs (User-Defined Functions):**
```sql
-- SQL UDF
CREATE OR REPLACE FUNCTION calculate_tax(amount FLOAT, tax_rate FLOAT)
RETURNS FLOAT
LANGUAGE SQL
AS $$ amount * tax_rate / 100 $$;

-- JavaScript UDF (for complex string operations)
CREATE OR REPLACE FUNCTION clean_phone(ph STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS $$
    if (!PH) return null;
    return PH.replace(/[^0-9]/g, '');
$$;

-- Python UDF (for ML inference or complex logic)
CREATE OR REPLACE FUNCTION predict_churn(features VARIANT)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('scikit-learn', 'numpy')
HANDLER = 'predict'
AS $$
import pickle
import numpy as np

def predict(features):
    model = pickle.loads(open('/tmp/model.pkl', 'rb').read())
    X = np.array([features['age'], features['tenure'], features['monthly_charges']])
    return float(model.predict_proba([X])[0][1])
$$;

-- Use in SQL
SELECT order_id, calculate_tax(amount, 18.0) AS tax_amount FROM orders;
SELECT customer_id, clean_phone(phone) AS clean_phone FROM customers;
```

**Stored Procedures:**
```sql
-- Python stored procedure for complex pipeline logic
CREATE OR REPLACE PROCEDURE run_daily_pipeline(run_date DATE)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS $$
def run(session, run_date):
    try:
        # Step 1: Validate input
        count = session.sql(f"SELECT COUNT(*) FROM raw_orders WHERE DATE(loaded_at) = '{run_date}'").collect()[0][0]
        if count == 0:
            return f"WARNING: No raw data for {run_date}"

        # Step 2: Clean and load
        session.sql(f"""
            INSERT INTO clean_orders
            SELECT order_id, customer_id, amount, '{run_date}'::DATE
            FROM raw_orders
            WHERE DATE(loaded_at) = '{run_date}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY loaded_at DESC) = 1
        """).collect()

        # Step 3: Update aggregations
        session.sql(f"""
            MERGE INTO mart_daily_summary m
            USING (SELECT '{run_date}'::DATE as day, SUM(amount) as rev, COUNT(*) as cnt FROM clean_orders WHERE order_date = '{run_date}') s
            ON m.day = s.day
            WHEN MATCHED THEN UPDATE SET m.revenue = s.rev, m.order_count = s.cnt
            WHEN NOT MATCHED THEN INSERT VALUES (s.day, s.rev, s.cnt)
        """).collect()

        return f"SUCCESS: Processed {count} records for {run_date}"

    except Exception as e:
        return f"ERROR: {str(e)}"
$$;

-- Call the procedure
CALL run_daily_pipeline('2024-01-15');
```

**Decision matrix:**

| Use Case | Use |
|---|---|
| Reusable transformation logic (text, math) | UDF |
| Inline in SELECT statement | UDF |
| Multi-step pipeline with DML | Stored Procedure |
| Error handling / try-catch logic | Stored Procedure |
| Loops and conditionals | Stored Procedure |
| Calling multiple SQL statements | Stored Procedure |

---

## 10.5 Search Optimization Service

### Question: What is the Search Optimization Service?

Search Optimization Service builds a **persistent secondary index-like structure** on a table that improves performance for highly selective point-lookup queries — without requiring a clustering key.

**When to use:**
- Queries filtering on non-clustered columns with high selectivity: `WHERE user_id = '12345'`
- Substring searches: `WHERE email LIKE '%@gmail.com'`
- Equality searches on VARIANT/JSON fields: `WHERE raw:event_type::STRING = 'PURCHASE'`
- When clustering key isn't appropriate (column isn't range-based, or table has multiple different query patterns)

**Setup:**
```sql
-- Enable search optimization on a table
ALTER TABLE customers ADD SEARCH OPTIMIZATION;

-- Enable for specific columns only (more cost-efficient)
ALTER TABLE customers ADD SEARCH OPTIMIZATION ON EQUALITY(user_id, email);
ALTER TABLE customers ADD SEARCH OPTIMIZATION ON SUBSTRING(description);

-- Check status
SELECT * FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_NAME = 'CUSTOMERS';
```

**Cost:**
Search Optimization has ongoing maintenance compute cost + storage for the index structure. Evaluate ROI:
- How frequently are the target queries run?
- How much do they currently cost without the index?
- Does the maintenance cost exceed the savings?

**Clustering key vs Search Optimization:**

| Feature | Clustering Key | Search Optimization |
|---|---|---|
| Best for | Range queries on high-cardinality columns | Equality point lookups |
| Works on | Columns with natural ranges (dates) | Any column type |
| Cost | Automatic re-clustering credits | Build + maintenance credits |
| Multi-pattern | Single key only | Multiple column patterns |

---

## 10.6 Dynamic Tables

### Question: What are dynamic tables and how do they differ from materialized views?

A Dynamic Table automatically maintains its contents based on a SQL query you define, with a configurable **target lag** (freshness window). Snowflake handles incremental refresh automatically.

```sql
-- Create a dynamic table with 5-minute freshness
CREATE DYNAMIC TABLE sales_by_region
  TARGET_LAG = '5 minutes'
  WAREHOUSE = TRANSFORM_WH
AS
  SELECT
      r.region_name,
      DATE_TRUNC('day', f.order_date) AS sale_day,
      SUM(f.revenue) AS total_revenue,
      COUNT(*) AS order_count
  FROM fact_sales f
  JOIN dim_region r ON f.region_key = r.region_key  -- JOINs work (unlike MVs)!
  GROUP BY 1, 2;

-- Chain dynamic tables (A feeds B):
CREATE DYNAMIC TABLE enriched_sales
  TARGET_LAG = '15 minutes'
  WAREHOUSE = TRANSFORM_WH
AS
  SELECT s.*, c.customer_segment
  FROM sales_by_region s
  JOIN dim_customer c ON s.customer_key = c.customer_key;
-- Snowflake automatically chains the refresh
```

**Dynamic Tables vs Materialized Views:**

| Feature | Materialized View | Dynamic Table |
|---|---|---|
| JOINs | Not supported | Fully supported |
| Complex SQL | Limited | Full SQL |
| Chaining | No | Yes (A → B → C) |
| Lag control | Automatic | User-defined target lag |
| Incremental refresh | Yes (limited) | Yes (automatic) |
| Use case | Simple aggregations | Complex pipelines |

**Monitoring dynamic tables:**
```sql
SELECT name, state, data_timestamp, target_lag, scheduling_state
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
ORDER BY data_timestamp DESC;
```

---

## 10.7 Database Replication and Failover

### Question: What is database replication in Snowflake?

Database Replication copies a Snowflake database to another account — in a different region or different cloud provider — to enable disaster recovery or cross-region data access.

```sql
-- On PRIMARY account: enable replication
ALTER DATABASE prod_db ENABLE REPLICATION TO ACCOUNTS aws_us_east.my_account, azure_eu.my_account;

-- On SECONDARY account: create replica
CREATE DATABASE prod_db_replica
  AS REPLICA OF aws_us_east.my_account.prod_db;

-- Refresh the replica (pull latest changes from primary)
ALTER DATABASE prod_db_replica REFRESH;

-- Check replication status
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATABASE_REPLICATION_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())));
```

**Failover/Failback:**
```sql
-- Promote replica to primary (in disaster scenario)
ALTER DATABASE prod_db_replica PRIMARY;

-- Original primary becomes secondary after failback
ALTER DATABASE prod_db PRIMARY;  -- switch back when original is restored
```

**Use cases:**
- Business continuity / disaster recovery
- Cross-region data access for global teams (lower latency)
- Zero-downtime Snowflake account migrations
- Dev/prod isolation across accounts

---

## 10.8 Horizontal vs Vertical Scaling

### Question: Explain horizontal vs vertical scaling in Snowflake

**Vertical Scaling (Scale Up):**
Increasing the size of a single warehouse (XS → S → M → L → XL → 2XL).
- More CPU per query
- More memory per query — reduces disk spillage
- Faster completion of complex single queries
- Use when: individual queries are slow, disk spillage exists

```sql
ALTER WAREHOUSE my_wh SET WAREHOUSE_SIZE = 'X-LARGE';
```

**Horizontal Scaling (Scale Out):**
Adding more compute clusters to a multi-cluster warehouse while keeping warehouse size the same.
- More queries can run concurrently
- Each cluster still runs at the same size — no individual query gets faster
- Use when: queue time is high, many concurrent users

```sql
ALTER WAREHOUSE my_wh
  SET MAX_CLUSTER_COUNT = 5
      MIN_CLUSTER_COUNT = 1
      SCALING_POLICY = 'ECONOMY';  -- STANDARD for more aggressive scale-out
```

**Choosing the right strategy:**
```
Problem: My query runs slowly on a Medium warehouse
→ Scale UP (try Large, then XL)

Problem: 50 users are queuing for the same Medium warehouse
→ Scale OUT (multi-cluster, add more Medium clusters)

Problem: Both slow queries AND high concurrency
→ Scale UP the warehouse size AND enable multi-cluster
```

---

---

# PART 11 — REAL PROJECT QUESTIONS

---

## 11.1 Explain Your Project End-to-End

### Question: Explain your project end-to-end

**Framework (adapt to your actual project):**

**1. Business Problem:**
"We needed to consolidate sales data from 3 source systems (Salesforce, SAP ERP, and a custom web app) into a single analytics platform so the sales leadership team could get accurate, consistent metrics."

**2. Architecture:**
```
Salesforce API → Fivetran → S3
SAP ERP (nightly export) → S3          → Snowpipe → Raw Layer
Web App Events → Kafka → S3 (micro-batches)

Raw Layer (VARIANT, append-only, 90-day retention)
    │ Streams + Tasks
    ▼
Clean Layer (typed, deduplicated, validated)
    │ dbt models
    ▼
Mart Layer (fact_sales, dim_customer, dim_product)
    │
    ▼
Tableau / Databricks
```

**3. Your role:** "I owned the entire pipeline — from S3 ingestion to mart layer. I designed the Snowflake architecture (table structure, warehouse sizing, RBAC), built the Snowpipe pipelines, wrote the dbt models, and set up monitoring."

**4. Scale:** "50GB/day ingestion, 8 tables, 15 dbt models, 200+ active report users."

**5. Challenges:** "Late-arriving data from the SAP ERP system — sometimes 2-day late reconciliation files. We handled it by using `event_time` not `load_time` for all aggregations, and rebuilding a 7-day rolling window daily."

**6. Outcome:** "Report generation time went from 4 hours (Excel) to 30 seconds. We reduced data inconsistencies between teams by 90% — everyone now references the same Snowflake-powered numbers."

---

## 11.2 Challenges You Faced

Use STAR: Situation → Task → Action → Result.

"Our nightly dbt runs were taking 6+ hours and sometimes failing, blocking morning reporting.

Task: Reduce runtime to under 2 hours without increasing infrastructure cost.

Action:
1. Used Query Profile to identify the 3 slowest dbt models — all doing full table scans on 500M row tables.
2. Added incremental materialization to these models — only processing new data each run.
3. Added clustering keys on date columns for two large tables.
4. Identified 12 models running sequentially that had no dependency — parallelized them in dbt.

Result: Runtime dropped from 6 hours to 90 minutes. No cost increase — warehouse idle time actually reduced. Business gets data 4 hours earlier every morning."

---

## 11.3 How Did You Optimize Performance?

"I noticed our end-of-day reporting queries were timing out for some analysts.

Step 1 — Query Profile: Found the main query was scanning 98% of partitions on a 1.5TB fact table.

Step 2 — Root cause: Filter was `WHERE YEAR(order_date) = 2024`. The `YEAR()` function wrapper prevented Snowflake from using min/max metadata for pruning.

Step 3 — Fix: Rewrote all such filters to `order_date >= '2024-01-01' AND order_date < '2025-01-01'`. Also documented this as a team coding standard.

Step 4 — Result: Partition pruning went from 2% to 97% of partitions scanned. Query time dropped from 3.5 minutes to 18 seconds.

Step 5 — Proactive governance: Added a dbt test that scans for function-wrapped date filters in all models using a SQL linting rule."

---

## 11.4 How Did You Handle Data Quality?

**Multi-layer approach:**

**Layer 1 — Ingestion:**
- COPY_HISTORY monitoring — automated alert on LOAD_FAILED
- Row count check: compare loaded rows to expected range based on historical average
- Null checks on mandatory columns during COPY using VALIDATION_MODE

**Layer 2 — Transform (dbt tests):**
```yaml
# schema.yml in dbt
models:
  - name: fact_sales
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: revenue
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
      - name: customer_key
        tests:
          - relationships:
              to: ref('dim_customer')
              field: customer_key
```

**Layer 3 — Business validation:**
```sql
-- Daily reconciliation: Snowflake total vs source system API total
SELECT
    'fact_sales' AS table_name,
    COUNT(*) AS snowflake_row_count,
    SUM(revenue) AS snowflake_revenue
FROM fact_sales
WHERE order_date = CURRENT_DATE - 1;
-- Compare to source system's API count for the same date
```

**Layer 4 — Anomaly detection:**
```sql
-- Alert if today's count is >50% below 7-day average
WITH daily_stats AS (
    SELECT order_date, COUNT(*) AS daily_count
    FROM fact_sales
    WHERE order_date >= CURRENT_DATE - 8
    GROUP BY order_date
)
SELECT
    CURRENT_DATE AS today,
    COUNT(*) AS today_count,
    AVG(daily_count) OVER () AS avg_7day,
    COUNT(*) / AVG(daily_count) OVER () AS ratio
FROM fact_sales, daily_stats
WHERE order_date = CURRENT_DATE
HAVING ratio < 0.5;  -- alert if 50% below normal
```

---

## 11.5 How Did You Design the Pipeline?

"I follow a 4-layer architecture: Raw → Cleaned → Conformed → Mart.

**Raw layer:** Land data exactly as received from source. VARIANT for API data, typed staging tables for CSVs. Add `_loaded_at` and `_source_file` metadata columns. Never transform here. This is the recovery layer — if anything breaks downstream, I can reprocess from raw.

**Cleaned layer:** Parse types, handle nulls, standardize formats (date formats, text case), deduplicate using QUALIFY ROW_NUMBER(). No business logic yet — purely technical cleaning.

**Conformed layer:** Apply business rules (revenue = gross_amount - discount_amount - returns). Build dimension tables with SCD Type 2. Build fact tables with proper surrogate keys. Implement referential integrity.

**Mart layer:** Business-specific pre-joined views and aggregations for each domain team (sales mart, finance mart, product mart). This is what Tableau, Databricks, and APIs connect to directly.

**For orchestration:** Snowpipe for real-time ingestion. Snowflake Tasks for incremental transforms. dbt for the SQL transform layer with built-in testing.

**Key design principle:** Every layer is reprocessable. If something breaks in the mart layer, I rebuild from the cleaned layer. If cleaned is broken, I rebuild from raw. If raw is gone — we have a problem, so we keep raw data for 90 days minimum."

---

## 11.6 How Do You Handle Failures?

**Prevention:**
```sql
-- Idempotent pipelines: running twice = same result as once
-- Use MERGE instead of INSERT
-- Wrap in transactions: either all changes commit or none do
```

**Detection:**
```sql
-- Task monitoring
SELECT name, state, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(...))
WHERE state = 'FAILED';

-- Pipeline completeness check (data expected but missing)
SELECT expected_date
FROM date_dimension
WHERE date_actual BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE - 1
  AND date_actual NOT IN (SELECT DISTINCT order_date FROM fact_sales);
```

**Response (runbook):**

1. Identify the failure layer (Snowpipe? Task? dbt model?)
2. Read the error message — most failures have clear error text
3. Determine the data gap (what time range is missing?)
4. Fix root cause
5. Backfill the gap:
```sql
-- Reprocess from raw for specific date range
INSERT INTO clean_orders
SELECT ... FROM raw_orders
WHERE DATE(_loaded_at) = '2024-01-15'
  AND DATE(_loaded_at) NOT IN (SELECT DISTINCT order_date FROM clean_orders);
```

**Post-mortem:**
Every significant failure gets a written root cause analysis. The fix addresses both the immediate issue AND adds a detection mechanism (monitoring query or dbt test) to catch the same failure pattern in the future.

---

---

# PART 12 — BEHAVIORAL QUESTIONS

---

## 12.1 Why Do You Want to Switch?

"I've had a great experience at my current company — I built and owned the end-to-end data pipeline for [X], which gave me deep experience in Snowflake, dbt, and pipeline design at scale.

I'm looking to switch because I want [pick your real reason: bigger scale, more ownership, different domain, leadership opportunity, better engineering culture]. When I saw this role at your company, [specific thing about the company/role] really stood out as the kind of challenge I'm looking for next.

I'm particularly excited about [specific technical aspect of the new role] — it aligns well with what I've been building toward."

**What NOT to say:** Never say anything negative about your current employer, team, or manager. Frame everything as moving toward something, not away from something.

---

## 12.2 Describe Your Current Role

"I'm a Data Engineer at [Company]. I own the end-to-end data pipeline from source systems to analytics-ready data in Snowflake.

Day-to-day: I build and maintain ingestion pipelines using Snowpipe and COPY INTO, write and optimize dbt transformation models, manage our Snowflake environment (warehouse sizing, cost optimization, access control, monitoring), and collaborate with data analysts on data model design.

Scale: We process roughly [X] GB/day across [N] source systems, supporting [N] analysts and business users.

My most impactful work in the last year was [specific project]. I [specific action] which resulted in [specific metric: reduced query time by X%, cut pipeline cost by $Y, enabled Z new use cases for the business]."

---

## 12.3 Your Contribution in the Project

Be specific. Avoid vague team credits.

"My primary contribution was the ingestion layer. I designed and built the Snowpipe pipeline from our S3 data lake to Snowflake — handling 50GB/day across 12 data sources. I created the storage integrations, external stages, file formats, and the monitoring framework that alerts on load failures within 5 minutes.

I also owned the dbt models for the core fact tables — fact_orders and fact_payments — including SCD Type 2 logic for dim_customer using Snowflake Streams.

The performance optimization work was mine — I identified and resolved a partition pruning issue that reduced our main report query from 4 minutes to 18 seconds.

I worked closely with the analytics team weekly to refine data models and ran our data quality review meetings."

---

## 12.4 Describe a Challenge You Solved

"Situation: Our sales dashboard that sales leadership used every morning was showing inconsistent revenue figures — sometimes duplicates appeared after the nightly load.

Task: Find the root cause and fix it permanently.

Action:
1. Queried COPY_HISTORY and found our scheduled COPY INTO job was running twice some mornings — the Airflow DAG was timing out on the success check, then retrying, causing double loads.
2. Added a load tracking table that records each file path with a hash of its content. Before every COPY INTO, we check if the hash is already recorded.
3. Added a dbt test that runs after every load: row count reconciliation against the source system's API total. Any >5% discrepancy sends a Slack alert.

Result: Duplicates eliminated entirely. The monitoring also caught two other subtle data quality issues we hadn't noticed before. The sales team regained full trust in the dashboard within one week, and our daily active user count on the dashboard went from 12 to 34 — people were avoiding it before because they didn't trust the numbers."

---

---

# PART 13 — TRICKY / DIFFERENTIATOR QUESTIONS

---

## 13.1 Why Snowflake over Redshift/BigQuery?

### Question: Why choose Snowflake over Amazon Redshift or Google BigQuery?

**Snowflake vs Redshift:**

| Aspect | Snowflake | Redshift |
|---|---|---|
| Compute-storage separation | True separation (any scale) | Coupled in classic; Serverless is partial |
| Zero-copy cloning | Yes — instant | No equivalent |
| Multi-cloud | AWS + Azure + GCS natively | AWS only (Redshift-native) |
| Semi-structured data | VARIANT with FLATTEN | SUPER type (less mature) |
| Data sharing | Cross-account without ETL | Limited |
| Time Travel | Up to 90 days | No equivalent |
| Concurrency | Multi-cluster auto-scale | Manual concurrency scaling |
| Maintenance | Zero (fully managed SaaS) | Cluster tuning required (classic) |

**Snowflake vs BigQuery:**

| Aspect | Snowflake | BigQuery |
|---|---|---|
| Cost model | Predictable credits/hour | Per-TB scanned (can surprise) |
| Multi-cloud | AWS + Azure + GCS | GCP only |
| SQL features | Streams, Tasks, Time Travel, Cloning | Fewer native pipeline features |
| Security | Dynamic masking, Row Access Policies | Column-level security, row policies |
| ML integration | Snowpark ML, Cortex AI | Vertex AI (deeper GCP integration) |
| Data sharing | Marketplace + cross-account | Analytics Hub |

**Honest answer for the interview:**
Snowflake isn't always the right choice. If the company is all-in on GCP and has heavy ML needs → BigQuery + Vertex AI is a strong stack. If deeply invested in AWS with existing Redshift expertise → migration cost must justify benefits. Snowflake wins when: multi-cloud flexibility matters, cross-account data sharing is needed, zero-maintenance SaaS is preferred, or the team is building on multiple clouds simultaneously.

---

## 13.2 When NOT to Use Snowflake?

### Question: When would you NOT recommend Snowflake?

This question tests maturity. The right answer shows you can think critically.

**1. Transactional / OLTP workloads:**
Snowflake is OLAP. For high-frequency small inserts, point-lookups with sub-millisecond latency, row-level updates — use PostgreSQL, MySQL, or Aurora.

**2. Very small data with tight budget:**
Snowflake has a cost floor (minimum account costs, storage minimums). For 10–50GB of data, DuckDB (free, embedded) or a small PostgreSQL instance is far cheaper and simpler.

**3. Sub-second real-time streaming:**
Snowpipe latency is 30–120 seconds minimum. For true real-time event processing with sub-second latency — use Apache Kafka with ClickHouse or Apache Druid.

**4. Online ML inference:**
Don't use Snowflake for serving ML predictions in real-time API calls. Use a dedicated model serving layer (SageMaker endpoints, FastAPI + Redis).

**5. Pure GCP stack with heavy ML needs:**
If the entire engineering stack is GCP and the ML team is on Vertex AI, BigQuery's native integration is superior. Migration to Snowflake would create unnecessary friction.

**6. Deep Redshift investment with no clear gain:**
If a team has years of optimized Redshift queries, established processes, and deep expertise — the migration cost and disruption must clearly justify the benefits. Sometimes it doesn't.

> **Say in interview:** "I evaluate tools based on the specific requirements, not brand preference. The honest answer is that Snowflake is the right choice for most analytics workloads, but I've seen teams waste resources migrating to Snowflake for workloads where their existing setup was perfectly sufficient."

---

## 13.3 Cost vs Performance Tradeoff

### Question: How do you balance cost and performance in Snowflake?

**The key levers:**

**Warehouse sizing — cost vs speed:**
```
XL warehouse = 2x cost of L, but often 2x faster
→ Same total cost, better user experience
→ But if L is already fast enough — XL wastes money

Rule: Right-size to meet SLA, not to maximize speed
      Interactive users: queries under 5 seconds = success
      Batch: finish before business hours = success
```

**Auto-suspend settings:**
```sql
-- Aggressive (60s): Saves credits. Lose warm cache each time. Good for sporadic use.
ALTER WAREHOUSE adhoc_wh SET AUTO_SUSPEND = 60;

-- Conservative (300s): Warmer cache for repeated queries. Good for BI dashboards.
ALTER WAREHOUSE reporting_wh SET AUTO_SUSPEND = 300;
```

**Result cache (free queries):**
Identical queries within 24 hours cost zero compute. The optimization is ensuring queries are parameterized consistently — same text = cache hit.

**Clustering key ROI:**
```
Clustering credit cost per month: X credits
Query savings from pruning per month: Y credits
If Y > X → clustering is cost-positive
If Y < X → remove clustering key
```

**Time Travel retention trade-off:**
```sql
-- 90-day retention on a 1TB table with 10% daily change:
-- ~27TB of Time Travel storage at ~$23/TB/month = ~$621/month in storage alone

-- Reduce to 14 days:
-- ~2.8TB → ~$64/month. Saves ~$557/month if 90-day history isn't actually needed
ALTER TABLE fact_events SET DATA_RETENTION_TIME_IN_DAYS = 14;
```

**Monitoring for cost governance:**
```sql
-- Top 10 credit-consuming queries this week
SELECT LEFT(query_text, 100), execution_time/1000 AS seconds, warehouse_name
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY execution_time DESC
LIMIT 10;
```

---

## 13.4 Explain Your Design Decisions

### Question: How do you explain your design decisions in an interview?

**Framework: State the decision → State the alternative → Explain the trade-off → State why you chose what you chose.**

**Example 1 — VARIANT raw layer:**
"I chose to land all data as VARIANT in the raw layer rather than typed columns. The alternative was typed staging tables which query faster and fail early on bad data. The trade-off: VARIANT is slightly more storage and slightly slower to query in raw. But the benefit: when any of our 15 source APIs changed their schema, no pipeline code changed — we just updated the transform layer. Given the high rate of schema changes we experienced (3–4 per month across all sources), the stability benefit far outweighed the minor performance cost."

**Example 2 — Snowpipe over batch COPY:**
"I chose Snowpipe over scheduled COPY INTO for our event stream. The alternative was cheaper per-GB using our own warehouse. The trade-off: Snowpipe's serverless billing is slightly higher. But the benefit: sub-60-second data freshness for our operations team who needed near-real-time order data. The extra cost was ~$200/month — the business value of 60-second vs 30-minute freshness far exceeded that."

**Example 3 — Streams + Tasks over dbt for CDC:**
"For CDC pipelines, I chose Snowflake Streams + Tasks over Debezium + Kafka. The alternative provides true sub-second latency. The trade-off: Snowflake Streams has 1–5 minute latency. But the benefit: no additional infrastructure (no Kafka cluster, no Debezium connectors, no schema registry). For our use case (daily reporting with 5-minute acceptable lag), the operational simplicity saved 2 weeks of setup and ongoing maintenance overhead."

---

## 13.5 How Do You Handle Trade-offs in Architecture?

### Question: How do you make architecture decisions with competing trade-offs?

"I follow three steps:

**Step 1 — Define constraints precisely:**
What is the latency requirement? (30 seconds? 5 minutes? 24 hours?)
What is the budget? (credits per month)
What is the team's skillset? (can we maintain Kafka?)
What is the data volume? (MB/day vs TB/day)
These constraints eliminate most options immediately.

**Step 2 — Identify the primary tension:**
Cost vs freshness? Simplicity vs flexibility? Operational overhead vs feature richness?

**Step 3 — Prototype and measure:**
For one decision (Streams+Tasks vs Dynamic Tables), I built both approaches on a small data sample. Measured: data freshness achieved, development time, maintenance burden, credit cost. Dynamic Tables won on development time (70% less code) and operational simplicity, at the cost of slightly less control over exact scheduling. Given our team size, that trade-off was right.

**I document decisions as Architecture Decision Records (ADRs):**
```
Decision: Use Dynamic Tables for the order summary pipeline
Date: 2024-02-15
Status: Accepted

Context: We need ~5-minute freshness for the order summary mart
Options evaluated: Dynamic Tables, Streams+Tasks, dbt+Airflow
Decision: Dynamic Tables
Rationale: Achieves 5-minute freshness with 60% less code than Streams+Tasks.
           No external scheduler dependency.
           Acceptable for our SLA.
Trade-offs accepted: Less control over exact scheduling.
                     Feature still maturing in Snowflake.
```

Six months later, when someone asks why we made this choice — the ADR answers it completely."

---

## 13.6 ETL vs ELT in Snowflake

### Question: What is the difference between ETL and ELT? How does Snowflake fit?

**ETL (Extract → Transform → Load):**
```
Source → [Extract] → [Transform in external tool] → [Load to warehouse]
                          (Informatica, SSIS,
                           custom scripts)
```
- Transformation happens BEFORE loading
- Data arrives clean and structured
- Best for: Legacy on-premise warehouses with limited compute
- Limitation: External compute for transforms = bottleneck, slower, harder to maintain

**ELT (Extract → Load → Transform):**
```
Source → [Extract] → [Load raw to warehouse] → [Transform inside warehouse]
                                                      (SQL, dbt, Snowpark)
```
- Data loaded raw first, transformation happens INSIDE the warehouse
- Leverage the warehouse's own compute for transforms
- Best for: Cloud data warehouses with elastic compute (Snowflake, BigQuery)

**Why Snowflake is ideal for ELT:**
1. Elastic compute — spin up a large warehouse for transforms, scale down after
2. Separation of storage and compute — store raw data cheaply, transform on demand
3. SQL power — complex transforms via window functions, arrays, JSON parsing in SQL
4. Snowpark — Python/Scala transforms that run inside Snowflake without data movement
5. Streams + Tasks — built-in incremental ELT pipeline primitives

**Modern ELT stack with Snowflake:**
```
Source Systems
    │
    │ (Fivetran / Airbyte / Snowpipe — extract + load)
    ▼
Raw Layer (Snowflake — VARIANT or typed)
    │
    │ (dbt / Snowpark / Snowflake Tasks — transform inside Snowflake)
    ▼
Mart Layer (Snowflake — analytics-ready)
    │
    │ (Tableau / Power BI / Databricks)
    ▼
Business Users
```

> **Say in interview:** "We use the ELT pattern. We load raw data as fast as possible into Snowflake — then all transformation logic lives in dbt models that run inside Snowflake. The advantage: transforms are version-controlled SQL, tested with dbt tests, and run on elastic compute. If we need to reprocess 2 years of history, we scale up the warehouse and run it in minutes."

---

---

*End of Snowflake Interview Guide*

*Covers 100+ questions across 13 categories with detailed answers, SQL examples, diagrams, and interview-ready talking points.*

*Good luck with your interviews!*
