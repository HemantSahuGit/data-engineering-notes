# System Design for Data Engineers — Comprehensive Reference
> Based on: Fundamentals of Data Engineering, Designing Data-Intensive
Applications, Streaming Systems, The Data Warehouse Toolkit, Data
Engineering with Python, Data Pipelines Pocket Reference, Designing Cloud
Data Platforms
---
## Table of Contents
### Part 1 — Data Engineering Foundations
- [1.1 The Data Engineering Lifecycle](#11-the-data-engineering-lifecycle)
- [1.2 Undercurrents of Data Engineering](#12-undercurrents-of-dataengineering)
- [1.3 Data Engineering Maturity Model](#13-data-engineering-maturity-model)
- [1.4 Data Architecture Principles](#14-data-architecture-principles)
- [1.5 Types of Data](#15-types-of-data)
- [1.6 Source Systems](#16-source-systems)
- [1.7 OLTP vs OLAP Systems](#17-oltp-vs-olap-systems)
- [1.8 Change Data Capture (CDC)](#18-change-data-capture-cdc)
- [1.9 Batch vs Streaming](#19-batch-vs-streaming)
- [1.10 Real-World Technology Selection](#110-real-world-technologyselection)
### Part 2 — Distributed Systems & Databases
- [2.1 Storage Engines: B-Trees vs LSM Trees](#21-storage-engines-b-treesvs-lsm-trees)
- [2.2 Replication](#22-replication)
- [2.3 Partitioning and Sharding](#23-partitioning-and-sharding)
- [2.4 Transactions: ACID Properties](#24-transactions-acid-properties)
- [2.5 Isolation Levels and Anomalies](#25-isolation-levels-and-anomalies)
- [2.6 Multi-Version Concurrency Control (MVCC)](#26-multi-versionconcurrency-control-mvcc)
- [2.7 Distributed Transactions: Two-Phase Commit](#27-distributedtransactions-two-phase-commit)
- [2.8 Consensus Algorithms: Raft and Paxos](#28-consensus-algorithms-raftand-paxos)
This PDF document was created using the online html editor powered by CKEditor.
- [2.9 CAP Theorem](#29-cap-theorem)
- [2.10 PACELC Theorem](#210-pacelc-theorem)
- [2.11 Consistency Models](#211-consistency-models)
- [2.12 MapReduce and Batch Processing](#212-mapreduce-and-batch-processing)
- [2.13 Apache Kafka Deep Dive](#213-apache-kafka-deep-dive)
- [2.14 Specialized Database Types](#214-specialized-database-types)
- [2.15 Real-World Database Selection](#215-real-world-database-selection)
### Part 3 — Data Warehousing & Dimensional Modeling
- [3.1 The Kimball Four-Step Dimensional Design Process](#31-the-kimballfour-step-dimensional-design-process)
- [3.2 Star Schema](#32-star-schema)
- [3.3 Snowflake Schema](#33-snowflake-schema)
- [3.4 Types of Facts](#34-types-of-facts)
- [3.5 Types of Fact Tables](#35-types-of-fact-tables)
- [3.6 Slowly Changing Dimensions (SCD)](#36-slowly-changing-dimensions-scd)
- [3.7 Conformed Dimensions and the Bus Matrix](#37-conformed-dimensionsand-the-bus-matrix)
- [3.8 Surrogate Keys](#38-surrogate-keys)
- [3.9 Factless Fact Tables](#39-factless-fact-tables)
- [3.10 Data Marts: Kimball vs Inmon](#310-data-marts-kimball-vs-inmon)
- [3.11 Date Dimension](#311-date-dimension)
- [3.12 ETL Patterns and MERGE](#312-etl-patterns-and-merge)
- [3.13 dbt Incremental Models](#313-dbt-incremental-models)
- [3.15 Advanced Dimensional Patterns](#315-advanced-dimensional-patterns)
- [3.16 Real-World Data Warehouse Examples](#316-real-world-data-warehouseexamples)
- [3.17 DWH Technology Selection Guide](#317-dwh-technology-selection-guide)
### Part 4 — Streaming Systems
- [4.1 The What, Where, When, How Framework](#41-the-what-where-when-howframework)
- [4.2 Event Time vs Processing Time](#42-event-time-vs-processing-time)
- [4.3 Windowing Strategies](#43-windowing-strategies)
- [4.4 Watermarks](#44-watermarks)
- [4.5 Triggers and Accumulation Modes](#45-triggers-and-accumulation-modes)
- [4.6 Exactly-Once Semantics](#46-exactly-once-semantics)
- [4.7 Apache Kafka Architecture](#47-apache-kafka-architecture)
This PDF document was created using the online html editor powered by CKEditor.
- [4.8 Apache Flink DataStream API](#48-apache-flink-datastream-api)
- [4.9 Spark Structured Streaming](#49-spark-structured-streaming)
- [4.10 Stream-Table Duality and Kafka Streams](#410-stream-table-dualityand-kafka-streams)
- [4.11 Streaming Design Patterns](#411-streaming-design-patterns)
- [4.15 Kafka vs Kinesis vs Event Hubs](#415-kafka-vs-kinesis-vs-event-hubs)
- [4.16 Real-World Streaming Examples](#416-real-world-streaming-examples)
- [4.17 Streaming vs Batch Decision Matrix](#417-streaming-vs-batchdecision-matrix)
### Part 5 — Data Pipelines & Orchestration
- [5.1 What is a Data Pipeline?](#51-what-is-a-data-pipeline)
- [5.2 ETL vs ELT](#52-etl-vs-elt)
- [5.3 Ingestion Patterns](#53-ingestion-patterns)
- [5.4 Apache Airflow Deep Dive](#54-apache-airflow-deep-dive)
- [5.5 Modern Orchestration: Prefect and Dagster](#55-modern-orchestrationprefect-and-dagster)
- [5.6 Data Ingestion Tools: Fivetran and Airbyte](#56-data-ingestion-toolsfivetran-and-airbyte)
- [5.7 Custom Python Ingestion](#57-custom-python-ingestion)
- [5.8 Data Quality: Great Expectations and dbt](#58-data-quality-greatexpectations-and-dbt)
- [5.9 Pipeline Reliability Patterns](#59-pipeline-reliability-patterns)
- [5.10 PySpark ETL and Delta Lake MERGE](#510-pyspark-etl-and-delta-lakemerge)
- [5.11 Pipeline Observability](#511-pipeline-observability)
- [5.12 Data Contracts](#512-data-contracts)
- [5.13 Real-World Pipeline Examples](#513-real-world-pipeline-examples)
### Part 6 — Cloud Data Platforms
- [6.1 Cloud Platform Overview](#61-cloud-platform-overview)
- [6.2 Medallion Architecture](#62-medallion-architecture)
- [6.3 Cloud Provider Stacks](#63-cloud-provider-stacks)
- [6.4 Lambda Architecture](#64-lambda-architecture)
- [6.5 Kappa Architecture](#65-kappa-architecture)
- [6.6 Lakehouse Architecture](#66-lakehouse-architecture)
- [6.7 Data Mesh](#67-data-mesh)
- [6.8 Cloud Storage](#68-cloud-storage)
This PDF document was created using the online html editor powered by CKEditor.
- [6.9 Snowflake Architecture](#69-snowflake-architecture)
- [6.10 BigQuery Architecture](#610-bigquery-architecture)
- [6.11 Security, RBAC and Compliance](#611-security-rbac-and-compliance)
- [6.12 Reverse ETL and the Modern Data Stack](#612-reverse-etl-and-themodern-data-stack)
- [6.14 Microsoft Fabric Deep Dive](#614-microsoft-fabric-deep-dive)
- [6.15 Real-World Cloud Platform Decisions](#615-real-world-cloud-platformdecisions)
### Part 7 — System Design Patterns & Case Studies
- [7.1 System Design Framework](#71-system-design-framework)
- [7.2 Case Study: Real-Time Fraud Detection](#72-case-study-real-timefraud-detection)
- [7.3 Case Study: Healthcare Data Lake (HIPAA)](#73-case-study-healthcaredata-lake-hipaa)
- [7.4 Case Study: E-commerce Data Warehouse](#74-case-study-e-commercedata-warehouse)
- [7.5 Case Study: Real-Time Analytics for Ride-Sharing](#75-case-studyreal-time-analytics-for-ride-sharing)
- [7.6 Case Study: ML Feature Engineering Platform](#76-case-study-mlfeature-engineering-platform)
- [7.7 Design Patterns Reference](#77-design-patterns-reference)
- [7.8 Terminology Quick Reference](#78-terminology-quick-reference)
- [7.9 Interview Question Bank (50 Questions)](#79-interview-question-bank50-questions)
- [7.10 Anti-Patterns in Data Engineering](#710-anti-patterns-in-dataengineering)
- [7.11 Case Study: IoT Smart Manufacturing](#711-case-study-iot-smartmanufacturing)
- [7.12 Case Study: Social Media Analytics Platform](#712-case-study-socialmedia-analytics-platform)
- [7.13 Consolidated Decision Trees](#713-consolidated-decision-trees)
- [7.14 QPS Claims Analytics Platform on Microsoft Fabric](#714-qps-claimsanalytics-platform-on-microsoft-fabric)
### Part 8 — Apache Spark: Architecture, Internals & Optimization ⭐ NEW
- [8.1 Why Spark Replaced MapReduce](#81-why-spark-replaced-mapreduce)
- [8.2 Spark Architecture](#82-spark-architecture)
This PDF document was created using the online html editor powered by CKEditor.
- [8.3 Lazy Evaluation & DAG Execution](#83-lazy-evaluation--dag-execution)
- [8.4 RDDs vs DataFrames vs Datasets](#84-rdds-vs-dataframes-vs-datasets)
- [8.5 The Catalyst Optimizer](#85-the-catalyst-optimizer)
- [8.6 Spark Memory Model](#86-spark-memory-model)
- [8.7 Partitioning in Spark](#87-partitioning-in-spark)
- [8.8 Join Strategies](#88-join-strategies)
- [8.9 Data Skew & Salting](#89-data-skew--the-silent-performance-killer)
- [8.10 Adaptive Query Execution (AQE)](#810-adaptive-query-execution-aqe--
spark-3x)
- [8.11 Caching & Persistence](#811-caching--persistence)
- [8.12 Spark Performance Tuning Checklist](#812-spark-performance-tuningchecklist)
- [8.13 Spark on Cloud](#813-spark-on-cloud)
- [8.14 Real-World Spark Use Cases](#814-real-world-spark-use-cases)
### Part 9 — SQL Mastery, Query Optimization & Data Normalization ⭐ NEW
- [9.1 Database Normalization (1NF–BCNF)](#91-database-normalization)
- [9.2 SQL Window Functions](#92-sql-window-functions)
- [9.3 Common Table Expressions (CTEs)](#93-common-table-expressions-ctes)
- [9.4 Advanced SQL Patterns for Data Engineers](#94-advanced-sql-patternsfor-data-engineers)
- [9.5 Query Optimization & EXPLAIN Plans](#95-query-optimization--explainplans)
- [9.6 SQL for Data Engineering — Key Patterns](#96-sql-for-dataengineering--key-patterns)
- [9.7 Query Optimization in Distributed Databases](#97-query-optimizationin-distributed-databases)
### Part 10 — File Formats, Open Table Formats & Modern Python Tools ⭐ NEW
- [10.1 File Formats Comparison](#101-file-formats-comparison)
- [10.2 Apache Parquet — Internal Structure](#102-apache-parquet--internalstructure)
- [10.3 Delta Lake Internals — The Transaction Log](#103-delta-lakeinternals--the-transaction-log)
- [10.4 Apache Iceberg — Metadata Architecture](#104-apache-iceberg--
metadata-architecture)
- [10.5 Apache Hudi — Streaming Lakehouse](#105-apache-hudi--streaminglakehouse)
This PDF document was created using the online html editor powered by CKEditor.
- [10.6 Polars — The Modern Alternative to Pandas](#106-polars--the-modernalternative-to-pandas)
- [10.7 DuckDB — Analytical Queries on Local Files](#107-duckdb--analyticalqueries-on-local-files)
### Part 11 — Modern Data Engineering: Vector DBs, Caching, Kubernetes & IaC
⭐ NEW
- [11.1 Vector Databases & AI/LLM Data Pipelines](#111-vector-databases--
aillm-data-pipelines)
- [11.2 Caching Patterns](#112-caching-patterns)
- [11.3 Kubernetes for Data Engineering](#113-kubernetes-for-dataengineering)
- [11.4 Infrastructure as Code — Terraform](#114-infrastructure-as-code--
terraform-for-data-engineering)
- [11.5 Data Versioning — lakeFS](#115-data-versioning--lakefs)
- [11.6 Semantic / Metrics Layer](#116-semantic--metrics-layer)
- [11.7 Real-World Architecture Decisions](#117-real-world-architecturedecisions)
---
> **Total coverage:** ~8,600+ lines | 11 Parts | 100+ Cross Questions | 7
Case Studies | 50-Question Interview Bank
---
# Part 1: Data Engineering Foundations
> Reference: *Fundamentals of Data Engineering* — Joe Reis & Matt Housley
---
## 1.1 What is Data Engineering?
**Data Engineering** is the development, implementation, and maintenance of
systems and processes that take raw data and produce high-quality,
consistent information that supports downstream use cases (analytics, ML,
reporting).
This PDF document was created using the online html editor powered by CKEditor.
A **Data Engineer** sits at the intersection of:
- Software Engineering (building reliable systems)
- Data Science (understanding data needs)
- DevOps (deploying and operating pipelines)
### Key Responsibilities
| Responsibility | Description |
|---|---|
| Data Ingestion | Pulling data from source systems (APIs, DBs, streams) |
| Data Transformation | Cleaning, enriching, aggregating raw data |
| Data Storage | Choosing and managing appropriate storage systems |
| Data Serving | Making processed data available for consumers |
| Orchestration | Scheduling and monitoring pipelines |
| Data Quality | Ensuring accuracy, completeness, and freshness |
---
## 1.2 Data Engineering Lifecycle
The **Data Engineering Lifecycle** is the end-to-end process a data engineer
manages — from raw data ingestion to serving to end consumers.
```mermaid
flowchart LR
 A[Source Systems] --> B[Ingestion]
 B --> C[Storage]
 C --> D[Transformation]
 D --> E[Serving]
 E --> F[Analytics / ML / Reports]
 subgraph Undercurrents
 G[Security]
 H[Data Management]
 I[DataOps]
 J[Data Architecture]
 K[Orchestration]
 L[Software Engineering]
 end
This PDF document was created using the online html editor powered by CKEditor.
```
### Lifecycle Stages Explained
#### 1. Source Systems
The origin of all data. Can be:
- **OLTP databases** (MySQL, PostgreSQL) — transactional systems
- **Event streams** (Kafka topics) — real-time clickstreams
- **SaaS APIs** (Salesforce, Stripe) — third-party platforms
- **Files** (CSV, JSON, Parquet) — batch uploads
- **IoT sensors** — time-series telemetry
**Key questions to ask about source systems:**
- How often does data change?
- What is the data volume and velocity?
- What is the schema — fixed or dynamic?
- Is there an SLA on freshness?
#### 2. Ingestion
Moving data from source systems into the data platform. Two main paradigms:
| Pattern | Description | Tools |
|---|---|---|
| **Batch** | Data moved at scheduled intervals | Airflow, Spark, SSIS |
| **Streaming** | Data moved continuously as events occur | Kafka, Kinesis,
Flink |
| **Micro-batch** | Hybrid — small batches at short intervals | Spark
Structured Streaming |
**Push vs Pull:**
- **Pull**: Ingestion system queries source (e.g., cron + SQL SELECT)
- **Push**: Source system pushes events (e.g., webhooks, Kafka producers)
#### 3. Storage
Where data lives between lifecycle stages. Choices depend on:
- Access patterns (read-heavy vs write-heavy)
- Data structure (structured, semi-structured, unstructured)
- Latency requirements (milliseconds vs hours)
This PDF document was created using the online html editor powered by CKEditor.
```mermaid
graph TD
 A[Storage Options] --> B[Object Storage\\nS3, ADLS, GCS]
 A --> C[Data Warehouses\\nSnowflake, BigQuery, Redshift]
 A --> D[Data Lakes\\nDelta Lake, Iceberg, Hudi]
 A --> E[OLTP Databases\\nPostgres, MySQL]
 A --> F[NoSQL\\nCassandra, MongoDB, DynamoDB]
 A --> G[Cache / In-memory\\nRedis, Memcached]
```
#### 4. Transformation
Converting raw data into a usable form. Also called **T in ETL** or **T in
ELT**.
Transformations include:
- **Cleansing**: removing nulls, fixing formats
- **Deduplication**: removing duplicate records
- **Enrichment**: joining with reference data
- **Aggregation**: computing KPIs, metrics
- **Normalization / Denormalization**: restructuring for query efficiency
Popular tools: **dbt**, **Spark**, **SQL stored procedures**, **Pandas**,
**Flink**
#### 5. Serving
Making data available to consumers:
- **Analytics**: dashboards, reports (Tableau, Power BI)
- **ML / AI**: feature stores, training datasets
- **Reverse ETL**: pushing data back to operational systems
- **APIs**: data products served via REST/GraphQL
---
## 1.3 The Undercurrents
These cross-cutting concerns underpin the entire lifecycle:
This PDF document was created using the online html editor powered by CKEditor.
### Security
- Encrypt data at rest and in transit (TLS, AES-256)
- Role-based access control (RBAC) on tables and columns
- Column-level masking for PII (Personally Identifiable Information)
- Audit logging for all data access
### Data Management
Encompasses: data governance, metadata management, data cataloging, data
lineage, master data management (MDM).
> **Data Catalog**: A centralized inventory of data assets with metadata,
ownership, and lineage (e.g., Collibra, Apache Atlas, DataHub).
> **Data Lineage**: Tracking where data came from, how it was transformed,
and where it went.
### DataOps
Applies DevOps principles to data pipelines:
- CI/CD for data pipelines
- Automated testing (data quality tests)
- Observability and alerting
- Infrastructure as Code (Terraform)
### Data Architecture
The high-level design of how data systems are organized, integrated, and
governed. Principles:
- **Modularity**: Decouple pipeline stages
- **Interoperability**: Use open standards (Parquet, Avro, Arrow)
- **Reversibility**: Allow for architectural evolution
- **Least Privilege**: Minimize access
### Orchestration
Coordinating multi-step pipeline execution:
- Dependency management (task A before task B)
- Retry and failure handling
- Alerting and SLA monitoring
Tools: **Apache Airflow**, **Prefect**, **Dagster**, **Mage**, **dbt Cloud**
This PDF document was created using the online html editor powered by CKEditor.
---
## 1.4 Data Maturity Model
Organizations evolve through stages of data maturity. Understanding where a
company sits helps a data engineer prioritize work.
```mermaid
graph LR
 A[Stage 1\\nStarting with Data] --> B[Stage 2\\nScaling with Data]
 B --> C[Stage 3\\nLeading with Data]
 A1[Ad-hoc queries\\nNo pipelines\\nSpreadsheet BI] --> A
 B1[Formal pipelines\\nData Warehouse\\nBasic Governance] --> B
 C1[Self-serve analytics\\nReal-time pipelines\\nML at scale\\nData Mesh]
--> C
```
### Stage 1: Starting with Data
- Data efforts are ad hoc; everything is manual
- A data engineer here should: get data wins quickly, build reliable
pipelines, establish basic governance
### Stage 2: Scaling with Data
- Formal data infrastructure exists
- A data engineer here should: reduce technical debt, introduce testing,
build a data catalog, implement SLAs
### Stage 3: Leading with Data
- Data is a core business asset
- A data engineer here should: build self-serve platforms, automate
everything, enable ML/AI at scale
---
## 1.5 Data Architecture Principles
This PDF document was created using the online html editor powered by CKEditor.
Good data architecture enables agility, reliability, and cost-effectiveness.
Key principles from Reis & Housley:
### Principle 1: Choose Common Components Wisely
Shared components (e.g., object storage, orchestrator) affect all teams.
Choose battle-tested, open options when possible.
### Principle 2: Plan for Failure
Every component will fail. Design for:
- **Fault tolerance**: Automatic recovery
- **Graceful degradation**: Partial service during outages
- **Idempotency**: Safe to rerun without duplicating data
### Principle 3: Architect for Scalability
- **Vertical scaling** (scale-up): Add more CPU/RAM to one machine — limited
ceiling
- **Horizontal scaling** (scale-out): Add more machines — preferred for
distributed systems
### Principle 4: Architecture is Leadership
Data architects set the technical direction. Good architecture enables teams
to move fast independently.
### Principle 5: Always Be Architecting
Architecture is never "done." Continuously re-evaluate as business needs,
data volumes, and technologies change.
---
## 1.6 Types of Data
| Type | Description | Examples |
|---|---|---|
| **Structured** | Fixed schema, tabular | Relational DB tables, CSV |
| **Semi-structured** | Flexible schema, self-describing | JSON, XML, Avro,
Parquet |
| **Unstructured** | No inherent schema | Images, video, audio, text |
This PDF document was created using the online html editor powered by CKEditor.
### Data Formats Comparison
| Format | Type | Encoding | Splittable | Schema | Use Case |
|---|---|---|---|---|---|
| CSV | Row | Text | Yes (with care) | No | Simple interchange |
| JSON | Row | Text | No | No | APIs, configs |
| Avro | Row | Binary | Yes | Yes (embedded) | Kafka streaming |
| Parquet | Columnar | Binary | Yes | Yes | Analytics, OLAP |
| ORC | Columnar | Binary | Yes | Yes | Hive, Spark OLAP |
| Delta Lake | Columnar | Binary | Yes | Yes + ACID | Lakehouse |
> **Why Columnar?** Analytics queries often read only a subset of columns.
Columnar formats store all values for a column together, enabling efficient
compression and skipping irrelevant data.
---
## 1.7 Source Systems Deep Dive
### OLTP vs OLAP
| Characteristic | OLTP | OLAP |
|---|---|---|
| Purpose | Transactional operations | Analytical queries |
| Query type | Many small reads/writes | Few large scans |
| Schema | Normalized (3NF) | Denormalized (star schema) |
| Latency | Milliseconds | Seconds to minutes |
| Examples | PostgreSQL, MySQL, Oracle | Snowflake, BigQuery, Redshift |
### Change Data Capture (CDC)
CDC is the process of detecting and capturing changes (INSERT, UPDATE,
DELETE) in a source database and propagating them downstream.
```mermaid
sequenceDiagram
 participant App as Application
 participant DB as Source DB (PostgreSQL)
 participant CDC as CDC Tool (Debezium)
This PDF document was created using the online html editor powered by CKEditor.
 participant Kafka as Kafka
 participant DWH as Data Warehouse
 App->>DB: INSERT / UPDATE / DELETE
 DB->>CDC: WAL (Write-Ahead Log) change events
 CDC->>Kafka: Publish change event
 Kafka->>DWH: Consume and apply changes
```
**CDC Approaches:**
1. **Log-based CDC** (preferred): Reads the database transaction log (WAL in
Postgres, binlog in MySQL) — no query overhead
2. **Query-based CDC**: Polls source with `WHERE updated_at > last_run` —
simpler but misses deletes
3. **Trigger-based CDC**: DB triggers write to audit tables — high overhead
Tools: **Debezium**, **AWS DMS**, **Fivetran**, **Airbyte**
---
## 1.8 Batch vs Streaming — When to Use What
```mermaid
graph TD
 A{Data Freshness\\nRequirement?}
 A -->|Hours / Days| B[Batch Processing]
 A -->|Minutes| C[Micro-batch]
 A -->|Seconds / Sub-second| D[Real-time Streaming]

 B --> B1[Spark, Hive, dbt\\nAirflow, SSIS]
 C --> C1[Spark Structured Streaming\\nFlink with checkpoints]
 D --> D1[Apache Flink\\nKafka Streams\\nApache Storm]
```
### Batch Processing Characteristics
- Simple to reason about
- Reprocessable — can rerun from scratch
- High throughput, higher latency
This PDF document was created using the online html editor powered by CKEditor.
- Good for: nightly reports, ML training datasets, data warehouse loads
### Streaming Processing Characteristics
- Low latency, continuous output
- Complex to reason about (out-of-order events, failures)
- Good for: fraud detection, real-time dashboards, alerting
---
## 1.9 Cross Questions — Data Engineering Foundations
**Q1: What is the difference between a Data Engineer and a Data Scientist?**
> A Data Engineer builds and maintains the infrastructure for data
collection, storage, and processing. A Data Scientist analyzes and models
data to extract insights. Data Engineers are the plumbers; Data Scientists
are the chefs.
**Q2: What is the Data Engineering Lifecycle and why is it important?**
> The lifecycle covers Ingestion → Storage → Transformation → Serving,
underpinned by security, governance, orchestration, and DataOps. It gives a
mental model for designing complete data systems rather than solving point
problems.
**Q3: When would you choose streaming over batch?**
> Streaming is preferred when business SLAs require data freshness under a
few minutes (fraud detection, real-time recommendations). Batch is simpler
and preferred for overnight reporting, ML training, and historical
analytics.
**Q4: What is CDC and what are its advantages over query-based extraction?**
> CDC (Change Data Capture) captures every INSERT, UPDATE, DELETE from a
source DB. Log-based CDC reads the WAL without querying, which adds zero
overhead to the source and captures all changes including deletes — unlike a
timestamp-based poll which misses hard-deleted rows.
**Q5: What is a Data Maturity Model and how does it guide a data engineer?**
> It's a framework describing how data-driven an organization is (from adhoc Stage 1 to self-serve Stage 3). It guides engineers to pick the right
This PDF document was created using the online html editor powered by CKEditor.
battles — building quick wins in Stage 1 vs. scaling and automating in Stage
3.
**Q6: What are "undercurrents" in the data engineering lifecycle?**
> They are cross-cutting concerns that apply to every lifecycle stage:
Security, Data Management, DataOps, Data Architecture, Orchestration, and
Software Engineering. They're invisible but foundational.
**Q7: Why is idempotency critical in data pipelines?**
> Pipelines fail and get rerun. An idempotent pipeline produces the same
result regardless of how many times it runs — preventing data duplication
and corruption on retries.
**Q8: Explain the difference between ETL and ELT.**
> ETL (Extract-Transform-Load): Data is transformed before loading — used
when the destination has limited compute. ELT (Extract-Load-Transform): Raw
data is loaded first, then transformed using the warehouse's compute —
preferred in modern cloud DWHs (Snowflake, BigQuery) because compute is
elastic and cheap.
---
## 1.10 Real-World Technology Selection Guide
> "What should I use and why?" — the most common question in data
engineering interviews and on the job.
### Ingestion Tool Selection
```mermaid
flowchart TD
 A{Source Type?}
 A -->|SaaS apps\\nSalesforce, Stripe, Shopify| B[Fivetran /
Airbyte\\nPre-built connectors\\nNo code needed]
 A -->|Relational DB CDC| C[Debezium + Kafka\\nLog-based, zero-overhead]
 A -->|REST/GraphQL API| D[Custom Python\\nrequests + Airflow]
 A -->|Real-time events| E[Kafka Producer\\nor AWS Kinesis]
 A -->|Files: CSV/JSON/Parquet| F[Airflow file sensor\\n+ Spark/pandas
This PDF document was created using the online html editor powered by CKEditor.
loader]
```
| Company | Problem | Tool Chosen | Why |
|---|---|---|---|
| **Uber** | 1M+ trips/day across 500+ microservices | Kafka + Flink | Need
real-time event streaming with complex aggregations; high volume requires
distributed architecture |
| **Netflix** | 200M+ members generating events | Kafka + Apache Spark |
Kafka for event backbone; Spark for batch training jobs on viewing history |
| **Airbnb** | Nightly ETL from 100+ sources | Apache Airflow (they built
it!) | Complex multi-step DAG dependencies; scheduling across teams |
| **LinkedIn** | Own data to own analytics | Apache Kafka (they built it!) |
Born from the need to stream activity data across services without tight
coupling |
| **Shopify** | SaaS customer analytics | Fivetran + Snowflake | Fast timeto-value; no engineer needed to maintain 50+ connectors |
| **Stripe** | Financial event streaming | Kafka + internal tools | ACIDlevel reliability needed for payment events; can't afford message loss |
### Storage Technology Selection
```mermaid
flowchart TD
 Q1{What is the\\nread/write pattern?}
 Q1 -->|Transactional\\nMany small R/W| OLTP[PostgreSQL / MySQL\\nACID
guaranteed\\nNormalized schema]
 Q1 -->|Analytical\\nFew large scans| OLAP[Snowflake /
BigQuery\\nColumnar, MPP\\nDenormalized]
 Q1 -->|High-write\\nTime series / IoT| TS[TimescaleDB / InfluxDB\\nLSMtree based\\nAuto-compression]
 Q1 -->|Key-value lookups\\nSub-millisecond| KV[Redis / DynamoDB\\nInmemory or hash-based]
 Q1 -->|Graph traversal\\nNetwork relationships| GR[Neo4j / Amazon
Neptune\\nGraph-native traversal]
 Q1 -->|Full-text search| SRCH[Elasticsearch / OpenSearch\\nInverted
index]
 Q1 -->|Mixed: batch + ML + BI| LH[Delta Lake / Iceberg\\nData Lakehouse]
This PDF document was created using the online html editor powered by CKEditor.
```
| Scenario | Technology | Real Company Example | Reason |
|---|---|---|---|
| User session data, sub-ms reads | **Redis** | Twitter (tweet counters),
Snapchat (friend lists) | In-memory, O(1) lookup, TTL support |
| Social graph — "friends of friends" | **Neo4j** | LinkedIn (connections),
Facebook (social graph) | Graph queries in SQL are exponentially slow;
graph-native is O(depth) |
| IoT sensor data, 1M writes/sec | **Cassandra / InfluxDB** | Apple (iCloud
device telemetry), Tesla (vehicle telemetry) | LSM tree write speed; timepartitioned data model |
| Search product catalog | **Elasticsearch** | Amazon, Zalando | Inverted
index; fuzzy search, faceted filtering |
| Analytical reporting | **Snowflake** | DoorDash, Capital One |
Compute/storage separation; SQL-native; handles 10TB+ |
| ML feature store | **Redis + S3** | Spotify (recommendations), Instacart |
Redis for low-latency online serving; S3 for offline training |
### Transformation Tool Selection
| Scenario | Tool | Why |
|---|---|---|
| SQL-based transformations in DWH | **dbt** | Version-controlled SQL,
testing, lineage, incremental models |
| Large-scale data processing > 100GB | **Apache Spark** | Distributed inmemory; handles data that doesn't fit on one machine |
| Real-time stream transformation | **Apache Flink** | Event-time semantics,
exactly-once, stateful operations |
| Quick prototyping, small data | **Pandas** | Simple, familiar, fast for <
10GB on one machine |
| Lightweight SQL on files | **DuckDB** | Runs in-process on Parquet/CSV; no
cluster needed; excellent for analytics on laptop |
### Real-World: What DoorDash Uses
```mermaid
graph LR
This PDF document was created using the online html editor powered by CKEditor.
 subgraph Ingestion
 K[Kafka\\nOrder + delivery events]
 FT[Fivetran\\nSalesforce, Stripe]
 end
 subgraph Storage
 S3[S3 + Delta Lake\\nRaw + processed events]
 SF[Snowflake\\nDWH for BI]
 end
 subgraph Processing
 SP[Spark on Databricks\\nBatch ETL]
 FL[Flink\\nReal-time driver ETA]
 end
 subgraph Serving
 TB[Tableau\\nBusiness dashboards]
 ML[ML Platform\\nDelivery time prediction]
 end
 K --> S3 --> SP --> SF --> TB
 K --> FL --> ML
 FT --> SF
```
**Why each choice:**
- **Kafka**: Decouples order service from 20+ downstream consumers
(analytics, notifications, fraud)
- **Delta Lake on S3**: Cheap storage + ACID for upserts (order status
changes)
- **Snowflake**: Elastic compute; DoorDash scales to 100K orders on peak
nights without pre-provisioning
- **Flink**: Sub-second driver ETA calculation needs event-time processing
of GPS events
### Real-World: What Optum/UnitedHealth Uses
```mermaid
graph LR
 subgraph Source Systems
 EHR[EHR Systems\\nEpic, Cerner]
 CLM[Claims Processing\\nSQL Server]
This PDF document was created using the online html editor powered by CKEditor.
 PHA[Pharmacy\\nSFTP CSV drops]
 end
 subgraph Platform - Azure
 ADLS[ADLS Gen2\\nData Lake - Bronze/Silver]
 SYN[Azure Synapse\\nDWH + Spark]
 ADF[Azure Data Factory\\nBatch Ingestion ETL]
 PV[Microsoft Purview\\nData Governance]
 end
 subgraph Serving
 PBI[Power BI\\nClinical Dashboards]
 API[Internal APIs\\nQuality Scores, Risk Scores]
 end
 EHR --> ADF --> ADLS
 CLM --> ADF --> ADLS
 PHA --> ADF --> ADLS
 ADLS --> SYN --> PBI
 SYN --> API
 PV -.-> ADLS
 PV -.-> SYN
```
**Why Azure ecosystem:** HIPAA compliance, Microsoft enterprise agreements,
Purview for PHI governance, native integration across ADF → ADLS → Synapse.
---
# Part 2: Distributed Systems & Storage for Data Engineers
> Reference: *Designing Data-Intensive Applications* — Martin Kleppmann
---
## 2.1 Why Distributed Systems Matter for Data Engineers
Modern data platforms process terabytes-to-petabytes of data. No single
machine can handle this. Distributed systems spread data and computation
across many machines — but introduce complexity around:
- **Reliability**: What happens when nodes crash?
- **Scalability**: How do we handle growing data volume?
- **Maintainability**: How do we evolve systems over time?
This PDF document was created using the online html editor powered by CKEditor.
---
## 2.2 Storage Engines: How Databases Work Internally
Understanding internals helps data engineers choose the right database and
explain performance characteristics.
### B-Tree Index (Used in OLTP)
```
 [30 | 60]
 / | \\
 [10|20] [40|50] [70|80]
```
- **B-Trees** split data into fixed-size pages (typically 4KB)
- Reads are O(log n) — great for point lookups and range queries
- Writes update pages in-place — durable but write-heavy
- Used in: **PostgreSQL, MySQL (InnoDB), Oracle**
### LSM Tree + SSTable (Used in Write-heavy NoSQL)
```mermaid
flowchart LR
 A[Write] --> B[MemTable\\nin-memory sorted buffer]
 B -->|flush when full| C[SSTable L0\\non disk]
 C -->|compaction| D[SSTable L1]
 D -->|compaction| E[SSTable L2 ...]

 F[Read] --> G{Check MemTable}
 G -->|not found| H{Check L0 SSTables}
 H -->|not found| I{Check L1 ...}
```
- **LSM Trees** (Log-Structured Merge Trees) write to an in-memory buffer
first → periodically flush to disk as immutable **SSTables** (Sorted String
Tables)
This PDF document was created using the online html editor powered by CKEditor.
- Compaction merges and removes stale/deleted data
- **Bloom filters** avoid checking SSTables that don't contain a key
- Used in: **Cassandra, HBase, RocksDB, LevelDB**
| Aspect | B-Tree | LSM Tree |
|---|---|---|
| Write performance | Slower (in-place update) | Faster (sequential append)
|
| Read performance | Faster | Slower (multiple levels) |
| Space amplification | Lower | Higher (before compaction) |
| Write amplification | Lower | Higher (compaction rewrites) |
| Best for | OLTP, point queries | High-throughput writes, time series |
---
## 2.3 Data Replication
**Replication** means keeping copies of data on multiple nodes (replicas)
for fault tolerance and read scaling.
### Leader–Follower Replication (Master–Slave)
```mermaid
graph TD
 C[Client Write] --> L[Leader Node]
 L -->|Replication log| F1[Follower 1]
 L -->|Replication log| F2[Follower 2]
 CR[Client Read] --> F1
 CR2[Client Read] --> F2
```
- All **writes go to the leader**
- Followers replicate the leader's write log
- **Reads can go to followers** — scales read throughput
**Replication Lag**: Followers may be seconds behind the leader.
- **Read-your-writes consistency**: User reads their own writes — may need
to route to leader temporarily
This PDF document was created using the online html editor powered by CKEditor.
- **Monotonic reads**: User never reads older data than they already saw
### Synchronous vs Asynchronous Replication
| Type | Behavior | Tradeoff |
|---|---|---|
| **Synchronous** | Leader waits for follower to confirm write | Strong
consistency, lower availability |
| **Asynchronous** | Leader doesn't wait | Higher availability, risk of data
loss on leader failure |
| **Semi-synchronous** | One follower is sync, rest are async | Balance of
durability and performance |
### Multi-Leader Replication
Multiple nodes accept writes. Used for:
- Multi-datacenter deployments
- Offline-first mobile apps
**Challenge**: Write conflicts when two leaders modify the same record.
- **Last Write Wins (LWW)**: Keep the write with the highest timestamp —
risk of data loss
- **Conflict-free Replicated Data Types (CRDTs)**: Data structures designed
to merge automatically
### Leaderless Replication (Dynamo-style)
Used by: **Cassandra, DynamoDB, Riak**
```mermaid
graph LR
 C[Client] -->|Write to N nodes| N1[Node 1]
 C -->|Write to N nodes| N2[Node 2]
 C -->|Write to N nodes| N3[Node 3]

 Note["W + R > N = Quorum\\nTypically: N=3, W=2, R=2"]
```
**Quorum reads/writes**:
- N = replication factor (e.g., 3)
This PDF document was created using the online html editor powered by CKEditor.
- W = write quorum (e.g., 2 — write must succeed on 2 nodes)
- R = read quorum (e.g., 2 — read from 2 nodes, take latest)
- If **W + R > N**, you get overlap → consistent reads
---
## 2.4 Data Partitioning (Sharding)
**Partitioning** splits a large dataset across multiple nodes so each node
holds a subset. Essential for scaling beyond a single machine.
### Partition by Key Range
```
Node 1: A–G
Node 2: H–N
Node 3: O–Z
```
- Natural ordering enables range scans
- Risk of **hot spots**: If all writes go to one key prefix (e.g.,
timestamps starting with "2024-")
### Partition by Hash of Key
```mermaid
graph LR
 A[Key: user_123] -->|hash function| B[hash = 47]
 B -->|47 mod 3 = 2| C[Node 2]
```
- Uniform distribution across nodes
- Loses key ordering — can't do efficient range queries
- Used in: Cassandra (consistent hashing), MongoDB
### Consistent Hashing
```
This PDF document was created using the online html editor powered by CKEditor.
 0
 / \\
 270 90
 \\ /
 180
Node A at 45, Node B at 165, Node C at 285
Key hash at 100 → goes to Node B (next clockwise)
```
- Adding/removing nodes only moves a fraction of keys
- Used in distributed caches and NoSQL stores
### Secondary Indexes in Partitioned Databases
**Local secondary index** (document-based): Each partition maintains its own
index for its data.
- Writes are efficient (local)
- Reads scatter across all partitions (scatter-gather)
**Global secondary index** (term-based): Index itself is partitioned
differently from data.
- Reads are efficient (query specific partition of index)
- Writes require updating multiple partitions
---
## 2.5 Transactions & ACID
### ACID Properties
| Property | Meaning |
|---|---|
| **Atomicity** | All operations in a transaction succeed or all are rolled
back |
| **Consistency** | Database moves from one valid state to another |
| **Isolation** | Concurrent transactions don't interfere with each other |
| **Durability** | Committed data persists even after a crash |
This PDF document was created using the online html editor powered by CKEditor.
### Isolation Levels
```mermaid
graph TD
 A[Read Uncommitted\\nLowest isolation] --> B[Read Committed\\nNo dirty
reads]
 B --> C[Repeatable Read\\nNo non-repeatable reads]
 C --> D[Serializable\\nHighest isolation\\nNo phantom reads]
```
| Isolation Level | Dirty Read | Non-repeatable Read | Phantom Read |
|---|---|---|---|
| Read Uncommitted | Possible | Possible | Possible |
| Read Committed | No | Possible | Possible |
| Repeatable Read | No | No | Possible |
| Serializable | No | No | No |
- **Dirty Read**: Reading uncommitted data from another transaction
- **Non-repeatable Read**: Re-reading a row returns different data (another
txn updated it)
- **Phantom Read**: Re-reading a range returns different rows (another txn
inserted)
**PostgreSQL defaults to Read Committed; MySQL InnoDB defaults to Repeatable
Read.**
### Snapshot Isolation (MVCC)
Most databases implement isolation using **Multi-Version Concurrency Control
(MVCC)**:
- Each transaction sees a consistent snapshot of the database
- Old versions of rows are kept alongside new ones
- Readers don't block writers; writers don't block readers
---
## 2.6 Distributed Transactions & Consensus
This PDF document was created using the online html editor powered by CKEditor.
### Two-Phase Commit (2PC)
```mermaid
sequenceDiagram
 participant C as Coordinator
 participant N1 as Node 1
 participant N2 as Node 2
 C->>N1: PREPARE
 C->>N2: PREPARE
 N1->>C: VOTE YES
 N2->>C: VOTE YES
 C->>N1: COMMIT
 C->>N2: COMMIT
```
- Phase 1 (Prepare): Coordinator asks all participants if they can commit
- Phase 2 (Commit/Abort): If all say yes → commit; otherwise → abort
- **Problem**: If coordinator crashes after PREPARE, participants are
blocked ("in-doubt transactions")
### Consensus Algorithms
Used to agree on a single value across distributed nodes even if some nodes
fail.
| Algorithm | Used In | Key Property |
|---|---|---|
| **Paxos** | ZooKeeper (internally) | Foundational, complex |
| **Raft** | etcd, CockroachDB | Understandable alternative to Paxos |
| **Zab** | Apache ZooKeeper | Primary-backup protocol |
**Raft in brief:**
1. One node is elected **leader** (via election timeout + votes)
2. Leader receives all writes, appends to log
3. Entries committed once acknowledged by majority (quorum)
4. On leader failure, new election occurs
---
This PDF document was created using the online html editor powered by CKEditor.
## 2.7 The CAP Theorem
> **CAP Theorem (Brewer's Theorem)**: A distributed system can guarantee at
most **two** of the following three:
```mermaid
graph TD
 A[CAP Theorem]
 A --> C[Consistency\\nAll nodes see same data at same time]
 A --> AV[Availability\\nEvery request gets a response]
 A --> P[Partition Tolerance\\nSystem works despite network failures]

 CP[CP Systems\\nHBase, Zookeeper\\nMongoDB] -.-> C
 CP -.-> P
 AP[AP Systems\\nCassandra, CouchDB\\nDynamoDB] -.-> AV
 AP -.-> P
 CA[CA Systems\\nSingle-node RDBMS\\nNot realistic for distributed] -.->
C
 CA -.-> AV
```
**In practice**: Network partitions always happen in distributed systems. So
you're choosing between **CP** (sacrifice availability during partition) or
**AP** (sacrifice consistency during partition). Pure CA is only possible
for single-node systems.
### PACELC Model (Extension of CAP)
> **PACELC**: If Partition → choose Availability or Consistency; **Else**
(normal operation) → choose Latency or Consistency.
| System | Partition choice | Else choice |
|---|---|---|
| DynamoDB | AP | EL (latency) |
| Cassandra | AP | EL (latency) |
| HBase | CP | EC (consistency) |
| PostgreSQL (multi-master) | CP | EC (consistency) |
This PDF document was created using the online html editor powered by CKEditor.
---
## 2.8 Consistency Models
A spectrum of guarantees about what readers can observe:
```mermaid
graph LR
 A[Strict Linearizability\\nStrongest] --> B[Linearizability]
 B --> C[Sequential Consistency]
 C --> D[Causal Consistency]
 D --> E[Eventual Consistency\\nWeakest]
```
| Model | Guarantee | Example |
|---|---|---|
| **Linearizability** | Operations appear instantaneous; after a write
completes, all reads see new value | Single-node database |
| **Sequential Consistency** | Operations appear in some global order
consistent with program order | Some distributed locks |
| **Causal Consistency** | Causally related operations appear in order;
unrelated can be reordered | Version vectors, CRDTs |
| **Eventual Consistency** | All replicas converge to the same value
*eventually* if no new writes occur | Cassandra, DynamoDB |
### Eventual Consistency Example (Cassandra)
```
Time 0: Node A and B both have user.email = alice@old.com
Time 1: Write email = alice@new.com → Node A (success)
Time 2: Read from Node B → still returns alice@old.com ← inconsistent!
Time 3: Replication catches up → Node B = alice@new.com ← eventually
consistent
```
---
## 2.9 Batch Processing with MapReduce
This PDF document was created using the online html editor powered by CKEditor.
**MapReduce** is the original distributed batch processing paradigm (Google,
2004).
```mermaid
flowchart LR
 A[Input\\nHDFS Blocks] --> B[Map Phase\\nProcess each record\\nEmit keyvalue pairs]
 B --> C[Shuffle & Sort\\nGroup by key\\nMove across network]
 C --> D[Reduce Phase\\nAggregate values\\nPer key]
 D --> E[Output\\nHDFS Blocks]
```
**Word Count Example (MapReduce):**
```
Input: "the cat sat the cat"
Map: (the,1),(cat,1),(sat,1),(the,1),(cat,1)
Shuffle: (cat,[1,1]),(sat,[1]),(the,[1,1])
Reduce: (cat,2),(sat,1),(the,2)
```
**Limitations of MapReduce:**
- Writes intermediate results to disk between stages → slow for iterative
algorithms
- No native support for streaming
- High programming overhead
**Apache Spark** superseded MapReduce by keeping intermediate data in-memory
(RDDs).
---
## 2.10 Stream Processing in Distributed Systems
Key challenge: **Events arrive out of order** due to network delays.
This PDF document was created using the online html editor powered by CKEditor.
### Event Time vs Processing Time
| Time Type | Definition |
|---|---|
| **Event Time** | When the event actually occurred (timestamp in the event)
|
| **Processing Time** | When the event arrived at the processing system |
| **Ingestion Time** | When the event entered the message broker (Kafka) |
```
Real world: E1(t=1) E2(t=2) E3(t=3)
Network delay: E1 arrives at t=1.5, E3 at t=2.5, E2 at t=4 ← out of order!
Processing time window [0-3]: would incorrectly exclude E2
Event time window [0-3]: includes E2 if we wait long enough (watermark)
```
### Fault Tolerance in Stream Processing
**Checkpointing**: Periodically save processing state to durable storage. On
failure, restore from last checkpoint.
**Delivery Semantics:**
| Semantic | Meaning | How |
|---|---|---|
| **At-most-once** | May lose messages | No retry |
| **At-least-once** | May duplicate messages | Retry on failure |
| **Exactly-once** | No loss, no duplication | Idempotent writes +
transactions |
---
## 2.11 Message Brokers: Kafka Deep Dive
Apache Kafka is the de facto standard for distributed event streaming.
This PDF document was created using the online html editor powered by CKEditor.
```mermaid
graph TD
 subgraph Producers
 P1[App Server]
 P2[CDC / Debezium]
 end

 subgraph Kafka Cluster
 T[Topic: orders\\nPartition 0\\nPartition 1\\nPartition 2]
 end

 subgraph Consumers
 CG1[Consumer Group A\\nStream Processor]
 CG2[Consumer Group B\\nData Warehouse Loader]
 end

 P1 --> T
 P2 --> T
 T --> CG1
 T --> CG2
```
### Kafka Core Concepts
| Concept | Description |
|---|---|
| **Topic** | Named stream of records (like a database table) |
| **Partition** | A topic is split into ordered, immutable log segments |
| **Offset** | Sequential ID of a record within a partition |
| **Consumer Group** | Multiple consumers share partition load; each
partition assigned to one consumer |
| **Broker** | A Kafka server hosting partitions |
| **Replication Factor** | Number of copies of each partition |
| **Leader/Follower** | One leader per partition; followers replicate |
### Kafka Ordering Guarantee
- Messages are ordered **within a partition**
- No ordering guaranteed **across partitions**
This PDF document was created using the online html editor powered by CKEditor.
- Use a **partition key** (e.g., `user_id`) to ensure related events go to
the same partition
### Kafka Retention
Kafka retains messages for a configurable period (default 7 days) or size —
regardless of whether they've been consumed. This enables:
- **Replay**: Reprocess historical events
- **Multiple consumers**: Independent consumer groups at different offsets
- **Decoupling**: Producer and consumer speeds don't need to match
---
## 2.12 Encoding & Schema Evolution
Data formats matter for long-term maintainability and interoperability.
### Formats Comparison
| Format | Language-neutral | Schema | Versioning | Use Case |
|---|---|---|---|---|
| JSON | Yes | No | Manual | APIs, configs |
| XML | Yes | Optional (XSD) | Manual | Legacy enterprise |
| **Avro** | Yes | Yes (embedded) | Schema registry | Kafka, Hadoop |
| **Protobuf** | Yes | Yes (.proto files) | Field numbers | gRPC, Google |
| **Thrift** | Yes | Yes | Field IDs | Meta internal |
### Schema Evolution Rules (Avro/Protobuf)
**Backward compatible**: New schema can read data written with old schema
- Safe: Add optional fields with defaults
- Unsafe: Remove required fields, change field types
**Forward compatible**: Old schema can read data written with new schema
- Safe: Remove fields, add fields with defaults
- Unsafe: Add required fields without defaults
**Schema Registry** (Confluent/AWS Glue): Central store for schemas.
Producers register schemas; consumers fetch them. Enforces compatibility
This PDF document was created using the online html editor powered by CKEditor.
rules.
---
## 2.13 Cross Questions — Distributed Systems
**Q1: Explain the CAP theorem with a real-world data engineering example.**
> During a network partition in Cassandra (AP system): if a write goes to
Node A and replication to Node B fails, a subsequent read from Node B
returns stale data (sacrifices consistency for availability). In HBase (CP),
during a partition, some reads return errors rather than stale data
(sacrifices availability for consistency).
**Q2: What is the difference between linearizability and eventual
consistency?**
> Linearizability means once a write completes, any subsequent read anywhere
in the system sees that value — it behaves as if there's only one copy.
Eventual consistency only guarantees that replicas *will* converge, but a
read immediately after a write might still return old data.
**Q3: When would you use an LSM tree database vs a B-Tree database?**
> LSM tree (Cassandra, RocksDB) when write throughput is the priority —
e.g., ingesting IoT sensor data at millions of writes/sec. B-tree
(PostgreSQL) when read latency matters and writes are moderate — e.g.,
transactional order management.
**Q4: How does Kafka guarantee message ordering?**
> Kafka only guarantees ordering within a single partition. To ensure all
events for a given entity (e.g., a user's orders) are ordered, produce them
to the same partition using a consistent partition key (e.g., `user_id`).
**Q5: What is MVCC and why do databases use it?**
> MVCC (Multi-Version Concurrency Control) keeps multiple versions of a row
so readers and writers don't block each other. A transaction reads its
consistent snapshot without needing locks. This is why PostgreSQL can handle
high concurrency without excessive locking.
**Q6: What is the difference between at-least-once and exactly-once
This PDF document was created using the online html editor powered by CKEditor.
delivery? How do you achieve exactly-once?**
> At-least-once retries on failure, potentially producing duplicates.
Exactly-once ensures no duplicates and no data loss — achieved by combining
idempotent writes (safe to replay) with transactional semantics (Kafka
transactions or Flink's checkpointing with idempotent sinks).
**Q7: Explain consistent hashing and why it matters for data systems.**
> Consistent hashing maps both data keys and nodes to a ring. A key belongs
to the first node clockwise from its position. When nodes are added/removed,
only a fraction (1/N) of keys need to move — minimizing data reshuffling.
Used in Cassandra, distributed caches.
**Q8: What is schema evolution and why does it matter in a streaming data
pipeline?**
> Schema evolution is the ability to change data schemas over time without
breaking producers or consumers. In a streaming pipeline with Avro + Schema
Registry, adding an optional field with a default is backward compatible —
old consumers can still read new messages. Without schema management, a
producer changing a field name breaks all downstream consumers.
**Q9: How does 2PC work and what are its failure modes?**
> Two-Phase Commit: Phase 1 — coordinator sends PREPARE to all participants
who vote yes/no. Phase 2 — if all vote yes, coordinator sends COMMIT.
Failure mode: if coordinator crashes after all participants voted yes but
before sending COMMIT, participants are in-doubt and hold locks indefinitely
until the coordinator recovers. This is called a "blocking commit protocol."
---
## 2.14 Missing Storage Systems — Graph, Search, Time-Series
### Graph Databases — Neo4j / Amazon Neptune
**When to use:** When the relationships between data points ARE the data.
SQL JOINs across deeply nested relationships become exponentially expensive.
```mermaid
graph LR
This PDF document was created using the online html editor powered by CKEditor.
 U1[User: Alice] -->|FOLLOWS| U2[User: Bob]
 U2 -->|FOLLOWS| U3[User: Carol]
 U1 -->|LIKED| P1[Post: Data Engineering 101]
 P1 -->|TAGGED| T1[Tag: #DataEng]
 U3 -->|COMMENTED_ON| P1
```
**Cypher Query (Neo4j) — "Who are friends of Alice within 2 hops?"**
```cypher
MATCH (alice:User {name: 'Alice'})-[:FOLLOWS*1..2]->(friend:User)
WHERE friend <> alice
RETURN DISTINCT friend.name
ORDER BY friend.name;
-- Equivalent SQL would require 2 self-joins — slow and complex at scale
```
**Real-world uses:**
| Company | Problem | Why Graph DB |
|---|---|---|
| **LinkedIn** | "People you may know" recommendations | Traverse 3-hop
connection graph across 900M nodes |
| **PayPal** | Fraud detection via transaction network | Identify fraud
rings: A pays B, B pays C, C pays A in a loop |
| **Walmart** | Product recommendation engine | Graph of "bought together"
relationships across 100M products |
| **NASA** | Knowledge graph for space missions | Complex dependency graph
between mission components |
**Key interview point:** Use Neo4j/Neptune when you have many-to-many
relationships with variable depth traversal. If your SQL query has 4+ selfjoins, consider graph.
---
### Elasticsearch / OpenSearch — Full-Text Search & Log Analytics
**When to use:** Full-text search, fuzzy matching, log aggregation, realThis PDF document was created using the online html editor powered by CKEditor.
time analytics over documents.
```mermaid
flowchart LR
 A[Data Source\\nLogs, Products, Docs] -->|Logstash / Fluentd\\nor direct
API| E[Elasticsearch\\nInverted Index]
 E --> K[Kibana\\nDashboards]
 E --> API[Search API\\nFull-text queries]
```
**How Elasticsearch works (Inverted Index):**
```
Document 1: "Apache Kafka is a distributed event streaming platform"
Document 2: "Kafka enables real-time data pipelines"
Inverted Index:
 "kafka" → [Doc1, Doc2]
 "streaming" → [Doc1]
 "real-time" → [Doc2]
 "pipeline" → [Doc2]
Query: "kafka streaming"
→ Intersect [Doc1, Doc2] ∩ [Doc1] = [Doc1] ← Instant lookup, no full scan
```
**Elasticsearch Query Example:**
```json
GET /products/_search
{
 "query": {
 "bool": {
 "must": {
 "match": { "name": "running shoes" }
 },
 "filter": [
 { "term": { "brand": "Nike" } },
 { "range": { "price": { "gte": 50, "lte": 200 } } }
 ]
This PDF document was created using the online html editor powered by CKEditor.
 }
 },
 "aggs": {
 "by_size": { "terms": { "field": "size" } }
 }
}
```
**Real-world uses:**
| Company | Use Case | Why Elasticsearch |
|---|---|---|
| **Amazon** | Product search with faceting | Inverted index for instant
text lookup; facets for filtering by category/price |
| **Uber** | Real-time log analysis (500GB/day) | ELK stack for
troubleshooting microservice failures |
| **Wikipedia** | Site search | Full-text search across 60M articles in
milliseconds |
| **GitHub** | Code search | Search across billions of files by keyword |
---
### Time-Series Databases — InfluxDB / TimescaleDB / Prometheus
**When to use:** Data where time is the primary axis — IoT telemetry,
metrics, monitoring, financial tick data.
**Why not PostgreSQL for time-series?**
- No automatic compression for repeated values over time
- No automatic data retention/downsampling policies
- Performance degrades without manual partition management
**TimescaleDB** (PostgreSQL extension):
```sql
-- Create hypertable (auto-partitioned by time)
CREATE TABLE sensor_readings (
 time TIMESTAMPTZ NOT NULL,
 sensor_id TEXT,
This PDF document was created using the online html editor powered by CKEditor.
 temperature DOUBLE PRECISION,
 humidity DOUBLE PRECISION
);
SELECT create_hypertable('sensor_readings', 'time');
-- Continuous aggregate: auto-updated hourly averages
CREATE MATERIALIZED VIEW sensor_hourly
WITH (timescaledb.continuous) AS
SELECT
 time_bucket('1 hour', time) AS bucket,
 sensor_id,
 AVG(temperature) AS avg_temp,
 MAX(temperature) AS max_temp
FROM sensor_readings
GROUP BY bucket, sensor_id
WITH NO DATA;
-- Retention policy: drop data older than 90 days
SELECT add_retention_policy('sensor_readings', INTERVAL '90 days');
```
**InfluxDB** — purpose-built, not PostgreSQL-based:
```sql
-- InfluxDB Flux query: average CPU per host last 1 hour
from(bucket: "infrastructure_metrics")
 |> range(start: -1h)
 |> filter(fn: (r) => r._measurement == "cpu_usage")
 |> filter(fn: (r) => r._field == "usage_percent")
 |> aggregateWindow(every: 5m, fn: mean)
 |> group(columns: ["host"])
```
| Database | Best For | Real-World User |
|---|---|---|
| **InfluxDB** | Metrics, monitoring, pure time-series | Cisco, Tesla
(vehicle telemetry), NASA |
| **TimescaleDB** | Time-series + SQL familiarity, mixed queries | Grafana,
Walmart (supply chain) |
This PDF document was created using the online html editor powered by CKEditor.
| **Prometheus** | Kubernetes/microservice metrics scraping | Every cloudnative company |
| **Apache Druid** | Real-time OLAP on event streams | Netflix (A/B
testing), Airbnb (analytics) |
| **ClickHouse** | High-throughput OLAP, sub-second on billions |
Cloudflare, ByteDance (TikTok analytics) |
---
## 2.15 Real-World Database Selection Guide
```mermaid
flowchart TD
 A{Primary\\nWorkload?}
 A -->|Transactional\\nACID required| B{Scale?}
 B -->|Single region, moderate| PG[PostgreSQL\\nStandard OLTP]
 B -->|Global, high write| CK[CockroachDB\\nDistributed ACID]

 A -->|Analytics\\nRead-heavy| C{Data Size?}
 C -->|< 10GB, single node| DDB[DuckDB\\nIn-process analytics]
 C -->|10GB–10TB, cloud| SFL[Snowflake / BigQuery\\nCloud DWH]
 C -->|Real-time, low latency| CH[ClickHouse / Druid\\nReal-time OLAP]

 A -->|High write throughput\\nTime-series / IoT| D{Query Pattern?}
 D -->|Time-range queries| TS[TimescaleDB / InfluxDB]
 D -->|Key-value or wide-row| CAS[Cassandra / HBase]

 A -->|Graph traversal| GR[Neo4j / Amazon Neptune]
 A -->|Full-text search| ES[Elasticsearch / OpenSearch]
 A -->|Caching sub-ms lookup| RD[Redis / Memcached]
```
### Real Company Database Choices
| Company | System | Database Used | Key Reason |
|---|---|---|---|
| **Uber** | Trip data (current state) | MySQL (Vitess sharding) | ACID for
money-critical trip lifecycle |
This PDF document was created using the online html editor powered by CKEditor.
| **Uber** | Driver GPS positions | Cassandra | 1M+ writes/sec; eventual
consistency fine for GPS |
| **Netflix** | Viewing history | Cassandra | High write throughput; lowlatency reads; global replication |
| **Netflix** | Search & recommendations | Elasticsearch | Full-text search
+ inverted index for title lookup |
| **Airbnb** | Listing metadata | MySQL + Redis | MySQL for source of truth;
Redis for fast listing reads |
| **Twitter** | Tweet storage | Manhattan (custom Cassandra-like) | 500M
tweets/day write throughput |
| **Slack** | Message history | MySQL (sharded) + Solr | MySQL for ACID
message writes; Solr for full-text search |
| **Spotify** | Playlist & song metadata | Cassandra | 456M users; global
replication; eventual consistency ok |
| **LinkedIn** | Professional graph | Voldemort (custom) + Espresso | Keyvalue for profile data; graph traversal for connections |
---
# Part 3: Data Warehousing & Dimensional Modeling
> Reference: *The Data Warehouse Toolkit* — Ralph Kimball & Margy Ross
---
## 3.1 What is a Data Warehouse?
A **Data Warehouse (DWH)** is a centralized repository of integrated data
from one or more disparate sources, optimized for analytical querying
(OLAP). It stores historical data and supports business intelligence
reporting.
### Data Warehouse vs Data Lake vs Lakehouse
| Characteristic | Data Warehouse | Data Lake | Data Lakehouse |
|---|---|---|---|
| Data type | Structured only | All types | All types |
| Schema | Schema-on-write | Schema-on-read | Schema-on-write (with
flexibility) |
| Query engine | Built-in SQL | Spark, Hive, Athena | Spark, SQL engines |
This PDF document was created using the online html editor powered by CKEditor.
| ACID support | Yes | No (unless Delta/Iceberg) | Yes (Delta, Iceberg,
Hudi) |
| Cost | High (compute + storage together) | Low (cheap object storage) |
Medium |
| Examples | Snowflake, Redshift, BigQuery | S3 + Glue, ADLS | Databricks,
Delta Lake |
---
## 3.2 Kimball's Dimensional Modeling Philosophy
Ralph Kimball's approach is **business-process oriented**:
> "Build the data warehouse around the business processes (not org charts),
using dimensional models that are intuitive and high-performance."
### Four-Step Dimensional Modeling Process
```mermaid
flowchart TD
 A[Step 1: Select the Business Process\\ne.g. Sales Orders, Patient
Visits] --> B[Step 2: Declare the Grain\\ne.g. One row per order line item]
 B --> C[Step 3: Identify the Dimensions\\ne.g. Customer, Product, Date,
Store]
 C --> D[Step 4: Identify the Facts\\ne.g. Quantity, Revenue, Discount]
```
### Step 1: Business Process
Select a measurable event the business performs:
- Sales orders processed
- Hotel bookings made
- Insurance claims filed
- Website page views
### Step 2: Grain
The grain defines **exactly what one row in the fact table represents**.
This is the most critical decision.
**Examples:**
This PDF document was created using the online html editor powered by CKEditor.
- "One row per order line item" (finest grain for orders)
- "One row per hospital visit per day" (daily summary grain)
- "One row per patient per care plan" (event grain)
> The grain must be declared first — dimensions and facts follow from it.
Never mix grains in one fact table.
### Step 3: Dimensions
Descriptive context for measurements. Answer the **who, what, where, when,
why, how** of a business event.
### Step 4: Facts
Numeric measurements that result from a business event.
---
## 3.3 Star Schema
The **Star Schema** is the primary dimensional model structure. A central
fact table surrounded by dimension tables.
```mermaid
erDiagram
 FACT_SALES {
 int sales_key PK
 int date_key FK
 int customer_key FK
 int product_key FK
 int store_key FK
 decimal quantity_sold
 decimal revenue
 decimal discount_amount
 decimal profit
 }
 DIM_DATE {
 int date_key PK
 date full_date
This PDF document was created using the online html editor powered by CKEditor.
 int year
 int quarter
 int month
 string month_name
 int week_number
 string day_name
 boolean is_holiday
 }
 DIM_CUSTOMER {
 int customer_key PK
 string customer_id
 string first_name
 string last_name
 string email
 string city
 string state
 string country
 }
 DIM_PRODUCT {
 int product_key PK
 string product_id
 string product_name
 string category
 string subcategory
 string brand
 decimal unit_cost
 }
 DIM_STORE {
 int store_key PK
 string store_id
 string store_name
 string region
 string district
 string manager_name
 }
This PDF document was created using the online html editor powered by CKEditor.
 FACT_SALES }o--|| DIM_DATE : "sold on"
 FACT_SALES }o--|| DIM_CUSTOMER : "sold to"
 FACT_SALES }o--|| DIM_PRODUCT : "of product"
 FACT_SALES }o--|| DIM_STORE : "at store"
```
**Why Star Schema?**
- Simple, intuitive structure
- Fewer joins → fast query performance
- Easy for BI tools to auto-generate queries
- Optimized for columnar storage engines
---
## 3.4 Snowflake Schema
An extension of star schema where dimension tables are **normalized** into
multiple related tables.
```mermaid
erDiagram
 FACT_SALES {
 int sales_key PK
 int product_key FK
 }
 DIM_PRODUCT {
 int product_key PK
 string product_name
 int subcategory_key FK
 }
 DIM_SUBCATEGORY {
 int subcategory_key PK
 string subcategory_name
 int category_key FK
 }
This PDF document was created using the online html editor powered by CKEditor.
 DIM_CATEGORY {
 int category_key PK
 string category_name
 }
 FACT_SALES }o--|| DIM_PRODUCT : ""
 DIM_PRODUCT }o--|| DIM_SUBCATEGORY : ""
 DIM_SUBCATEGORY }o--|| DIM_CATEGORY : ""
```
**Star vs Snowflake:**
| Aspect | Star Schema | Snowflake Schema |
|---|---|---|
| Normalization | Denormalized dimensions | Normalized dimensions |
| Join complexity | Fewer joins | More joins |
| Query performance | Faster | Slower |
| Storage | More (duplicated data) | Less |
| Maintenance | Simpler | More complex |
| Kimball recommendation | **Preferred** | Avoid for analytics |
---
## 3.5 Types of Facts
### Additive Facts
Can be summed across **all** dimensions.
- Revenue, quantity sold, profit
- `SUM(revenue) GROUP BY month, region` — perfectly valid
### Semi-Additive Facts
Can be summed across **some** but not all dimensions.
- Account balance — makes sense to sum across accounts, but not across time
(you can't sum today's balance + yesterday's balance)
- Use `AVG` or snapshot logic across time
### Non-Additive Facts
This PDF document was created using the online html editor powered by CKEditor.
Cannot be meaningfully summed across **any** dimension.
- Unit price, ratios, percentages
- Store the numerator and denominator separately, compute ratio at query
time
### Fact Table Types
```mermaid
graph TD
 A[Fact Table Types]
 A --> B[Transaction Fact\\nOne row per transaction event\\nHighest
grain, most detail]
 A --> C[Periodic Snapshot Fact\\nOne row per period per entity\\ne.g.
Monthly account balance]
 A --> D[Accumulating Snapshot Fact\\nOne row per process
instance\\nUpdated as process progresses\\ne.g. Insurance claim lifecycle]
```
**Accumulating Snapshot Example (Loan Application):**
```sql
CREATE TABLE fact_loan_pipeline (
 loan_key INT,
 application_date_key INT,
 approval_date_key INT, -- NULL until approved
 funding_date_key INT, -- NULL until funded
 close_date_key INT, -- NULL until closed
 loan_amount DECIMAL,
 days_to_approval INT,
 days_to_funding INT,
 current_status VARCHAR(50)
)
```
Each row is updated as the loan moves through stages — unlike transaction
facts which are immutable.
---
## 3.6 Slowly Changing Dimensions (SCD)
This PDF document was created using the online html editor powered by CKEditor.
Dimensions change over time. SCDs define how to handle these changes.
### SCD Type 0 — Retain Original
Never change the dimension. Used for attributes that should never change.
- Example: Customer's original signup date
### SCD Type 1 — Overwrite
Update the record in place. Old value is lost.
```sql
UPDATE dim_customer
SET email = 'newemail@example.com'
WHERE customer_key = 1234;
```
- **Pros**: Simple, no history overhead
- **Cons**: History lost — can't report "what was the email when this order
was placed?"
- Use when: Historical accuracy of the attribute doesn't matter (fix typos)
### SCD Type 2 — Add New Row (Most Common)
Add a new row for each change, with validity date ranges. Maintains full
history.
```sql
-- Before change
| customer_key | customer_id | city | is_current | valid_from |
valid_to |
|--------------|-------------|-----------|------------|------------|--------
----|
| 1001 | C-001 | New York | TRUE | 2022-01-01 | 9999-
12-31 |
-- After customer moves to Boston
| customer_key | customer_id | city | is_current | valid_from |
valid_to |
|--------------|-------------|-----------|------------|------------|--------
----|
| 1001 | C-001 | New York | FALSE | 2022-01-01 | 2024-
This PDF document was created using the online html editor powered by CKEditor.
03-15 | ← expire old
| 1005 | C-001 | Boston | TRUE | 2024-03-16 | 9999-
12-31 | ← new row
```
**SCD Type 2 Query Pattern:**
```sql
-- Get customer location at time of order
SELECT o.order_id, c.city, o.revenue
FROM fact_orders o
JOIN dim_customer c ON o.customer_key = c.customer_key
 AND o.order_date BETWEEN c.valid_from AND c.valid_to
WHERE o.order_date = '2024-03-10';
```
- **Pros**: Full history preserved, correct historical reporting
- **Cons**: Table grows with each change, complex joins
### SCD Type 3 — Add New Attribute Column
Add a column for "previous value" alongside "current value".
```sql
| customer_key | city_current | city_previous | change_date |
|---|---|---|---|
| 1001 | Boston | New York | 2024-03-16 |
```
- **Pros**: Simple, single row per customer
- **Cons**: Only tracks one previous value (not full history)
### SCD Type 4 — History Table
Keep current values in main dimension table; full history in a separate
history table.
### SCD Type 6 — Hybrid (Type 1 + 2 + 3)
Combines Type 1 (overwrite some fields), Type 2 (new rows for tracked
changes), and Type 3 (current + previous column).
This PDF document was created using the online html editor powered by CKEditor.
```
SCD Type 6 = 1 + 2 + 3
```
---
## 3.7 Conformed Dimensions
A **Conformed Dimension** is a dimension shared across multiple fact tables
(and data marts) with the same meaning.
```mermaid
graph TD
 DD[DIM_DATE\\nConformed] --> FS[FACT_SALES]
 DD --> FI[FACT_INVENTORY]
 DD --> FHR[FACT_HR_HEADCOUNT]
 DC[DIM_CUSTOMER\\nConformed] --> FS
 DC --> FM[FACT_MARKETING_SPEND]
```
**Why Conformed Dimensions?**
- Enable cross-process analysis (join sales and inventory on same date
dimension)
- Consistent definitions across the enterprise (one definition of
"customer")
- Foundation of the **Enterprise Data Warehouse Bus Architecture**
### Data Warehouse Bus Matrix
A planning tool showing which dimensions apply to which business processes:
| Business Process | Date | Customer | Product | Store | Employee |
|---|---|---|---|---|---|
| Sales | ✓ | ✓ | ✓ | ✓ | |
| Inventory | ✓ | | ✓ | ✓ | |
| HR | ✓ | | | ✓ | ✓ |
| Marketing | ✓ | ✓ | ✓ | | |
This PDF document was created using the online html editor powered by CKEditor.
---
## 3.8 Surrogate Keys vs Natural Keys
**Surrogate Key**: System-generated integer key for dimension rows
(`customer_key = 1001`).
**Natural Key**: Business identifier from source system (`customer_id = 'C2024-00123'`).
### Why Surrogate Keys?
| Reason | Explanation |
|---|---|
| SCD Type 2 support | Multiple rows for same natural key; surrogate
distinguishes them |
| Source system independence | Natural keys change (company acquisitions,
system migrations) |
| Performance | Integer joins faster than string joins |
| Null handling | Dimension tables have a row for "Unknown" (key = -1) |
**Unknown/Default Dimension Row:**
```sql
INSERT INTO dim_customer (customer_key, customer_id, city, ...)
VALUES (-1, 'UNKNOWN', 'Unknown', ...);
-- Fact rows without a matching customer get customer_key = -1
```
---
## 3.9 Fact-less Fact Tables
A fact table with **no numeric measurements** — captures the occurrence of
an event.
**Example: Student-Course-Enrollment**
```sql
CREATE TABLE fact_enrollment (
 date_key INT, -- When enrolled
This PDF document was created using the online html editor powered by CKEditor.
 student_key INT,
 course_key INT,
 instructor_key INT
 -- No numeric facts!
);
```
Used to answer: "How many students enrolled in Biology last semester?"
```sql
SELECT COUNT(*) FROM fact_enrollment
JOIN dim_course ON fact_enrollment.course_key = dim_course.course_key
WHERE dim_course.subject = 'Biology'
AND fact_enrollment.date_key BETWEEN 20240101 AND 20240630;
```
---
## 3.10 Data Marts
A **Data Mart** is a subset of the data warehouse, focused on a specific
business domain or department.
```mermaid
graph TD
 ODS[Operational Data Store\\nNear-real-time operational data] -->
EDW[Enterprise Data Warehouse\\nIntegrated, conformed, historical]
 EDW --> SM[Sales Data Mart]
 EDW --> FM[Finance Data Mart]
 EDW --> HRM[HR Data Mart]
 EDW --> MKT[Marketing Data Mart]

 SM --> BI1[Tableau Dashboard]
 FM --> BI2[Power BI Report]
 HRM --> BI3[Workday Analytics]
```
**Kimball's philosophy**: Build data marts from a shared enterprise bus.
**Inmon's philosophy**: Build EDW first (normalized 3NF), then derive data
This PDF document was created using the online html editor powered by CKEditor.
marts.
| Approach | Description | Pros | Cons |
|---|---|---|---|
| Kimball (Bottom-Up) | Build business-process focused data marts first |
Faster time to value | Risk of inconsistency if not governed |
| Inmon (Top-Down) | Build normalized EDW first, then data marts |
Consistent enterprise view | Slower, more upfront design |
---
## 3.11 Date Dimension
The **Date Dimension** is the most commonly joined dimension in any DWH.
```sql
CREATE TABLE dim_date (
 date_key INT PRIMARY KEY, -- 20240315 (YYYYMMDD)
 full_date DATE, -- 2024-03-15
 year INT, -- 2024
 quarter INT, -- 1
 quarter_name VARCHAR(6), -- Q1 2024
 month INT, -- 3
 month_name VARCHAR(20), -- March
 week_of_year INT, -- 11
 day_of_month INT, -- 15
 day_of_week INT, -- 5 (Friday=5)
 day_name VARCHAR(20), -- Friday
 is_weekday BOOLEAN, -- TRUE
 is_holiday BOOLEAN, -- FALSE
 fiscal_year INT, -- 2024
 fiscal_quarter INT, -- 2 (if fiscal year starts
Oct)
 fiscal_month INT -- 6
);
```
**Pre-populate** for 10–30 years ahead. Date keys as integers (YYYYMMDD) for
This PDF document was created using the online html editor powered by CKEditor.
efficient sorting and range queries.
---
## 3.12 ETL Design for Data Warehouses
```mermaid
flowchart LR
 subgraph Source Systems
 S1[(OLTP DB)]
 S2[CSV Files]
 S3[API]
 end

 subgraph Staging Area
 ST[Raw Staging Tables\\nNo transformations\\nDrop & reload each run]
 end

 subgraph DWH Processing
 DIM[Load Dimension Tables\\nHandle SCD logic\\nAssign surrogate
keys]
 FACT[Load Fact Tables\\nLookup surrogate keys\\nValidate referential
integrity]
 end

 subgraph Presentation Layer
 DM[Data Marts / Star Schemas\\nServed to BI tools]
 end

 S1 --> ST
 S2 --> ST
 S3 --> ST
 ST --> DIM
 DIM --> FACT
 FACT --> DM
```
### ETL Best Practices (Kimball)
This PDF document was created using the online html editor powered by CKEditor.
1. **Stage data first**: Never transform directly from source; use a staging
area
2. **Load dimensions before facts**: Facts require surrogate keys from
dimensions
3. **Idempotent loads**: Safe to rerun — use MERGE/UPSERT patterns
4. **Audit trail**: Track when records were loaded and from where
5. **Error handling**: Log rejected records, don't silently drop them
### MERGE Pattern for SCD Type 2 (SQL Server / Snowflake)
```sql
MERGE dim_customer AS target
USING staging_customer AS source
ON target.customer_id = source.customer_id
 AND target.is_current = TRUE
WHEN MATCHED AND (
 target.city <> source.city OR
 target.email <> source.email
) THEN
 -- Expire old row
 UPDATE SET target.is_current = FALSE,
 target.valid_to = CURRENT_DATE - 1
WHEN NOT MATCHED THEN
 -- Insert new row (handles new customers + changed existing)
 INSERT (customer_id, city, email, is_current, valid_from, valid_to)
 VALUES (source.customer_id, source.city, source.email, TRUE,
CURRENT_DATE, '9999-12-31');
-- Insert new version of changed existing customers (requires separate
INSERT after expiry)
```
---
## 3.13 Modern Data Warehouse: dbt
**dbt (data build tool)** applies software engineering practices to SQL
transformations:
- Models are `.sql` SELECT statements
- Handles dependencies, testing, documentation
This PDF document was created using the online html editor powered by CKEditor.
- Supports incremental models (only process new/changed records)
```sql
-- models/marts/fact_sales.sql
{{
 config(
 materialized='incremental',
 unique_key='sales_key',
 on_schema_change='sync_all_columns'
 )
}}
WITH orders AS (
 SELECT * FROM {{ ref('stg_orders') }}
 {% if is_incremental() %}
 WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
 {% endif %}
),
customer_lookup AS (
 SELECT customer_id, customer_key
 FROM {{ ref('dim_customer') }}
 WHERE is_current = TRUE
)
SELECT
 {{ dbt_utils.surrogate_key(['o.order_id', 'o.line_item_id']) }} AS
sales_key,
 c.customer_key,
 d.date_key,
 o.quantity,
 o.unit_price,
 o.quantity * o.unit_price AS revenue
FROM orders o
JOIN customer_lookup c ON o.customer_id = c.customer_id
JOIN dim_date d ON o.order_date = d.full_date
```
This PDF document was created using the online html editor powered by CKEditor.
---
## 3.14 Cross Questions — Data Warehousing
**Q1: What is the grain of a fact table and why is it the most important
design decision?**
> The grain defines exactly what one row represents. If mixed grains exist
(e.g., order header and line items in one table), aggregations produce
incorrect results. Declare grain first; every dimension and fact must be
consistent with it.
**Q2: Explain SCD Type 2 and how you would implement it in a cloud DWH.**
> SCD Type 2 adds a new row for each tracked change with `valid_from`,
`valid_to`, and `is_current` flags. In Snowflake/BigQuery, implement using
MERGE: match on natural key + is_current=TRUE, update to expire changed
rows, insert new versions. Use a surrogate key to distinguish versions.
**Q3: What is the difference between additive, semi-additive, and nonadditive facts?**
> Additive: can SUM across all dimensions (revenue). Semi-additive: can SUM
across some dimensions but not time (account balances — sum across accounts
makes sense, but not across days). Non-additive: cannot SUM meaningfully
(ratios, unit prices — store numerator/denominator and compute at query
time).
**Q4: Why are surrogate keys preferred over natural keys in dimensional
models?**
> Surrogate keys enable SCD Type 2 (multiple rows per entity), insulate the
DWH from source system changes (system migrations, key format changes),
improve join performance (integer vs string), and allow unknown/default
dimension members (key=-1).
**Q5: What is a conformed dimension and why is it important?**
> A conformed dimension has the same meaning across multiple fact tables and
data marts. The Date dimension is the classic example — used in every
business process. Conformed dimensions enable cross-process analysis (drill
across) and ensure consistent enterprise-wide definitions.
This PDF document was created using the online html editor powered by CKEditor.
**Q6: When would you use a fact-less fact table?**
> When you need to record the occurrence of an event with no numeric
measurement. Examples: student course enrollment, product-store coverage
(which products are available in which stores), insurance policy coverage
events.
**Q7: Compare Kimball vs Inmon methodology.**
> Kimball (bottom-up): Start with business-process data marts using star
schemas. Faster value but requires governance to avoid silos. Inmon (topdown): Build a normalized 3NF enterprise DWH first, then derive dimensional
data marts. More consistent but slower to deliver. Modern DWHs often blend
both (Lakehouse pattern).
**Q8: How does dbt enable software engineering practices for data
transformation?**
> dbt manages SQL models as code with version control, dependency graphs
(DAG), automated testing (schema tests, data tests), documentation, and
incremental materialization. This enables CI/CD for data pipelines, peer
code reviews for SQL, and automated regression testing.
**Q9: What is the difference between a Data Warehouse and a Data Lake? When
would you use each?**
> DWH: structured data, schema-on-write, SQL-native, ACID, optimized for BI
queries (Snowflake, BigQuery). Data Lake: all data types, schema-on-read,
flexible, low cost (S3 + Parquet). Use DWH for reliable BI and reporting;
Data Lake for raw storage, ML training data, and exploratory analysis.
Modern Lakehouse (Delta Lake) merges both.
---
## 3.15 Missing Dimensional Modeling Concepts
### Role-Playing Dimensions
A **role-playing dimension** is a single physical dimension table used
multiple times in a fact table, each time playing a different role.
**Example: Date dimension plays three roles in a flight fact table:**
This PDF document was created using the online html editor powered by CKEditor.
```sql
CREATE TABLE fact_flight (
 flight_key INT,
 departure_date_key INT, -- Role 1: Departure date
 arrival_date_key INT, -- Role 2: Arrival date
 booking_date_key INT, -- Role 3: Booking date
 customer_key INT,
 route_key INT,
 ticket_price DECIMAL,
 miles_flown INT
);
-- Query using role-playing date dimension
SELECT
 dep.month_name AS departure_month,
 arr.month_name AS arrival_month,
 SUM(f.ticket_price) AS total_revenue
FROM fact_flight f
JOIN dim_date dep ON f.departure_date_key = dep.date_key -- Role 1
JOIN dim_date arr ON f.arrival_date_key = arr.date_key -- Role 2
JOIN dim_date bk ON f.booking_date_key = bk.date_key -- Role 3
GROUP BY dep.month_name, arr.month_name;
```
**Other role-playing examples:**
- Customer dimension as "Sender" and "Receiver" in a payments fact
- Employee dimension as "Sales Rep" and "Manager" in a sales fact
- Location dimension as "Origin" and "Destination" in logistics
---
### Bridge Tables — Many-to-Many Dimensions
Standard dimensional models handle one-to-many (one product per order line).
**Bridge tables** handle many-to-many relationships.
**Example: A patient can have multiple diagnoses; a diagnosis can apply to
This PDF document was created using the online html editor powered by CKEditor.
multiple patients.**
```mermaid
erDiagram
 FACT_PATIENT_ENCOUNTER {
 int encounter_key PK
 int patient_key FK
 int date_key FK
 int diagnosis_group_key FK
 int provider_key FK
 decimal total_charges
 }
 BRIDGE_PATIENT_DIAGNOSIS {
 int diagnosis_group_key FK
 int diagnosis_key FK
 decimal weighting_factor
 }
 DIM_DIAGNOSIS {
 int diagnosis_key PK
 string icd_code
 string diagnosis_name
 string category
 string severity
 }
 FACT_PATIENT_ENCOUNTER }o--o{ BRIDGE_PATIENT_DIAGNOSIS : "via group key"
 BRIDGE_PATIENT_DIAGNOSIS }o--|| DIM_DIAGNOSIS : ""
```
```sql
-- Query: Total charges by diagnosis (many-to-many)
SELECT
 d.diagnosis_name,
 SUM(f.total_charges * b.weighting_factor) AS weighted_charges,
 COUNT(DISTINCT f.patient_key) AS patient_count
FROM fact_patient_encounter f
This PDF document was created using the online html editor powered by CKEditor.
JOIN bridge_patient_diagnosis b ON f.diagnosis_group_key =
b.diagnosis_group_key
JOIN dim_diagnosis d ON b.diagnosis_key = d.diagnosis_key
GROUP BY d.diagnosis_name
ORDER BY weighted_charges DESC;
```
**Weighting factor:** When a patient has 3 diagnoses, each diagnosis gets
weight 1/3 so totals don't triple-count. Use `weighting_factor = 1.0` if you
want each diagnosis to get full credit.
---
### Aggregate Fact Tables
**Aggregate fact tables** pre-summarize the atomic fact table at a higher
grain — dramatically speeding up common queries.
```mermaid
graph TD
 A[Atomic Fact Table\\nfact_order_line\\nGrain: 1 row per order
line\\n500M rows] --> B[Aggregate Fact: Monthly
Sales\\nfact_monthly_sales_by_product\\nGrain: 1 row per product per
month\\n50K rows]
 A --> C[Aggregate Fact: Daily Sales by
Region\\nfact_daily_sales_by_region\\n500K rows]

 B --> Q1[Q: Monthly revenue by category\\n→ 50K rows vs 500M rows
scanned]
 C --> Q2[Q: Regional sales trend\\n→ 500K rows vs 500M rows]
```
```sql
-- Create aggregate fact table (dbt model)
-- models/marts/fct_monthly_sales_by_product.sql
SELECT
 d.year,
 d.month,
This PDF document was created using the online html editor powered by CKEditor.
 p.product_key,
 p.category,
 p.brand,
 COUNT(*) AS order_count,
 SUM(f.quantity) AS total_units_sold,
 SUM(f.revenue) AS total_revenue,
 SUM(f.gross_profit) AS total_profit,
 AVG(f.unit_price) AS avg_selling_price
FROM fact_order_line f
JOIN dim_date d ON f.order_date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY d.year, d.month, p.product_key, p.category, p.brand
```
**Key rule (Kimball):** Aggregate fact tables must be complementary to, not
a replacement for, atomic fact tables. BI tools should be able to drill down
to atomic grain.
---
### Outrigger Dimensions
An **outrigger** is a secondary dimension that a primary dimension
references.
```
DIM_STORE (primary) → references → DIM_GEOGRAPHY (outrigger)
DIM_EMPLOYEE (primary) → references → DIM_DEPARTMENT (outrigger)
```
**Use sparingly** — Kimball recommends denormalizing attributes into the
primary dimension instead. Outriggers are acceptable when the secondary
dimension is shared and large.
---
## 3.16 Real-World Dimensional Modeling Examples
This PDF document was created using the online html editor powered by CKEditor.
### Example 1: Retail — Walmart
```mermaid
erDiagram
 FACT_RETAIL_SALES {
 int sales_key PK
 int date_key FK
 int store_key FK
 int product_key FK
 int customer_key FK
 int promotion_key FK
 decimal units_sold
 decimal revenue
 decimal cost
 decimal markdown_amount
 }
 DIM_PRODUCT {
 int product_key PK
 string upc
 string item_description
 string department
 string category
 string private_label_flag
 decimal unit_cost
 decimal unit_retail_price
 }
 DIM_STORE {
 int store_key PK
 string store_number
 string store_name
 string format
 string city
 string state
 string district
 string region
 int store_sqft
 date open_date
 }
This PDF document was created using the online html editor powered by CKEditor.
 FACT_RETAIL_SALES }o--|| DIM_PRODUCT : ""
 FACT_RETAIL_SALES }o--|| DIM_STORE : ""
```
**Walmart-specific dimensional modeling decisions:**
- **Product dimension** has 100K+ items → denormalize all attributes to
avoid snowflaking
- **Promotion dimension** is a mini-dimension (changes frequently) — split
from product to avoid SCD2 row explosion
- **Weekly snapshot fact** alongside transaction fact — inventory position
every Sunday
- **Markdown tracking**: `markdown_amount` fact answers "how much did
promotions cost?"
---
### Example 2: Healthcare — Claims Processing (Optum)
```mermaid
erDiagram
 FACT_MEDICAL_CLAIM {
 int claim_key PK
 int service_date_key FK
 int paid_date_key FK
 int member_key FK
 int provider_key FK
 int diagnosis_group_key FK
 int procedure_key FK
 int plan_key FK
 decimal billed_amount
 decimal allowed_amount
 decimal paid_amount
 decimal member_responsibility
 int claim_status_key FK
 }
 DIM_MEMBER {
 int member_key PK
This PDF document was created using the online html editor powered by CKEditor.
 string member_id
 int age_band_key FK
 string gender
 string plan_type
 string lob
 boolean is_current
 date valid_from
 date valid_to
 }
 DIM_PROVIDER {
 int provider_key PK
 string npi
 string provider_name
 string specialty
 string network_status
 string state
 }
```
**Healthcare-specific decisions:**
- **SCD Type 2 on Member** — plan enrollment changes frequently; need
correct plan at time of claim
- **Bridge table for Diagnosis** — ICD-10 allows multiple diagnoses per
claim (M2M)
- **Accumulating snapshot fact** for claim lifecycle: submitted →
adjudicated → paid → appealed
- **Role-playing date**: `service_date_key` (when care was given) ≠
`paid_date_key` (when claim was paid)
---
### Example 3: Fintech — Stripe Payments
**Grain:** One row per payment transaction
```sql
CREATE TABLE fact_payment (
This PDF document was created using the online html editor powered by CKEditor.
 payment_key BIGINT,
 created_date_key INT,
 settled_date_key INT, -- Role-playing date: when funds settled
 merchant_key INT,
 customer_key INT,
 payment_method_key INT, -- Card brand, type (debit/credit),
country
 currency_key INT,
 amount_usd DECIMAL(18,4),
 fee_usd DECIMAL(18,4),
 net_usd DECIMAL(18,4),
 is_dispute BOOLEAN,
 is_refund BOOLEAN,
 stripe_outcome_code VARCHAR(50) -- 'authorized', 'declined', 'blocked'
);
```
**Stripe modeling decisions:**
- **Currency dimension** — exchange rate on transaction date; `amount_usd`
is always converted to USD for consistent reporting
- **Payment method dimension** — card_brand (Visa/Mastercard), card_type
(credit/debit), issuing_country — frequent analysis axis
- **Non-additive fact**: `fee_rate` (fee/amount) — store
numerator/denominator separately
---
### Example 4: SaaS — Salesforce CRM Analytics
**Accumulating Snapshot for Sales Pipeline:**
```sql
CREATE TABLE fact_opportunity_pipeline (
 opportunity_key INT,
 owner_key INT,
 account_key INT,
 create_date_key INT,
 qualify_date_key INT, -- NULL until qualified
This PDF document was created using the online html editor powered by CKEditor.
 demo_date_key INT, -- NULL until demo done
 proposal_date_key INT, -- NULL until proposal sent
 close_date_key INT, -- NULL until won/lost
 current_stage VARCHAR(50),
 amount DECIMAL,
 probability INT,
 days_in_pipeline INT,
 is_won BOOLEAN,
 is_lost BOOLEAN
);
-- Updated at each stage transition — NOT append-only!
```
**Why accumulating snapshot:** Sales leaders want to see "where is each deal
in the funnel right now?" — a single row per opportunity updated as it
progresses, not a new row each time.
---
## 3.17 DWH Technology Selection: When to Use What
| Requirement | Technology | Why |
|---|---|---|
| < 100GB, startup, fast setup | **DuckDB + dbt** | Zero infrastructure;
runs on laptop; excellent SQL; free |
| Cloud-native, mixed workloads, SQL-first | **Snowflake** | Compute/storage
separation; auto-scale; excellent SQL compatibility |
| Google Cloud ecosystem, serverless | **BigQuery** | No cluster management;
per-query pricing; ML built-in |
| AWS ecosystem, existing Redshift investment | **Redshift** | Deep AWS
integrations; Spectrum for S3 queries |
| Databricks shop, ML-heavy | **Delta Lake on Databricks** | Unified batch +
streaming + ML; open format |
| Open source, no vendor lock-in | **Apache Hive / Trino on Iceberg** | Full
control; open table format; Trino is fast |
| Real-time BI, sub-second dashboard | **ClickHouse / Apache Druid** | Not
traditional DWH — OLAP engine for real-time |
This PDF document was created using the online html editor powered by CKEditor.
**Real-World DWH choices:**
| Company | DWH | Why |
|---|---|---|
| **Airbnb** | Druid + Hive (migrating to Trino + Iceberg) | Started with
Hive; moved to Druid for real-time host/guest metrics |
| **Slack** | Snowflake | SQL-native; easy for analysts; no ops burden |
| **Lyft** | Hive → Presto → Trino on S3 | Cost-conscious; open source;
Trino gives interactive speeds |
| **Instacart** | Redshift → Snowflake | Moved from Redshift when data grew
to multi-TB; Snowflake's concurrency handling better |
| **Spotify** | Google BigQuery | Already on GCP; BigQuery ML for in-DWH
recommendations |
---
# Part 4: Streaming Systems
> Reference: *Streaming Systems* — Tyler Akidau, Slava Chernyak, Reuven Lax
---
## 4.1 What is Stream Processing?
**Stream processing** is the continuous processing of data as it arrives, in
contrast to batch processing which processes accumulated data at intervals.
> "Streaming is a superset of batch. Batch processing is just a special case
of streaming with infinite latency."
> — Tyler Akidau
### The Streaming Mental Model
```mermaid
graph LR
 A[Event occurs\\nin real world] --> B[Event produced\\nto message
broker\\nKafka / Kinesis]
 B --> C[Stream processor\\nconsumes event\\nFlink / Spark Streaming]
 C --> D[Result emitted\\nto sink\\nDB / Dashboard / Alert]

This PDF document was created using the online html editor powered by CKEditor.
 Note1["Batch: process data at T+24h"]
 Note2["Streaming: process data at T+ms"]
```
---
## 4.2 Core Concepts: The What, Where, When, How Framework
Akidau's framework for understanding streaming:
| Question | Answered by | Description |
|---|---|---|
| **What** results? | Transformations | What computation are we doing? (sum,
count, join) |
| **Where** in event time? | Windowing | What time range does this
computation cover? |
| **When** in processing time? | Triggers | When do we emit results? |
| **How** do refinements relate? | Accumulation | How do late data updates
affect previous results? |
---
## 4.3 Event Time vs Processing Time
This is the most fundamental concept in streaming.
```mermaid
graph LR
 subgraph Real World - Event Time
 E1[Order placed\\nat 10:00:00]
 E2[Order placed\\nat 10:00:05]
 E3[Order placed\\nat 10:00:03]
 end

 subgraph System - Processing Time
 P1[Arrives at processor\\nat 10:00:02]
 P2[Arrives at processor\\nat 10:00:06]
 P3[Arrives at processor\\nat 10:00:09]
This PDF document was created using the online html editor powered by CKEditor.
 end

 E1 --> P1
 E2 --> P2
 E3 --> P3

 Note["E3 (event time 10:00:03) arrives AFTER E2 — out of order!"]
```
### Skew, Lag, and Delay
| Term | Meaning |
|---|---|
| **Event-time skew** | Gap between event time and processing time |
| **Out-of-order events** | Events arriving with lower event timestamps than
previously seen |
| **Late data** | Events arriving after the window they belong to has
already been processed |
### Why Processing Time is Unreliable
Processing time depends on:
- Network latency (device → server)
- Message broker throughput
- Processing system load
Two identical events occurring 1 second apart might arrive at the processor
30 seconds apart due to network congestion.
**Event time processing** gives accurate results regardless of when data
arrives, but requires handling late data.
---
## 4.4 Windowing
Windows carve an infinite stream into finite, processable chunks.
### Tumbling Windows (Fixed Windows)
This PDF document was created using the online html editor powered by CKEditor.
```
|--- Window 1 ---|--- Window 2 ---|--- Window 3 ---|
0 5 10 15 (minutes)
Non-overlapping, fixed size
```
```python
# Flink - Tumbling window of 5 minutes
stream \\
 .key_by(lambda event: event.user_id) \\
 .window(TumblingEventTimeWindows.of(Time.minutes(5))) \\
 .sum('purchase_amount')
```
**Use case**: Hourly sales totals, daily active users
### Sliding Windows
```
|---W1---|
 |---W2---|
 |---W3---|
 |---W4---|
0 2 4 6 8 10 (minutes)
Window size = 6 min, slide = 2 min → overlap
```
**Use case**: 5-minute rolling average CPU usage, 1-hour sliding window
fraud detection
### Session Windows
```
[E1,E2,E3]---gap>30s---[E4,E5]---gap>30s---[E6]
|--Session 1--| |--Session 2--| |--S3--|
Dynamic size based on inactivity gap
```
This PDF document was created using the online html editor powered by CKEditor.
- Session window closes when there's an inactivity gap greater than a
threshold
- No fixed size — determined by user behavior
- **Use case**: User session analytics, clickstream analysis, call center
session durations
### Global Windows
A single window spanning all time. Only useful with custom triggers.
---
## 4.5 Watermarks
**Watermarks** are the mechanism for dealing with out-of-order events in
event time processing.
> A watermark is a timestamp `W(t)` that asserts: "No event with event time
≤ t will be seen in the future."
```
Event stream (event times): 1, 3, 5, 2, 7, 4, 9, 6, 8, ...
Watermark = max_event_time_seen - allowed_lateness
If allowed_lateness = 2 minutes:
After seeing event at t=7, watermark = 7-2 = 5
→ Window [0,5] can be safely closed and computed
```
### Perfect vs Heuristic Watermarks
| Type | Description | Tradeoff |
|---|---|---|
| **Perfect watermark** | Exactly when no more late data will arrive (known
only in specific cases) | Zero late data, but may cause long delays waiting
|
| **Heuristic watermark** | Estimate based on observed data patterns | May
This PDF document was created using the online html editor powered by CKEditor.
have late data, but lower latency |
### Watermark in Flink
```java
// Flink - BoundedOutOfOrdernessWatermarks
WatermarkStrategy
 .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
 .withTimestampAssigner((event, timestamp) -> event.getEventTime());
```
This tells Flink: "Wait up to 10 seconds past the max event time seen before
closing a window."
### Visualizing Watermarks
```
Timeline:
Events: 1 2 4 7 3 8 5 9 11 6 ...
Max seen: 1 2 4 7 7 8 8 9 11 11 ...
Watermark: - - 2 5 5 6 6 7 9 9 (allowed_lateness=2)

Window [0,5] closes when watermark passes 5 → at position where max=7
Event 3 (arrives after window closes) → late data!
```
---
## 4.6 Triggers
Triggers determine **when** to emit results from a window. Without triggers,
a window would wait forever for the watermark to pass.
### Trigger Types
```mermaid
graph TD
 T[Trigger Types]
This PDF document was created using the online html editor powered by CKEditor.
 T --> ET[Event-time Trigger\\nFire when watermark passes end of
window\\nDefault behavior]
 T --> PT[Processing-time Trigger\\nFire at fixed processing-time
intervals\\ne.g., every 30 seconds]
 T --> EC[Element-count Trigger\\nFire after N elements received]
 T --> COMP[Composite Triggers\\nCombine multiple trigger conditions]
```
### Early / On-time / Late Firings Pattern
```mermaid
graph LR
 A[Elements arriving in window] --> B{Trigger}
 B -->|Early firing\\nevery 1 min| C[Speculative early result\\nFast but
incomplete]
 B -->|On-time\\nwatermark passes| D[Final complete result]
 B -->|Late firing\\nfor late data| E[Refined result with late data]
```
**Example (Flink-style pseudocode):**
```
Window: 5-minute tumbling, event time
Trigger:
 - Early: fire every 1 minute of processing time (speculative updates)
 - On-time: fire when watermark passes end of window (complete result)
 - Late: fire for each late element arriving within 10 minutes (refinement)
```
---
## 4.7 Accumulation Modes
When a window fires multiple times (early + on-time + late), how do results
relate?
### Discarding Mode
Each firing emits only the delta since last firing. Old results discarded.
This PDF document was created using the online html editor powered by CKEditor.
```
Early fire at t=1min: result = 50 (50 elements processed)
On-time fire at t=5min: result = 80 (only the 80 new elements, not the
cumulative 130)
```
- **Use when**: Downstream aggregates independently (e.g., writes to a
running total in a counter table)
### Accumulating Mode
Each firing emits cumulative results from window start.
```
Early fire at t=1min: result = 50 (50 so far)
On-time fire at t=5min: result = 130 (full 130 elements cumulative)
Late fire: result = 135 (with 5 late elements)
```
- **Use when**: Downstream needs to overwrite previous result (e.g., UPDATE
the window's metric row)
### Accumulating & Retracting Mode
Each firing emits both the new cumulative value AND a retraction of the
previous value.
```
Early fire: emit +50
On-time fire: emit -50 (retract early) AND +130 (new value)
```
- **Use when**: Downstream cannot overwrite (e.g., append-only log, BI tool
cache)
- Most complex, rarely needed outside advanced use cases
---
## 4.8 Exactly-Once Semantics
This PDF document was created using the online html editor powered by CKEditor.
Achieving exactly-once processing is the holy grail of streaming:
```mermaid
graph LR
 A[Kafka Source] --> B[Stream Processor\\nFlink / Spark]
 B --> C[Sink\\nDB / Kafka]

 subgraph Exactly-Once Guarantees
 D[Kafka transactions\\nsource side]
 E[Flink checkpointing\\nprocessor state]
 F[Idempotent writes\\nor sink transactions]
 end
```
### Flink Checkpointing + Two-Phase Commit
Flink achieves exactly-once end-to-end using:
1. **Distributed snapshots (Chandy-Lamport algorithm)**: Periodic state
snapshots to durable storage (S3, HDFS)
2. **Source offset tracking**: Kafka offsets stored as part of checkpoint
3. **Transactional sink**: Two-phase commit with sink (e.g., Kafka
transactions, database transactions)
```
Failure scenario:
1. Flink processes records, advances Kafka offset to 1000
2. Checkpoint saved: {kafka_offset: 900, state: {...}} ← last successful
checkpoint
3. Flink crashes before committing sink write
4. On restart: roll back to checkpoint {offset: 900}
5. Reprocess from offset 900 → idempotent sink handles duplicate writes
safely
```
### Idempotent Sinks for Exactly-Once
- **Database UPSERT**: `INSERT ... ON CONFLICT DO UPDATE` — safe to replay
- **Kafka idempotent producer**: Deduplication using producer ID + sequence
number
This PDF document was created using the online html editor powered by CKEditor.
- **File-based**: Write to temp file, atomic rename on completion
---
## 4.9 Apache Kafka — Streaming Backbone
### Kafka Architecture
```mermaid
graph TD
 subgraph ZooKeeper/KRaft
 ZK[Cluster Coordination\\nLeader election\\nConfig management]
 end

 subgraph Kafka Cluster
 B1[Broker 1\\nLeader for P0,P2\\nFollower for P1]
 B2[Broker 2\\nLeader for P1\\nFollower for P0]
 B3[Broker 3\\nFollower for P0,P1,P2]
 end

 subgraph Topic: orders - RF=3, Partitions=3
 P0[Partition 0\\nOffset 0..N]
 P1[Partition 1\\nOffset 0..M]
 P2[Partition 2\\nOffset 0..K]
 end

 ZK --> B1
 B1 --- B2
 B2 --- B3
```
### Producer Configuration (Key Settings)
| Setting | Options | Impact |
|---|---|---|
| `acks` | `0` / `1` / `all` | `0`: no ack (fastest, may lose); `1`: leader
ack; `all`: all ISR ack (safest) |
| `retries` | Integer | How many times to retry on failure |
This PDF document was created using the online html editor powered by CKEditor.
| `enable.idempotence` | `true/false` | Prevents duplicate messages on retry
|
| `compression.type` | `none/gzip/snappy/lz4/zstd` | Reduce network
bandwidth |
| `batch.size` | Bytes | Max batch size before send |
| `linger.ms` | Milliseconds | Wait time to fill batch |
### Consumer Configuration (Key Settings)
| Setting | Options | Impact |
|---|---|---|
| `auto.offset.reset` | `earliest/latest/none` | Where to start if no
committed offset |
| `enable.auto.commit` | `true/false` | `false` for manual offset control
(at-least-once) |
| `max.poll.records` | Integer | Max records per poll loop |
| `group.id` | String | Consumer group identifier |
### Kafka Streams vs Apache Flink
| Aspect | Kafka Streams | Apache Flink |
|---|---|---|
| Deployment | Library (in your app) | Separate cluster |
| Source | Kafka only | Kafka, Kinesis, files, custom |
| State management | RocksDB (local) | Managed state + checkpointing |
| Exactly-once | Yes (Kafka transactions) | Yes (2PC + checkpointing) |
| Windowing | Yes | Yes (more flexible) |
| CEP (Complex Event Processing) | No | Yes (Flink CEP) |
| Learning curve | Lower | Higher |
| Use case | Kafka-native apps | Complex stateful stream processing |
---
## 4.10 Apache Flink — Deep Dive
Flink is the industry standard for stateful distributed stream processing.
### Flink Architecture
This PDF document was created using the online html editor powered by CKEditor.
```mermaid
graph TD
 subgraph Flink Cluster
 JM[JobManager\\nCoordinator\\nCheckpoint coordinator\\nJob
scheduling]
 TM1[TaskManager 1\\nTask slots\\nState storage]
 TM2[TaskManager 2\\nTask slots\\nState storage]
 TM3[TaskManager 3\\nTask slots\\nState storage]
 end

 JM --> TM1
 JM --> TM2
 JM --> TM3

 CP[Checkpoint Storage\\nS3 / HDFS] <--> JM
 CP <--> TM1
 CP <--> TM2
 CP <--> TM3
```
### Flink DataStream API Example
```python
# PyFlink - Count orders per user per 5-minute tumbling window
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from datetime import timedelta
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(4)
kafka_source = FlinkKafkaConsumer(
 topics='orders',
 deserialization_schema=...,
This PDF document was created using the online html editor powered by CKEditor.
 properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'flinkconsumer'}
)
stream = env.add_source(kafka_source) \\
 .assign_timestamps_and_watermarks(
 WatermarkStrategy
 .for_bounded_out_of_orderness(timedelta(seconds=10))
 .with_timestamp_assigner(lambda event, _: event['event_time'])
 ) \\
 .key_by(lambda event: event['user_id']) \\
 .window(TumblingEventTimeWindows.of(Time.minutes(5))) \\
 .reduce(lambda a, b: {**a, 'order_count': a['order_count'] +
b['order_count']})
stream.print()
env.execute("Order Count Job")
```
### Flink State Management
**Keyed State**: State scoped per key (e.g., per user)
- `ValueState`: Single value per key
- `ListState`: List of values per key
- `MapState`: Map of key-value pairs per key
- `ReducingState`: Aggregate (reduce) values per key
**Operator State**: State scoped per operator instance (e.g., Kafka offset
per partition)
```java
// Flink stateful operator - fraud detection example
public class FraudDetector extends KeyedProcessFunction<Long, Transaction,
Alert> {
 private transient ValueState<Boolean> flagState; // keyed by user_id
 @Override
 public void open(Configuration config) {
This PDF document was created using the online html editor powered by CKEditor.
 flagState = getRuntimeContext().getState(
 new ValueStateDescriptor<>("flag", Boolean.class)
 );
 }
 @Override
 public void processElement(Transaction tx, Context ctx, Collector<Alert>
out) throws Exception {
 Boolean lastWasSmall = flagState.value();

 if (lastWasSmall != null && lastWasSmall && tx.getAmount() > 500) {
 out.collect(new Alert(tx.getUserId())); // Small tx followed by
large tx
 }

 flagState.update(tx.getAmount() < 1);
 }
}
```
---
## 4.11 Spark Structured Streaming
Spark's high-level streaming API using micro-batch (default) or continuous
processing.
```python
# PySpark Structured Streaming - Read from Kafka, write to Delta Lake
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, LongType, DoubleType
spark = SparkSession.builder \\
 .appName("OrderStreamProcessor") \\
 .config("spark.sql.streaming.checkpointLocation", "s3://mybucket/checkpoints") \\
 .getOrCreate()
This PDF document was created using the online html editor powered by CKEditor.
schema = StructType() \\
 .add("order_id", StringType()) \\
 .add("user_id", StringType()) \\
 .add("amount", DoubleType()) \\
 .add("event_time", LongType())
# Read stream from Kafka
raw_stream = spark.readStream \\
 .format("kafka") \\
 .option("kafka.bootstrap.servers", "kafka:9092") \\
 .option("subscribe", "orders") \\
 .option("startingOffsets", "earliest") \\
 .load()
# Parse JSON, apply watermark, aggregate
orders = raw_stream \\
 .select(from_json(col("value").cast("string"), schema).alias("data")) \\
 .select("data.*") \\
 .withWatermark("event_time", "10 minutes") \\
 .groupBy(
 window(col("event_time"), "5 minutes"),
 col("user_id")
 ) \\
 .agg({"amount": "sum"})
# Write to Delta Lake
query = orders.writeStream \\
 .format("delta") \\
 .outputMode("append") \\
 .option("checkpointLocation", "s3://my-bucket/checkpoints/order-agg") \\
 .start("s3://my-bucket/delta/order_aggregates")
query.awaitTermination()
```
### Output Modes in Spark Structured Streaming
This PDF document was created using the online html editor powered by CKEditor.
| Mode | Behavior | Use case |
|---|---|---|
| `append` | Only new rows added since last trigger | Append-only results
(no updates) |
| `complete` | Full result table every trigger | Aggregations without
watermark |
| `update` | Only changed rows since last trigger | Aggregations with
watermark |
---
## 4.12 Stream-Table Duality
> "A stream is a changelog of a table. A table is a snapshot of a stream."
This duality is fundamental to Kafka Streams, KSQL, and Flink SQL.
```
Stream (changelog): Table (current state):
+INSERT user=Alice city=NY user=Alice city=NY
+UPDATE user=Alice city=BOS user=Alice city=BOS
+DELETE user=Bob (Bob removed)
Table → Stream: emit every INSERT/UPDATE/DELETE as an event
Stream → Table: apply changestream to materialize current state
```
### Kafka Streams KStream vs KTable
```java
// KStream: unbounded sequence of events
KStream<String, Order> orders = builder.stream("orders-topic");
// KTable: materialized view (latest value per key)
KTable<String, Customer> customers = builder.table("customers-topic");
// Stream-Table Join: enrich each order with customer data
KStream<String, EnrichedOrder> enriched = orders.join(
This PDF document was created using the online html editor powered by CKEditor.
 customers,
 (order, customer) -> new EnrichedOrder(order, customer),
 Joined.with(Serdes.String(), orderSerde, customerSerde)
);
```
---
## 4.13 Streaming Design Patterns
### Pattern 1: Event Enrichment (Stream-Table Join)
```mermaid
sequenceDiagram
 participant K as Kafka (events)
 participant F as Flink
 participant DB as Reference DB / KTable
 participant S as Output Sink
 K->>F: Raw order event {user_id, amount}
 F->>DB: Lookup user details by user_id
 DB->>F: {name, tier, location}
 F->>S: Enriched order {user_id, name, tier, amount}
```
### Pattern 2: Aggregation with Windowing
```mermaid
graph LR
 A[Clickstream events] --> B[5-min tumbling window\\ngroup by user_id]
 B --> C[COUNT clicks per user per 5min]
 C --> D{clicks > threshold?}
 D -->|Yes| E[Emit bot-detection alert]
 D -->|No| F[Write to analytics DB]
```
### Pattern 3: Event-Driven Microservices (Choreography)
This PDF document was created using the online html editor powered by CKEditor.
```mermaid
sequenceDiagram
 participant OS as Order Service
 participant Kafka
 participant IS as Inventory Service
 participant PS as Payment Service
 participant NS as Notification Service
 OS->>Kafka: order.placed {order_id, items}
 Kafka->>IS: Consume order.placed → reserve inventory
 IS->>Kafka: inventory.reserved {order_id}
 Kafka->>PS: Consume inventory.reserved → charge payment
 PS->>Kafka: payment.completed {order_id}
 Kafka->>NS: Consume payment.completed → send confirmation
```
### Pattern 4: CQRS (Command Query Responsibility Segregation)
```mermaid
graph LR
 A[Write Model\\nOLTP DB\\nNormalized] -->|CDC| B[Kafka Change Stream]
 B -->|Stream Processor| C[Read Model 1\\nSearch Index\\nElasticsearch]
 B -->|Stream Processor| D[Read Model 2\\nAnalytics\\nClickHouse]
 B -->|Stream Processor| E[Read Model 3\\nCache\\nRedis]
```
---
## 4.14 Cross Questions — Streaming Systems
**Q1: What is the difference between event time and processing time? Why
does it matter?**
> Event time is when the event actually occurred; processing time is when it
arrives at the processor. They can differ by seconds or minutes due to
network delays. Processing time windows produce incorrect results when
events arrive out of order. Event time windows give correct business results
regardless of when events arrive, but require watermarks to handle late
data.
This PDF document was created using the online html editor powered by CKEditor.
**Q2: What is a watermark in streaming systems?**
> A watermark is a timestamp assertion that says "no event with event time ≤
t will arrive after this point." The processor uses it to know when a window
is complete enough to compute. A watermark of `max_event_time -
allowed_lateness` trades latency for completeness — larger allowed_lateness
catches more late events but delays results.
**Q3: Explain the difference between tumbling, sliding, and session
windows.**
> Tumbling: fixed-size, non-overlapping (hourly sales). Sliding: fixed-size
but overlapping (5-min window, every 1 min — 80% overlap for rolling
averages). Session: dynamic size, based on inactivity gap (user sessions —
window closes after 30 minutes of no events). Each window type aligns with
different business questions.
**Q4: How does Flink achieve exactly-once semantics?**
> Flink uses distributed snapshots (Chandy-Lamport): barriers injected into
the stream cause each operator to snapshot its state. The checkpoint
coordinator commits once all operators confirm. Kafka offsets are part of
the state. On failure, Flink rolls back to the last checkpoint (replays from
saved Kafka offset) and uses transactional or idempotent sinks to prevent
double-writing.
**Q5: What is the stream-table duality?**
> A stream is a changelog — an ordered sequence of events. A table is a
snapshot of the current state. They are two representations of the same
underlying truth: a table can be materialized from a stream by applying all
changes, and a stream can be generated from a table by emitting every change
(CDC). Kafka Streams and Flink SQL exploit this duality for stream-table
joins.
**Q6: What are the three output modes in Spark Structured Streaming?**
> Append (only new rows, no updates — for append-only computations),
Complete (full result table every trigger — for stateful aggregations
without watermark), Update (only changed rows since last trigger — for
aggregations with watermark). Update mode is most efficient for continuous
aggregation.
This PDF document was created using the online html editor powered by CKEditor.
**Q7: When would you choose Kafka Streams over Apache Flink?**
> Kafka Streams if: Kafka is the only source/sink, team prefers a library
(no cluster ops), simpler stateful aggregations. Flink if: multiple sources
(Kafka + DB + files), complex CEP, fine-grained watermark control, advanced
state management (large state that needs RocksDB or managed state backend),
or very high throughput requiring horizontal scaling.
**Q8: What is back-pressure in streaming and how is it handled?**
> Back-pressure occurs when a downstream operator processes slower than
upstream produces — the buffer fills and upstream is forced to slow down (or
drop data). Flink handles back-pressure through credit-based flow control:
downstream sends "credits" (buffer slots) to upstream; upstream only sends
when it has credits. This creates natural back-pressure propagation without
message loss.
---
## 4.15 Kafka vs AWS Kinesis vs Azure Event Hubs
When to choose which managed streaming platform:
| Feature | Apache Kafka | AWS Kinesis | Azure Event Hubs |
|---|---|---|---|
| **Hosting** | Self-managed or Confluent Cloud | Fully managed AWS | Fully
managed Azure |
| **Retention** | Configurable (days to forever) | 1–365 days | 1–90 days |
| **Ordering** | Within partition | Within shard | Within partition |
| **Throughput** | Unlimited (add brokers) | 1MB/s per shard | Scalable to
TB/s |
| **Consumer model** | Consumer group pull | KCL / Lambda polling | AMQP /
Kafka-compatible |
| **Exactly-once** | Yes (transactions) | No (at-least-once) | No (at-leastonce) |
| **Replay** | Full retention period | Retention period | Retention period |
| **Ecosystem** | Richest (Kafka Streams, KSQL, Flink, Spark) | AWS-native |
Azure-native |
| **Cost** | Infra + ops or Confluent cost | Per shard-hour + data | Per
This PDF document was created using the online html editor powered by CKEditor.
throughput unit |
**When to choose:**
| Scenario | Use This | Why |
|---|---|---|
| Multi-cloud or on-prem | **Apache Kafka** | No vendor lock-in; richest
ecosystem |
| AWS-native, simple streaming | **Kinesis** | Zero ops; Lambda integration;
pay per use |
| Azure stack + HIPAA/GDPR data residency | **Event Hubs** | Azure Purview
integration; geo-redundancy in Azure |
| Complex stream processing (joins, CEP) | **Kafka + Flink** | Kafka for
backbone; Flink for stateful processing |
---
## 4.16 Real-World Streaming Architectures
### Twitter / X — Real-Time Tweet Processing
**Problem:** 500M tweets/day; trending topics must update every 30 seconds;
global CDN delivery.
```mermaid
graph LR
 A[Tweet Posted\\nvia API] --> K[Kafka\\nTopic: tweets-raw\\n200
partitions]
 K --> F1[Flink Job\\nSpam filtering\\nContent moderation]
 F1 --> K2[Kafka\\nTopic: tweets-clean]
 K2 --> F2[Flink Job\\nTrending hashtag\\n5-min sliding window\\nper
region]
 F2 --> RD[Redis\\nTrending topics\\nPer region cache]
 RD --> CDN[CDN Edge\\nServe trending to users]
 K2 --> F3[Flink Job\\nUser home timeline\\nfan-out on write]
 F3 --> HB[HBase\\nTimeline storage]
```
This PDF document was created using the online html editor powered by CKEditor.
**Why Kafka here:** 200 partitions handle 500M tweets/day (≈5,800
tweets/sec); partition key = `user_id` ensures ordering per user; 7-day
retention allows replay for timeline recomputation.
**Why Flink here:** Sliding window for trending topics requires stateful
computation across millions of hashtag keys; Flink's keyed state stores perhashtag counts in RocksDB backend.
---
### Uber — Real-Time Surge Pricing
**Problem:** Calculate surge multiplier per geohex cell (≈500m × 500m) in
real-time. Update every 5 seconds based on supply (available drivers) and
demand (ride requests).
```mermaid
sequenceDiagram
 participant DA as Driver App
 participant RA as Rider App
 participant K as Kafka
 participant F as Flink
 participant RD as Redis
 participant UI as Rider UI
 DA->>K: GPS location event (every 5s)
 RA->>K: Ride request event
 K->>F: Consume supply + demand streams
 Note over F: Geohash both events\\nTumbling 30s window\\nCount
supply/demand per cell
 F->>RD: surge_multiplier per geohash
 UI->>RD: GET surge for pickup location
 RD->>UI: 1.8x surge → show to rider
```
**Geohashing:** Encode GPS coordinates into a fixed-length string (geohash).
Nearby locations share a common prefix — allows grouping by proximity
without complex spatial SQL.
This PDF document was created using the online html editor powered by CKEditor.
```python
import geohash2
# Driver at lat=40.7128, lon=-74.0060 (New York)
cell = geohash2.encode(40.7128, -74.0060, precision=6) # 'dr5reg'
# All drivers within ~1.2km share the same 6-char geohash prefix
```
---
### LinkedIn — Real-Time Feed Ranking
**Problem:** When a connection posts, deliver the post to relevant followers
within 1–2 seconds, ranked by relevance.
```mermaid
flowchart LR
 A[Post Created\\nby User A] --> K[Kafka\\nTopic: feed-events]
 K --> F[Flink\\nFan-out to followers\\nFilter by engagement score]
 F --> K2[Kafka\\nPer-user feed topics]
 K2 --> FS[Feature Store\\nRedis\\nUser interests, recency]
 FS --> RS[Ranking Service\\nML model scoring\\nper post+user pair]
 RS --> FDB[Feed DB\\nCassandra\\nUser feed per user_id]
 FDB --> APP[LinkedIn App\\nFetch user's feed]
```
**Why Kafka fan-out:** LinkedIn has 1B+ members. When a popular member (10M
followers) posts, Kafka handles fan-out to 10M feed queues without blocking
the post API.
**Why Cassandra for feed storage:** Feed is a per-user time-sorted list —
perfect for Cassandra's partition-by-user, cluster-by-timestamp model. High
write throughput from fan-out.
---
### Netflix — Distributed Tracing & Anomaly Detection
This PDF document was created using the online html editor powered by CKEditor.
**Problem:** Netflix has 1000+ microservices. When a streaming issue occurs,
find the root cause in < 5 minutes. Also detect video quality drops in realtime.
```mermaid
graph LR
 subgraph Client Events
 CE[Netflix Player\\nPlayback quality events\\nevery 30s per stream]
 end
 subgraph Streaming Pipeline
 K[Kafka\\n500GB/day of events]
 F[Flink\\nBitrate drop detection\\nBuffering ratio alerts\\nSliding
2-min window]
 end
 subgraph Alert System
 A[PagerDuty Alert\\nif P90 bitrate < threshold\\nfor a CDN region]
 end
 subgraph Historical
 ES[Elasticsearch\\nLog aggregation\\nTrace correlation]
 CH[ClickHouse\\nHistorical playback\\nquality metrics]
 end
 CE --> K
 K --> F --> A
 K --> ES
 K --> CH
```
**Why ClickHouse:** Netflix needs to query "what was the P95 bitrate for
users on Comcast in the US yesterday?" across billions of events.
ClickHouse's columnar storage + vectorized execution returns this in
seconds.
---
### Walmart — Real-Time Inventory Visibility
**Problem:** 4,700 stores; track inventory of 100K products in real-time;
prevent overselling on e-commerce during Black Friday (50x normal traffic).
This PDF document was created using the online html editor powered by CKEditor.
```mermaid
sequenceDiagram
 participant POS as Point of Sale
 participant K as Kafka (in-store broker)
 participant CK as Central Kafka Cluster
 participant F as Flink
 participant INV as Inventory Service (Redis)
 participant WEB as Walmart.com
 POS->>K: item_sold event {store_id, sku, qty}
 K->>CK: Replicate to central
 CK->>F: Consume inventory delta stream
 F->>INV: DECRBY inventory[sku][store] qty
 WEB->>INV: GET inventory[sku] (all stores)
 INV->>WEB: In-stock / Out-of-stock
```
**Why Redis for inventory:** O(1) DECRBY atomic operation; handles 1M
inventory updates/sec during peak. Sorted sets for "find nearest store with
stock" using geo commands.
**Why Flink:** Correlates POS events with online orders to detect potential
overselling (same item being purchased simultaneously in-store and online).
---
## 4.17 Streaming vs Batch — Real-World Decision Framework
| Business Question | Answer Needed In | Technology Choice | Real Example |
|---|---|---|---|
| "Is this credit card transaction fraud?" | < 200ms | Flink + ML serving |
Visa, Mastercard |
| "What is the current surge multiplier?" | < 5 seconds | Flink + Redis |
Uber, Lyft |
| "How many users are on the app right now?" | < 30 seconds | Spark
Structured Streaming | Netflix, Spotify |
| "What were yesterday's sales by region?" | < 1 hour (batch OK) | Spark +
This PDF document was created using the online html editor powered by CKEditor.
dbt | Any retailer |
| "Monthly revenue report for the board" | Next day (batch) | dbt +
Snowflake | Any company |
| "Product recommendations on homepage" | < 100ms (precomputed) | Flink for
feature update + Redis for serving | Amazon, Netflix |
**Key insight from Akidau:** The distinction between batch and streaming is
one of latency tolerance. A batch system is simply a streaming system with
very high latency tolerance. Design for streaming if you might ever need
lower latency — retooling is expensive.
---
# Part 5: Data Pipelines & Python Engineering
> Reference: *Data Pipelines Pocket Reference* — James Densmore
> Reference: *Data Engineering with Python* — Paul Crickard
---
## 5.1 What is a Data Pipeline?
A **data pipeline** is a series of data processing steps where data is
collected, processed, and stored in sequence — each step's output becoming
the next step's input.
```mermaid
graph LR
 A[Source\\nDB / API / File] -->|Extract| B[Raw / Staging\\nLayer]
 B -->|Transform| C[Processed\\nLayer]
 C -->|Load| D[Destination\\nDWH / Data Lake / App]

 E[Orchestrator\\nAirflow / Prefect] -.->|Schedule & Monitor| A
 E -.-> B
 E -.-> C
 E -.-> D
```
### Pipeline Components
This PDF document was created using the online html editor powered by CKEditor.
| Component | Role | Examples |
|---|---|---|
| **Ingestion** | Pull/receive data from sources | Fivetran, Airbyte, custom
Python |
| **Transport** | Move data between systems | Kafka, Kinesis, message queues
|
| **Storage** | Hold data at each stage | S3, PostgreSQL, Snowflake |
| **Transformation** | Process and reshape data | dbt, Spark, pandas, SQL |
| **Orchestration** | Schedule, monitor, retry | Airflow, Prefect, Dagster |
| **Serving** | Deliver results to consumers | BI tools, APIs, reverse ETL |
---
## 5.2 ETL vs ELT
### ETL (Extract, Transform, Load)
Transform data **before** loading into destination.
```mermaid
flowchart LR
 S[Source] -->|Extract| T[Transform\\nOn ETL Server]
 T -->|Load cleaned data| D[Destination DWH]
```
**When to use ETL:**
- Destination has limited compute (on-premise DWH)
- PII must be masked before entering the DWH
- Source data is very dirty (heavy transformation needed before storage)
### ELT (Extract, Load, Transform)
Load raw data **first**, then transform using destination compute.
```mermaid
flowchart LR
 S[Source] -->|Extract| L[Load raw data\\ninto Data Warehouse]
 L -->|Transform\\nusing DWH SQL engine| D[Analytics Layer]
```
This PDF document was created using the online html editor powered by CKEditor.
**When to use ELT:**
- Cloud DWH with elastic compute (Snowflake, BigQuery, Redshift)
- Need raw data for debugging and reruns
- Multiple consumers need different transformations from the same raw data
### Comparison
| Aspect | ETL | ELT |
|---|---|---|
| When transformed | Before loading | After loading |
| Raw data stored | No | Yes |
| Compute location | ETL server | DWH/Lakehouse |
| Reprocessing | Harder | Easy (raw data available) |
| Cost | ETL server cost | DWH query cost |
| Modern usage | Legacy on-prem | Preferred in cloud |
---
## 5.3 Pipeline Patterns
### Pattern 1: Full Refresh
Truncate destination and reload all source data every run.
```python
def full_refresh_load(source_conn, dest_conn, table_name):
 df = pd.read_sql(f"SELECT * FROM {table_name}", source_conn)
 dest_conn.execute(f"TRUNCATE TABLE {table_name}")
 df.to_sql(table_name, dest_conn, if_exists='append', index=False)
```
- **Pros**: Simple, always consistent
- **Cons**: Slow for large tables, high compute cost
- **Use when**: Small tables, no incremental key available
### Pattern 2: Incremental Load (Watermark-based)
Load only new/changed records since last run.
```python
This PDF document was created using the online html editor powered by CKEditor.
def incremental_load(source_conn, dest_conn, table_name,
last_run_timestamp):
 query = f"""
 SELECT * FROM {table_name}
 WHERE updated_at > '{last_run_timestamp}'
 """
 df = pd.read_sql(query, source_conn)

 # UPSERT into destination
 df.to_sql(f"staging_{table_name}", dest_conn, if_exists='replace',
index=False)
 dest_conn.execute(f"""
 MERGE INTO {table_name} AS target
 USING staging_{table_name} AS source
 ON target.id = source.id
 WHEN MATCHED THEN UPDATE SET ...
 WHEN NOT MATCHED THEN INSERT ...
 """)

 # Save new watermark
 new_watermark = df['updated_at'].max()
 save_watermark(table_name, new_watermark)
```
- **Pros**: Fast, low resource usage
- **Cons**: Misses hard deletes, requires reliable `updated_at` column
### Pattern 3: CDC-based Pipeline
```mermaid
sequenceDiagram
 participant PG as PostgreSQL
 participant D as Debezium
 participant K as Kafka
 participant SC as Schema Registry
 participant SP as Spark Streaming
 participant DL as Delta Lake
This PDF document was created using the online html editor powered by CKEditor.
 PG->>D: WAL change events
 D->>SC: Register/validate Avro schema
 D->>K: Publish CDC event (Avro)
 K->>SP: Consume CDC events
 SP->>DL: MERGE changes into Delta table
```
---
## 5.4 Apache Airflow — Pipeline Orchestration
**Apache Airflow** is the most widely used workflow orchestration platform
for data pipelines.
### Core Concepts
| Concept | Description |
|---|---|
| **DAG** | Directed Acyclic Graph — defines pipeline structure and
dependencies |
| **Task** | A unit of work within a DAG |
| **Operator** | Template for a task type (PythonOperator, BashOperator,
SparkSubmitOperator) |
| **Sensor** | Waits for a condition (file present, table updated, API
response) |
| **Connection** | Stored credentials for external systems |
| **Variable** | Runtime configuration values |
| **XCom** | Cross-communication: pass data between tasks |
| **Scheduler** | Parses DAGs, triggers tasks based on schedule |
| **Executor** | Executes tasks (Local, Celery, Kubernetes) |
### DAG Example: Daily Sales ETL
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import
This PDF document was created using the online html editor powered by CKEditor.
SnowflakeOperator
from datetime import datetime, timedelta
default_args = {
 'owner': 'data-engineering',
 'retries': 2,
 'retry_delay': timedelta(minutes=5),
 'email_on_failure': True,
 'email': ['alerts@company.com'],
}
with DAG(
 dag_id='daily_sales_etl',
 default_args=default_args,
 schedule_interval='0 6 * * *', # 6 AM daily
 start_date=datetime(2024, 1, 1),
 catchup=False, # Don't backfill missed runs
 tags=['sales', 'etl'],
 description='Daily sales data pipeline from OLTP to Snowflake'
) as dag:
 start = EmptyOperator(task_id='start')
 extract_orders = PythonOperator(
 task_id='extract_orders',
 python_callable=extract_orders_from_postgres,
 op_kwargs={'execution_date': '{{ ds }}'}, # Jinja templating
 )
 extract_customers = PythonOperator(
 task_id='extract_customers',
 python_callable=extract_customers_from_crm,
 )
 load_to_staging = PythonOperator(
 task_id='load_to_staging',
 python_callable=load_to_s3_staging,
 )
This PDF document was created using the online html editor powered by CKEditor.
 transform_in_snowflake = SnowflakeOperator(
 task_id='transform_in_snowflake',
 snowflake_conn_id='snowflake_prod',
 sql='sql/transform_daily_sales.sql',
 params={'run_date': '{{ ds }}'},
 )
 data_quality_check = PythonOperator(
 task_id='data_quality_check',
 python_callable=run_great_expectations_suite,
 )
 end = EmptyOperator(task_id='end')
 # Define dependencies
 start >> [extract_orders, extract_customers] >> load_to_staging
 load_to_staging >> transform_in_snowflake >> data_quality_check >> end
```
### Airflow Scheduling
```
Cron expression: 0 6 * * *
 │ │ │ │ │
 │ │ │ │ └── Day of week (0=Sun, 7=Sun)
 │ │ │ └──── Month (1-12)
 │ │ └────── Day of month (1-31)
 │ └──────── Hour (0-23)
 └────────── Minute (0-59)
Examples:
 @daily = 0 0 * * * (midnight)
 @hourly = 0 * * * *
 @weekly = 0 0 * * 0
 0 6 * * 1-5 = 6 AM weekdays only
```
This PDF document was created using the online html editor powered by CKEditor.
### Airflow Execution Modes
| Executor | Description | Use case |
|---|---|---|
| **SequentialExecutor** | Run tasks sequentially, one at a time |
Development only |
| **LocalExecutor** | Run tasks in parallel on same machine | Small teams,
single server |
| **CeleryExecutor** | Distribute tasks to worker pool via message queue |
Production, horizontal scaling |
| **KubernetesExecutor** | Each task runs in a fresh Kubernetes pod | Cloudnative, isolation |
### Common Pitfalls with Airflow
| Pitfall | Issue | Fix |
|---|---|---|
| Long-running Python in tasks | Memory leak in Airflow worker | Use
ExternalTaskSensor + KubernetesPodOperator |
| Fat DAG files | Slow parsing, scheduler lag | Keep DAG files thin — logic
in modules |
| XCom for large data | XComs stored in Airflow DB — size limit | Use S3/GCS
for large payloads, XCom for keys |
| Backfill confusion | `catchup=True` triggers missed runs | Set
`catchup=False` unless backfill intended |
| Secrets in DAGs | Credentials in code | Use Airflow Connections + Secrets
Backend (Vault) |
---
## 5.5 Prefect & Dagster — Modern Alternatives
### Prefect
```python
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
This PDF document was created using the online html editor powered by CKEditor.
@task(
 retries=3,
 retry_delay_seconds=60,
 cache_key_fn=task_input_hash, # Cache based on inputs
 cache_expiration=timedelta(hours=1)
)
def extract_data(start_date: str) -> pd.DataFrame:
 return pd.read_sql(
 f"SELECT * FROM orders WHERE order_date = '{start_date}'",
 conn
 )
@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
 return df.dropna().assign(revenue=df['qty'] * df['price'])
@task
def load_data(df: pd.DataFrame, table: str):
 df.to_sql(table, dest_conn, if_exists='append', index=False)
@flow(name="Daily Sales ETL", log_prints=True)
def daily_sales_flow(run_date: str = "2024-01-01"):
 df_raw = extract_data(run_date)
 df_clean = transform_data(df_raw)
 load_data(df_clean, "fact_sales")
if __name__ == "__main__":
 daily_sales_flow.serve(
 name="daily-sales-deployment",
 cron="0 6 * * *"
 )
```
**Prefect vs Airflow:**
| Aspect | Airflow | Prefect |
|---|---|---|
This PDF document was created using the online html editor powered by CKEditor.
| Config | DAG files (Python) | Flow files (Python) |
| Dynamic workflows | Complex (dynamic DAGs) | Native support |
| Local testing | Difficult | Easy (just `python flow.py`) |
| Deployment | Always requires Airflow cluster | Prefect Cloud or selfhosted |
| Learning curve | Steeper | Gentler |
### Dagster
Dagster is **asset-centric**: focuses on the data assets produced, not just
the tasks.
```python
from dagster import asset, AssetIn, define_asset_job, ScheduleDefinition
@asset(group_name="raw")
def raw_orders(context) -> pd.DataFrame:
 """Raw orders from PostgreSQL"""
 return pd.read_sql("SELECT * FROM orders", pg_conn)
@asset(
 group_name="staging",
 ins={"raw_orders": AssetIn()}
)
def cleaned_orders(raw_orders: pd.DataFrame) -> pd.DataFrame:
 """Cleaned and validated orders"""
 return raw_orders.dropna(subset=["customer_id", "amount"])
@asset(
 group_name="marts",
 ins={"cleaned_orders": AssetIn()}
)
def daily_sales_summary(cleaned_orders: pd.DataFrame) -> pd.DataFrame:
 """Daily sales aggregation"""
 return cleaned_orders.groupby("order_date")
["amount"].sum().reset_index()
# Schedule
This PDF document was created using the online html editor powered by CKEditor.
daily_job = define_asset_job("daily_pipeline", selection="*")
daily_schedule = ScheduleDefinition(job=daily_job, cron_schedule="0 6 * *
*")
```
---
## 5.6 Data Ingestion Tools
### Fivetran / Airbyte — Managed Connectors
```mermaid
graph LR
 subgraph Sources
 S1[Salesforce]
 S2[Stripe]
 S3[Google Analytics]
 S4[PostgreSQL]
 end

 subgraph Connector Platform
 FT[Fivetran / Airbyte\\nPre-built connectors\\nAuto schema
detection\\nScheduled syncs]
 end

 subgraph Destination
 SF[Snowflake]
 BQ[BigQuery]
 RD[Redshift]
 end

 S1 --> FT
 S2 --> FT
 S3 --> FT
 S4 --> FT
 FT --> SF
 FT --> BQ
 FT --> RD
This PDF document was created using the online html editor powered by CKEditor.
```
**Fivetran vs Airbyte:**
| Aspect | Fivetran | Airbyte |
|---|---|---|
| Pricing | Connector-based (expensive) | Open source + cloud option |
| Connector count | 300+ (managed) | 300+ (community) |
| Custom connectors | Limited | Yes (Python SDK) |
| Hosting | Fully managed | Self-host or Airbyte Cloud |
| Enterprise features | Yes | Yes (EE) |
### Custom Python Ingestion
```python
import requests
import boto3
import json
from datetime import datetime
def ingest_from_rest_api(api_url: str, api_key: str, s3_bucket: str,
s3_key_prefix: str):
 """
 Pulls data from REST API and lands in S3 as newline-delimited JSON.
 Idempotent: uses run timestamp in S3 key.
 """
 headers = {"Authorization": f"Bearer {api_key}"}

 records = []
 page = 1
 while True:
 response = requests.get(
 f"{api_url}?page={page}&per_page=1000",
 headers=headers,
 timeout=30
 )
 response.raise_for_status()
 data = response.json()
This PDF document was created using the online html editor powered by CKEditor.

 if not data.get("results"):
 break

 records.extend(data["results"])
 page += 1

 # Land in S3 with timestamp partition for idempotency
 run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
 s3_key = f"{s3_key_prefix}/run={run_ts}/data.json"

 s3 = boto3.client("s3")
 ndjson = "\\n".join(json.dumps(r) for r in records)
 s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=ndjson.encode())

 print(f"Loaded {len(records)} records to s3://{s3_bucket}/{s3_key}")
 return len(records)
```
---
## 5.7 Data Quality & Validation
Data quality is the silent killer of data trust. Poor quality data leads to
wrong decisions.
### Data Quality Dimensions
| Dimension | Description | Example Test |
|---|---|---|
| **Completeness** | No missing required values | `NOT NULL` checks, null
rate < 1% |
| **Accuracy** | Values are correct | Revenue > 0, age between 0-120 |
| **Consistency** | No contradictions | Order date ≤ ship date |
| **Timeliness** | Data is fresh | Max event_date within last 24 hours |
| **Uniqueness** | No duplicates | Primary key is unique |
| **Validity** | Values in allowed set | Status IN
('active','inactive','pending') |
This PDF document was created using the online html editor powered by CKEditor.
| **Referential Integrity** | Foreign keys valid | Every order has a valid
customer |
### Great Expectations — Data Validation Framework
```python
import great_expectations as gx
context = gx.get_context()
# Define datasource (connect to Pandas DataFrame)
datasource = context.sources.add_pandas("orders_datasource")
asset = datasource.add_csv_asset("orders", filepath_or_buffer="orders.csv")
batch_request = asset.build_batch_request()
# Create expectation suite
suite = context.add_or_update_expectation_suite("orders_suite")
validator = context.get_validator(
 batch_request=batch_request,
 expectation_suite=suite
)
# Define expectations
validator.expect_column_to_exist("order_id")
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_be_between("amount", min_value=0,
max_value=1_000_000)
validator.expect_column_values_to_be_in_set(
 "status", {"pending", "confirmed", "shipped", "cancelled"}
)
validator.expect_column_pair_values_A_to_be_greater_than_B(
 "ship_date", "order_date"
This PDF document was created using the online html editor powered by CKEditor.
)
# Validate and get results
validation_result = validator.validate()
print(f"Validation passed: {validation_result.success}")
# Save suite
validator.save_expectation_suite()
```
### dbt Data Tests
```yaml
# models/schema.yml
models:
 - name: fact_sales
 description: "Daily sales transactions"
 columns:
 - name: sales_key
 tests:
 - unique
 - not_null
 - name: customer_key
 tests:
 - not_null
 - relationships:
 to: ref('dim_customer')
 field: customer_key
 - name: revenue
 tests:
 - not_null
 - dbt_utils.accepted_range:
 min_value: 0
 max_value: 1000000
# Custom singular test
# tests/assert_no_future_orders.sql
SELECT order_date
This PDF document was created using the online html editor powered by CKEditor.
FROM {{ ref('fact_sales') }}
WHERE order_date > CURRENT_DATE
```
---
## 5.8 Pipeline Reliability Patterns
### Idempotency
An operation is **idempotent** if running it multiple times produces the
same result as running it once.
```python
# NON-idempotent (appends duplicates on rerun)
def load_data_bad(df, table):
 df.to_sql(table, conn, if_exists='append')
# Idempotent (UPSERT: no duplicates on rerun)
def load_data_good(df, table, unique_key):
 # Write to temp staging table
 df.to_sql(f"staging_{table}", conn, if_exists='replace')
 # Merge into target
 conn.execute(f"""
 MERGE INTO {table} t
 USING staging_{table} s ON t.{unique_key} = s.{unique_key}
 WHEN MATCHED THEN UPDATE SET ...
 WHEN NOT MATCHED THEN INSERT ...
 """)
```
### Dead Letter Queue (DLQ)
```mermaid
graph LR
 A[Source] --> P[Pipeline]
 P -->|Success| D[Destination]
 P -->|Failure after retries| DLQ[Dead Letter Queue\\nS3 / Kafka DLQ
This PDF document was created using the online html editor powered by CKEditor.
topic]
 DLQ --> E[Alert + Manual Review]
 DLQ -->|After fix| P2[Reprocess Pipeline]
```
Records that fail after all retries go to DLQ for investigation — they're
not silently dropped.
### Backfilling
```python
# Airflow backfill — rerun historical DAG runs
# airflow dags backfill --start-date 2024-01-01 --end-date 2024-01-31
daily_sales_etl
# Programmatic backfill with Prefect
@flow
def backfill_sales():
 for date in pd.date_range("2024-01-01", "2024-01-31"):
 daily_sales_flow(run_date=str(date.date()))
```
### Circuit Breaker Pattern
```python
from circuitbreaker import circuit
@circuit(failure_threshold=5, recovery_timeout=60)
def call_external_api(url: str) -> dict:
 """
 After 5 failures, circuit 'opens' for 60 seconds.
 Prevents cascade failures from a slow/down API.
 """
 response = requests.get(url, timeout=10)
 response.raise_for_status()
 return response.json()
```
This PDF document was created using the online html editor powered by CKEditor.
---
## 5.9 Python for Data Engineering — Key Patterns
### PySpark ETL Pattern
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, upper, when
from pyspark.sql.types import StructType, StructField, StringType,
DoubleType, DateType
def run_sales_etl(spark: SparkSession, input_path: str, output_path: str,
run_date: str):
 """
 ETL: Raw CSV → Cleaned Parquet (partitioned by date)
 Idempotent: overwrites partition for run_date
 """
 raw_df = spark.read \\
 .option("header", "true") \\
 .option("inferSchema", "false") \\
 .csv(input_path)

 cleaned_df = raw_df \\
 .filter(col("order_id").isNotNull()) \\
 .filter(col("amount").cast("double") > 0) \\
 .withColumn("customer_name", trim(upper(col("customer_name")))) \\
 .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
\\
 .withColumn("amount", col("amount").cast(DoubleType())) \\
 .withColumn("status", when(col("status").isin(
 "pending", "confirmed", "shipped", "cancelled"
 ), col("status")).otherwise("unknown")) \\
 .dropDuplicates(["order_id"])

 # Write partitioned by date — overwrite only today's partition
 cleaned_df \\
 .filter(col("order_date") == run_date) \\
This PDF document was created using the online html editor powered by CKEditor.
 .write \\
 .mode("overwrite") \\
 .partitionBy("order_date") \\
 .parquet(output_path)

 count = cleaned_df.count()
 print(f"Processed {count} records for {run_date}")
 return count
if __name__ == "__main__":
 spark = SparkSession.builder \\
 .appName("SalesETL") \\
 .config("spark.sql.adaptive.enabled", "true") \\
 .getOrCreate()

 run_sales_etl(spark, "s3://raw/sales/", "s3://processed/sales/", "2024-
03-15")
```
### Delta Lake / Lakehouse Pattern
```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
def upsert_to_delta(spark: SparkSession, new_data_df, delta_path: str,
merge_keys: list):
 """
 Merge new data into Delta Lake table (handles inserts + updates).
 """
 if DeltaTable.isDeltaTable(spark, delta_path):
 delta_table = DeltaTable.forPath(spark, delta_path)

 merge_condition = " AND ".join(
 [f"target.{k} = source.{k}" for k in merge_keys]
 )

 delta_table.alias("target") \\
This PDF document was created using the online html editor powered by CKEditor.
 .merge(new_data_df.alias("source"), merge_condition) \\
 .whenMatchedUpdateAll() \\
 .whenNotMatchedInsertAll() \\
 .execute()
 else:
 # First run — create Delta table
 new_data_df.write.format("delta").mode("overwrite").save(delta_path)

 # Time travel: read previous version
 # spark.read.format("delta").option("versionAsOf", 3).load(delta_path)
 # spark.read.format("delta").option("timestampAsOf", "2024-01-
01").load(delta_path)
```
---
## 5.10 Cross Questions — Data Pipelines
**Q1: What is the difference between ETL and ELT? Which is preferred in
modern cloud architectures?**
> ETL transforms before loading (useful when destination has limited compute
or PII must be masked). ELT loads raw data first and transforms using the
warehouse's elastic compute (Snowflake, BigQuery). ELT is preferred in
modern cloud because: raw data is retained for reprocessing, compute is
cheap and elastic, and multiple teams can build different transformations
from the same raw data.
**Q2: What makes a data pipeline idempotent and why is it important?**
> An idempotent pipeline produces the same result regardless of how many
times it runs. It's critical because pipelines fail and get retried. Without
idempotency, retries cause duplicate data. Achieve idempotency using:
UPSERT/MERGE instead of INSERT, overwriting partitions rather than
appending, and using deterministic keys (content hashes, natural keys) for
deduplication.
**Q3: What is a Dead Letter Queue and when would you use it?**
> A DLQ captures records that fail processing after all retries, rather than
silently dropping them. Use it when: individual record failures shouldn't
This PDF document was created using the online html editor powered by CKEditor.
block the pipeline, data must be retained for manual review, or failed
records need to be reprocessed after a bug fix. Common implementations:
Kafka DLQ topic, S3 error bucket.
**Q4: What are the key components of Apache Airflow?**
> DAG (pipeline definition + dependencies), Tasks (units of work), Operators
(task templates — PythonOperator, BashOperator, SQLOperator), Sensors (wait
for conditions), Scheduler (trigger tasks), Executor (run tasks — Local,
Celery, Kubernetes), XCom (inter-task communication), Connections (external
system credentials).
**Q5: How do you handle late-arriving data in a batch pipeline?**
> Several strategies: (1) Rerun the pipeline for the affected day when late
data arrives. (2) Use a longer lookback window (e.g., process last 3 days of
data each run to catch late arrivals). (3) Use Delta Lake with MERGE — late
records are upserted into the correct partition. (4) Use SCD Type 2 with an
effective date — late records are inserted with correct timestamps.
**Q6: What is Great Expectations and how does it improve data pipeline
reliability?**
> Great Expectations is a data quality framework that defines expectations
(assertions) about data — nullability, value ranges, uniqueness, referential
integrity. Running these tests in a pipeline catches data issues before they
propagate downstream. Results are published as data docs (HTML reports) and
can block pipeline progression on failure.
**Q7: What is the difference between Airflow, Prefect, and Dagster?**
> Airflow: mature, DAG-based, requires infrastructure, task-centric.
Prefect: modern, Python-first, easier local testing, native dynamic
workflows, Prefect Cloud for SaaS. Dagster: asset-centric (models data
assets produced, not just tasks), strong lineage, type-checked IO. Use
Airflow for established large teams, Prefect for agile teams, Dagster when
data assets and lineage are primary concerns.
**Q8: How do you test a data pipeline?**
> Unit tests: test individual transformation functions with mock data.
Integration tests: test with real (or containerized) source/destination
systems. Data quality tests: Great Expectations or dbt tests on the output.
This PDF document was created using the online html editor powered by CKEditor.
End-to-end tests: run the full pipeline on a sample dataset in a staging
environment. Regression tests: compare output counts/aggregates between runs
to detect anomalies.
---
## 5.11 Pipeline Observability & Monitoring
Observability is knowing not just that something failed, but **why**.
### The Three Pillars
```mermaid
graph TD
 O[Observability]
 O --> M[Metrics\\nNumeric measurements\\nover time\\nPrometheus +
Grafana]
 O --> L[Logs\\nStructured event records\\nof what happened\\nELK /
Datadog]
 O --> T[Traces\\nEnd-to-end request flow\\nacross
services\\nOpenTelemetry / Jaeger]
```
### Key Pipeline Metrics to Monitor
| Metric | Description | Alert If |
|---|---|---|
| **Records processed/sec** | Throughput of pipeline | Drops > 30% vs
baseline |
| **Consumer lag (Kafka)** | How far behind is the consumer? | Lag > SLAdefined threshold |
| **Pipeline duration** | How long does a DAG run take? | Duration > 2× p90
baseline |
| **Error rate** | % records sent to DLQ | > 0.1% |
| **Data freshness** | Age of latest record in destination | Older than SLA
freshness window |
| **Row count delta** | Rows today vs rows yesterday | Deviation > ±20% |
| **Null rate** | % null values in required columns | > 1% on NOT NULL
This PDF document was created using the online html editor powered by CKEditor.
columns |
### Airflow Observability Setup
```python
from airflow.callbacks import on_failure_callback
from airflow.providers.slack.operators.slack_webhook import
SlackWebhookOperator
def on_failure_alert(context):
 """Send Slack alert on task failure."""
 dag_id = context.get('dag').dag_id
 task_id = context.get('task_instance').task_id
 run_id = context.get('run_id')
 log_url = context.get('task_instance').log_url

 slack_msg = f"""
 :red_circle: *Pipeline Failure*
 *DAG:* {dag_id}
 *Task:* {task_id}
 *Run ID:* {run_id}
 *Logs:* <{log_url}|View Logs>
 """
 SlackWebhookOperator(
 task_id='slack_alert',
 slack_webhook_conn_id='slack_alerts',
 message=slack_msg
 ).execute(context)
default_args = {
 'on_failure_callback': on_failure_alert,
 'sla': timedelta(hours=2), # Alert if task takes > 2 hours
}
```
### Data Observability Tools
| Tool | What it monitors | Company Use |
This PDF document was created using the online html editor powered by CKEditor.
|---|---|---|
| **Monte Carlo** | Automated anomaly detection on tables (row count, nulls,
schema drift) | Robinhood, Fox |
| **Bigeye** | Rule-based + ML anomaly detection | Snowflake, JetBlue |
| **Great Expectations** | Explicit expectation-based quality checks | Many
companies |
| **dbt tests** | Schema tests embedded in transformation layer | dbt users
|
| **Datafold** | Diff between pipeline runs, column-level impact | Stitch
Fix |
### Structured Logging for Pipelines
```python
import logging
import json
from datetime import datetime
# Configure structured JSON logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline")
class StructuredLogger:
 def __init__(self, pipeline_name: str, run_id: str):
 self.pipeline_name = pipeline_name
 self.run_id = run_id

 def log(self, event: str, level: str = "INFO", **kwargs):
 record = {
 "timestamp": datetime.utcnow().isoformat(),
 "level": level,
 "pipeline": self.pipeline_name,
 "run_id": self.run_id,
 "event": event,
 **kwargs
 }
 print(json.dumps(record)) # → Parsed by Datadog / ELK
This PDF document was created using the online html editor powered by CKEditor.
# Usage in pipeline
log = StructuredLogger("sales_etl", run_id="2024-03-15-001")
log.log("extraction_complete", records_extracted=150000, source="postgres",
duration_s=45.2)
log.log("load_complete", records_loaded=149850, records_rejected=150,
destination="snowflake")
```
---
## 5.12 Data Contracts
A **Data Contract** is a formal, schema-versioned agreement between a data
producer and its consumers — specifying structure, semantics, SLAs, and
ownership.
```yaml
# Example: data_contract_orders.yaml
apiVersion: v1
kind: DataContract
metadata:
 name: orders
 domain: commerce
 owner: commerce-platform-team
 version: "2.3.0"
schema:
 - name: order_id
 type: STRING
 required: true
 description: "Unique order identifier, format ORD-YYYYMMDD-NNNNNNN"
 pii: false
 - name: customer_id
 type: STRING
 required: true
 pii: true
 masking: "hash_sha256"
 - name: order_total_usd
This PDF document was created using the online html editor powered by CKEditor.
 type: DECIMAL(10,2)
 required: true
 constraints:
 min: 0.01
 max: 999999.99
quality:
 - rule: row_count_not_zero
 - rule: no_nulls
 columns: [order_id, customer_id, order_date]
 - rule: unique
 columns: [order_id]
 - rule: freshness
 max_age_hours: 6
sla:
 availability: "99.9%"
 freshness: "6 hours"
 support_contact: "commerce-data@company.com"
changelog:
 - version: "2.3.0"
 date: "2024-03-01"
 change: "Added discount_code column (backward compatible)"
 - version: "2.2.0"
 date: "2024-01-15"
 change: "Added shipping_method column"
```
**Why data contracts matter:**
- Prevents "silent breaking changes" — producers changing schemas without
notifying consumers
- Creates clear ownership and accountability
- Enables automated enforcement in CI/CD (schema validation on PR)
- Soda Core and Atlan can validate contracts at runtime
---
This PDF document was created using the online html editor powered by CKEditor.
## 5.13 Real-World Pipeline Architecture Examples
### Airbnb — How They Built Airflow
Airbnb built Apache Airflow in 2014 to solve their pipeline problem:
- **Before Airflow**: Cron jobs with no dependency management; failed jobs
silently skipped; no visibility
- **After Airflow**: 4,000+ DAGs in production; dependency graph visible in
UI; SLA monitoring
**What they found critical:**
```python
# 1. Idempotent tasks — every task safe to rerun
# 2. Parametrized by execution_date — pure function of time
# 3. Short tasks (< 30 min) — easier to retry
# 4. Separating heavy computation to Spark (not Airflow workers)
with DAG('airbnb_pricing_pipeline', schedule_interval='@daily') as dag:
 # Task 1: Extract (thin — just move data)
 extract = SparkSubmitOperator(
 task_id='extract_bookings',
 application='jobs/extract_bookings.py',
 application_args=['--date', '{{ ds }}']
 )
 # Task 2: Transform (Spark job — not in Airflow worker memory)
 transform = SparkSubmitOperator(
 task_id='compute_dynamic_pricing',
 application='jobs/pricing_model.py',
 application_args=['--date', '{{ ds }}']
 )
 # Task 3: Quality gate
 quality_check = PythonOperator(
 task_id='validate_pricing_output',
 python_callable=run_price_sanity_checks,
 op_kwargs={'date': '{{ ds }}'}
 )
 extract >> transform >> quality_check
```
This PDF document was created using the online html editor powered by CKEditor.
---
### Spotify — Data Pipeline for 456M Users
**Challenge:** Generate personalized "Discover Weekly" playlist for 456M
users every Monday.
```mermaid
flowchart TD
 A[Streaming History\\nKafka → S3\\n~10TB/day] --> B[Spark Job\\nWeekly:
Compute\\nuser-song interaction\\nmatrix]
 B --> C[ML Model\\nCollaborative Filtering\\nMatrix
Factorization\\nApache Spark MLlib]
 C --> D[Feature Store\\nS3 + Redis\\nUser embeddings]
 D --> E[Recommendation Job\\n~100M users\\nTop-30 songs per user\\nSpark
distributed]
 E --> F[Cassandra\\nPlaylist storage\\nper user_id]
 F --> G[Spotify App\\nFetch Discover Weekly\\non Monday]

 H[Airflow DAG\\nScheduled Sunday midnight\\n4-hour SLA] -.-> B
 H -.-> C
 H -.-> E
```
**Scale:**
- 100M Discover Weekly playlists generated in 4 hours
- Spark cluster: 2,000 executors for matrix factorization
- Cassandra: 3TB of playlist data, written in 90 minutes
---
### Robinhood — Financial Data Pipeline with Compliance
**Challenge:** Process 2M+ daily trades; generate regulatory reports (SEC,
FINRA) by 8 PM EST; every trade must appear in reports (zero tolerance for
data loss).
This PDF document was created using the online html editor powered by CKEditor.
```mermaid
graph TD
 subgraph Trading Engine
 TE[Trade Execution\\nPostgreSQL OLTP]
 end

 subgraph CDC Layer
 D[Debezium\\nLog-based CDC from PostgreSQL\\nPublishes to Kafka]
 K[Kafka\\nTopic: trades\\nRF=3, acks=all\\nRetention=30 days]
 end

 subgraph Validation Gate
 FV[Flink Job\\nFinancial Validation\\nBalance checks\\nRegulatory
flags]
 DLQ[DLQ - S3\\nFailed trades for\\nhuman review]
 end

 subgraph Regulatory Reports
 SP[Spark Batch\\n3 PM daily\\nAggregate + format\\nSEC/FINRA
reports]
 SF[Snowflake\\nAudit trail\\n7-year retention]
 end

 TE --> D --> K --> FV
 FV -->|Valid| SF
 FV -->|Invalid| DLQ
 SF --> SP --> REG[Regulatory Report\\nSubmitted by 8 PM]
```
**Key choices:**
- **Debezium log CDC**: Captures every trade change without missing any —
critical for financial compliance
- **Kafka RF=3, acks=all**: All 3 replicas acknowledge before write
confirmed — zero message loss
- **7-year retention in Snowflake**: SEC requires broker-dealers to retain
trade records 7 years
- **DLQ with human review**: Regulatory data can't be auto-dropped; every
failure requires human sign-off
This PDF document was created using the online html editor powered by CKEditor.
---
### Orchestration Tool Selection Guide
```mermaid
flowchart TD
 A{Team size &\\nmaturity?}
 A -->|Small team, prototype| PREF[Prefect Cloud\\nZero ops, Pythonnative\\nFast setup]
 A -->|Medium team, growth stage| DGSTR[Dagster\\nAsset-centric
lineage\\nStrong typing]
 A -->|Large team, multiple pipelines| AF[Apache Airflow\\nEnterprise
adoption\\nRich ecosystem]
 A -->|Fully managed, ML-heavy| DBT[dbt Cloud + Airflow\\nSQL transforms
+ orchestration]
 A -->|Azure ecosystem| ADF[Azure Data Factory\\nNo-code + code\\nNative
Azure integration]
```
| Company | Orchestrator | Why |
|---|---|---|
| **Airbnb** | Airflow (built it) | Complex multi-team dependency management
|
| **Lyft** | Flyte (built it) | ML pipeline orchestration with type safety |
| **Shopify** | Prefect | Easy Python; Cloud removes ops burden |
| **GitLab** | dbt Cloud | SQL-first; all analytics in dbt |
| **Optum / UHG** | Azure Data Factory | Azure-native; no-code for nonengineers; GUI-driven |
---
# Part 6: Cloud Data Platforms & Architecture Patterns
> Reference: *Designing Cloud Data Platforms* — Danil Zburivsky & Lynda
Partner
---
## 6.1 Cloud Data Platform Overview
This PDF document was created using the online html editor powered by CKEditor.
A **Cloud Data Platform** is an integrated set of cloud-native services
enabling the full data engineering lifecycle — from ingestion to serving —
leveraging cloud elasticity, managed services, and pay-per-use economics.
```mermaid
graph TD
 subgraph Ingestion Layer
 I1[Batch APIs / Files]
 I2[CDC / Debezium]
 I3[Streaming Events]
 end
 subgraph Storage Layer
 L1[Raw / Landing Zone\\nS3 / ADLS / GCS]
 L2[Processed Zone\\nDelta / Iceberg / Hudi]
 L3[Serving Zone\\nData Warehouse\\nSnowflake / BigQuery]
 end
 subgraph Processing Layer
 P1[Batch\\nSpark / dbt]
 P2[Streaming\\nFlink / Kafka Streams]
 end
 subgraph Serving Layer
 S1[BI & Reporting\\nTableau / Power BI]
 S2[ML Platform\\nMLflow / SageMaker]
 S3[Self-serve\\nData Catalog + SQL]
 S4[Reverse ETL\\nHightouch / Census]
 end
 subgraph Platform Services
 G[Governance\\nAtlas / Purview]
 ORC[Orchestration\\nAirflow / Prefect]
 MON[Monitoring\\nDatadog / Monte Carlo]
 SEC[Security\\nIAM / KMS / RBAC]
 end
This PDF document was created using the online html editor powered by CKEditor.
 I1 --> L1
 I2 --> L1
 I3 --> L2
 L1 --> P1 --> L2
 L2 --> P1 --> L3
 L2 --> P2 --> L3
 L3 --> S1
 L3 --> S2
 L3 --> S3
 L3 --> S4
```
---
## 6.2 Cloud Data Platform Architecture Layers
### Landing / Raw Zone (Bronze)
- Store data **exactly as received** from source — no transformations
- Immutable: never modify or delete raw data
- Format: native source format (JSON, CSV, Avro, binary)
- Enables full reprocessing if transformation logic changes
### Processed / Curated Zone (Silver)
- Cleaned, validated, enriched data
- Format: Parquet or Delta Lake (columnar, compressed)
- Deduplication applied
- Conforms to enterprise schema standards
- Partitioned for efficient querying
### Serving / Gold Zone
- Aggregated, business-ready data
- Dimensional models (star schemas)
- Materialized into data warehouse or query-optimized tables
- Accessed by BI tools, analysts, ML pipelines
### Medallion Architecture (Databricks)
```mermaid
This PDF document was created using the online html editor powered by CKEditor.
flowchart LR
 A[(Source Systems)] --> B[Bronze\\nRaw / Landing\\nImmutable
copies\\nSchema-on-read]
 B --> C[Silver\\nCleaned / Normalized\\nSchema-on-write\\nDeduped +
joined]
 C --> D[Gold\\nBusiness-ready\\nAggregated\\nStar schemas / Mart tables]
 D --> E[(BI Tools / ML)]
```
---
## 6.3 Major Cloud Data Platforms Compared
### AWS Data Platform Stack
```mermaid
graph TD
 subgraph Ingestion
 A1[AWS Kinesis\\nStreaming ingestion]
 A2[AWS DMS\\nDatabase migration + CDC]
 A3[AWS Glue\\nManaged ETL]
 end
 subgraph Storage
 A4[S3\\nObject storage - Data Lake]
 A5[Redshift\\nCloud Data Warehouse]
 A6[DynamoDB\\nNoSQL]
 end
 subgraph Processing
 A7[EMR\\nManaged Spark/Hadoop]
 A8[Glue\\nServerless ETL]
 A9[Lambda\\nServerless event processing]
 end
 subgraph Catalog & Governance
 A10[AWS Glue Catalog\\nMetastore]
 A11[Lake Formation\\nSecurity + Governance]
 end
```
This PDF document was created using the online html editor powered by CKEditor.
### Azure Data Platform Stack
```mermaid
graph TD
 subgraph Ingestion
 Z1[Azure Event Hubs\\nKafka-compatible streaming]
 Z2[Azure Data Factory\\nBatch ETL + CDC]
 end
 subgraph Storage
 Z3[ADLS Gen2\\nAzure Data Lake Storage]
 Z4[Azure Synapse Analytics\\nDWH + Spark]
 Z5[Cosmos DB\\nNoSQL - globally distributed]
 end
 subgraph Processing
 Z6[Azure Databricks\\nSpark + Delta Lake]
 Z7[Synapse Spark\\nIntegrated Spark]
 end
 subgraph Governance
 Z8[Microsoft Purview\\nData Catalog + Lineage]
 end
```
### GCP Data Platform Stack
```mermaid
graph TD
 subgraph Ingestion
 G1[Pub/Sub\\nEvent streaming]
 G2[Dataflow\\nBatch + stream ETL\\nApache Beam]
 end
 subgraph Storage
 G3[Google Cloud Storage\\nObject storage]
 G4[BigQuery\\nSQLDWH + ML]
 G5[Bigtable\\nNoSQL time-series]
 end
 subgraph Processing
 G6[Dataproc\\nManaged Spark]
 G7[BigQuery ML\\nML in SQL]
This PDF document was created using the online html editor powered by CKEditor.
 end
 subgraph Governance
 G8[Dataplex\\nData governance + Catalog]
 end
```
---
## 6.4 Lambda Architecture
The Lambda Architecture handles both batch and streaming use cases by
maintaining two separate paths.
```mermaid
graph LR
 subgraph All Data
 D[Data Stream]
 end

 subgraph Batch Layer
 BL[Batch Store\\nS3 / HDFS\\nAll historical data]
 BV[Batch Views\\nPrecomputed aggregates\\nSpark / Hive]
 end

 subgraph Speed Layer
 SL[Stream Store\\nKafka / Kinesis]
 SV[Real-time Views\\nLow-latency increments\\nFlink / Storm]
 end

 subgraph Serving Layer
 SRV[Query Service\\nMerges batch + speed views\\nDruid / Cassandra /
BigQuery]
 end

 subgraph Consumers
 Q[Analyst Query]
 end

This PDF document was created using the online html editor powered by CKEditor.
 D --> BL
 D --> SL
 BL --> BV --> SRV
 SL --> SV --> SRV
 SRV --> Q
```
**Lambda Architecture Flow:**
- **Batch layer**: Reprocesses all data periodically, producing accurate
batch views
- **Speed layer**: Processes recent data in real-time, producing approximate
speed views
- **Serving layer**: Merges batch + speed views to answer queries
**Problems with Lambda:**
- Duplicate logic: Same computation in both batch (Spark) and streaming
(Flink)
- Operational complexity: Two separate codebases to maintain
- Synchronization: Ensuring batch and speed results merge correctly
---
## 6.5 Kappa Architecture
Kappa simplifies Lambda by using **only a streaming layer** — treating batch
as a special case of streaming.
```mermaid
graph LR
 D[All Data\\nKafka with long retention] -->|Stream processor\\nFlink /
Spark Streaming| SV[Serving Layer\\nMaterialized Views]
 SV --> Q[Query]

 subgraph Reprocessing
 RP[Reprocess from Kafka beginning\\n= equivalent to batch rerun]
 end

 D -->|On demand| RP
This PDF document was created using the online html editor powered by CKEditor.
 RP --> SV
```
**Kappa Benefits:**
- Single codebase for batch + streaming
- Kafka as the source of truth (long retention = replay history)
- Simpler operational model
**Kappa Limitations:**
- All historical data must fit in Kafka (or be replayable)
- Reprocessing from scratch is slower than batch on optimized storage
- Complex aggregations harder to express in streaming
**When to use Lambda vs Kappa:**
| Scenario | Use Lambda | Use Kappa |
|---|---|---|
| Very complex batch aggregations | ✓ | |
| Team has batch + streaming expertise | ✓ | |
| Single-paradigm simplicity preferred | | ✓ |
| Kafka long retention feasible | | ✓ |
| Latency requirements < 1 min | | ✓ |
---
## 6.6 Data Lakehouse Architecture
The **Lakehouse** combines the flexibility and low cost of a Data Lake with
the ACID guarantees and SQL performance of a Data Warehouse.
```mermaid
graph TD
 subgraph Data Lake Issues
 DLI1[No ACID transactions]
 DLI2[No schema enforcement]
 DLI3[Slow query performance]
 DLI4[No BI support]
 end
This PDF document was created using the online html editor powered by CKEditor.
 subgraph Data Warehouse Issues
 DWI1[Expensive at scale]
 DWI2[Unstructured data not supported]
 DWI3[No ML / AI native]
 DWI4[Closed formats]
 end
 subgraph Data Lakehouse Solution
 LS1[Open storage formats\\nParquet + Delta/Iceberg]
 LS2[ACID transactions on object storage]
 LS3[Schema enforcement]
 LS4[High-performance query engine\\nSpark, Presto, Trino]
 LS5[Supports ML and BI natively]
 end
```
### Open Table Formats Comparison
| Feature | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| ACID transactions | Yes | Yes | Yes |
| Time travel | Yes | Yes | Yes |
| Schema evolution | Yes | Yes | Yes |
| Partition evolution | Limited | Yes (best) | Yes |
| Upserts/Deletes | Yes | Yes | Yes (specialty) |
| Supported engines | Spark, Databricks | Spark, Trino, Flink, Presto |
Spark, Flink |
| Creator | Databricks | Netflix, Apple | Uber |
| Streaming support | Good | Good | Best (incremental) |
### Delta Lake Key Features
```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
spark = SparkSession.builder \\
This PDF document was created using the online html editor powered by CKEditor.
 .config("spark.sql.extensions",
"io.delta.sql.DeltaSparkSessionExtension") \\
 .config("spark.sql.catalog.spark_catalog",
"org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
 .getOrCreate()
# Create Delta table
df.write.format("delta").mode("overwrite").save("s3://mybucket/delta/orders")
# Time Travel
# Read version 0 (original)
spark.read.format("delta").option("versionAsOf", 0).load("s3://mybucket/delta/orders")
# Read as of timestamp
spark.read.format("delta").option("timestampAsOf", "2024-01-
01").load("s3://my-bucket/delta/orders")
# OPTIMIZE — compact small files
spark.sql("OPTIMIZE delta.`s3://my-bucket/delta/orders` ZORDER BY
(customer_id)")
# VACUUM — remove old files (default: 7 days retention)
spark.sql("VACUUM delta.`s3://my-bucket/delta/orders` RETAIN 168 HOURS")
# Schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
new_df.write.format("delta").mode("append").option("mergeSchema",
"true").save("s3://my-bucket/delta/orders")
```
---
## 6.7 Data Mesh Architecture
**Data Mesh** is a sociotechnical approach that decentralizes data ownership
to domain teams, treating data as a product.
This PDF document was created using the online html editor powered by CKEditor.
### Four Principles of Data Mesh (Zhamak Dehghani)
```mermaid
graph TD
 DM[Data Mesh]
 DM --> P1[1. Domain Ownership\\nDomain teams own and serve their data
products]
 DM --> P2[2. Data as a Product\\nData must be discoverable, addressable,
trustworthy, self-describing]
 DM --> P3[3. Self-serve Platform\\nCentral platform team provides
infrastructure\\nnot data processing]
 DM --> P4[4. Federated Governance\\nPolicies enforced
globally\\nImplementation local to domains]
```
### Data Mesh vs Centralized Data Platform
| Aspect | Centralized (Monolithic) | Data Mesh |
|---|---|---|
| Ownership | Central data team | Domain teams |
| Scalability | Bottleneck at central team | Scales with org |
| Time to insight | Slow (backlog) | Fast (domain autonomy) |
| Data quality | Central team responsible | Domain team responsible |
| Governance | Centralized | Federated |
| Technical complexity | Lower | Higher |
| Organizational fit | Small/medium orgs | Large enterprises |
### Data Product Interface Contract
```yaml
# data-product-contract.yaml
name: customer-360
domain: customer
owner: customer-platform-team
sla:
 freshness: 6 hours
 availability: 99.9%
This PDF document was created using the online html editor powered by CKEditor.
 row_count_min: 1000000
schema:
 - name: customer_id
 type: string
 description: Unique customer identifier
 pii: false
 - name: email
 type: string
 description: Customer email address
 pii: true
 masking: hash
access:
 - team: analytics
 permission: read
 - team: ml-platform
 permission: read
output_port:
 type: bigquery_table
 location: project.customer_domain.customer_360
 format: columnar
```
---
## 6.8 Cloud Storage Deep Dive
### Object Storage (S3, ADLS, GCS)
- Blob storage for files of any size and format
- Durability: 99.999999999% (11 nines) — S3 SLA
- Cheap: ~$0.023/GB/month (S3 Standard)
- No filesystem semantics (no rename, limited list performance)
**S3 Partitioning Strategy:**
```
# Good: Hive-style partition for Spark
s3://bucket/orders/year=2024/month=03/day=15/data.parquet
# Bad: Timestamp prefix (causes hot partitions in S3)
This PDF document was created using the online html editor powered by CKEditor.
s3://bucket/orders/2024-03-15T10:00:00/data.parquet
# Good: Partition by query pattern (if always querying by region)
s3://bucket/orders/region=us-east/year=2024/month=03/data.parquet
```
**S3 Small File Problem:**
- Spark creates many small files (one per partition)
- Small files = many S3 API calls = slow reads
- Solution: OPTIMIZE/COMPACT after writes (Delta Lake OPTIMIZE, or coalesce
in Spark)
```python
# Coalesce small files before writing
df.coalesce(10).write.parquet("s3://bucket/output/")
# Or use Delta Lake OPTIMIZE
spark.sql("OPTIMIZE delta.`s3://bucket/delta_table` ZORDER BY
(customer_id)")
```
### Cloud Data Warehouse Storage Architecture
#### Snowflake Architecture
```mermaid
graph TD
 subgraph Cloud Services Layer
 CS[Query parsing, optimization\\nMetadata management\\nAccess
control]
 end

 subgraph Compute Layer - Virtual Warehouses
 VW1[XS Warehouse\\nAnalytics team]
 VW2[L Warehouse\\nML Training jobs]
 VW3[M Warehouse\\nReporting]
 end

This PDF document was created using the online html editor powered by CKEditor.
 subgraph Storage Layer
 ST[Micro-partitioned columnar storage\\nS3 / Azure Blob /
GCS\\nShared across all warehouses]
 end

 CS --> VW1
 CS --> VW2
 CS --> VW3
 VW1 --> ST
 VW2 --> ST
 VW3 --> ST
```
**Snowflake Key Concepts:**
- **Virtual Warehouses**: Separate compute clusters — different teams use
different warehouses, no contention
- **Micro-partitions**: ~50-500MB compressed columnar storage chunks with
metadata (min/max per column)
- **Cluster keys**: Manual partitioning for very large tables (`ALTER TABLE
CLUSTER BY (date_col)`)
- **Time travel**: Query data at any point in time (default 1 day,
enterprise up to 90 days)
- **Zero-copy cloning**: Clone tables/schemas/databases instantly without
duplicating data
#### BigQuery Architecture
```mermaid
graph TD
 subgraph BigQuery
 D[Dremel Query Engine\\nMassively parallel SQL]
 C[Colossus Storage\\nColumnar compressed files]
 J[Jupiter Network\\n1 Petabit/s internal]
 Z[Borg\\nCompute resource manager]
 end
 D --> J --> C
 D --> Z
```
This PDF document was created using the online html editor powered by CKEditor.
**BigQuery Key Concepts:**
- **Serverless**: No cluster to manage — compute auto-scales
- **Partitioning**: By ingestion time, column, or integer range
- **Clustering**: Sort data within partitions (like Snowflake Z-ordering)
- **Slots**: Units of compute (on-demand or reserved)
- **Materialized views**: Auto-refreshed pre-computed aggregations
---
## 6.9 Data Governance & Security in the Cloud
### Data Governance Framework
```mermaid
graph TD
 DG[Data Governance]
 DG --> DC[Data Catalog\\nDiscoverability\\nMetadata\\nDataHub / Purview]
 DG --> DL[Data Lineage\\nWhere did this data come from?\\nOpenLineage /
Marquez]
 DG --> DQ[Data Quality\\nGreat Expectations\\ndbt tests\\nMonte Carlo]
 DG --> DPP[Data Privacy\\nPII classification\\nMasking /
Tokenization\\nGDPR / HIPAA]
 DG --> DM[Data Mastery\\nGolden records\\nMDM]
 DG --> DA[Data Access\\nRBAC / ABAC\\nColumn-level security\\nRow-level
security]
```
### RBAC in Cloud Data Platforms
```sql
-- Snowflake RBAC Example
CREATE ROLE analyst_read;
GRANT USAGE ON DATABASE analytics_db TO ROLE analyst_read;
GRANT USAGE ON ALL SCHEMAS IN DATABASE analytics_db TO ROLE analyst_read;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_db.marts TO ROLE
analyst_read;
This PDF document was created using the online html editor powered by CKEditor.
-- Column-level masking policy for PII
CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
 CASE
 WHEN CURRENT_ROLE() IN ('data_engineer', 'admin') THEN val
 ELSE SHA2(val) -- Hash email for non-privileged roles
 END;
ALTER TABLE dim_customer MODIFY COLUMN email SET MASKING POLICY email_mask;
-- Row-level security (dynamic data masking)
CREATE ROW ACCESS POLICY region_policy AS (region_col STRING) RETURNS
BOOLEAN ->
 region_col = CURRENT_SESSION_CONTEXT('user_region')
 OR CURRENT_ROLE() = 'admin';

ALTER TABLE fact_sales ADD ROW ACCESS POLICY region_policy ON (region);
```
### Data Privacy Compliance
| Regulation | Scope | Key Requirements |
|---|---|---|
| **GDPR** | EU personal data | Right to erasure, data minimization, breach
notification |
| **HIPAA** | US health data | PHI protection, audit controls, access
restrictions |
| **CCPA** | California consumers | Right to know, right to delete, opt-out
of sale |
| **PCI DSS** | Payment card data | Encryption, access control, audit trails
|
**Right to Erasure ("Right to be Forgotten") in Delta Lake:**
```python
# Delete specific user's data (GDPR compliance)
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "s3://bucket/delta/events")
delta_table.delete(f"user_id = '{user_id_to_delete}'")
This PDF document was created using the online html editor powered by CKEditor.
# VACUUM to permanently remove deleted data files
spark.sql(f"VACUUM delta.`s3://bucket/delta/events` RETAIN 0 HOURS")
# Note: Disabling 30-day retention check required for immediate deletion
```
---
## 6.10 Data Catalog & Lineage
### Apache Atlas / DataHub / OpenMetadata
A **Data Catalog** provides:
- **Discovery**: Find what data exists ("where is customer data?")
- **Metadata**: Schema, owner, descriptions, tags
- **Lineage**: How data flows from source to destination
- **Governance**: Data classification, sensitivity labels
```mermaid
graph LR
 PG[(PostgreSQL\\ncustomers table)] -->|dbt model|
DIM[dim_customer\\nSnowflake]
 DIM -->|dbt model| FS[fact_sales\\nSnowflake]
 FS -->|Tableau connector| DASH[Sales Dashboard]

 subgraph Data Catalog
 L[Lineage graph\\nautomatically tracked\\nvia OpenLineage]
 end
 PG -.-> L
 DIM -.-> L
 FS -.-> L
 DASH -.-> L
```
**OpenLineage** is an open standard for lineage metadata — Airflow, dbt,
Spark, and Flink all emit OpenLineage events to a compatible backend
(Marquez, DataHub).
This PDF document was created using the online html editor powered by CKEditor.
---
## 6.11 Data Platform Cost Optimization
### Cloud Cost Patterns
| Pattern | Description | Impact |
|---|---|---|
| **Compute/Storage Separation** | Scale compute and storage independently |
Only pay for compute when running |
| **Cluster Auto-scaling** | Scale down when idle | Reduce idle compute
costs 70%+ |
| **Spot/Preemptible Instances** | Use interruptible VMs for batch | 60-90%
cheaper for Spark jobs |
| **Data Tiering** | Move cold data to cheaper storage | S3 Standard →
Glacier for 80% savings |
| **Query optimization** | Partition pruning, clustering | Reduce
BigQuery/Snowflake scan costs |
| **Caching** | Result caching in Snowflake/BigQuery | Repeat queries cost
$0 |
```sql
-- Snowflake: Check query credit consumption
SELECT query_id, query_text, credits_used_cloud_services, execution_time
FROM snowflake.account_usage.query_history
WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
ORDER BY credits_used_cloud_services DESC
LIMIT 20;
-- BigQuery: Estimate query bytes before running
SELECT * FROM `project.dataset.table`
-- Use DRY RUN to estimate cost without executing
```
---
## 6.12 Modern Data Stack
This PDF document was created using the online html editor powered by CKEditor.
The **Modern Data Stack (MDS)** is an ecosystem of cloud-native, composable
tools for the full data lifecycle.
```mermaid
graph TD
 subgraph Ingestion
 FT[Fivetran / Airbyte\\nConnectors]
 end
 subgraph Storage + DWH
 SF[Snowflake / BigQuery / Redshift]
 end
 subgraph Transformation
 DBT[dbt\\nSQL transformations\\nTesting + Docs]
 end
 subgraph Orchestration
 AF[Airflow / Prefect / Dagster]
 end
 subgraph BI & Analytics
 LK[Looker / Tableau / Power BI\\nMetabase / Redash]
 end
 subgraph Data Quality
 GE[Great Expectations\\nMonte Carlo]
 end
 subgraph Data Catalog
 DH[DataHub / Collibra\\nAtlan]
 end
 subgraph Reverse ETL
 HT[Hightouch / Census\\nSync data back to SaaS apps]
 end

 FT --> SF
 SF --> DBT --> SF
 AF -.-> FT
 AF -.-> DBT
 SF --> LK
 GE -.-> SF
 DH -.-> SF
 SF --> HT --> CRM[(CRM / Ads Platform)]
This PDF document was created using the online html editor powered by CKEditor.
```
---
## 6.13 Cross Questions — Cloud Data Platforms
**Q1: What is the Medallion Architecture (Bronze/Silver/Gold)?**
> Bronze: Raw data as-is from sources — immutable, schema-on-read. Silver:
Cleaned, deduped, normalized data — schema-on-write. Gold: Business-ready
aggregations and dimensional models for analytics. Each layer adds value;
failures in one layer can be corrected by reprocessing from the layer
before.
**Q2: What are the differences between Lambda and Kappa architectures?**
> Lambda maintains two paths: batch (for accuracy, reprocessing) and speed
(for real-time), merging in a serving layer. This means duplicate code for
the same logic. Kappa uses only streaming — batch reprocessing is done by
replaying from Kafka. Kappa is simpler but requires Kafka to hold long
retention or replay capability. Lambda is more complex but handles scenarios
where full reprocessing from streaming is impractical.
**Q3: What is the Data Lakehouse and how does it solve Data Lake and Data
Warehouse problems?**
> Data Lakes have no ACID, no schema enforcement, and poor BI query
performance. Data Warehouses are expensive for large-scale unstructured/ML
workloads and use closed formats. Lakehouse (Delta Lake, Iceberg) adds ACID
transactions and schema enforcement on top of cheap object storage, while
supporting both SQL analytics (like DWH) and ML (like Data Lake). Open
formats mean no vendor lock-in.
**Q4: Explain Data Mesh and its four principles.**
> Data Mesh decentralizes data to domain teams. Principles: (1) Domain
ownership — teams own their data products end-to-end. (2) Data as a product
— data is discoverable, reliable, self-describing, with SLAs. (3) Self-serve
platform — central platform provides infrastructure (not data). (4)
Federated governance — global policies (PII, GDPR) enforced centrally, but
domain teams implement locally.
This PDF document was created using the online html editor powered by CKEditor.
**Q5: How does Snowflake separate compute from storage?**
> Snowflake stores data in micro-partitioned columnar files on S3/Azure
Blob/GCS. Multiple independent "Virtual Warehouses" (compute clusters)
access the same storage simultaneously. Teams use different warehouses — no
resource contention. Warehouses auto-suspend when idle — no charges when not
running. This means you pay for storage continuously but compute only when
active.
**Q6: What is Z-ordering/clustering in Delta Lake and BigQuery, and why use
it?**
> Z-ordering (Delta) / Clustering (BigQuery) physically co-locates related
data within storage files based on column values. When a query filters on a
clustered column (e.g., `WHERE customer_id = '123'`), the engine skips files
that don't contain that value (file pruning). For high-cardinality columns
frequently filtered in queries, this dramatically reduces data scanned and
query cost.
**Q7: How do you handle GDPR "right to erasure" in an append-only data lake?
**
> Delta Lake: DELETE the user's rows (logical delete), then VACUUM to
physically remove the files containing those rows. Iceberg: Same — snapshot
deletion + expiration. For extreme compliance requirements: tokenize PII at
ingestion, store tokens in the lake, store real PII in a separate key store
— delete only the key store entry to "forget" the user without rewriting
lake files.
**Q8: What is Reverse ETL and when is it needed?**
> Reverse ETL copies processed data from a warehouse back into operational
SaaS tools (CRM, marketing platforms, support tools). Example: Copy a
customer's ML-predicted churn score from BigQuery into Salesforce so the
sales team can see it in their daily workflow. Tools: Hightouch, Census. It
"closes the loop" — data collected from operations is analyzed and then fed
back to improve operations.
---
## 6.14 Microsoft Fabric — Unified Analytics Platform
This PDF document was created using the online html editor powered by CKEditor.
Microsoft Fabric is Microsoft's end-to-end unified analytics platform,
combining previously separate services (Synapse, Power BI, Data Factory,
Azure ML) into a single SaaS platform on a shared foundation called
**OneLake**.
### Why Microsoft Fabric Matters
```mermaid
graph TD
 subgraph Before Fabric - Fragmented
 ADF[Azure Data Factory\\nIngestion]
 ADLS[ADLS Gen2\\nStorage]
 ADB[Azure Databricks\\nSpark Processing]
 SYN[Azure Synapse Analytics\\nDWH + SQL]
 PBI[Power BI\\nBI & Reporting]
 AML[Azure ML\\nMachine Learning]
 end

 subgraph After Fabric - Unified
 OL[OneLake\\nSingle unified storage\\nDelta Parquet everywhere]
 FDE[Fabric Data Engineering\\nSpark Notebooks]
 FDW[Fabric Data Warehouse\\nSQL Analytics]
 FDF[Fabric Data Factory\\nPipelines + Dataflows]
 FPBI[Fabric Power BI\\nSemantic Layer]
 FDS[Fabric Data Science\\nML Notebooks]
 FRT[Fabric Real-Time Intelligence\\nEventstream + KQL]
 FDM[Fabric Data Activator\\nAlerts & Automation]
 end

 ADF -.->|unified into| FDF
 ADLS -.->|replaced by| OL
 ADB -.->|complemented by| FDE
 SYN -.->|replaced by| FDW
 PBI -.->|enhanced as| FPBI
 AML -.->|complemented by| FDS
```
### OneLake — The Foundation
This PDF document was created using the online html editor powered by CKEditor.
**OneLake** is Fabric's unified data lake — a single logical data lake for
an entire organization, automatically shared across all Fabric workloads.
```mermaid
graph TD
 OL[OneLake\\nSingle namespace\\nDelta Parquet format\\nAuto-shared
across workloads]
 OL --> FDE[Fabric Spark\\nRead/Write Delta]
 OL --> FDW[Fabric Warehouse\\nSQL read via shortcuts]
 OL --> FPBI[Power BI Semantic Model\\nDirectLake mode]
 OL --> FDS[Fabric ML Notebooks\\nPython on Delta tables]
 OL --> EXT[External Access\\nAzure Databricks\\nSynapse via Shortcuts]

 subgraph Storage
 OL --> A[Azure Blob Storage\\nAuto-managed\\nGeo-redundant]
 end
```
**Key OneLake concepts:**
| Concept | Description |
|---|---|
| **Lakehouse** | Core storage unit in Fabric — Tables (Delta) + Files (raw)
+ Shortcuts |
| **Shortcut** | Virtual link to external storage (ADLS, S3, GCS) — no data
copy |
| **Delta format** | All tables stored as Delta Parquet — open format, ACID
|
| **DirectLake** | Power BI reads directly from OneLake Delta files — no
import, no query |
| **Workspace** | Organizational container — like a project folder |
---
### Fabric Lakehouse
```mermaid
This PDF document was created using the online html editor powered by CKEditor.
graph LR
 subgraph Fabric Lakehouse
 TBL[Tables Section\\nDelta Tables\\nSQL queryable\\nManaged schema]
 FLS[Files Section\\nRaw files\\nParquet, CSV, JSON\\nUnmanaged]
 end

 subgraph Access Methods
 NB[Spark Notebooks\\nPySpark / Scala / SQL]
 SQ[SQL Analytics Endpoint\\nT-SQL on Delta tables\\nRead-only]
 PB[Power BI\\nDirectLake semantic model]
 end

 TBL --> NB
 TBL --> SQ
 TBL --> PB
 FLS --> NB
```
**Creating a Lakehouse and loading data (PySpark in Fabric Notebook):**
```python
# Fabric Notebook - PySpark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Read from Files section (raw CSV)
df = spark.read.option("header", "true").csv("Files/raw/claims/")
# Transform
df_clean = df \\
 .filter(df.claim_amount > 0) \\
 .dropDuplicates(["claim_id"]) \\
 .withColumn("claim_date", df.claim_date.cast("date"))
# Write to Tables section as Delta (queryable via SQL endpoint)
df_clean.write.format("delta").mode("overwrite").saveAsTable("silver_claims")
# Immediately queryable via SQL Analytics Endpoint or Power BI DirectLake
This PDF document was created using the online html editor powered by CKEditor.
```
---
### Fabric Data Warehouse vs Lakehouse — When to Use Which
| Aspect | Fabric Lakehouse | Fabric Warehouse |
|---|---|---|
| **Storage format** | Delta Lake (open) | Delta Lake (open) |
| **Write access** | Spark + SQL DML | T-SQL DML |
| **Read access** | Spark + SQL Analytics Endpoint (read-only SQL) | Full TSQL (read + write) |
| **Schema enforcement** | Flexible (Spark can evolve schema) | Strict T-SQL
schema |
| **Best for** | ELT with Spark, data science, ML workloads | SQL-first
analytics, BI, traditional DWH patterns |
| **Concurrent users** | Many Spark jobs | Many SQL users |
| **SCD support** | Via Delta MERGE in Spark | Via T-SQL MERGE |
**Recommendation:** Use **Lakehouse** as your Bronze/Silver layer (Sparkbased ELT), and **Warehouse** as your Gold layer (SQL-first, BI-served,
dimensional model).
---
### Fabric Real-Time Intelligence
For streaming and real-time analytics, Fabric offers:
```mermaid
graph LR
 subgraph Sources
 EH[Azure Event Hubs]
 KAFKA[Kafka]
 IOT[IoT Hub]
 end

 subgraph Fabric Real-Time Intelligence
This PDF document was created using the online html editor powered by CKEditor.
 ES[Eventstream\\nNo-code stream routing\\nTransformations]
 EH2[Eventhouse\\nKQL Database\\nTime-series optimized]
 DA[Data Activator\\nRule-based alerts\\nNo-code triggers]
 end

 subgraph Serving
 RT[Real-Time Dashboard\\nSub-second refresh]
 PB[Power BI\\nAuto-refresh]
 ACT[Actions\\nEmail, Teams alert\\nPower Automate]
 end

 EH --> ES --> EH2 --> RT
 IOT --> ES
 EH2 --> PB
 EH2 --> DA --> ACT
```
**KQL (Kusto Query Language) — Fabric's real-time query language:**
```kql
// Query Eventhouse - average response time last 1 hour, per service
AppRequests
| where TimeGenerated > ago(1h)
| summarize avg_duration = avg(DurationMs), request_count = count() by
ServiceName
| order by avg_duration desc
| take 20
```
**When to use Fabric Real-Time Intelligence:**
- IoT telemetry at scale (thousands of devices)
- Application log analytics (sub-second freshness)
- Real-time dashboard for operations teams
- Alert when KPI crosses threshold (Data Activator)
---
### Fabric Medallion Architecture (Recommended Pattern)
This PDF document was created using the online html editor powered by CKEditor.
```mermaid
flowchart TD
 subgraph Ingestion
 ADF[Data Factory Pipelines\\nor Dataflows Gen2]
 ES[Eventstream\\nfor real-time]
 end

 subgraph Bronze Lakehouse
 BRZ[Raw files + Delta tables\\nImmutable, as-is from
source\\nWorkspace: Bronze]
 end

 subgraph Silver Lakehouse
 SLV[Cleaned + conformed tables\\nDeduped + typed\\nWorkspace:
Silver]
 end

 subgraph Gold Warehouse
 GLD[Dimensional model\\nStar schema\\nSQL DML for SCD
logic\\nWorkspace: Gold]
 end

 subgraph Serving
 PBI[Power BI\\nDirectLake from Gold\\nSemantic model]
 API[Fabric API\\nExpose data products]
 end

 ADF --> BRZ
 ES --> BRZ
 BRZ -->|Spark Notebooks\\nor Dataflows| SLV
 SLV -->|Spark or T-SQL| GLD
 GLD --> PBI
 GLD --> API
```
**Fabric Workspace separation by layer** allows:
- Fine-grained RBAC (Bronze = data engineers only; Gold = all analysts)
- Independent compute scaling per layer
This PDF document was created using the online html editor powered by CKEditor.
- Clear ownership (Bronze team vs Gold/BI team)
---
### Microsoft Fabric vs Databricks vs Snowflake
| Feature | Microsoft Fabric | Databricks | Snowflake |
|---|---|---|---|
| **Open format** | Delta Parquet (open) | Delta Parquet (open) |
Proprietary (FDN) |
| **Unified platform** | Yes (all-in-one) | Yes (data + ML) | No (DWH + some
extras) |
| **BI built-in** | Yes (Power BI native) | No (partner BI tools) | No
(partner BI tools) |
| **Real-time** | Yes (Eventhouse + KQL) | Yes (Structured Streaming) |
Limited |
| **ML/AI** | Yes (Data Science workload) | Yes (MLflow, AutoML) | Snowpark
ML |
| **SQL DWH** | Yes (Fabric Warehouse) | Yes (Databricks SQL) | Yes (primary
strength) |
| **Pricing** | Per-capacity (F SKU) | DBU + cloud | Credits + storage |
| **Best for** | Microsoft shops, Azure, Power BI users | Data engineering +
ML, multi-cloud | SQL-first BI, finance, multi-cloud |
| **Vendor lock-in** | Medium (Azure + Microsoft) | Low (open format, multicloud) | Medium (proprietary format) |
**When to choose Fabric:**
- Organization is Microsoft-first (Azure, Microsoft 365, Teams)
- Power BI is the primary BI tool
- Want a single monthly F-SKU cost covering all workloads
- Healthcare/government needing Microsoft compliance certifications
---
## 6.15 Real-World Cloud Platform Decision Stories
### Capital One — Moved to AWS + Snowflake
This PDF document was created using the online html editor powered by CKEditor.
**Before:** On-premise data centers; Oracle DWH; weeks to provision new
capacity.
**After:** AWS + Snowflake + dbt + Looker (Modern Data Stack).
**Why Snowflake over Redshift:**
- Redshift required manual cluster resizing for peak loads
- Snowflake auto-scales Virtual Warehouses instantly — critical for monthend financial reporting spikes
- Zero-copy cloning for dev/test environments saved 60% on storage costs
**Architecture:**
```mermaid
graph LR
 S[Mainframe\\nCore banking] -->|AWS DMS\\nCDC| K[Kafka on MSK]
 K --> S3[S3 + Glue Catalog]
 S3 --> SF[Snowflake\\nDelta format via Iceberg]
 SF --> DBT[dbt Cloud\\nTransformations]
 DBT --> LK[Looker\\nBI dashboards]
```
---
### Stripe — Why They Chose ClickHouse for Real-Time Analytics
**Problem:** Stripe processes 250M+ API calls/day. Merchants need real-time
dashboards showing payment success rates, revenue by country, and decline
reasons — with < 1 second query latency on billions of rows.
**Why not Snowflake/BigQuery:** Expensive at query volume; latency of 3–30
seconds too slow for real-time merchant dashboards.
**Why ClickHouse:**
- Sub-100ms queries on 10B+ rows
- Columnar + vectorized execution
- Excellent compression (payment data has high repetition)
- Cost-effective: 5–10× cheaper per query than cloud DWHs at this scale
---
This PDF document was created using the online html editor powered by CKEditor.
### JPMorgan Chase — On-Premise to Hybrid Cloud
**Challenge:** Regulatory requirements (OCC, Fed) mandate data residency;
can't fully move to public cloud; but need modern analytics.
**Solution:** **Hybrid cloud** with Azure Private Link:
- Sensitive trading data stays on-premise (IBM mainframe + Oracle)
- CDC via Kafka on dedicated hardware
- Replicate to Azure in a private VNet (never touches public internet)
- Azure Synapse Analytics (Fabric predecessor) for analytics
- Microsoft Purview for compliance-grade lineage tracking
**Key insight:** HIPAA, SOX, and banking regulations don't prevent cloud —
they require specific controls (encryption, audit, access). Azure +
Microsoft Purview satisfies these controls.
---
### Choosing Between AWS, Azure, GCP — Decision Framework
```mermaid
flowchart TD
 A{Primary Constraint?}
 A -->|Microsoft shop\\nOffice 365, Teams, Azure AD| AZ[Azure\\nFabric +
Synapse + ADF\\nPurview for governance]
 A -->|Google Workspace\\nBigQuery already used| GCP[GCP\\nBigQuery +
Vertex AI\\nDataproc for Spark]
 A -->|Most mature cloud\\nBest ML services| AWS[AWS\\nS3 + EMR +
Glue\\nRedshift or Snowflake on top]
 A -->|Multi-cloud\\nNo vendor lock-in| MC[Snowflake + Databricks\\nRun
on any cloud\\nOpen Delta/Iceberg format]
 A -->|Regulatory: HIPAA / Banking\\nStay on-prem partially|
HYB[Hybrid\\nOn-prem CDC + Kafka\\nCloud DWH in private VNet]
```
---
# Part 7: System Design Patterns & Case Studies for Data Engineers
This PDF document was created using the online html editor powered by CKEditor.
---
## 7.1 System Design Framework for Data Engineers
When designing a data system, follow this structured approach:
```mermaid
flowchart TD
 A[1. Clarify Requirements\\nFunctional + Non-functional] --> B[2.
Estimate Scale\\nVolume, Velocity, Variety]
 B --> C[3. Define Data Model\\nSource schema, target schema]
 C --> D[4. Design Architecture\\nIngestion → Storage → Processing →
Serving]
 D --> E[5. Deep Dive Components\\nCritical paths, bottlenecks]
 E --> F[6. Address Trade-offs\\nConsistency vs Availability\\nLatency vs
Throughput]
 F --> G[7. Discuss Failure Modes\\nRetries, DLQ, monitoring]
```
### Requirements Clarification Checklist
**Functional:**
- What are the data sources? (databases, APIs, streams, files)
- What transformations are needed?
- Who are the consumers? (BI, ML, APIs)
- What is the expected freshness? (real-time, near-real-time, daily)
**Non-functional:**
- What is the data volume? (GB/TB/PB)
- What is the velocity? (events/second, GB/day)
- What are the latency requirements? (seconds, minutes, hours)
- What is the availability SLA? (99.9%, 99.99%)
- What are the consistency requirements? (eventual, strong)
- What are compliance requirements? (GDPR, HIPAA, PCI)?
---
## 7.2 Case Study 1: Real-Time Fraud Detection Pipeline
This PDF document was created using the online html editor powered by CKEditor.
### Problem Statement
Design a real-time fraud detection system for a payment processing company:
- 10,000 transactions per second peak
- Flag suspicious transactions within 500ms
- Store all transactions for historical analysis
- Generate daily fraud summary reports
### Requirements Analysis
| Requirement | Details |
|---|---|
| Volume | 10K TPS peak, ~5K average → ~432M transactions/day |
| Velocity | Real-time, sub-500ms flagging |
| Freshness | Real-time for fraud; batch for reports |
| Storage | 5 years retention → ~780B records |
| Availability | 99.99% (payment critical path) |
### Architecture Design
```mermaid
graph TD
 subgraph Producers
 A[Payment Gateway\\nAPI]
 B[Mobile App\\nSDK]
 end
 subgraph Streaming Backbone
 K[Apache Kafka\\nTopic: transactions\\n30 partitions\\nRF=3]
 end
 subgraph Real-time Processing - Flink
 F[Flink Job 1\\nFraud Scoring\\nUser behavior features\\nLast 5 min
window]
 F2[Flink Job 2\\nVelocity Check\\nN txns in M minutes\\nper
user/card]
 end
This PDF document was created using the online html editor powered by CKEditor.
 subgraph Feature Store
 FS[Redis\\nUser features\\nCard features\\nMerchant risk scores]
 end
 subgraph ML Model Serving
 ML[ML Model\\nXGBoost / Neural Net\\nServed via FastAPI\\nP99 <
100ms]
 end
 subgraph Decision & Action
 DE[Decision Engine\\nThreshold evaluation\\nRule engine]
 AC[Action Service\\nBlock / Allow / Review]
 end
 subgraph Storage - Batch
 S3[S3 / ADLS\\nRaw transaction events\\nParquet + Delta Lake]
 DWH[Snowflake / BigQuery\\nAggregated fraud reports\\nStar schema]
 end
 subgraph Batch Processing
 SP[Spark\\nNightly batch\\nFeature engineering\\nReport generation]
 DBT[dbt\\nSQL transformations\\nData mart models]
 end
 A --> K
 B --> K
 K --> F
 K --> F2
 F --> FS
 F --> ML
 F2 --> ML
 ML --> DE
 DE --> AC
 K -->|Kafka Connect S3 Sink| S3
 S3 --> SP --> DWH
 DWH --> DBT --> DWH
```
This PDF document was created using the online html editor powered by CKEditor.
### Key Design Decisions
**1. Kafka Partitioning Strategy:**
```
Partition key = card_number_hash
→ All events for same card go to same partition
→ Ensures ordering for velocity checks
→ 30 partitions handle 10K TPS with headroom
```
**2. Feature Store Design:**
```
Redis sorted sets for sliding window counts:
Key: "card:{card_id}:txns:{bucket}"
ZADD card:12345:txns:{minute_bucket} score=timestamp member=txn_id
Count txns in last 5 minutes:
ZCOUNT card:12345:txns:{bucket} {5_min_ago} {now}
```
**3. Exactly-Once Guarantees:**
- Kafka producer idempotent mode enabled
- Flink checkpoint every 30 seconds
- ML scoring service idempotent (same input → same output)
- Final decision written with transaction_id as idempotency key
**4. Failure Handling:**
```
Fraud scoring timeout (>500ms):
→ Default to "allow" with manual review flag
→ Alert on-call team
Kafka consumer lag > 10K:
→ PagerDuty alert
→ Scale Flink job parallelism
DLQ for malformed events:
→ S3 error bucket for investigation
This PDF document was created using the online html editor powered by CKEditor.
```
---
## 7.3 Case Study 2: Data Lake Ingestion Platform
### Problem Statement
Design a scalable data ingestion platform for a healthcare organization:
- 50+ source systems (EHR, claims, pharmacy, labs)
- Mix of batch (nightly dumps) and streaming (HL7 FHIR events)
- HIPAA compliance required
- PHI must be de-identified before analytics
### Architecture
```mermaid
graph TD
 subgraph Source Systems
 EHR[EHR Systems\\nEpic, Cerner]
 CLM[Claims Processing\\nPostgreSQL]
 LAB[Lab Systems\\nHL7 FHIR API]
 PHA[Pharmacy Systems\\nCSV exports]
 end
 subgraph Ingestion Layer
 CDC[Debezium\\nCDC from Claims DB]
 AV[Airbyte\\nBatch connectors for EHR]
 API[Custom Python\\nFHIR API client]
 FS[File Scanner\\nAirflow sensor\\nfor CSV drops]
 end
 subgraph Landing Zone - Raw
 RAW[ADLS Gen2 - Bronze\\nRaw Zone\\nImmutable\\nEncrypted at rest
AES-256]
 end
 subgraph PHI Processing - Secure Enclave
 PIE[PHI Identification Engine\\nPresidio / custom NER\\nDetect SSN,
This PDF document was created using the online html editor powered by CKEditor.
DOB, Name, MRN]
 DID[De-identification Service\\nTokenize PII\\nAge bucket DOB\\nMask
address to ZIP+3]
 TKV[Token Vault\\nAzure Key Vault\\nMaps token → real PHI]
 end
 subgraph Processed Zone - Silver
 SIL[ADLS Gen2 - Silver\\nDe-identified data\\nParquet / Delta
Lake\\nAccessible to analysts]
 end
 subgraph Analytics Zone - Gold
 GLD[Synapse Analytics / BigQuery\\nDe-identified aggregated
data\\nDimensional models\\nPopulation health KPIs]
 end
 subgraph Governance
 PV[Microsoft Purview\\nData Catalog\\nLineage tracking\\nPHI
classification labels]
 AF[Airflow\\nOrchestration\\nAudit logging]
 end
 EHR --> AV --> RAW
 CLM --> CDC --> RAW
 LAB --> API --> RAW
 PHA --> FS --> RAW
 RAW --> PIE --> DID --> SIL
 DID --> TKV
 SIL --> GLD
 PV -.-> RAW
 PV -.-> SIL
 AF -.-> AV
 AF -.-> CDC
 AF -.-> PIE
```
### HIPAA Compliance Checklist in Architecture
This PDF document was created using the online html editor powered by CKEditor.
| Requirement | Implementation |
|---|---|
| PHI encryption at rest | AES-256 on ADLS Gen2 with customer-managed keys |
| PHI encryption in transit | TLS 1.2+ for all data movement |
| Access control | RBAC — analysts only access Silver (de-id), engineers
access Bronze with audit |
| Audit logging | Airflow + Azure Monitor — every data access logged |
| Minimum necessary | Column-level access control — analysts can't see
token-to-PHI mapping |
| De-identification | Safe Harbor method: remove 18 HIPAA identifiers |
| Breach notification | Azure Defender for Storage — anomaly detection +
alert |
---
## 7.4 Case Study 3: E-commerce Data Warehouse
### Problem Statement
Design a data warehouse for an e-commerce company:
- 5M daily orders, 50M product views, 2M customers
- Dimensional model for: Sales, Inventory, Customer behavior, Marketing
- Support BI reporting with < 10 second dashboard load
- Historical data: 5 years
### Dimensional Model Design
```mermaid
erDiagram
 FACT_ORDER_LINE {
 bigint order_line_key PK
 int order_date_key FK
 int customer_key FK
 int product_key FK
 int channel_key FK
 int promo_key FK
 decimal quantity
 decimal unit_price
 decimal discount_amount
This PDF document was created using the online html editor powered by CKEditor.
 decimal revenue
 decimal cogs
 decimal gross_profit
 }
 FACT_PAGE_VIEW {
 bigint page_view_key PK
 int event_date_key FK
 int session_key FK
 int customer_key FK
 int product_key FK
 int referrer_key FK
 int time_on_page_seconds
 boolean converted
 }
 DIM_CUSTOMER {
 int customer_key PK
 string customer_id
 string email_hash
 string acquisition_channel
 string tier
 string country
 date first_purchase_date
 boolean is_current
 date valid_from
 date valid_to
 }
 DIM_PRODUCT {
 int product_key PK
 string product_sku
 string product_name
 string category
 string subcategory
 string brand
 decimal current_list_price
 }
This PDF document was created using the online html editor powered by CKEditor.
 DIM_DATE {
 int date_key PK
 date full_date
 int year
 int quarter
 int month
 string month_name
 boolean is_holiday
 boolean is_weekday
 }
 DIM_CHANNEL {
 int channel_key PK
 string channel_name
 string channel_type
 string platform
 }
 FACT_ORDER_LINE }o--|| DIM_DATE : ""
 FACT_ORDER_LINE }o--|| DIM_CUSTOMER : ""
 FACT_ORDER_LINE }o--|| DIM_PRODUCT : ""
 FACT_ORDER_LINE }o--|| DIM_CHANNEL : ""
 FACT_PAGE_VIEW }o--|| DIM_DATE : ""
 FACT_PAGE_VIEW }o--|| DIM_CUSTOMER : ""
 FACT_PAGE_VIEW }o--|| DIM_PRODUCT : ""
```
### Pipeline Architecture
```mermaid
flowchart TD
 subgraph Sources
 ODB[(Order DB\\nPostgreSQL)]
 AB[Analytics Events\\nSegment / Amplitude]
 INV[(Inventory DB\\nMySQL)]
 MKT[Marketing API\\nGoogle Ads, Meta]
 end
This PDF document was created using the online html editor powered by CKEditor.
 subgraph Ingestion
 FT[Fivetran\\nConnectors for all sources]
 end
 subgraph Raw Layer - Snowflake
 STG[RAW schema\\nUnmodified source tables\\ne.g. raw.orders,
raw.events]
 end
 subgraph Transformation - dbt
 DBT_STG[Staging models\\nstg_orders, stg_events\\nType casting,
renaming]
 DBT_INT[Intermediate models\\nint_customer_orders\\nJoins + business
logic]
 DBT_MART[Mart
models\\nfact_order_line\\ndim_customer\\nfact_page_view]
 end
 subgraph Serving - Snowflake
 SNOW[Snowflake\\nMarts schema\\nBI access]
 end
 subgraph BI
 TB[Tableau / Looker\\nDashboards]
 API[Embedded analytics\\nCharts in product]
 end
 ODB --> FT --> STG
 AB --> FT --> STG
 INV --> FT --> STG
 MKT --> FT --> STG
 STG --> DBT_STG --> DBT_INT --> DBT_MART --> SNOW --> TB
 SNOW --> API
```
### Performance Optimization for < 10s Dashboards
This PDF document was created using the online html editor powered by CKEditor.
```sql
-- 1. Clustering key for fact tables
ALTER TABLE fact_order_line CLUSTER BY (order_date_key, customer_key);
-- 2. Materialized aggregates for common queries
CREATE MATERIALIZED VIEW daily_sales_summary AS
SELECT
 order_date_key,
 channel_key,
 COUNT(*) AS order_count,
 SUM(revenue) AS total_revenue,
 SUM(gross_profit) AS total_profit
FROM fact_order_line
GROUP BY 1, 2;
-- 3. Snowflake result cache (transparent — free repeat queries)
-- Enable: ALTER SESSION SET USE_CACHED_RESULT = TRUE;
-- 4. Pre-aggregate for dashboards (dbt mart model)
-- marts/dashboard_sales_trend.sql
SELECT
 d.year,
 d.month,
 d.month_name,
 c.tier AS customer_tier,
 p.category,
 SUM(f.revenue) AS revenue,
 COUNT(DISTINCT f.customer_key) AS unique_customers,
 COUNT(*) AS order_count
FROM fact_order_line f
JOIN dim_date d ON f.order_date_key = d.date_key
JOIN dim_customer c ON f.customer_key = c.customer_key AND c.is_current
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY 1,2,3,4,5
```
---
This PDF document was created using the online html editor powered by CKEditor.
## 7.5 Case Study 4: Real-Time Analytics Platform
### Problem Statement
Design a real-time analytics platform for a ride-sharing company:
- Track live driver positions (GPS updates every 5 seconds)
- Real-time ride matching metrics (acceptance rate, ETA accuracy)
- Operational dashboard: update every 30 seconds
- Historical analytics: trips per region per hour
### Architecture
```mermaid
graph TD
 subgraph Data Sources
 DRV[Driver App\\nGPS 1/5sec]
 RDR[Rider App\\nBooking events]
 BE[Backend Services\\nTrip lifecycle events]
 end
 subgraph Streaming Ingestion
 K[Apache Kafka\\n- driver.positions\\n- trip.events\\nbooking.requests]
 end
 subgraph Real-time Processing Layer
 F1[Flink Job\\nDriver position aggregation\\nRegion density per 30s]
 F2[Flink Job\\nRide acceptance rate\\nSliding 5-min window]
 F3[Flink Job\\nETA accuracy scoring\\nEvent-time join: predicted vs
actual]
 end
 subgraph Real-time Serving
 CH[ClickHouse\\nOLAP - sub-second queries\\nReal-time aggregated
metrics]
 RD[Redis\\nDriver positions\\nGeo spatial index]
 end
 subgraph Historical Processing
This PDF document was created using the online html editor powered by CKEditor.
 S3[S3\\nParquet - event archive]
 SP[Spark\\nHourly batch aggregation]
 SF[Snowflake\\nHistorical analytics\\nBI reporting]
 end
 subgraph Dashboard
 DB[Grafana\\nOperational 30s refresh]
 TB[Tableau\\nHistorical reports]
 end
 DRV --> K
 RDR --> K
 BE --> K
 K --> F1 --> CH
 K --> F2 --> CH
 K --> F3 --> CH
 F1 --> RD
 K -->|Kafka Connect S3 Sink| S3
 S3 --> SP --> SF
 CH --> DB
 SF --> TB
```
### ClickHouse for Real-Time Analytics
ClickHouse is a columnar OLAP database optimized for real-time analytical
queries at very high throughput.
```sql
-- ClickHouse: Create table for real-time trip metrics
CREATE TABLE trip_metrics (
 event_time DateTime,
 region_id String,
 metric_name String,
 metric_value Float64,
 driver_count UInt32
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
This PDF document was created using the online html editor powered by CKEditor.
ORDER BY (event_time, region_id)
TTL event_time + INTERVAL 30 DAY; -- Auto-delete data older than 30 days
-- Real-time query: driver density last 5 minutes
SELECT
 region_id,
 COUNT(*) AS active_drivers,
 AVG(metric_value) AS avg_acceptance_rate
FROM trip_metrics
WHERE event_time >= now() - INTERVAL 5 MINUTE
 AND metric_name = 'acceptance_rate'
GROUP BY region_id
ORDER BY active_drivers DESC;
-- Executes in < 100ms even on billions of rows
```
---
## 7.6 Case Study 5: ML Feature Engineering Pipeline
### Problem Statement
Design a feature engineering pipeline for a recommendation system:
- Generate user and item features for a product recommendation model
- Features must be available for both online (real-time inference) and
offline (training)
- Handle 100M users, 10M products
- Feature freshness: real-time for user activity, daily for user history
### Feature Store Architecture
```mermaid
graph TD
 subgraph Feature Computation
 BATCH[Spark Batch\\nDaily feature computation\\n- User purchase
history\\n- User click patterns\\n- Product popularity\\n- Collaborative
filtering features]
 STREAM[Flink Streaming\\nReal-time feature updates\\n- Last session
activity\\n- Real-time clicks\\n- Current cart contents]
This PDF document was created using the online html editor powered by CKEditor.
 end
 subgraph Feature Store
 OFFLINE[Offline Store\\nS3 + Delta Lake\\nPoint-in-time
correct\\nFor model training]
 ONLINE[Online Store\\nRedis / DynamoDB\\nLow latency <10ms\\nFor
inference]
 end
 subgraph Training Pipeline
 TRN[Spark ML / PyTorch\\nHistorical features from offline
store\\nPoint-in-time join]
 end
 subgraph Inference
 API[Recommendation API\\nReal-time feature fetch from online
store\\nModel scoring]
 end
 subgraph Sync
 SYNC[Feature Materialization\\nBatch → Online store\\nStreaming →
Online store]
 end
 BATCH --> OFFLINE
 STREAM --> ONLINE
 OFFLINE --> TRN
 OFFLINE --> SYNC --> ONLINE
 ONLINE --> API
```
### Point-in-Time Correct Feature Join (Critical for ML)
Avoiding **data leakage**: features must only use data available at the time
of each training example's label event.
```python
# Point-in-time correct join using Feast / custom implementation
This PDF document was created using the online html editor powered by CKEditor.
# For each training label (timestamp, user_id, item_id, was_purchased)
# Join only features that were known BEFORE the label timestamp
from feast import FeatureStore
import pandas as pd
store = FeatureStore(repo_path=".")
# Training data with timestamps
labels_df = pd.DataFrame({
 "event_timestamp": pd.to_datetime(["2024-01-15 10:00:00", "2024-01-16
14:30:00"]),
 "user_id": [12345, 67890],
 "item_id": [111, 222],
 "label": [1, 0]
})
# Point-in-time join — only uses features available at event_timestamp
training_df = store.get_historical_features(
 entity_df=labels_df,
 features=[
 "user_features:purchase_count_7d",
 "user_features:avg_order_value",
 "item_features:view_count_30d",
 "item_features:conversion_rate",
 ]
).to_df()
# Result: features as they existed at each event_timestamp — no leakage!
```
---
## 7.7 Essential Data Engineering Design Patterns Reference
### Pattern 1: Slowly Changing Dimension (SCD Type 2)
Already covered in Part 3. Key implementation:
```sql
MERGE → UPDATE is_current=FALSE on old row → INSERT new row with
This PDF document was created using the online html editor powered by CKEditor.
is_current=TRUE
```
### Pattern 2: Late Arriving Facts
```sql
-- Fact arrives late — order was placed in January, but records reach DWH in
February
-- Solution: Use the event date from the source, not the load date
INSERT INTO fact_sales (order_date_key, ..., etl_load_date_key)
SELECT
 d.date_key, -- ← from order_date in source (correct
business date)
 ...,
 ld.date_key -- ← from CURRENT_DATE (when ETL ran)
FROM staging_orders o
JOIN dim_date d ON o.order_date = d.full_date -- ← map to correct
business date
JOIN dim_date ld ON CURRENT_DATE = ld.full_date -- ← track when loaded
```
### Pattern 3: Near-Real-Time with Micro-batch
```mermaid
graph LR
 K[Kafka] --> SS[Spark Structured Streaming\\nTrigger: every 5
minutes\\nmicro-batch]
 SS --> DL[Delta Lake\\nUpsert on arrival]
 DL --> BI[BI Tool\\nQuery Delta Lake directly]
```
### Pattern 4: Data Sharing without Data Movement
```
Snowflake Data Sharing:
Provider creates share → Consumer queries provider's data directly
No data copy → Real-time access → Data never leaves provider storage
This PDF document was created using the online html editor powered by CKEditor.
BigQuery Analytics Hub:
Publish datasets to Hub → Subscribers query via linked datasets
```
### Pattern 5: Fan-out Pattern
```mermaid
graph LR
 K[Kafka Topic\\nraw_events] -->|Consumer 1| DL[Delta Lake\\nAnalytics]
 K -->|Consumer 2| ES[Elasticsearch\\nSearch]
 K -->|Consumer 3| CACHE[Redis\\nSession cache]
 K -->|Consumer 4| ALERT[Alert Engine\\nFraud detection]
```
One event stream, multiple independent consumers — Kafka's consumer group
model enables this without coupling.
### Pattern 6: Idempotent Pipeline with Surrogate Keys
```python
import hashlib
def generate_surrogate_key(*values) -> str:
 """
 Deterministic surrogate key from business keys.
 Safe to generate multiple times → same result (idempotent).
 """
 composite = "|".join(str(v) for v in values)
 return hashlib.sha256(composite.encode()).hexdigest()
# Usage
order_key = generate_surrogate_key(order_id, order_date, source_system)
# Same inputs → always same key → UPSERT won't create duplicate
```
---
This PDF document was created using the online html editor powered by CKEditor.
## 7.8 Critical Terminologies Quick Reference
| Term | Definition |
|---|---|
| **OLTP** | Online Transaction Processing — optimized for write-heavy
transactional workloads |
| **OLAP** | Online Analytical Processing — optimized for read-heavy
analytical queries |
| **ETL** | Extract, Transform, Load — transform before loading |
| **ELT** | Extract, Load, Transform — load raw, transform in DWH |
| **CDC** | Change Data Capture — capture DB changes
(inserts/updates/deletes) |
| **WAL** | Write-Ahead Log — Postgres transaction log used for CDC |
| **SCD** | Slowly Changing Dimension — pattern for tracking historical
changes in dimensions |
| **Surrogate Key** | System-generated integer key in DWH, independent of
source |
| **Natural Key** | Business identifier from source system |
| **Grain** | What one row in a fact table represents |
| **Conformed Dimension** | Shared dimension used across multiple fact
tables |
| **Watermark** | Upper bound on event time — tells processor "no events ≤ t
will arrive" |
| **Idempotency** | Property where re-running produces same result as
running once |
| **CAP Theorem** | At most 2 of: Consistency, Availability, Partition
Tolerance |
| **ACID** | Atomicity, Consistency, Isolation, Durability (transaction
properties) |
| **MVCC** | Multi-Version Concurrency Control — readers don't block writers
|
| **LSM Tree** | Log-Structured Merge Tree — write-optimized storage
(Cassandra) |
| **B-Tree** | Balanced tree index — balanced read/write (PostgreSQL) |
| **Replication Factor** | Number of copies of data across nodes |
| **Quorum** | Minimum nodes that must agree for read/write success |
| **Medallion Architecture** | Bronze (raw) → Silver (clean) → Gold
(analytics) layers |
This PDF document was created using the online html editor powered by CKEditor.
| **Data Mesh** | Decentralized, domain-owned data products |
| **Data Lakehouse** | Data Lake + DWH properties (ACID + SQL on cheap
object storage) |
| **Feature Store** | Centralized repository for ML features (online +
offline) |
| **Data Lineage** | End-to-end tracking of data origin and transformations
|
| **Back-pressure** | Upstream slows down when downstream is overwhelmed |
| **Dead Letter Queue** | Storage for events that fail processing after all
retries |
| **Partition Pruning** | Skipping irrelevant data partitions during a scan
|
| **Z-ordering** | Multi-dimensional clustering to co-locate related data |
| **Schema Evolution** | Ability to change schema without breaking existing
readers/writers |
---
## 7.9 Mega Interview Question Bank
### Foundations
1. Walk me through the Data Engineering Lifecycle.
2. What is the difference between a Data Engineer and a Data Scientist?
3. Explain the difference between ETL and ELT. Which would you choose for a
cloud-native architecture?
4. What are the key undercurrents of the Data Engineering Lifecycle?
5. How do you assess the data maturity of an organization you're joining?
### Storage & Databases
6. When would you choose a NoSQL database over a relational database?
7. Explain how B-Trees and LSM Trees differ. When would you use each?
8. How does MVCC enable concurrent reads and writes without locking?
9. What is sharding/partitioning and what are the tradeoffs between range
and hash partitioning?
10. How would you design a schema for a time-series IoT sensor database?
### Distributed Systems
11. Explain the CAP theorem with a concrete example.
This PDF document was created using the online html editor powered by CKEditor.
12. What is the difference between strong consistency, eventual consistency,
and causal consistency?
13. How does Kafka guarantee message ordering? What are its limitations?
14. What is consistent hashing and why is it useful for distributed systems?
15. Explain 2PC (Two-Phase Commit) and its failure modes.
### Data Warehousing
16. What is dimensional modeling? Walk me through the 4-step design process.
17. Explain SCD Type 2 with an example and SQL implementation.
18. What is the difference between a star schema and a snowflake schema?
Which is preferred?
19. What are the different types of facts (additive, semi-additive, nonadditive)?
20. What is a conformed dimension and why is it important?
21. When would you use a fact-less fact table?
22. What is the difference between Kimball and Inmon methodologies?
### Streaming
23. Explain event time vs processing time with a real-world example.
24. What is a watermark and how does it help with out-of-order events?
25. Describe the differences between tumbling, sliding, and session windows.
26. How does Flink achieve exactly-once semantics end-to-end?
27. What is the stream-table duality?
28. When would you choose Kafka Streams vs Apache Flink?
29. What is back-pressure and how is it handled in Flink?
### Data Pipelines
30. What makes a data pipeline idempotent? Give an example.
31. What is a Dead Letter Queue and when would you use it?
32. How do you test a data pipeline?
33. What are the key components of Apache Airflow?
34. What is the difference between Airflow, Prefect, and Dagster?
35. How would you implement CDC-based ingestion from PostgreSQL to a data
warehouse?
36. How do you handle late-arriving data in a batch pipeline?
### Cloud & Architecture
37. Explain the Medallion Architecture (Bronze/Silver/Gold).
This PDF document was created using the online html editor powered by CKEditor.
38. What is the Lambda Architecture and what problems does Kappa solve?
39. What is a Data Lakehouse? How does Delta Lake enable it?
40. Explain the 4 principles of Data Mesh.
41. How does Snowflake separate compute from storage?
42. What is Z-ordering and when should you use it?
43. How do you handle GDPR "right to erasure" in an append-only data lake?
44. What is Reverse ETL? Give a business use case.
### System Design
45. Design a real-time fraud detection system for a payment processor
handling 10K TPS.
46. Design a data ingestion platform for a healthcare organization with
HIPAA compliance.
47. Design an e-commerce data warehouse supporting sales, inventory, and
marketing analytics.
48. How would you design a feature store for an ML recommendation system?
49. Design a data pipeline for tracking millions of IoT sensor readings in
real-time.
50. How would you build a self-serve analytics platform that scales across
100 data teams?
---
## 7.10 Common System Design Anti-Patterns
| Anti-Pattern | Problem | Solution |
|---|---|---|
| **God pipeline** | Single monolithic pipeline doing everything | Decompose
into modular, single-purpose stages |
| **Schema drift** | Source schema changes break downstream pipelines |
Schema registry + schema evolution testing |
| **Hot partition** | All writes go to one Kafka partition | Choose
partition key that distributes evenly |
| **Mixed grain fact table** | Header + line items in same fact table |
Separate fact tables per grain |
| **Processing time windows** | Incorrect results for out-of-order events |
Use event time + watermarks |
| **Silent failures** | Bad records dropped without alerting | Dead Letter
This PDF document was created using the online html editor powered by CKEditor.
Queue + monitoring |
| **No idempotency** | Duplicate data on pipeline reruns | MERGE/UPSERT
patterns, surrogate keys |
| **PII in DWH** | Compliance violation | De-identify at ingestion; tokenize
PII |
| **No backfill strategy** | Can't reprocess historical data | Design
pipelines as pure functions of date partition |
| **Cargo cult metrics** | Collecting data nobody uses | Start with business
questions, work backward |
---
## 7.11 Case Study 6: IoT Data Platform — Smart Manufacturing
### Problem Statement
A manufacturing company has 5,000 machines across 20 factories:
- Each machine emits 50 sensor readings/second (vibration, temperature,
pressure, RPM)
- Predict machine failure 4–6 hours before it happens (predictive
maintenance)
- Operations dashboard: real-time health status per machine
- Compliance: retain raw sensor data for 5 years for regulatory audit
### Scale Estimation
```
5,000 machines × 50 readings/sec = 250,000 events/second
= 15M events/minute
= 900M events/hour
= ~21.6B events/day
= ~1TB raw data/day (assuming 50 bytes/event after encoding)
= ~365TB/year → 5-year retention = ~1.8PB
```
### Architecture
```mermaid
flowchart TD
 subgraph Edge Layer - Factories
This PDF document was created using the online html editor powered by CKEditor.
 PLC[PLC Controllers\\nOPC-UA protocol\\n50 readings/sec/machine]
 GW[Edge Gateway\\nKafka edge broker\\nBuffer during WAN outage]
 end
 subgraph Cloud - Ingestion
 EH[Azure Event Hubs\\nor Kafka on AKS\\n250K events/sec]
 end
 subgraph Stream Processing
 F[Apache Flink\\nJob 1: Anomaly detection\\ntemperature 3σ above
baseline\\nJob 2: Predictive model scoring\\nJob 3: Aggregation per
machine/5min]
 end
 subgraph Real-Time Serving
 TS[TimescaleDB\\nTime-series storage\\nLast 90 days hot data]
 RD[Redis\\nLive machine status\\nCurrent health score per machine]
 DASH[Grafana Dashboard\\nFactory floor ops team]
 end
 subgraph Cold Storage
 S3[Azure Blob / S3\\nParquet files\\n5-year retention\\nIceberg
table format]
 end
 subgraph ML Platform
 ML[Azure ML\\nPredictive maintenance model\\nTraining: weekly Spark
job on S3\\nScoring: Flink real-time]
 FS[Feature Store\\nPer-machine rolling features\\nAvg vibration 1h,
6h, 24h]
 end
 subgraph Alerting
 PA[PagerDuty / Teams\\nAlert when failure\\nprobability > 80%]
 end
 PLC --> GW --> EH
 EH --> F
This PDF document was created using the online html editor powered by CKEditor.
 F --> TS
 F --> RD
 F --> ML
 EH -->|Kafka Connect S3 Sink| S3
 S3 --> ML
 RD --> DASH
 TS --> DASH
 ML --> PA
```
### Key Design Decisions
**Edge buffering:** Kafka on the edge gateway buffers events when WAN is
down. On reconnect, replays to cloud. Without edge buffering, a 5-minute WAN
outage loses 75M events.
**TimescaleDB for hot data (90 days):** Hypertable auto-partitioned by
`machine_id + time`. Continuous aggregates compute 5-min, 1-hour, 1-day
rollups automatically. Range queries by time + machine are fast.
**Iceberg on S3 for cold storage (5 years):** Iceberg partition evolution —
today partition by `(factory_id, year, month)`, can change partition scheme
in future without rewriting data.
**Flink for anomaly detection:**
```python
# Flink pattern: detect temperature spike > 3 standard deviations
from pyflink.cep import Pattern, CEP
# Define pattern: 3 consecutive readings above 3σ threshold
anomaly_pattern = Pattern \\
 .begin("start") \\
 .where(lambda e: e['temperature'] > e['threshold_3sigma']) \\
 .times(3) \\
 .consecutive()
# Apply to keyed stream (per machine)
pattern_stream = CEP.pattern(
This PDF document was created using the online html editor powered by CKEditor.
 sensor_stream.key_by('machine_id'),
 anomaly_pattern
)
```
---
## 7.12 Case Study 7: Social Media Analytics Platform
### Problem Statement
A social media company (100M users):
- Track post engagement metrics (views, likes, shares, comments) in realtime
- Trending content detection per region, per topic (update every minute)
- Creator analytics dashboard: "How is my post performing?"
- Ad targeting: segment users by interest graph
### Architecture
```mermaid
flowchart LR
 subgraph Events
 E1[Post view events]
 E2[Like / share events]
 E3[Comment events]
 end

 subgraph Kafka
 K1[Topic: post-views\\n100 partitions]
 K2[Topic: engagement\\n50 partitions]
 end

 subgraph Flink Processing
 F1[Engagement Aggregation\\n1-min tumbling window\\nper post_id]
 F2[Trending Detection\\n5-min sliding window\\nper
topic+region\\nVelocity = Δengagement/Δtime]
 F3[User Interest Graph\\nStream-Table join\\nPost topics → user
interests]
This PDF document was created using the online html editor powered by CKEditor.
 end

 subgraph Storage
 CH[ClickHouse\\nPost metrics history\\nSub-second OLAP]
 CASS[Cassandra\\nCreator post-level metrics\\nPer post_id partition]
 RD[Redis\\nReal-time trending topics\\nTTL 1 hour]
 NEO[Neo4j\\nUser interest graph\\nFor ad targeting]
 end

 subgraph Serving
 CD[Creator Dashboard\\nPer-post analytics]
 TR[Trending Feed\\nBy region/topic]
 ADS[Ad Targeting API\\nUser segments]
 end

 E1 --> K1 --> F1 --> CASS
 E1 --> K1 --> F2 --> RD
 E2 --> K2 --> F1
 E3 --> K2 --> F3 --> NEO
 F1 --> CH
 CASS --> CD
 RD --> TR
 NEO --> ADS
 CH --> CD
```
**Why ClickHouse for post metrics history:**
Queries like "Show me hourly engagement for my top 50 posts over the last 30
days" scan billions of rows. ClickHouse returns this in < 500ms; BigQuery
would take 5–15 seconds and cost $0.005 per query.
**Why Neo4j for user interest graph:**
"Find users who follow 3+ accounts in category X and engaged with Y in the
last 7 days" — this is a graph traversal. SQL would require 4+ joins across
100M rows. Neo4j traverses in milliseconds.
**Trending detection logic:**
```python
This PDF document was created using the online html editor powered by CKEditor.
# Flink: trending = posts with engagement velocity > threshold
# Velocity = (engagements_last_1min - engagements_prev_1min) /
engagements_prev_1min
class TrendingDetector(ProcessWindowFunction):
 def process(self, key, context, elements, out):
 posts = list(elements)
 current_engagement = sum(p['engagement'] for p in posts if
p['is_current_window'])
 prev_engagement = sum(p['engagement'] for p in posts if
p['is_prev_window'])

 if prev_engagement > 0:
 velocity = (current_engagement - prev_engagement) /
prev_engagement
 if velocity > 0.5: # 50% growth in 1 minute
 out.collect({
 'post_id': key,
 'velocity': velocity,
 'current_engagement': current_engagement,
 'region': posts[0]['region']
 })
```
---
## 7.13 Technology Decision Trees — Consolidated Reference
### Which Database?
```mermaid
flowchart TD
 A{What are you storing?}
 A -->|Financial transactions\\nACID critical| PG[PostgreSQL\\nMySQL
InnoDB\\nCockroachDB for global]
 A -->|User sessions\\nFast K-V lookups| RD2[Redis\\nMemcached]
 A -->|Analytics data\\nBig scans| ANA{How big?}
 ANA -->|< 10GB| DDB[DuckDB\\nSQLite]
This PDF document was created using the online html editor powered by CKEditor.
 ANA -->|> 10GB cloud| CLOUD{Real-time or batch?}
 CLOUD -->|Batch, BI reports| SFB[Snowflake\\nBigQuery\\nRedshift]
 CLOUD -->|Real-time, sub-second| COLT[ClickHouse\\nApache Druid\\nPinot]
 A -->|Time-series, IoT metrics| TS2[TimescaleDB\\nInfluxDB\\nPrometheus]
 A -->|Text search, log analytics| ES2[Elasticsearch\\nOpenSearch]
 A -->|Graphs, relationships| GR2[Neo4j\\nAmazon Neptune]
 A -->|Document, flexible schema| MG[MongoDB\\nCouchDB]
 A -->|Wide-column, high write| CAS2[Cassandra\\nHBase\\nScyllaDB]
```
### Which Processing Engine?
```mermaid
flowchart TD
 B{Processing type?}
 B -->|SQL transforms\\nDWH layer| DBT2[dbt\\nSQL stored procs]
 B -->|Batch > 1GB| SPK[Apache Spark\\nDatabricks\\nGlue]
 B -->|Real-time streaming\\nstateful| FLK[Apache Flink\\nKafka Streams]
 B -->|Real-time streaming\\nSimple filters| SS[Spark Structured
Streaming\\nKSQL]
 B -->|< 1GB, Python, prototyping| PD[Pandas\\nPolars\\nDuckDB]
 B -->|SQL on files, ad-hoc| TR2[Trino\\nPresto\\nAthena]
```
### Which Orchestrator?
```mermaid
flowchart TD
 C{Team situation?}
 C -->|Large team, complex DAGs\\nEnterprise scale| AF2[Apache
Airflow\\nAstronomer / MWAA]
 C -->|Python-native, startup\\nFast iterations| PREF2[Prefect
2.0\\nPrefect Cloud]
 C -->|Data assets, lineage first\\nType safety| DGS[Dagster]
 C -->|SQL + dbt primary\\nSimpler pipelines| DBTC[dbt Cloud\\nScheduled
models]
 C -->|Microsoft Azure stack| ADF2[Azure Data Factory\\nFabric Pipelines]
 C -->|ML pipelines, experiments| MLF[Metaflow\\nFlyte\\nZenML]
This PDF document was created using the online html editor powered by CKEditor.
```
---
## 7.14 End-to-End System Design Walk-through: QPS Claims Analytics Platform
### Problem Statement (Healthcare + Microsoft Fabric Context)
Design a claims quality analytics platform for a health plan:
- 50M+ medical claims/year from 200+ provider networks
- Quality Performance Scores (QPS) calculated from HEDIS measures
- Near-real-time claims ingestion (< 4 hour lag from adjudication)
- Regulatory: HIPAA, CMS reporting deadlines
- BI: Power BI dashboards for medical directors, quality teams
### Architecture
```mermaid
flowchart TD
 subgraph Source Systems
 CLM[Claims Adjudication System\\nSQL Server 2019]
 ELG[Eligibility System\\nOracle DB]
 PRV[Provider Directory\\nREST API]
 NCH[NCQA / CMS Reference\\nAnnual file drops]
 end
 subgraph Ingestion - Azure Data Factory
 ADF2[ADF Pipelines\\nCDC via SQL Server Change Tracking\\nor full
refresh for small tables]
 end
 subgraph Bronze Lakehouse - OneLake
 BRZ2[Raw claims, eligibility, provider\\nDelta format\\nImmutable,
as-is\\nPHI encrypted at rest]
 end
 subgraph PHI De-identification - Spark Notebook
 DID2[Presidio NER\\nDetect: MRN, DOB, SSN, Name\\nTokenize with
This PDF document was created using the online html editor powered by CKEditor.
Azure Key Vault]
 end
 subgraph Silver Lakehouse - OneLake
 SLV2[Cleaned claims\\nDe-identified member data\\nConformed
schema\\nDelta MERGE for upserts]
 end
 subgraph Gold Warehouse - Fabric Warehouse
 GLD2[Dimensional Model\\nfact_medical_claim\\ndim_member,
dim_provider\\ndim_hedis_measure\\nSCD Type 2 on dim_member]
 HEDIS[HEDIS Measure
Tables\\nfact_hedis_numerator\\nfact_hedis_denominator\\nComputed by measure
specs]
 end
 subgraph Serving
 PBI2[Power BI - DirectLake\\nQPS Dashboards\\nMedical Director
views\\nDrilldown to member level]
 API2[Internal API\\nQuality score per provider\\nfor contracting
team]
 CMS2[CMS Reporting\\nStarRatings submission\\nAnnual export]
 end
 subgraph Governance
 PV2[Microsoft Purview\\nPHI classification labels\\nData
lineage\\nAccess audit logs]
 AKV[Azure Key Vault\\nPHI token mapping\\nColumn encryption keys]
 end
 CLM --> ADF2 --> BRZ2 --> DID2 --> SLV2
 ELG --> ADF2
 PRV --> ADF2
 NCH --> ADF2
 SLV2 --> GLD2 --> HEDIS
 GLD2 --> PBI2
 HEDIS --> PBI2
 GLD2 --> API2
This PDF document was created using the online html editor powered by CKEditor.
 HEDIS --> CMS2
 PV2 -.-> BRZ2
 PV2 -.-> SLV2
 PV2 -.-> GLD2
 AKV -.-> DID2
```
### HEDIS Measure Example — Diabetes Care (HbA1c Testing)
**Numerator:** Members with diabetes who had an HbA1c test in the
measurement year.
**Denominator:** Members 18–75 with a diabetes diagnosis.
```sql
-- Gold layer: HEDIS_HBA1C measure
-- Denominator: Members with diabetes
INSERT INTO fact_hedis_denominator (measure_key, member_key,
measurement_year)
SELECT
 m.measure_key,
 c.member_key,
 2024 AS measurement_year
FROM fact_medical_claim c
JOIN dim_member mem ON c.member_key = mem.member_key AND mem.is_current
JOIN bridge_member_diagnosis bd ON c.claim_key = bd.claim_key
JOIN dim_diagnosis d ON bd.diagnosis_key = d.diagnosis_key
JOIN dim_hedis_measure m ON m.measure_code = 'CDC-HBA1C'
WHERE d.icd10_code BETWEEN 'E10' AND 'E13.9' -- Diabetes ICD-10 codes
 AND mem.age BETWEEN 18 AND 75
 AND c.service_date_key BETWEEN 20240101 AND 20241231
 AND mem.continuous_enrollment_months >= 11 -- HEDIS continuous
enrollment req
-- Numerator: Members who had HbA1c lab test
INSERT INTO fact_hedis_numerator (measure_key, member_key, measurement_year)
SELECT DISTINCT
 m.measure_key,
 c.member_key,
This PDF document was created using the online html editor powered by CKEditor.
 2024
FROM fact_medical_claim c
JOIN dim_procedure p ON c.procedure_key = p.procedure_key
JOIN dim_hedis_measure m ON m.measure_code = 'CDC-HBA1C'
WHERE p.cpt_code IN ('83036', '83037') -- HbA1c CPT codes
 AND c.service_date_key BETWEEN 20240101 AND 20241231
 AND c.member_key IN (SELECT member_key FROM fact_hedis_denominator
 WHERE measure_key = m.measure_key)
```
**QPS Rate Calculation:**
```sql
SELECT
 m.measure_name,
 COUNT(DISTINCT n.member_key) AS numerator_count,
 COUNT(DISTINCT d.member_key) AS denominator_count,
 ROUND(COUNT(DISTINCT n.member_key) * 100.0 / NULLIF(COUNT(DISTINCT
d.member_key), 0), 2) AS rate_pct,
 -- Benchmark against national average (from NCQA)
 ncqa.national_avg_pct,
 ROUND(COUNT(DISTINCT n.member_key) * 100.0 / NULLIF(COUNT(DISTINCT
d.member_key), 0) - ncqa.national_avg_pct, 2) AS delta_from_benchmark
FROM fact_hedis_denominator d
LEFT JOIN fact_hedis_numerator n USING (measure_key, member_key,
measurement_year)
JOIN dim_hedis_measure m ON d.measure_key = m.measure_key
JOIN ncqa_benchmarks ncqa ON m.measure_code = ncqa.measure_code
WHERE d.measurement_year = 2024
GROUP BY m.measure_name, ncqa.national_avg_pct
ORDER BY delta_from_benchmark ASC;
```
---
# Part 8: Apache Spark — Architecture, Internals & Optimization
> The most commonly used distributed processing engine in data engineering
---
This PDF document was created using the online html editor powered by CKEditor.
## 8.1 Why Spark Replaced MapReduce
| Aspect | MapReduce | Apache Spark |
|---|---|---|
| Intermediate storage | Disk (HDFS) between every stage | In-memory (RDDs /
DataFrames) |
| Iterative algorithms | O(n) disk reads per iteration | Single load,
iterate in memory |
| API | Java/Python verbose | Concise DataFrame / SQL API |
| Streaming | No native support | Spark Structured Streaming |
| Speed | Baseline | 10–100× faster for iterative workloads |
---
## 8.2 Spark Architecture
```mermaid
graph TD
 subgraph Driver Program
 SC[SparkContext / SparkSession\\nJob coordinator\\nBuilds logical
plan\\nSchedules stages]
 end
 subgraph Cluster Manager
 CM[YARN / Kubernetes / Standalone\\nAllocates resources\\nLaunches
executors]
 end
 subgraph Worker Nodes
 E1[Executor 1\\nTask Slot 1\\nTask Slot 2\\nTask Slot 3\\nIn-memory
store]
 E2[Executor 2\\nTask Slot 1\\nTask Slot 2\\nTask Slot 3\\nIn-memory
store]
 E3[Executor 3\\nTask Slot 1\\nTask Slot 2\\nTask Slot 3\\nIn-memory
store]
 end
 SC -->|Submit application| CM
This PDF document was created using the online html editor powered by CKEditor.
 CM -->|Launch| E1
 CM -->|Launch| E2
 CM -->|Launch| E3
 SC -->|Send tasks| E1
 SC -->|Send tasks| E2
 SC -->|Send tasks| E3
```
### Key Components
| Component | Role |
|---|---|
| **Driver** | Runs `main()`, creates SparkSession, builds DAG, sends tasks
to executors, collects results |
| **SparkSession** | Unified entry point (replaces SparkContext, SQLContext,
HiveContext) |
| **Executor** | JVM process on worker node; runs tasks, stores data in
memory/disk |
| **Task** | Smallest unit of work — processes one partition |
| **Stage** | Group of tasks with no shuffle between them |
| **Job** | Triggered by an action (`.count()`, `.collect()`, `.write()`);
contains stages |
| **DAG** | Directed Acyclic Graph of transformations — built lazily before
execution |
| **Cluster Manager** | YARN (Hadoop), Kubernetes, or Spark Standalone |
---
## 8.3 Lazy Evaluation & DAG Execution
Spark is **lazy**: transformations are not executed until an **action** is
called.
```python
# All of these are transformations — no computation yet
df = spark.read.parquet("s3://bucket/sales/") # Transformation 1
df2 = df.filter(df.amount > 100) # Transformation 2
df3 = df2.groupBy("region").sum("amount") # Transformation 3
This PDF document was created using the online html editor powered by CKEditor.
# This action TRIGGERS execution of the entire DAG
result = df3.collect() # ← Job submitted, stages planned, tasks executed
```
### Wide vs Narrow Transformations
```mermaid
graph LR
 subgraph Narrow Transformations\\nNo shuffle — same partition
 NT1[filter\\nmap\\nselect\\nunion\\nwithColumn]
 end
 subgraph Wide Transformations\\nRequires shuffle across network
 WT1[groupBy\\njoin\\ndistinct\\norderBy\\nrepartition]
 end
 NT1 -->|Stage boundary\\ncrossed only for| WT1
```
**Wide transformation = new stage boundary = shuffle** — the most expensive
operation in Spark.
---
## 8.4 RDDs vs DataFrames vs Datasets
| | RDD | DataFrame | Dataset |
|---|---|---|---|
| API Level | Low-level | High-level SQL-like | High-level typed |
| Language | Java, Scala, Python, R | All | Java, Scala only |
| Type Safety | Yes (Scala/Java) | No (runtime errors) | Yes (compile-time)
|
| Catalyst Optimizer | No | Yes | Yes |
| Tungsten Execution | No | Yes | Yes |
| Use Today | Rarely — only when fine-grained control needed | Primary API |
Java/Scala apps |
> **Rule of thumb:** Always use DataFrames (or Spark SQL) in Python/PySpark.
The Catalyst optimizer + Tungsten make them dramatically faster than RDDs.
This PDF document was created using the online html editor powered by CKEditor.
---
## 8.5 The Catalyst Optimizer
Catalyst is Spark's query optimizer — it automatically rewrites your logical
plan into a more efficient physical plan.
```mermaid
flowchart LR
 A[Unresolved\\nLogical Plan\\nSQL / DataFrame API] -->
B[Analysis\\nResolve column names\\ntable references]
 B --> C[Logical Optimization\\nPredicate pushdown\\nConstant
folding\\nColumn pruning]
 C --> D[Physical Planning\\nJoin strategy selection\\nPartition pruning]
 D --> E[Code Generation\\nWhole-stage codegen\\nTungsten JVM bytecode]
 E --> F[Execution]
```
### What Catalyst Does Automatically
**Predicate pushdown:** Move filters as early as possible (before joins,
scans).
```python
# You write:
df.join(orders, "customer_id").filter(df.country == "US")
# Catalyst rewrites to:
df.filter(df.country == "US").join(orders, "customer_id") # Filter first,
join less data
```
**Column pruning:** Only read columns that are actually used.
```python
# You select only 2 of 50 columns
df.select("order_id", "amount").write.parquet(...)
# Catalyst tells the Parquet reader to skip the other 48 columns
```
This PDF document was created using the online html editor powered by CKEditor.
**Constant folding:** Evaluate constant expressions at planning time.
```python
df.filter(df.year == 2020 + 4) # Catalyst evaluates to: df.year == 2024 at
plan time
```
---
## 8.6 Spark Memory Model
```mermaid
graph TD
 subgraph Executor JVM Heap
 RM[Reserved Memory\\n~300MB\\nSpark internal]
 UM[User Memory\\n~40% of remaining\\nUser data structures\\nUDF
state]
 SM[Spark Memory\\n~60% of remaining]
 end
 subgraph Spark Memory Pool
 EM[Execution Memory\\nShuffle buffers\\nSort buffers\\nJoin hash
tables]
 STM[Storage Memory\\nCached RDDs/DFs\\nBroadcast variables]
 end
 SM --> EM
 SM --> STM
 EM <-->|Unified memory\\nshared dynamically| STM
```
**Key config parameters:**
```python
spark = SparkSession.builder \\
 .config("spark.executor.memory", "8g") # Total executor heap
 .config("spark.memory.fraction", "0.6") # Fraction of heap for
Spark memory
 .config("spark.memory.storageFraction", "0.5") # Of Spark memory,
fraction for storage
This PDF document was created using the online html editor powered by CKEditor.
 .config("spark.executor.memoryOverhead", "2g") # Off-heap for
native/Python memory
 .getOrCreate()
```
---
## 8.7 Partitioning in Spark
A **partition** is the unit of parallelism. Each partition maps to one task
on one executor.
### Default Partitioning
```python
# Reading from files: one partition per file block (128MB default HDFS
block)
df = spark.read.parquet("s3://bucket/data/")
# After a shuffle (groupBy, join): spark.sql.shuffle.partitions (default:
200)
spark.conf.set("spark.sql.shuffle.partitions", "400") # Tune for data size
```
### Rule of Thumb for Partition Count
```
Target partition size: 100–200MB per partition
Number of partitions = Total data size / target partition size
Example: 400GB data → 400GB / 128MB ≈ 3,200 partitions
```
### Repartition vs Coalesce
| Operation | What it does | Shuffle? | Use when |
|---|---|---|---|
| `repartition(n)` | Full shuffle, equal-sized partitions | Yes | Increasing
partitions, fixing skew |
| `coalesce(n)` | Merges partitions, no full shuffle | No (partial) |
This PDF document was created using the online html editor powered by CKEditor.
Decreasing partitions (before write) |
```python
# Before writing small output — reduce to avoid small file problem
df_result.coalesce(10).write.parquet("s3://bucket/output/")
# After reading many small files — increase parallelism
df = spark.read.parquet("s3://bucket/small_files/").repartition(200)
```
---
## 8.8 Join Strategies
This is the most interview-critical Spark topic.
```mermaid
flowchart TD
 J{Join Strategy Selection}
 J -->|One side < 10MB\\nauto-broadcast| BJ[Broadcast Hash Join\\nNo
shuffle\\nFastest\\nBroadcast small table to all executors]
 J -->|Both sides medium\\nboth sorted| SMJ[Sort-Merge Join\\nShuffle
both sides\\nSort then merge\\nDefault for large tables]
 J -->|One side small\\nnot broadcast threshold| SHJ[Shuffle Hash
Join\\nShuffle larger side\\nBuild hash map from smaller]
 J -->|Nested loop\\ncross join or inequality| NLJ[Broadcast Nested
Loop\\nSlowest\\nO n×m]
```
### Broadcast Hash Join (Best)
```python
from pyspark.sql.functions import broadcast
# Force broadcast of small dimension table
result = fact_df.join(broadcast(dim_country_df), "country_code")
# dim_country (~240 rows) is sent to every executor
# No shuffle of the large fact table!
This PDF document was created using the online html editor powered by CKEditor.
# Auto-broadcast threshold (default 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")
```
### Sort-Merge Join (Default for large tables)
```
1. Shuffle fact_df by join key → all rows with key=X go to same partition
2. Shuffle dim_df by join key → same
3. Sort both within partitions
4. Merge-join sorted partitions
Cost: 2 network shuffles (expensive!)
```
---
## 8.9 Data Skew — The Silent Performance Killer
**Skew** happens when one partition has vastly more data than others. One
task takes 10× longer — the job is as slow as the slowest task.
```
Without skew: With skew:
Task 1: 5min ████ Task 1: 5min ████
Task 2: 5min ████ Task 2: 5min ████
Task 3: 5min ████ Task 3: 47min ████████████████████████
Task 4: 5min ████ Task 4: 5min ████
Total: 5min Total: 47min ← bottleneck
```
**How to detect:**
```python
# Check partition sizes
df.groupBy(spark_partition_id()).count().orderBy("count",
ascending=False).show(10)
# Check Spark UI → Stages → Task duration distribution
```
This PDF document was created using the online html editor powered by CKEditor.
### Fix 1: Salting (for highly skewed join keys)
```python
from pyspark.sql.functions import col, concat, lit, floor, rand,
monotonically_increasing_id
NUM_SALTS = 10
# Salt the larger (skewed) side
skewed_df = skewed_df.withColumn(
 "salted_key",
 concat(col("join_key"), lit("_"), (rand() *
NUM_SALTS).cast("int").cast("string"))
)
# Explode the smaller side to match all salt values
from pyspark.sql.functions import array, explode
small_df = small_df.withColumn("salt_range", array([lit(i) for i in
range(NUM_SALTS)]))
small_df = small_df.withColumn("salt", explode("salt_range"))
small_df = small_df.withColumn(
 "salted_key",
 concat(col("join_key"), lit("_"), col("salt").cast("string"))
)
# Join on salted key
result = skewed_df.join(small_df, "salted_key")
```
### Fix 2: AQE Skew Join (Spark 3.0+)
```python
# Let Spark automatically split skewed partitions at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
"256mb")
```
---
This PDF document was created using the online html editor powered by CKEditor.
## 8.10 Adaptive Query Execution (AQE) — Spark 3.x
AQE dynamically re-optimizes the query plan **at runtime** based on actual
data statistics.
```mermaid
flowchart LR
 A[Query Plan\\ncompiled with\\nestimates] --> B[Stage 1
executes\\nActual shuffle\\nstatistics collected]
 B --> C[AQE Re-optimizer\\nAdjusts plan for\\nnext stages]
 C --> D[Stage 2 executes\\nwith better plan]
 D --> E[...]
```
**Three AQE features:**
| Feature | What it does | Config |
|---|---|---|
| **Coalesce shuffle partitions** | Merges small post-shuffle partitions
automatically | `spark.sql.adaptive.coalescePartitions.enabled=true` |
| **Convert sort-merge to broadcast** | If one side turns out small at
runtime, switch to broadcast join |
`spark.sql.adaptive.localShuffleReader.enabled=true` |
| **Skew join handling** | Automatically splits skewed partitions |
`spark.sql.adaptive.skewJoin.enabled=true` |
```python
# Enable all AQE features (recommended for all Spark 3.x jobs)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```
---
## 8.11 Caching & Persistence
This PDF document was created using the online html editor powered by CKEditor.
```python
from pyspark import StorageLevel
# Default: MEMORY_AND_DISK — cache in memory, spill to disk if needed
df.cache() # alias for persist(StorageLevel.MEMORY_AND_DISK)
# Explicit levels
df.persist(StorageLevel.MEMORY_ONLY) # Fastest, fails if OOM
df.persist(StorageLevel.MEMORY_AND_DISK) # Balanced
df.persist(StorageLevel.DISK_ONLY) # Slowest, good for very
large DFs
df.persist(StorageLevel.MEMORY_AND_DISK_SER) # Serialized — less memory,
slower deserialization
# Always unpersist when done
df.unpersist()
```
**When to cache:**
- DataFrames used **more than once** in the same job
- Iterative ML algorithms (same training data used many times)
- Small lookup tables joined repeatedly
**When NOT to cache:**
- DataFrames used only once
- Very large DataFrames with no reuse
- DataFrames after a write (Spark doesn't reuse anyway)
---
## 8.12 Spark Performance Tuning Checklist
```mermaid
flowchart TD
 A[Spark Job Slow?]
 A --> B{Check Spark UI\\nStages tab}
 B -->|One task much\\nslower than others| SK[Data Skew\\n→ Salt join
key\\nor enable AQE]
This PDF document was created using the online html editor powered by CKEditor.
 B -->|Many small tasks\\nshort duration| PT[Too many
small\\npartitions\\n→ Increase partition size\\nor reduce shuffle
partitions]
 B -->|All tasks slow\\nstage takes hours| BJ2[Missing broadcast join\\n→
Enable broadcast\\nfor small dimension]
 B -->|OOM errors| MEM[Memory issue\\n→ Increase executor.memory\\nor
reduce parallelism]
 B -->|GC time high| GC[Garbage collection\\n→ Use Kryo serializer\\nor
reduce storage fraction]
```
### Performance Tuning Reference
```python
# Full optimized Spark session configuration
spark = SparkSession.builder \\
 .appName("OptimizedJob") \\
 # Parallelism
 .config("spark.default.parallelism", "400") # For RDD ops
 .config("spark.sql.shuffle.partitions", "400") # For DataFrame
shuffles
 # Memory
 .config("spark.executor.memory", "16g")
 .config("spark.executor.memoryOverhead", "4g") # Python +
native overhead
 .config("spark.driver.memory", "8g")
 # AQE
 .config("spark.sql.adaptive.enabled", "true")
 .config("spark.sql.adaptive.skewJoin.enabled", "true")
 .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
 # Joins
 .config("spark.sql.autoBroadcastJoinThreshold", "50mb")
 # Serialization
 .config("spark.serializer",
"org.apache.spark.serializer.KryoSerializer")
 # Dynamic allocation
 .config("spark.dynamicAllocation.enabled", "true")
 .config("spark.dynamicAllocation.minExecutors", "5")
This PDF document was created using the online html editor powered by CKEditor.
 .config("spark.dynamicAllocation.maxExecutors", "100")
 .getOrCreate()
```
### Common Spark Anti-Patterns
| Anti-Pattern | Problem | Fix |
|---|---|---|
| `.collect()` on large DF | OOM on driver | Use `.write()` instead, or
`.take(n)` for samples |
| UDFs on every row | Disables Catalyst, 10× slower than SQL | Use built-in
Spark SQL functions |
| `df.count()` in loop | Triggers a full scan each time | Cache the DF first
|
| 200 shuffle partitions (default) | Wrong for most real workloads | Tune to
data size or enable AQE |
| cartesian join | O(n²) — destroys cluster | Always specify join key |
| Reading many tiny files | Thousands of tasks, high overhead | Repartition
after read, or compact files first |
---
## 8.13 Spark on Cloud
### AWS — Elastic MapReduce (EMR)
```bash
# Submit Spark job to EMR
aws emr add-steps \\
 --cluster-id j-XXXXXXXX \\
 --steps Type=Spark,Name="Sales ETL",\\
ActionOnFailure=CONTINUE,\\
Args=[--deploy-mode,cluster,--class,com.company.SalesETL,\\
s3://bucket/jars/sales-etl.jar,--date,2024-03-15]
```
**EMR key decisions:**
- **Instance fleet vs instance group**: Fleet lets EMR use Spot Instances
automatically — 60–90% cost savings
This PDF document was created using the online html editor powered by CKEditor.
- **EMR on EKS**: Run Spark on Kubernetes; better isolation per job
- **EMR Serverless**: No cluster management; good for intermittent jobs
### Databricks
```python
# Databricks Utilities — file system access, secrets, widgets
dbutils.fs.ls("dbfs:/mnt/data-lake/")
dbutils.secrets.get(scope="prod-secrets", key="snowflake-password")
# Delta Live Tables (DLT) — declarative pipeline
import dlt
from pyspark.sql.functions import col
@dlt.table(comment="Silver layer: cleaned orders")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def silver_orders():
 return (
 dlt.read("bronze_orders")
 .filter(col("order_status") != "TEST")
 .dropDuplicates(["order_id"])
 )
```
### Azure — Spark in Microsoft Fabric
```python
# Fabric Notebook — no SparkSession creation needed, auto-injected
# Access Lakehouse tables directly
df = spark.read.format("delta").load("Tables/silver_claims")
# Write back to Lakehouse
df.write.format("delta").mode("overwrite").saveAsTable("gold_claims_summary")
```
---
## 8.14 Real-World Spark Use Cases
| Company | Use Case | Spark Config | Scale |
This PDF document was created using the online html editor powered by CKEditor.
|---|---|---|---|
| **Netflix** | nightly encoding job metrics processing | EMR, 500
executors, r5.4xlarge | 50TB/night |
| **Airbnb** | pricing model feature generation | Databricks, autoscaling
10–200 nodes | 100M rows/day |
| **Uber** | trip data aggregation for analytics | YARN on bare metal +
Kubernetes | 10PB dataset |
| **JPMorgan** | risk calculation (Monte Carlo simulation) | Spark on Azure,
broadcast small market data | 100K simulations/day |
| **Optum/UHG** | HEDIS measure computation across 30M members | Spark on
Databricks / Fabric | 500M+ claims/year |
### Example: Optimized HEDIS Computation in Spark
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, broadcast
spark = SparkSession.builder \\
 .config("spark.sql.adaptive.enabled", "true") \\
 .config("spark.sql.shuffle.partitions", "800") \\
 .getOrCreate()
# Read large claims table (partitioned by year/month in S3)
claims = spark.read.format("delta") \\
 .load("abfss://silver@datalake.dfs.core.windows.net/claims") \\
 .filter(col("service_year") == 2024) # Partition pruning!
# Small reference tables → broadcast
icd_codes = spark.read.format("delta").load(".../dim_diagnosis") #
~50K rows
cpt_codes = spark.read.format("delta").load(".../dim_procedure_codes") #
~10K rows
# HEDIS denominator: broadcast small lookup, avoid shuffle of claims
denominator = claims \\
 .join(broadcast(icd_codes.filter(col("is_diabetes_code"))),
"icd10_code") \\
This PDF document was created using the online html editor powered by CKEditor.
 .groupBy("member_id") \\
 .agg(countDistinct("claim_id").alias("claim_count")) \\
 .filter(col("claim_count") >= 1)
denominator.write.format("delta") \\
 .mode("overwrite") \\
 .option("overwriteSchema", "true") \\
 .saveAsTable("gold_hedis_denominator_cdc")
```
---
## 8.15 Cross Questions — Apache Spark
**Q1: Explain the difference between a transformation and an action in
Spark.**
> Transformations (filter, map, join, groupBy) are lazy — they build the DAG
but don't execute. Actions (collect, count, write, show) trigger the actual
execution. This allows Spark to optimize the entire computation plan before
running.
**Q2: What is a shuffle and why is it expensive?**
> A shuffle moves data across the network between executors — necessary for
wide transformations (groupBy, join, distinct). Expensive because: (1) data
serialized and written to disk, (2) sent over network, (3) deserialized on
receiving executor. Minimizing shuffles is the primary goal of Spark
optimization.
**Q3: When would you use a broadcast join vs sort-merge join?**
> Broadcast join: when one side fits in memory (< 10MB default,
configurable). Zero shuffle — small table broadcast to every executor; large
table stays local. Sort-merge join: both sides are large — shuffle both by
join key, sort, then merge. Always prefer broadcast if one table is small;
it eliminates the most expensive shuffle.
**Q4: What causes data skew in Spark and how do you fix it?**
> Skew: some join keys appear far more often than others (e.g., `user_id =
NULL` or popular products). One partition gets all those records → one task
This PDF document was created using the online html editor powered by CKEditor.
takes 10× longer. Fix: (1) Salting — add a random suffix to the skewed key,
explode the small side to match; (2) AQE skew join — Spark 3.x automatically
splits skewed partitions; (3) Pre-aggregate skewed keys before joining.
**Q5: What is AQE and what does it do?**
> Adaptive Query Execution (Spark 3.0+) re-optimizes the physical plan at
runtime using actual shuffle statistics. Three capabilities: (1) coalesce
post-shuffle partitions (avoids too many small tasks), (2) convert sortmerge join to broadcast join if one side turns out small, (3) automatically
split skewed partitions. Enable with `spark.sql.adaptive.enabled=true`.
**Q6: Explain the Catalyst optimizer.**
> Catalyst takes your DataFrame or SQL code through 4 phases: analysis
(resolve column names), logical optimization (predicate pushdown, constant
folding, column pruning), physical planning (choose join strategies), and
code generation (Tungsten compiles to JVM bytecode). It automatically makes
your query faster without changing your code.
**Q7: What is the difference between `repartition` and `coalesce`?**
> `repartition(n)` performs a full shuffle, producing n equal-sized
partitions. Use to increase partitions or fix skew. `coalesce(n)` merges
partitions without full shuffle (just combines adjacent partitions). Use to
decrease partitions before writing (avoids small files). Coalesce cannot
increase partition count.
**Q8: When should you cache a DataFrame?**
> Cache when: (1) the DataFrame is used more than once in the same job, (2)
it takes significant time to compute (multiple joins/aggregations), and (3)
it fits in available executor memory. Don't cache DataFrames that are cheap
to recompute or used only once — caching has serialization overhead.
---
# Part 9: SQL Mastery, Query Optimization & Data Normalization
> Essential SQL knowledge every data engineer must deeply understand
---
## 9.1 Database Normalization
This PDF document was created using the online html editor powered by CKEditor.
Normalization eliminates redundancy and ensures data integrity by
structuring data according to formal rules called **Normal Forms**.
### Why Normalization Matters
```mermaid
flowchart LR
 subgraph Unnormalized
 direction TB
 T1["OrderID | Customer | Email | Product | Price | Category
 1 | Alice | alice@co.com | Laptop | 999 | Electronics
 1 | Alice | alice@co.com | Mouse | 25 | Electronics
 2 | Bob | bob@co.com | Desk | 200 | Furniture"]
 end
 subgraph Problems
 P1[Update anomaly:\\nChange Alice's email\\nin every row]
 P2[Insert anomaly:\\nCan't add customer\\nwithout an order]
 P3[Delete anomaly:\\nDelete Bob's order\\nloses customer record]
 end
 Unnormalized --> Problems
```
---
### First Normal Form (1NF)
**Rule:** Every column must contain **atomic (indivisible) values**. No
repeating groups, no arrays.
```sql
-- VIOLATES 1NF: multiple values in one column
OrderID | Customer | Products
1 | Alice | 'Laptop, Mouse, Keyboard' ← Not atomic!
-- SATISFIES 1NF: one product per row
This PDF document was created using the online html editor powered by CKEditor.
OrderID | Customer | Product
1 | Alice | Laptop
1 | Alice | Mouse
1 | Alice | Keyboard
```
---
### Second Normal Form (2NF)
**Rule:** Must be in 1NF, AND every non-key attribute must depend on the
**entire primary key** (not a partial key).
Applies only when the primary key is **composite** (multi-column).
```sql
-- VIOLATES 2NF: composite PK = (OrderID, ProductID)
-- CustomerName depends only on OrderID (partial dependency)
OrderID | ProductID | CustomerName | Quantity | ProductPrice
1 | P1 | Alice | 2 | 999 ← CustomerName
partial dep!
-- SATISFIES 2NF: split into separate tables
-- Orders table: PK = OrderID
OrderID | CustomerName
1 | Alice
-- OrderItems table: PK = (OrderID, ProductID) — full dependency
OrderID | ProductID | Quantity
-- Products table: PK = ProductID
ProductID | ProductPrice
```
---
### Third Normal Form (3NF)
This PDF document was created using the online html editor powered by CKEditor.
**Rule:** Must be in 2NF, AND no non-key attribute should depend on another
non-key attribute (no **transitive dependencies**).
```sql
-- VIOLATES 3NF: ZipCode → City (transitive: OrderID → ZipCode → City)
OrderID | CustomerName | ZipCode | City
1 | Alice | 10001 | New York ← City depends on ZipCode,
not OrderID!
2 | Bob | 90210 | Beverly Hills
-- SATISFIES 3NF: separate Zip code lookup
Orders:
OrderID | CustomerName | ZipCode
1 | Alice | 10001
2 | Bob | 90210
ZipCodes:
ZipCode | City
10001 | New York
90210 | Beverly Hills
```
---
### Boyce-Codd Normal Form (BCNF)
**Rule:** Must be in 3NF, AND for every functional dependency X → Y, X must
be a **superkey** (candidate key).
BCNF is stricter than 3NF — catches edge cases where a non-key column
determines part of the composite key.
```sql
-- VIOLATES BCNF: (Student, Subject) → Teacher, but Teacher → Subject
(Teacher is not a key)
Student | Subject | Teacher
Alice | Math | Dr. Smith ← Dr. Smith always teaches Math (Teacher →
Subject)
This PDF document was created using the online html editor powered by CKEditor.
Bob | Math | Dr. Smith but Teacher is not a key!
Alice | Physics | Dr. Jones
-- SATISFIES BCNF: decompose
TeacherSubject:
Teacher | Subject
Dr. Smith | Math
Dr. Jones | Physics
StudentTeacher:
Student | Teacher
Alice | Dr. Smith
Alice | Dr. Jones
Bob | Dr. Smith
```
---
### Normal Forms Summary
| Normal Form | Requirement | Eliminates |
|---|---|---|
| **1NF** | Atomic values, no repeating groups | Repeating groups, arrays in
cells |
| **2NF** | 1NF + no partial dependencies | Redundancy from composite keys |
| **3NF** | 2NF + no transitive dependencies | Redundancy from non-key
dependencies |
| **BCNF** | 3NF + every determinant is a superkey | Edge cases with
multiple candidate keys |
| **4NF** | BCNF + no multi-valued dependencies | Independent multi-valued
facts in one table |
| **5NF** | 4NF + no join dependencies | Decomposition anomalies |
### When to Normalize vs Denormalize
| Scenario | Recommendation |
|---|---|
| OLTP (transactional) system | Normalize to 3NF — minimize write anomalies,
This PDF document was created using the online html editor powered by CKEditor.
fast inserts/updates |
| OLAP (analytical) system | Denormalize (star schema) — fewer joins, faster
reads |
| Hybrid (HTAP) | Normalize on write side, materialize read-optimized views
|
| High write throughput | 3NF — avoid updating the same data in multiple
places |
| High read throughput | Denormalize — pre-join, pre-aggregate |
---
## 9.2 SQL Window Functions
Window functions are the most powerful SQL tool for analytics. They compute
values **across a set of rows** without collapsing them (unlike GROUP BY).
```sql
-- Syntax
function_name() OVER (
 PARTITION BY column(s) -- Define groups (like GROUP BY but doesn't
collapse)
 ORDER BY column(s) -- Define order within each group
 ROWS/RANGE BETWEEN ... -- Define the window frame
)
```
### Ranking Functions
```sql
-- Employee salary ranking within each department
SELECT
 employee_id,
 department,
 salary,

 -- ROW_NUMBER: unique sequential rank (no ties)
 ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS
row_num,
This PDF document was created using the online html editor powered by CKEditor.

 -- RANK: same rank for ties, then gap (1,1,3,4)
 RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,

 -- DENSE_RANK: same rank for ties, no gap (1,1,2,3)
 DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS
dense_rank,

 -- NTILE: divide into N buckets (useful for percentiles)
 NTILE(4) OVER (PARTITION BY department ORDER BY salary DESC) AS quartile

FROM employees;
```
**Output example:**
```
dept | salary | row_num | rank | dense_rank | quartile
---------|--------|---------|------|------------|--------
Eng | 150000 | 1 | 1 | 1 | 1
Eng | 150000 | 2 | 1 | 1 | 1
Eng | 120000 | 3 | 3 | 2 | 2 ← RANK gaps,
DENSE_RANK doesn't
Eng | 90000 | 4 | 4 | 3 | 3
```
### Aggregate Window Functions
```sql
-- Running total and percentage of total — classic interview question
SELECT
 order_date,
 amount,

 -- Running sum (cumulative)
 SUM(amount) OVER (ORDER BY order_date) AS running_total,

 -- Running average
 AVG(amount) OVER (ORDER BY order_date
This PDF document was created using the online html editor powered by CKEditor.
 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS
running_avg,

 -- Percentage of total (no PARTITION BY = entire result set)
 ROUND(amount / SUM(amount) OVER () * 100, 2) AS pct_of_total,

 -- Percentage of department total
 ROUND(amount / SUM(amount) OVER (PARTITION BY department) * 100, 2) AS
pct_of_dept,

 -- 7-day moving average
 AVG(amount) OVER (
 ORDER BY order_date
 ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
 ) AS moving_avg_7d

FROM orders;
```
### Lead/Lag Functions (Time-Series Analysis)
```sql
-- Compare current period to previous/next periods
SELECT
 order_date,
 revenue,

 -- Previous day's revenue
 LAG(revenue, 1) OVER (ORDER BY order_date) AS prev_day_revenue,

 -- Next day's revenue
 LEAD(revenue, 1) OVER (ORDER BY order_date) AS next_day_revenue,

 -- Day-over-day change
 revenue - LAG(revenue, 1) OVER (ORDER BY order_date) AS dod_change,

 -- Year-over-year (365 rows back)
 LAG(revenue, 365) OVER (ORDER BY order_date) AS same_day_last_year,
This PDF document was created using the online html editor powered by CKEditor.

 -- First and last values in the window
 FIRST_VALUE(revenue) OVER (PARTITION BY DATE_TRUNC('month', order_date)
 ORDER BY order_date) AS month_start_revenue,
 LAST_VALUE(revenue) OVER (PARTITION BY DATE_TRUNC('month', order_date)
 ORDER BY order_date
ROWS BETWEEN UNBOUNDED PRECEDING AND
UNBOUNDED FOLLOWING
 ) AS month_end_revenue
FROM daily_revenue;
```
### Window Frame Clauses
```sql
-- ROWS vs RANGE
ROWS BETWEEN 3 PRECEDING AND CURRENT ROW -- Physical rows: exactly 3 rows
before current
RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW -- Logical range:
values within 7 days
-- Named frames
UNBOUNDED PRECEDING AND CURRENT ROW -- From start of partition to
current (cumulative)
UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING -- Entire partition (for
totals)
1 PRECEDING AND 1 FOLLOWING -- Centered moving average
(current ± 1)
```
---
## 9.3 Common Table Expressions (CTEs)
CTEs make complex queries readable and maintainable. Some are materialized,
acting as performance checkpoints.
```sql
This PDF document was created using the online html editor powered by CKEditor.
-- Pattern 1: Multi-step calculation with CTEs
WITH
-- Step 1: Get active members
active_members AS (
 SELECT member_id, plan_type
 FROM members
 WHERE status = 'ACTIVE' AND effective_date <= CURRENT_DATE
),
-- Step 2: Count their claims
member_claim_counts AS (
 SELECT m.member_id, m.plan_type, COUNT(c.claim_id) AS claim_count
 FROM active_members m
 LEFT JOIN claims c ON m.member_id = c.member_id
 AND c.service_date >= DATE_TRUNC('year', CURRENT_DATE)
 GROUP BY m.member_id, m.plan_type
),
-- Step 3: Assign risk tiers
risk_tiered AS (
 SELECT *,
 CASE
 WHEN claim_count >= 20 THEN 'High'
 WHEN claim_count >= 5 THEN 'Medium'
 ELSE 'Low'
 END AS risk_tier
 FROM member_claim_counts
)
SELECT plan_type, risk_tier, COUNT(*) AS member_count
FROM risk_tiered
GROUP BY plan_type, risk_tier
ORDER BY plan_type, risk_tier;
```
### Recursive CTEs — Hierarchical Queries
```sql
This PDF document was created using the online html editor powered by CKEditor.
-- Org chart: find all reports under a manager
WITH RECURSIVE org_tree AS (
 -- Anchor: start with the top-level manager
 SELECT employee_id, name, manager_id, 1 AS level
 FROM employees
 WHERE manager_id IS NULL -- CEO

 UNION ALL

 -- Recursive: add direct reports
 SELECT e.employee_id, e.name, e.manager_id, ot.level + 1
 FROM employees e
 INNER JOIN org_tree ot ON e.manager_id = ot.employee_id
)
SELECT level, employee_id, name, manager_id
FROM org_tree
ORDER BY level, name;
-- Real use: BOM (Bill of Materials), category hierarchies, claim
authorization chains
```
---
## 9.4 Advanced SQL Patterns for Data Engineers
### SCD Type 2 in SQL (without dbt)
```sql
-- Find members whose plan changed (for SCD Type 2 processing)
WITH current_snapshot AS (
 SELECT member_id, plan_type,
 MD5(CONCAT(plan_type, coverage_level)) AS row_hash
 FROM staging_members
),
existing_dim AS (
 SELECT member_id, plan_type,
 MD5(CONCAT(plan_type, coverage_level)) AS row_hash
This PDF document was created using the online html editor powered by CKEditor.
 FROM dim_member
 WHERE is_current = TRUE
)
-- Records to CLOSE (plan changed)
SELECT cs.member_id, 'CLOSE' AS action
FROM existing_dim ed
JOIN current_snapshot cs ON ed.member_id = cs.member_id
WHERE ed.row_hash != cs.row_hash -- Hash differs → something changed
UNION ALL
-- Records to INSERT (new version or new member)
SELECT cs.member_id, 'INSERT' AS action
FROM current_snapshot cs
LEFT JOIN existing_dim ed ON cs.member_id = ed.member_id
WHERE ed.member_id IS NULL OR ed.row_hash != cs.row_hash;
```
### Deduplication (keep latest)
```sql
-- Pattern 1: ROW_NUMBER deduplication (most common)
WITH deduped AS (
 SELECT *,
 ROW_NUMBER() OVER (
 PARTITION BY member_id
 ORDER BY updated_at DESC, claim_id DESC
 ) AS rn
 FROM raw_claims
)
SELECT * FROM deduped WHERE rn = 1;
-- Pattern 2: QUALIFY (Snowflake, BigQuery syntax)
SELECT * FROM raw_claims
QUALIFY ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY updated_at DESC)
= 1;
```
This PDF document was created using the online html editor powered by CKEditor.
### Gap and Island Detection
```sql
-- Find contiguous date ranges where a member was enrolled (no gaps)
WITH date_groups AS (
 SELECT
 member_id,
 effective_date,
 termination_date,
 -- If gap > 1 day from previous row, it's a new island
 SUM(CASE WHEN effective_date > LAG(termination_date) OVER
 (PARTITION BY member_id ORDER BY effective_date) + INTERVAL
'1 day'
 THEN 1 ELSE 0 END
 ) OVER (PARTITION BY member_id ORDER BY effective_date) AS island_id
 FROM enrollments
)
SELECT member_id, island_id,
 MIN(effective_date) AS continuous_start,
 MAX(termination_date) AS continuous_end
FROM date_groups
GROUP BY member_id, island_id;
```
### Pivot/Unpivot
```sql
-- Pivot: rows to columns (monthly revenue per product)
SELECT
 product_id,
 SUM(CASE WHEN month = 1 THEN revenue END) AS jan_revenue,
 SUM(CASE WHEN month = 2 THEN revenue END) AS feb_revenue,
 SUM(CASE WHEN month = 3 THEN revenue END) AS mar_revenue
FROM monthly_sales
GROUP BY product_id;
-- In Snowflake/BigQuery:
SELECT * FROM monthly_sales
This PDF document was created using the online html editor powered by CKEditor.
PIVOT (SUM(revenue) FOR month IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12));
```
---
## 9.5 Query Optimization & EXPLAIN Plans
Understanding how your query actually executes is critical for performance
tuning.
### Reading an EXPLAIN Plan
```sql
-- PostgreSQL
EXPLAIN ANALYZE SELECT m.name, COUNT(c.claim_id)
FROM members m
JOIN claims c ON m.member_id = c.member_id
WHERE m.state = 'TX'
GROUP BY m.name;
```
**Output to interpret:**
```
HashAggregate (cost=15234.50..15234.75 rows=25 width=40) (actual time=892ms)
 -> Hash Join (cost=1234.00..14234.00 rows=200000 width=36) (actual
time=45ms)
 Hash Cond: (c.member_id = m.member_id)
 -> Seq Scan on claims c (cost=0..8000.00 rows=500000) ← Full scan!
No index
 -> Hash (cost=900.00..900.00 rows=26720)
 -> Seq Scan on members m (cost=0..900.00 rows=26720)
 Filter: (state = 'TX') ← Filter applied after scan
-- RED FLAGS in EXPLAIN:
-- "Seq Scan" on large table: no index being used
-- "Nested Loop" with large outer table: O(n²) — often means missing index
-- Very high "cost" values: expensive operation
-- "actual time" >> "estimated time": statistics are stale → ANALYZE table
This PDF document was created using the online html editor powered by CKEditor.
```
### Index Types in PostgreSQL
| Index Type | Best For | Example |
|---|---|---|
| **B-tree** (default) | Equality, range queries, sorts | `member_id = 123`,
`amount > 100` |
| **GIN** | Full-text search, arrays, JSONB | `tags @> '{urgent}'`, `data-
>>'status'` |
| **GiST** | Geometric, range types, full-text | PostGIS spatial queries |
| **BRIN** | Very large tables, naturally ordered | `created_at` on appendonly logs |
| **Hash** | Equality only | `status = 'ACTIVE'` (rarely better than B-tree)
|
```sql
-- Create targeted indexes
CREATE INDEX idx_claims_member_date
ON claims(member_id, service_date)
WHERE claim_status = 'PAID'; -- Partial index: only on paid claims
-- Covering index: includes all columns needed by the query
CREATE INDEX idx_claims_covering
ON claims(member_id)
INCLUDE (claim_id, amount, service_date); -- No table lookup needed
-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read
FROM pg_stat_user_indexes
WHERE idx_scan = 0; -- Find unused indexes!
```
### Key Optimization Techniques
**1. Predicate Pushdown — always filter early**
```sql
-- SLOW: join first, filter after (moves more data)
This PDF document was created using the online html editor powered by CKEditor.
SELECT * FROM fact_claims f JOIN dim_member m ON f.member_id = m.member_id
WHERE m.state = 'TX';
-- FAST: filter before join (modern optimizers do this, but be explicit)
SELECT * FROM fact_claims f
JOIN (SELECT * FROM dim_member WHERE state = 'TX') m ON f.member_id =
m.member_id;
```
**2. Avoid functions on indexed columns in WHERE**
```sql
-- SLOW: function prevents index use
WHERE YEAR(service_date) = 2024
WHERE UPPER(member_name) = 'ALICE'
-- FAST: range query or case-insensitive collation
WHERE service_date BETWEEN '2024-01-01' AND '2024-12-31'
WHERE member_name ILIKE 'alice' -- PostgreSQL ILIKE can use functional
index
```
**3. Materialized Views — pre-compute expensive aggregations**
```sql
CREATE MATERIALIZED VIEW mv_monthly_claims AS
SELECT
 DATE_TRUNC('month', service_date) AS service_month,
 npi_provider_id,
 COUNT(claim_id) AS claim_count,
 SUM(allowed_amount) AS total_allowed
FROM claims
GROUP BY 1, 2;
CREATE INDEX ON mv_monthly_claims(service_month, npi_provider_id);
-- Refresh strategy
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_claims; -- No lock on
reads!
This PDF document was created using the online html editor powered by CKEditor.
-- Query hits materialized view instead of claims table
SELECT * FROM mv_monthly_claims WHERE service_month = '2024-01-01';
```
**4. Statistics & ANALYZE**
```sql
-- PostgreSQL: update statistics for optimizer
ANALYZE claims; -- Samples table, updates pg_statistic
-- Check if statistics are stale
SELECT relname, n_live_tup, last_analyze, last_autoanalyze
FROM pg_stat_user_tables
WHERE relname = 'claims';
```
---
## 9.6 SQL for Data Engineering — Key Patterns
### Incremental Processing Pattern
```sql
-- Find only records modified since last run
-- Watermark table stores last processed timestamp
SELECT c.*
FROM claims c
WHERE c.updated_at > (SELECT MAX(last_processed_at) FROM watermark WHERE
table_name = 'claims')
 AND c.updated_at <= CURRENT_TIMESTAMP;
-- After processing, update watermark
UPDATE watermark SET last_processed_at = CURRENT_TIMESTAMP WHERE table_name
= 'claims';
```
### Slowly Changing Dimensions in SQL
```sql
This PDF document was created using the online html editor powered by CKEditor.
-- MERGE pattern (Snowflake, SQL Server, BigQuery)
MERGE INTO dim_member AS target
USING staging_member AS source
ON target.member_id = source.member_id AND target.is_current = TRUE
WHEN MATCHED AND (
 target.plan_type != source.plan_type OR
 target.coverage_lvl != source.coverage_lvl
) THEN
 -- Close old record
 UPDATE SET
 target.is_current = FALSE,
 target.end_date = CURRENT_DATE - 1
WHEN NOT MATCHED THEN
 -- Insert new/updated record
 INSERT (member_id, plan_type, coverage_lvl, start_date, end_date,
is_current)
 VALUES (source.member_id, source.plan_type, source.coverage_lvl,
CURRENT_DATE, '9999-12-31', TRUE);
```
### Assert/Test Queries for Data Quality
```sql
-- Count non-null check
SELECT COUNT(*) AS null_member_ids FROM claims WHERE member_id IS NULL;
-- Referential integrity check
SELECT COUNT(*) AS orphan_claims
FROM claims c
LEFT JOIN members m ON c.member_id = m.member_id
WHERE m.member_id IS NULL;
-- Duplicate primary key check
SELECT claim_id, COUNT(*) FROM claims GROUP BY claim_id HAVING COUNT(*) > 1;
-- Date range sanity
This PDF document was created using the online html editor powered by CKEditor.
SELECT COUNT(*) FROM claims WHERE service_date > CURRENT_DATE;
-- All of these should return 0 — embed in your pipeline as data quality
gates
```
---
## 9.7 Query Optimization in Distributed Databases
### Snowflake Optimization
```sql
-- 1. Cluster keys: physical co-location of related rows
ALTER TABLE claims CLUSTER BY (service_date, member_id);
-- 2. Check clustering depth (lower = better)
SELECT SYSTEM$CLUSTERING_INFORMATION('claims', '(service_date, member_id)');
-- 3. Result caching (free!): same query = instant result
SELECT COUNT(*) FROM claims WHERE service_date = '2024-01-15';
-- Second run: 0ms — returned from result cache (24hr TTL)
-- 4. Query profile: find bottlenecks
-- Snowflake UI → Query History → Query Profile → look for:
-- TableScan: check partition pruning ratio
-- Sort: expensive if no clustering key
-- Remote disk IO: data spilled to disk → need larger warehouse
```
### BigQuery Optimization
```sql
-- 1. Partition pruning: ALWAYS filter on partition column
SELECT * FROM `project.dataset.claims`
WHERE DATE(service_date) = '2024-01-15' -- Reads only 1 day's data
-- vs
WHERE service_date = '2024-01-15' -- May read all partitions if not
This PDF document was created using the online html editor powered by CKEditor.
DATE type
-- 2. Clustering (columnar sorting within partitions)
CREATE TABLE claims
PARTITION BY DATE(service_date)
CLUSTER BY member_id, npi_provider_id -- Sorts within each partition
AS SELECT ...;
-- 3. Use approximation functions when exact count not needed
SELECT APPROX_COUNT_DISTINCT(member_id) FROM claims; -- Much faster than
COUNT(DISTINCT)
-- 4. Check bytes billed BEFORE running
SELECT ... FROM `project.dataset.claims` -- Check estimated bytes in
BigQuery UI
```
---
## 9.8 Cross Questions — SQL & Normalization
**Q1: What is the difference between RANK, DENSE_RANK, and ROW_NUMBER?**
> All are ranking window functions. `ROW_NUMBER()` gives unique sequential
numbers even for ties. `RANK()` gives tied rows the same rank but skips the
next rank (1,1,3). `DENSE_RANK()` gives tied rows the same rank without
skipping (1,1,2). Use `ROW_NUMBER` for deduplication (need exactly one row
per group), `RANK`/`DENSE_RANK` for leaderboards where ties are meaningful.
**Q2: What is a partial dependency (2NF violation)?**
> When a non-key attribute depends on only part of a composite primary key,
not the whole key. Example: in a table with PK=(OrderID, ProductID), if
CustomerName depends only on OrderID, it's a partial dependency. Fix: move
CustomerName to a separate Orders table with PK=OrderID.
**Q3: What is a transitive dependency (3NF violation)?**
> When a non-key attribute depends on another non-key attribute, creating an
indirect dependency on the primary key. Example: OrderID → ZipCode → City.
City depends on ZipCode (non-key), not directly on OrderID. Fix: extract
This PDF document was created using the online html editor powered by CKEditor.
ZipCode → City to a separate table.
**Q4: How would you find the top-N records per group?**
> Use ROW_NUMBER() window function: `SELECT * FROM (SELECT *, ROW_NUMBER()
OVER (PARTITION BY group_col ORDER BY value_col DESC) AS rn FROM table)
WHERE rn <= N`. This is the canonical pattern — more efficient than
correlated subqueries or self-joins.
**Q5: What is a covering index?**
> An index that contains all columns needed by a query, so the database can
answer the query entirely from the index without accessing the table (heap).
Specified with INCLUDE clause in PostgreSQL. Eliminates "table lookups"
visible in EXPLAIN plans as extra steps after index scan.
**Q6: Why does using a function on an indexed column in WHERE prevent index
usage?**
> The index stores raw column values. When you write `WHERE YEAR(date_col) =
2024`, the database must evaluate YEAR() for every row, which can't be done
using the index directly. Fix: use range predicates (`BETWEEN '2024-01-01'
AND '2024-12-31'`) or create a functional index on the expression.
**Q7: When would you NOT normalize a table?**
> When the system is read-heavy (OLAP/analytics). 3NF normalization means
more joins, which are expensive at scale. Data warehouses intentionally
denormalize (star schema) to pre-join dimension data — trades write
redundancy for read speed. Also, in NoSQL systems, normalization is often
impossible; you model for access patterns instead.
**Q8: What is a materialized view and when do you use it?**
> A pre-computed, physically stored result of a query. Unlike regular views
(re-execute on access), materialized views store results on disk. Use when:
the underlying query is expensive (multi-table joins, aggregations), data
doesn't change every second, and you can tolerate slightly stale data. Must
be refreshed on a schedule or on-demand.
---
# Part 10: File Formats, Open Table Formats & Modern Python Data Tools
> Deep dive into Parquet, Delta Lake, Iceberg, Hudi internals + Polars and
This PDF document was created using the online html editor powered by CKEditor.
DuckDB
---
## 10.1 File Formats Comparison
| Format | Type | Columnar | Compression | Schema | ACID | Best For |
|---|---|---|---|---|---|---|
| CSV | Row-oriented | No | None | None | No | Simple interchange, small
data |
| JSON | Row-oriented | No | Poor | None | No | Semi-structured, APIs |
| Avro | Row-oriented | No | Good | Embedded | No | Schema evolution in
Kafka, Hadoop |
| ORC | Columnar | Yes | Excellent | Embedded | No | Hive, Spark (Hadoop
ecosystem) |
| **Parquet** | Columnar | Yes | Excellent | Embedded | No | Standard
analytics format |
| **Delta Lake** | Table format on Parquet | Yes | Excellent | Enforced |
Yes | Databricks, Fabric, general lakehouse |
| **Iceberg** | Table format on Parquet/ORC | Yes | Excellent | Enforced |
Yes | Multi-engine (Spark, Trino, Flink) |
| **Hudi** | Table format on Parquet/ORC | Yes | Excellent | Enforced | Yes
| Near-real-time upserts |
---
## 10.2 Apache Parquet — Internal Structure
Parquet is the standard columnar format. Understanding its internals
explains WHY it's so fast for analytical queries.
```mermaid
graph TD
 subgraph Parquet File
 FH[File Header\\n4 bytes: PAR1]
 subgraph Row Group 1\\n~128MB of rows
 CG1[Column Chunk\\ncol: member_id]
 CG2[Column Chunk\\ncol: claim_amount]
This PDF document was created using the online html editor powered by CKEditor.
 CG3[Column Chunk\\ncol: service_date]
 end
 subgraph Row Group 2
 CG4[Column Chunk\\ncol: member_id]
 CG5[Column Chunk\\ncol: claim_amount]
 CG6[Column Chunk\\ncol: service_date]
 end
 FM[File Footer\\nSchema\\nRow group metadata\\nColumn
statistics\\nmin/max per column per row group]
 FF[4 bytes: PAR1]
 end
```
### Column Chunks → Pages → Encodings
```
Column Chunk (one column in one row group)
│
├── Dictionary Page (optional)
│ └── Unique values: [Alice, Bob, Charlie, ...]
│
├── Data Page 1
│ ├── Encoding: DICTIONARY (stores indices, not values)
│ ├── Values: [0, 1, 0, 2, 1, 0, ...] ← indices into dictionary
│ └── Definition/Repetition levels (for nested data)
│
├── Data Page 2
│ └── ...
│
└── Column Statistics
 ├── min: Alice
 ├── max: Zara
 └── null_count: 0
```
### Key Encoding Algorithms
| Encoding | How It Works | Best For |
This PDF document was created using the online html editor powered by CKEditor.
|---|---|---|
| **Dictionary encoding** | Store unique values once, use indices everywhere
| Low-cardinality columns (status, state, gender) |
| **RLE (Run-Length Encoding)** | `AAABBBBBCCCC` → `3A5B4C` | Sorted or
repeated values |
| **Bit-packing** | Pack small integers into minimal bits | Integer columns
with small values |
| **Delta encoding** | Store differences: `[100,101,102]` → `[100,+1,+1]` |
Monotonically increasing IDs, timestamps |
| **Plain** | Raw values, no encoding | High-cardinality columns, already
compressed |
### Why Parquet is Fast for Analytics
**1. Projection pushdown:** Only read the columns your query needs.
```python
# Query only needs 2 of 50 columns
# Parquet reads ONLY those 2 column chunks — skips 48 completely!
df = spark.read.parquet("s3://bucket/claims/").select("member_id", "amount")
```
**2. Predicate pushdown with column statistics:**
```python
# Parquet footer stores min/max per column per row group
# Filter: amount > 10000
# Spark reads footer → skips entire row groups where max(amount) <= 10000
df.filter(col("amount") > 10000)
```
**3. Bloom filters (optional, Parquet 2.x):**
```python
# Add bloom filter at write time for high-cardinality equality filters
df.write \\
 .option("parquet.bloom.filter.enabled#member_id", "true") \\
 .option("parquet.bloom.filter.expected.ndv#member_id", "1000000") \\
 .parquet("s3://bucket/claims/")
# Filter: member_id = 'M123456'
# Bloom filter answers: "definitely not here" or "maybe here"
This PDF document was created using the online html editor powered by CKEditor.
# Skips row groups that definitely don't contain M123456
```
---
## 10.3 Delta Lake Internals — The Transaction Log
Delta Lake = Parquet files + `_delta_log/` transaction log = ACID
transactions on object storage.
```
s3://bucket/silver/claims/
├── _delta_log/
│ ├── 00000000000000000000.json ← checkpoint 0: initial table creation
│ ├── 00000000000000000001.json ← add files, version 1
│ ├── 00000000000000000002.json ← schema change, version 2
│ ├── ...
│ └── 00000000000000000010.checkpoint.parquet ← snapshot at version 10
(every 10 commits)
├── part-00000-abc123.snappy.parquet
├── part-00001-def456.snappy.parquet
└── ...
```
### What a Transaction Log Entry Contains
```json
// 00000000000000000001.json — a typical commit
{
 "commitInfo": {
 "timestamp": 1710000000000,
 "operation": "WRITE",
 "operationParameters": {"mode": "Append"},
 "readVersion": 0
 }
}
{
 "add": {
This PDF document was created using the online html editor powered by CKEditor.
 "path": "part-00000-abc123.snappy.parquet",
 "size": 104857600,
 "modificationTime": 1710000000000,
 "dataChange": true,
 "stats": "{\\"numRecords\\":500000,\\"minValues\\":
{\\"member_id\\":\\"M0001\\"},\\"maxValues\\":
{\\"member_id\\":\\"M9999\\"},\\"nullCount\\":{}}"
 }
}
```
### Delta Lake Operations
```python
from delta.tables import DeltaTable
# Time travel — read historical version
df_v5 = spark.read.format("delta").option("versionAsOf",
5).load("s3://bucket/claims/")
df_yesterday = spark.read.format("delta").option("timestampAsOf", "2024-03-
14").load(...)
# Audit history
delta_table = DeltaTable.forPath(spark, "s3://bucket/claims/")
delta_table.history().show()
# MERGE (upsert) — the killer feature
delta_table.alias("target").merge(
 source_df.alias("source"),
 "target.claim_id = source.claim_id"
).whenMatchedUpdateAll() \\
.whenNotMatchedInsertAll() \\
.execute()
# Vacuum — remove files older than retention period
delta_table.vacuum(168) # Remove files older than 7 days
# Optimize — compact small files + Z-order
This PDF document was created using the online html editor powered by CKEditor.
delta_table.optimize().executeZOrderBy("member_id", "service_date")
```
### Z-Ordering vs Clustering
| Method | How | Best For |
|---|---|---|
| **Z-Ordering** | Interleave multiple columns in space-filling curve; colocate correlated values | Multi-column filter predicates |
| **Liquid Clustering (Delta 3.x)** | Automatic, incremental clustering;
replaces Z-order | Large, frequently updated tables |
| **CLUSTER BY (Iceberg/BigQuery)** | Sort-based clustering | Single or few
columns |
```python
# Z-Order: optimal for queries filtering on BOTH member_id AND service_date
delta_table.optimize().executeZOrderBy("member_id", "service_date")
# Queries like: WHERE member_id = 'M123' AND service_date > '2024-01-01'
skip ~90% of files
```
---
## 10.4 Apache Iceberg — Metadata Architecture
Iceberg is the most open table format — designed for multi-engine access
(Spark, Trino, Flink, Hive).
```mermaid
graph TD
 subgraph Iceberg Table Metadata Stack
 CAT[Catalog\\nHive Metastore / AWS Glue / Nessie / REST\\nStores:
table name → current metadata pointer]
 META[Metadata File\\ntable-metadata.json\\nContains: schema,
partition spec, snapshots list, properties]
 ML[Manifest List\\nsnap-12345.avro\\nList of all manifest files for
this snapshot\\nStats per manifest: min/max partition values]
 MF1[Manifest File 1\\nmanifest-aaa.avro\\nList of data files in this
This PDF document was created using the online html editor powered by CKEditor.
manifest\\nStats: min/max values per column]
 MF2[Manifest File 2\\nmanifest-bbb.avro]
 DF1[data-file-001.parquet]
 DF2[data-file-002.parquet]
 DF3[data-file-003.parquet]
 end
 CAT --> META
 META --> ML
 ML --> MF1
 ML --> MF2
 MF1 --> DF1
 MF1 --> DF2
 MF2 --> DF3
```
### Key Iceberg Features
**Hidden Partitioning:**
```sql
-- Iceberg handles partition transforms transparently
CREATE TABLE claims (
 claim_id BIGINT,
 member_id STRING,
 service_date TIMESTAMP,
 amount DECIMAL(12,2)
)
USING iceberg
PARTITIONED BY (months(service_date), bucket(100, member_id))
-- No need to add service_month column! Iceberg derives it automatically.
-- Query: just use the raw column — Iceberg prunes partitions automatically
SELECT * FROM claims WHERE service_date > '2024-01-01'
-- Iceberg checks manifest list → skips manifests for earlier months
```
**Partition Evolution:**
```sql
This PDF document was created using the online html editor powered by CKEditor.
-- Change partition scheme WITHOUT rewriting existing data!
ALTER TABLE claims REPLACE PARTITION FIELD months(service_date) WITH
days(service_date);
-- Old data stays in month partitions
-- New data goes into day partitions
-- Both are queryable together seamlessly
```
**Row-Level Deletes — Copy-on-Write (CoW) vs Merge-on-Read (MoR):**
| Approach | DELETE behavior | Read cost | Write cost | Best for |
|---|---|---|---|---|
| **Copy-on-Write (CoW)** | Rewrites entire file with row removed | Fast (no
delete files to merge) | Slow (full file rewrite) | Read-heavy workloads |
| **Merge-on-Read (MoR)** | Writes small delete file pointing to rows |
Slightly slower (merge at read) | Fast (just a delete file) | Write-heavy,
CDC patterns |
```python
# Iceberg DELETE (MoR by default)
spark.sql("DELETE FROM claims WHERE member_id = 'M123456' AND service_date <
'2020-01-01'")
# Writes a delete file — does NOT rewrite the data file
# Later: compaction merges delete files into clean data files
spark.sql("CALL spark_catalog.system.rewrite_data_files('db.claims')")
```
**Time Travel:**
```sql
-- By snapshot ID
SELECT * FROM claims VERSION AS OF 12345678;
-- By timestamp
SELECT * FROM claims TIMESTAMP AS OF '2024-03-01 00:00:00';
-- Compare two versions
SELECT * FROM claims VERSION AS OF 20
This PDF document was created using the online html editor powered by CKEditor.
MINUS
SELECT * FROM claims VERSION AS OF 15;
```
---
## 10.5 Apache Hudi — Streaming Lakehouse
Hudi (Hadoop Upserts Deletes and Incrementals) excels at near-real-time CDC
ingestion.
```mermaid
flowchart LR
 subgraph Hudi Table Types
 COW[Copy-on-Write\\nOn write: merge + rewrite parquet\\nOn read:
plain parquet scan\\nLower read latency\\nHigher write latency]
 MOR[Merge-on-Read\\nOn write: append delta log\\nOn read: merge base
+ delta log\\nLower write latency\\nHigher read latency]
 end

 DB[(OLTP DB)] -->|Debezium CDC| K[Kafka]
 K -->|Hudi DeltaStreamer| COW
 K -->|Hudi DeltaStreamer| MOR
```
### Hudi Timeline
Every Hudi table has a timeline — an ordered log of all operations:
```
Hudi Timeline:
t1: COMMIT (initial bulk load)
t2: COMMIT (incremental CDC batch)
t3: COMPACTION (MoR: merge base + delta into clean parquet)
t4: COMMIT
t5: CLEAN (remove old file versions)
t6: ROLLBACK (failed commit)
```
This PDF document was created using the online html editor powered by CKEditor.
### When to Use Each Format
| Criteria | Delta Lake | Iceberg | Hudi |
|---|---|---|---|
| **Primary ecosystem** | Databricks / Microsoft Fabric | Multi-engine
(Trino, Flink, Spark) | Kafka CDC / streaming upserts |
| **Multi-engine support** | Moderate (Unity Catalog, OSS) | Excellent
(universal) | Good (Spark, Flink) |
| **Streaming upserts** | MERGE statement | MERGE statement | Native
(DeltaStreamer) |
| **Partition evolution** | Limited | Excellent | Moderate |
| **Time travel** | Yes | Yes | Yes |
| **Who uses it** | Databricks, MS, many enterprises | Netflix, Apple,
Airbnb | Uber, Hulu, Robinhood |
---
## 10.6 Polars — The Modern Alternative to Pandas
Polars is a DataFrame library written in Rust, exposed via Python. It's
dramatically faster than Pandas for most operations.
```mermaid
graph LR
 subgraph Why Polars is Faster
 A[Written in Rust\\nZero-copy memory\\nNo GIL]
 B[Uses Apache Arrow\\nColumnar memory layout]
 C[Lazy evaluation\\nQuery optimizer\\nlike Spark]
 D[Native parallelism\\nAll CPUs automatically]
 end
```
### Polars vs Pandas Comparison
| Feature | Pandas | Polars |
|---|---|---|
| Evaluation | Eager | Lazy (optional) |
| Memory | Row-based | Columnar (Arrow) |
This PDF document was created using the online html editor powered by CKEditor.
| Null handling | NaN (confusing) | Proper null |
| String type | object (slow) | Categorical / Utf8 (fast) |
| Parallelism | Single thread (mostly) | Multi-threaded automatically |
| Speed (typical) | Baseline | 5–20× faster |
| Out-of-core | No | Yes (streaming mode) |
| Max practical size | ~10GB RAM limit | ~100GB+ with streaming |
### Polars API
```python
import polars as pl
# Read data
df = pl.read_parquet("claims.parquet")
df_lazy = pl.scan_parquet("s3://bucket/claims/**/*.parquet") # Lazy — no
data loaded yet
# Basic operations — similar to Pandas but different syntax
result = (
 df_lazy
 .filter(pl.col("amount") > 100)
 .filter(pl.col("service_date") >= pl.date(2024, 1, 1))
 .with_columns([
 (pl.col("amount") * 1.1).alias("amount_with_markup"),
 pl.col("member_id").str.to_uppercase().alias("member_id_upper"),
 pl.col("service_date").dt.year().alias("service_year"),
 ])
 .group_by(["member_id", "service_year"])
 .agg([
 pl.col("amount").sum().alias("total_amount"),
 pl.col("claim_id").count().alias("claim_count"),
 pl.col("amount").mean().alias("avg_amount"),
 ])
 .sort("total_amount", descending=True)
 .collect() # Trigger execution with optimizer applied
)
# Expressions are composable and can be parallelized
This PDF document was created using the online html editor powered by CKEditor.
# Multiple group_by aggregations run in parallel automatically!
```
### Polars Lazy API — Query Optimization
```python
# Scan only — nothing read from disk yet
q = pl.scan_parquet("s3://bucket/large_dataset/**.parquet")
# Build query plan
q = q.filter(pl.col("year") == 2024) # Pushes predicate to file
scan
q = q.select(["member_id", "amount", "date"]) # Column pruning
# Inspect optimized plan
print(q.explain(optimized=True))
# → Shows: projection pushdown, predicate pushdown, parallel reads
# Execute
result = q.collect()
# Streaming mode — process larger-than-memory datasets
result = q.collect(streaming=True) # Processes in chunks
```
### When to Use Polars
| Use Polars | Use Pandas | Use PySpark |
|---|---|---|
| Single machine, < 100GB | Small datasets < 1GB, ecosystem compatibility |
> 100GB, distributed compute |
| Performance critical ETL | Extensive library ecosystem needed | Clusterscale batch jobs |
| Data engineering scripts | Machine learning (sklearn, etc.) | Streaming +
batch in one system |
| Replacing slow Pandas jobs | Jupyter exploration | Production lakehouse
pipelines |
This PDF document was created using the online html editor powered by CKEditor.
---
## 10.7 DuckDB — Analytical Queries on Local Files
DuckDB is an in-process OLAP database — like SQLite for analytics. No server
needed.
### What Makes DuckDB Special
```mermaid
graph LR
 subgraph DuckDB Architecture
 A[Vectorized execution\\nProcesses columns in\\nbatches of ~2048
rows]
 B[Columnar storage\\nNative .duckdb file format]
 C[Query Parquet/CSV/JSON\\ndirectly — no import]
 D[Parallel query\\nAll cores automatically]
 E[Python/R/Java API\\nOr CLI]
 end
```
### DuckDB in Practice
```python
import duckdb
# Direct query on Parquet files (no loading!)
result = duckdb.sql("""
 SELECT
 strftime(service_date, '%Y-%m') AS service_month,
 COUNT(DISTINCT member_id) AS unique_members,
 SUM(amount) AS total_claims,
 AVG(amount) AS avg_claim
 FROM read_parquet('s3://bucket/claims/year=2024/**/*.parquet')
 WHERE claim_status = 'PAID'
 GROUP BY 1
 ORDER BY 1
""").df() # Returns pandas DataFrame
This PDF document was created using the online html editor powered by CKEditor.
# Query Delta Lake files
result = duckdb.sql("""
 SELECT * FROM delta_scan('s3://bucket/silver_claims/')
 WHERE service_date > '2024-01-01'
""")
# Query from a Pandas DataFrame
import pandas as pd
df_pandas = pd.read_csv("members.csv")
result = duckdb.sql("SELECT * FROM df_pandas WHERE state = 'TX'").df()
# Persistent DuckDB database
con = duckdb.connect("analytics.duckdb")
con.execute("CREATE TABLE claims AS SELECT * FROM
read_parquet('claims.parquet')")
con.execute("CREATE INDEX idx_claims_member ON claims(member_id)")
```
### DuckDB vs Other Tools
| Tool | Best For | Not Good For |
|---|---|---|
| **DuckDB** | Fast local analytics, Parquet/Delta scanning, data science |
Multi-user, distributed, writes at scale |
| **Pandas** | Small data, ML preprocessing, existing code | Large data,
performance |
| **Polars** | Fast in-process ETL, streaming large files | SQL interface
preference |
| **Spark** | Cluster-scale distributed processing | Small data, latencysensitive |
| **Snowflake/BigQuery** | Cloud DWH with sharing, governance | Local dev,
cost-sensitive exploration |
### DuckDB for Data Engineering
```python
This PDF document was created using the online html editor powered by CKEditor.
# Pattern: use DuckDB for fast local development and testing
# Then deploy the same SQL logic to Snowflake/BigQuery in production
# Local development: test SQL logic on sample data
con = duckdb.connect()
result = con.execute("""
 WITH member_claims AS (
 SELECT member_id, COUNT(*) as claim_count, SUM(amount) as total
 FROM read_parquet('sample_claims_1000.parquet')
 GROUP BY member_id
 )
 SELECT
 CASE WHEN claim_count >= 10 THEN 'High'
 WHEN claim_count >= 3 THEN 'Med'
 ELSE 'Low' END AS tier,
 COUNT(*) as member_count
 FROM member_claims
 GROUP BY tier
""").df()
# Same SQL works in Snowflake with minimal changes
# Just change read_parquet('...') to FROM claims_table
```
---
## 10.8 Cross Questions — File Formats & Modern Tools
**Q1: Why is Parquet faster than CSV for analytics?**
> Parquet is columnar — queries only read columns they need (projection
pushdown). It also stores min/max statistics per column per row group,
allowing entire row groups to be skipped if the filter doesn't match
(predicate pushdown). Additionally, columns compress much better than rows
(similar values together). CSV must read and parse every column/row, even
unused ones.
**Q2: What is the Delta Lake transaction log and why is it important?**
> The `_delta_log/` directory contains JSON files where each file represents
This PDF document was created using the online html editor powered by CKEditor.
one atomic commit (version). These logs record which files were added or
removed in each operation. This gives Delta Lake ACID properties: the
transaction log is the source of truth, not the raw Parquet files. Time
travel works by reading older log entries. Concurrent writes are serialized
through optimistic concurrency on this log.
**Q3: How does Iceberg differ from Delta Lake?**
> Delta Lake is tightly integrated with Databricks (though OSS exists).
Iceberg is designed to be engine-agnostic — readable by Spark, Trino, Flink,
Hive, DuckDB without a specific runtime. Iceberg has superior partition
evolution (change partitioning without rewriting data) and row-level deletes
with MoR semantics natively. For multi-engine environments, Iceberg is
preferred; for Databricks/Fabric environments, Delta Lake is the natural
choice.
**Q4: What is Copy-on-Write vs Merge-on-Read in Iceberg/Hudi?**
> CoW: when a row is deleted/updated, the entire Parquet file is rewritten
with the change applied. Read performance is excellent (no extra files to
merge), but write amplification is high. MoR: writes a small delete file
pointing to the affected rows; the base file is unchanged. Reads must merge
base files with delete files — slightly slower reads. MoR is much better for
high-frequency CDC/upsert workloads.
**Q5: When would you choose Polars over PySpark?**
> Polars for single-machine workloads up to ~100GB — it's simpler to deploy
(no cluster), much faster than Pandas, and has lazy evaluation. PySpark when
data exceeds single-machine memory, you need fault tolerance, or you're
integrating with a lakehouse platform (Databricks, Fabric). Polars is ideal
for ETL scripts, data quality jobs, and replacing slow Pandas code in
microservices.
**Q6: What makes DuckDB suitable for data engineering?**
> DuckDB can query Parquet, CSV, JSON, and Delta/Iceberg files directly
without importing. It's embedded (no server), extremely fast for singlemachine analytics due to vectorized columnar execution, and it supports
standard SQL. Data engineers use it for: local development/testing of SQL
logic before deploying to Snowflake/BigQuery, ad-hoc analysis on S3 files,
and fast data validation scripts.
This PDF document was created using the online html editor powered by CKEditor.
---
# Part 11: Modern Data Engineering — Vector DBs, Caching, Kubernetes &
Infrastructure as Code
> The frontier topics every modern data engineer needs to understand
---
## 11.1 Vector Databases & AI/LLM Data Pipelines
### Why Vector Databases Exist
Traditional databases answer: "Find all records WHERE name = 'Alice'"
Vector databases answer: **"Find all records SIMILAR TO this query"**
This is the foundation of:
- Semantic search (find documents about the same concept, not just keyword
matches)
- RAG (Retrieval-Augmented Generation) — giving LLMs access to your
enterprise data
- Recommendation systems (find products similar to what user bought)
- Anomaly detection (find records dissimilar from normal)
### How Embeddings Work
```mermaid
flowchart LR
 subgraph Input
 T1[Text: 'heart failure diagnosis']
 T2[Text: 'cardiac arrest in patient']
 T3[Text: 'quarterly revenue report']
 end

 subgraph Embedding Model\\nBERT / OpenAI / MiniLM
 EM[Converts text to\\nhigh-dimensional vector\\nCaptures semantic
meaning]
 end

This PDF document was created using the online html editor powered by CKEditor.
 subgraph Vector Space 1536 dimensions
 V1["[0.23, -0.11, 0.87, ..., 0.45]"]
 V2["[0.25, -0.09, 0.88, ..., 0.43]"]
 V3["[0.91, 0.73, -0.22, ..., 0.12]"]
 end

 T1 --> EM --> V1
 T2 --> EM --> V2
 T3 --> EM --> V3
 V1 <-->|Small distance\\ncosine similarity ~0.97| V2
 V1 <-->|Large distance\\ncosine similarity ~0.12| V3
```
### Similarity Search Algorithms
| Algorithm | Type | Speed | Accuracy | Memory | Best For |
|---|---|---|---|---|---|
| **Brute force** | Exact KNN | Slow O(n) | 100% | Low | Small datasets <
100K |
| **HNSW** | Approximate (ANN) | Very fast O(log n) | ~95-99% | High |
General purpose, < 1B vectors |
| **IVF (Inverted File)** | Approximate (ANN) | Fast | ~90-95% | Medium |
Large datasets, GPU-friendly |
| **ScaNN** | Approximate (ANN) | Google's fastest | ~97% | Medium | Googlescale production |
**HNSW (Hierarchical Navigable Small World):**
```mermaid
graph TD
 subgraph Layer 2\\nSparse long-range connections
 L2A((A)) --- L2B((F))
 L2B --- L2C((K))
 end
 subgraph Layer 1\\nMedium range
 L1A((A)) --- L1B((C))
 L1B --- L1C((F))
 L1C --- L1D((I))
 L1D --- L1E((K))
This PDF document was created using the online html editor powered by CKEditor.
 end
 subgraph Layer 0\\nDense short-range connections
 ALL((All nodes\\nfine-grained graph))
 end

 L2A -.-> L1A
 L1A -.-> ALL
```
### Vector Database Options
| Database | Type | Strengths | Weakness | Best For |
|---|---|---|---|---|
| **Pinecone** | Managed cloud | Simplest API, scales automatically |
Proprietary, cost | Quick RAG prototyping |
| **Weaviate** | Open-source | Built-in modules (OCR, NLP), GraphQL |
Complex setup | Multi-modal search |
| **Chroma** | Open-source | Embeds in Python, no server | Single machine |
Development, small scale |
| **pgvector** | PostgreSQL extension | Already in your Postgres DB | Slower
for huge datasets | Existing Postgres users |
| **Qdrant** | Open-source | Fast HNSW, filtering, Rust-based | Newer,
smaller ecosystem | Performance-critical apps |
| **Azure AI Search** | Managed Azure | Integrated with Azure OpenAI | Azure
lock-in | Azure + OpenAI stack |
| **Milvus** | Open-source | Scales to billions of vectors | Complex ops |
Billion-scale production |
### Building a RAG Pipeline — Full Example
RAG (Retrieval-Augmented Generation) lets an LLM answer questions about your
private data.
```mermaid
flowchart TD
 subgraph Indexing Pipeline\\nRun once / on update
 D[Documents\\nPDFs, Confluence, SharePoint] --> CH[Chunking\\nSplit
into 512-token chunks\\nwith 50-token overlap]
This PDF document was created using the online html editor powered by CKEditor.
 CH --> EMB[Embedding Model\\nOpenAI text-embedding-3-small\\nor
local MiniLM]
 EMB --> VDB[(Vector DB\\nPinecone / pgvector)]
 end
 subgraph Query Pipeline\\nRun on every user question
 Q[User Query:\\n'What is HEDIS CDC measure?'] --> QEMB[Embed
query\\nsame model]
 QEMB --> SIM[Similarity Search\\nFind top-5 similar chunks]
 VDB --> SIM
 SIM --> CTX[Context Assembly\\nQuery + top-5 chunks as context]
 CTX --> LLM[LLM\\nGPT-4o / Claude / Llama3]
 LLM --> ANS[Answer grounded\\nin your documents]
 end
```
```python
# RAG pipeline using LangChain + pgvector
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import PGVector
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.chains import RetrievalQA
from langchain.chat_models import ChatOpenAI
import psycopg2
# 1. Connect to pgvector
CONNECTION_STRING = "postgresql://user:pass@host:5432/vectordb"
# 2. Embed and store documents
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
text_splitter = RecursiveCharacterTextSplitter(
 chunk_size=512,
 chunk_overlap=50,
 separators=["\\n\\n", "\\n", " ", ""]
)
# Load and chunk documents
This PDF document was created using the online html editor powered by CKEditor.
with open("clinical_guidelines.txt") as f:
 raw_text = f.read()
chunks = text_splitter.split_text(raw_text)
# Store in pgvector
vectorstore = PGVector.from_texts(
 texts=chunks,
 embedding=embeddings,
 connection_string=CONNECTION_STRING,
 collection_name="clinical_guidelines"
)
# 3. Query time: retrieve + generate
retriever = vectorstore.as_retriever(
 search_type="similarity",
 search_kwargs={"k": 5} # Return top 5 most similar chunks
)
qa_chain = RetrievalQA.from_chain_type(
 llm=ChatOpenAI(model="gpt-4o", temperature=0),
 chain_type="stuff", # Stuff all context into one prompt
 retriever=retriever,
 return_source_documents=True
)
result = qa_chain({"query": "What is the HEDIS CDC measure numerator
criteria?"})
print(result["result"]) # Grounded answer
print(result["source_documents"]) # Which chunks it used
```
### Chunking Strategies
| Strategy | How | Best For |
|---|---|---|
| **Fixed-size** | Split every N tokens | Simple, consistent |
| **Recursive character** | Split on `\\n\\n`, then `\\n`, then ` ` |
This PDF document was created using the online html editor powered by CKEditor.
General text documents |
| **Semantic chunking** | Split when semantic content shifts | Technical
docs with distinct sections |
| **Sentence-based** | Keep sentences intact | Q&A, conversational data |
| **Hierarchical** | Parent chunks + smaller child chunks for retrieval |
Complex documents needing context + precision |
---
## 11.2 Caching Patterns
Caching is critical for reducing latency and database load. As a data
engineer you must know when and how to cache.
### Core Caching Patterns
```mermaid
flowchart TD
 subgraph Cache-Aside\\nLazy loading
 APP1[App] -->|1 Read| CACHE1{Cache}
 CACHE1 -->|2a Cache HIT\\nReturn immediately| APP1
 CACHE1 -->|2b Cache MISS| DB1[(Database)]
 DB1 -->|3 Fetch| APP1
 APP1 -->|4 Write to cache| CACHE1
 end
```
```mermaid
flowchart LR
 subgraph Write-Through
 APP2[App] -->|Write| CACHE2[(Cache)]
 CACHE2 -->|Synchronously write| DB2[(Database)]
 end
 subgraph Write-Behind\\nWrite-Back
 APP3[App] -->|Write| CACHE3[(Cache)]
 CACHE3 -->|Async batch write\\nlater| DB3[(Database)]
 end
This PDF document was created using the online html editor powered by CKEditor.
```
| Pattern | How It Works | Consistency | Use When |
|---|---|---|---|
| **Cache-Aside** | App manages cache; miss → DB → populate cache |
Eventually consistent | Read-heavy, sparse access |
| **Write-Through** | Every write goes to cache AND DB synchronously |
Strong | Write + read heavy, critical data |
| **Write-Behind** | Write to cache immediately; async write to DB |
Eventually consistent | Write-heavy, can tolerate async |
| **Read-Through** | Cache sits in front of DB; cache handles misses |
Strong | App doesn't know about DB |
| **Refresh-Ahead** | Cache proactively refreshes before expiry | Nearconsistent | Predictable access patterns |
### Cache Eviction Policies
| Policy | Removes | Best For |
|---|---|---|
| **LRU** (Least Recently Used) | Oldest access time | General purpose —
working set fits in cache |
| **LFU** (Least Frequently Used) | Least accessed | Items accessed
regularly (not just recently) |
| **FIFO** | First inserted | Queue-like access, append-heavy |
| **TTL** (Time-to-Live) | Expired entries | Data with known staleness
tolerance |
| **LRU + TTL** | Both old and expired | Production Redis — most common
combo |
### Redis for Data Engineering
```python
import redis
import json
from functools import wraps
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
This PDF document was created using the online html editor powered by CKEditor.
# --- Pattern 1: Cache-Aside for DB queries ---
def get_member_from_db(member_id: str) -> dict:
 # Expensive DB query
 return {"member_id": member_id, "name": "Alice", "plan": "GOLD"}
def get_member(member_id: str) -> dict:
 cache_key = f"member:{member_id}"

 # 1. Check cache
 cached = r.get(cache_key)
 if cached:
 return json.loads(cached)

 # 2. Cache miss → fetch from DB
 member = get_member_from_db(member_id)

 # 3. Store in cache with 1-hour TTL
 r.setex(cache_key, 3600, json.dumps(member))

 return member
# --- Pattern 2: Redis Sorted Set for leaderboards/rankings ---
# Used by Uber for surge pricing, Twitter for trending topics
def update_trending_topics(topic: str, score_increment: float):
 # Sorted set: O(log n) insert, O(log n) range query
 r.zincrby("trending:topics", score_increment, topic)
 r.expire("trending:topics", 3600) # Reset trending after 1 hour
def get_top_trending(n: int = 10) -> list:
 # Get top N with scores
 return r.zrevrange("trending:topics", 0, n - 1, withscores=True)
# --- Pattern 3: Redis as Rate Limiter ---
def is_rate_limited(api_key: str, limit: int = 1000) -> bool:
 window_key = f"rate:{api_key}:{int(time.time()) // 60}" # Per-minute
window
 current = r.incr(window_key)
 if current == 1:
This PDF document was created using the online html editor powered by CKEditor.
 r.expire(window_key, 60) # Expire after 1 minute
 return current > limit
# --- Pattern 4: Feature Store — serve ML features at low latency ---
def store_member_features(member_id: str, features: dict):
 """Store pre-computed ML features in Redis for real-time serving."""
 r.hset(f"features:{member_id}", mapping=features)
 r.expire(f"features:{member_id}", 86400) # 24-hour TTL
def get_member_features(member_id: str) -> dict:
 return r.hgetall(f"features:{member_id}")
```
### Cache Invalidation — The Hard Problem
> "There are only two hard things in Computer Science: cache invalidation
and naming things." — Phil Karlton
**Strategies:**
```python
# 1. TTL-based (simplest) — accept some staleness
r.setex("member:M123", 300, json.dumps(member)) # Auto-expire in 5 minutes
# 2. Event-driven invalidation — invalidate on write
def update_member(member_id: str, new_data: dict):
 db.execute("UPDATE members SET ... WHERE member_id = ?", member_id)
 r.delete(f"member:{member_id}") # Invalidate cache immediately
# 3. Write-through — always update cache on write
def update_member_write_through(member_id: str, new_data: dict):
 db.execute("UPDATE members SET ... WHERE member_id = ?", member_id)
 r.setex(f"member:{member_id}", 3600, json.dumps(new_data)) # Update
cache too
# 4. Cache tagging (for complex invalidation)
# Tag all member-related caches so you can invalidate by tag
r.sadd("tag:member:M123", "member:M123:profile", "member:M123:claims",
"member:M123:plan")
This PDF document was created using the online html editor powered by CKEditor.
# To invalidate all M123 caches:
keys = r.smembers("tag:member:M123")
r.delete(*keys)
r.delete("tag:member:M123")
```
---
## 11.3 Kubernetes for Data Engineering
### Why Kubernetes for Data?
```mermaid
graph LR
 subgraph Traditional
 A[Airflow Scheduler\\non VM 1]
 B[Airflow Worker 1\\non VM 2]
 C[Airflow Worker 2\\non VM 3]
 D[Each VM always\\nrunning at cost]
 end

 subgraph Kubernetes
 E[Airflow Scheduler\\npod]
 F[Worker Pod\\ncreated on demand]
 G[Worker Pod\\nauto-scaled\\ndestroyed when done]
 H[Spark Driver Pod]
 I[Spark Executor Pods\\nauto-scaled\\ndestroyed when done]
 end
```
**Benefits:**
- **Isolation:** Each DAG task in its own container → dependency conflicts
impossible
- **Auto-scaling:** Executor/worker pods scale with workload, destroyed when
done
- **Resource limits:** Guarantee memory/CPU per job — no noisy neighbors
- **Portability:** Same manifests work on AWS EKS, Azure AKS, GCP GKE
This PDF document was created using the online html editor powered by CKEditor.
### Airflow with KubernetesExecutor
```yaml
# airflow.cfg / values.yaml for Kubernetes
executor: KubernetesExecutor
# Each task gets its own pod:
kubernetes:
 worker_container_image: "my-registry/airflow-worker:2.8.0"
 worker_container_image_pull_policy: "Always"
 delete_worker_pods: true # Clean up after completion
 worker_pods_creation_batch_size: 16
# Per-task pod customization
```
```python
# DAG: run tasks in isolated Kubernetes pods with specific resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import
KubernetesPodOperator
from kubernetes.client import models as k8s
with dag:
 run_spark_etl = KubernetesPodOperator(
 task_id="spark_etl_claims",
 name="spark-etl-claims",
 namespace="airflow",
 image="my-registry/spark-etl:3.5.0",
 cmds=["python", "-m", "jobs.claims_etl"],
 arguments=["--date", "{{ ds }}"],

 # Resource requests and limits
 container_resources=k8s.V1ResourceRequirements(
 requests={"memory": "8Gi", "cpu": "2"},
 limits={"memory": "16Gi", "cpu": "4"}
 ),

 # Mount secrets
This PDF document was created using the online html editor powered by CKEditor.
 env_from=[
 k8s.V1EnvFromSource(
 secret_ref=k8s.V1SecretEnvSource(name="snowflakecredentials")
 )
 ],

 # Node affinity (run on nodes with GPU or specific instance type)
 node_selector={"instance-type": "memory-optimized"},

 is_delete_operator_pod=True, # Clean up pod after completion
 get_logs=True, # Stream pod logs to Airflow UI
 )
```
### Spark on Kubernetes
```bash
# Submit Spark job to Kubernetes cluster
spark-submit \\
 --master k8s://https://api.my-k8s-cluster.com:6443 \\
 --deploy-mode cluster \\
 --name spark-claims-etl \\
 --conf spark.kubernetes.container.image=my-registry/spark:3.5.0 \\
 --conf spark.kubernetes.namespace=spark-jobs \\
 --conf spark.executor.instances=20 \\
 --conf spark.executor.memory=8g \\
 --conf spark.executor.cores=2 \\
 --conf spark.kubernetes.executor.request.cores=1.5 \\
 --conf spark.kubernetes.driver.pod.name=claims-etl-driver \\
 # Ephemeral storage for shuffle
 --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.sparklocal-dir.mount.path=/tmp/spark \\
 --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.sparklocal-dir.claimName=spark-scratch \\
 local:///app/jobs/claims_etl.py
```
This PDF document was created using the online html editor powered by CKEditor.
**Kubernetes Spark Architecture:**
```mermaid
graph TD
 K[kubectl / spark-submit] -->|Submit| API[K8s API Server]
 API -->|Create| D[Driver Pod\\nnamespace: spark-jobs]
 D -->|Request n executors| API
 API -->|Create| E1[Executor Pod 1]
 API -->|Create| E2[Executor Pod 2]
 API -->|Create| EN[Executor Pod N]
 D <-->|Tasks, results| E1
 D <-->|Tasks, results| E2
 D <-->|Tasks, results| EN
 E1 -->|Shuffle read/write| S3[(S3 / ADLS)]
 E2 -->|Shuffle read/write| S3
 D -->|Job complete\\nall pods deleted| K
```
### Flink on Kubernetes
```yaml
# Flink Application mode on Kubernetes
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
 name: fraud-detection-flink
 namespace: flink-prod
spec:
 image: my-registry/flink-fraud:1.18.0
 flinkVersion: v1_18
 flinkConfiguration:
 taskmanager.numberOfTaskSlots: "4"
 state.backend: rocksdb
 state.checkpoints.dir: s3://my-bucket/flink-checkpoints/fraud/
 execution.checkpointing.interval: "60000" # Checkpoint every 60s
 serviceAccount: flink-service-account
 jobManager:
 resource:
 memory: "2048m"
This PDF document was created using the online html editor powered by CKEditor.
 cpu: 1
 taskManager:
 resource:
 memory: "4096m"
 cpu: 2
 replicas: 5
 job:
 jarURI: s3://my-bucket/jars/fraud-detection-1.0.jar
 entryClass: com.company.FraudDetectionJob
 args: ["--kafka-brokers", "kafka:9092", "--topic", "transactions"]
 parallelism: 16
 upgradeMode: stateless
```
---
## 11.4 Infrastructure as Code — Terraform for Data Engineering
### Why IaC Matters for Data Engineers
When your data platform spans dozens of cloud resources (S3 buckets,
Snowflake databases, Kafka clusters, IAM roles), manual setup is:
- Slow and error-prone
- Not reproducible between environments (dev/stage/prod)
- Not auditable — no record of who changed what
**Terraform** declaratively defines infrastructure; the state file tracks
reality.
### Core Terraform Concepts
```mermaid
flowchart LR
 HCL[HCL code\\n.tf files\\nDescribe desired state] --> PLAN[terraform
plan\\nCompute diff:\\nwhat will change?]
 PLAN --> APPLY[terraform apply\\nCreate/Update/Delete\\nresources to
match HCL]
 APPLY --> STATE[terraform.tfstate\\nRecord of current state\\nStore in
This PDF document was created using the online html editor powered by CKEditor.
S3 + DynamoDB lock]
 STATE --> PLAN
```
### Example: Data Platform Infrastructure
```hcl
# main.tf — Terraform for a data engineering platform
terraform {
 required_version = ">= 1.5.0"

 backend "s3" {
 bucket = "my-terraform-state"
 key = "data-platform/terraform.tfstate"
 region = "us-east-1"
 dynamodb_table = "terraform-state-lock" # Prevent concurrent applies
 encrypt = true
 }

 required_providers {
 aws = { source = "hashicorp/aws", version = "~> 5.0" }
 snowflake = { source = "Snowflake-Labs/snowflake", version = "~> 0.87" }
 }
}
# --- Variables ---
variable "environment" {
 description = "Environment name"
 type = string
 validation {
 condition = contains(["dev", "staging", "prod"], var.environment)
 error_message = "Environment must be dev, staging, or prod."
 }
}
variable "data_lake_bucket_name" {
 description = "Name of the S3 data lake bucket"
This PDF document was created using the online html editor powered by CKEditor.
 type = string
}
# --- S3 Data Lake ---
resource "aws_s3_bucket" "data_lake" {
 bucket = "${var.data_lake_bucket_name}-${var.environment}"

 tags = {
 Environment = var.environment
 Team = "data-engineering"
 ManagedBy = "terraform"
 }
}
resource "aws_s3_bucket_versioning" "data_lake" {
 bucket = aws_s3_bucket.data_lake.id
 versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
 bucket = aws_s3_bucket.data_lake.id
 rule {
 apply_server_side_encryption_by_default {
 sse_algorithm = "AES256"
 }
 }
}
resource "aws_s3_bucket_public_access_block" "data_lake" {
 bucket = aws_s3_bucket.data_lake.id
 block_public_acls = true
 block_public_policy = true
 ignore_public_acls = true
 restrict_public_buckets = true
}
# Lifecycle policy: move old data to cheaper storage
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
This PDF document was created using the online html editor powered by CKEditor.
 bucket = aws_s3_bucket.data_lake.id

 rule {
 id = "bronze-layer-archival"
 status = "Enabled"

 filter { prefix = "bronze/" }

 transition {
 days = 90
 storage_class = "STANDARD_IA" # Infrequent Access after 90 days
 }
 transition {
 days = 365
 storage_class = "GLACIER" # Archive after 1 year
 }
 }
}
# --- IAM: Data Pipeline Role ---
resource "aws_iam_role" "data_pipeline_role" {
 name = "data-pipeline-${var.environment}"

 assume_role_policy = jsonencode({
 Version = "2012-10-17"
 Statement = [{
 Effect = "Allow"
 Principal = { Service = "glue.amazonaws.com" }
 Action = "sts:AssumeRole"
 }]
 })
}
resource "aws_iam_role_policy" "data_lake_access" {
 name = "data-lake-access"
 role = aws_iam_role.data_pipeline_role.id

 policy = jsonencode({
This PDF document was created using the online html editor powered by CKEditor.
 Version = "2012-10-17"
 Statement = [
 {
 Effect = "Allow"
 Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
"s3:ListBucket"]
 Resource = [
 aws_s3_bucket.data_lake.arn,
 "${aws_s3_bucket.data_lake.arn}/*"
 ]
 }
 ]
 })
}
# --- Snowflake Resources ---
resource "snowflake_database" "analytics" {
 name = "ANALYTICS_${upper(var.environment)}"
 comment = "Analytics database for ${var.environment}"

 data_retention_time_in_days = var.environment == "prod" ? 90 : 7
}
resource "snowflake_warehouse" "transform" {
 name = "TRANSFORM_WH_${upper(var.environment)}"
 warehouse_size = var.environment == "prod" ? "X-LARGE" : "SMALL"
 auto_suspend = 60 # Suspend after 60 seconds idle
 auto_resume = true

 max_cluster_count = var.environment == "prod" ? 3 : 1 # Multi-cluster in
prod only
 scaling_policy = "ECONOMY"
}
resource "snowflake_role" "data_engineer" {
 name = "DATA_ENGINEER_${upper(var.environment)}"
}
This PDF document was created using the online html editor powered by CKEditor.
resource "snowflake_database_grant" "analytics_usage" {
 database_name = snowflake_database.analytics.name
 privilege = "USAGE"
 roles = [snowflake_role.data_engineer.name]
}
# --- Outputs ---
output "data_lake_bucket" {
 value = aws_s3_bucket.data_lake.bucket
}
output "snowflake_database" {
 value = snowflake_database.analytics.name
}
```
```bash
# Workflow
terraform init # Initialize providers
terraform workspace new staging # Create environment workspace
terraform plan -var="environment=staging" -out=plan.tfplan
terraform apply plan.tfplan # Apply the plan
# Environment promotion
terraform workspace select prod
terraform plan -var="environment=prod" -out=prod.tfplan
terraform apply prod.tfplan
```
---
## 11.5 Data Versioning — lakeFS
lakeFS provides Git-like version control for data lakes. It adds branches,
commits, and merges on top of your existing S3/ADLS/GCS.
```mermaid
gitGraph
This PDF document was created using the online html editor powered by CKEditor.
 commit id: "Initial data load\\nbronze/claims/2024"
 branch feature/add-member-data
 commit id: "Ingest member demographics"
 commit id: "Run quality checks"
 branch experiment/test-new-schema
 commit id: "Try new schema design"
 checkout feature/add-member-data
 merge experiment/test-new-schema
 checkout main
 merge feature/add-member-data id: "Merge to production"
 commit id: "Deploy to downstream"
```
```python
# lakeFS Python client
import lakefs_client
from lakefs_client.client import LakeFSClient
configuration = lakefs_client.Configuration(
 host="https://your-lakefs-server.com",
 username="access_key",
 password="secret_key"
)
client = LakeFSClient(configuration)
# Create a branch for a new pipeline run
client.branches.create_branch(
 repository="data-lake",
 branch_creation={
 "name": f"pipeline/claims-etl-{date.today().isoformat()}",
 "source": "main"
 }
)
# Write data to the branch (via S3 with lakeFS endpoint)
import boto3
s3 = boto3.client('s3', endpoint_url='https://your-lakefs-server.com')
This PDF document was created using the online html editor powered by CKEditor.
s3.put_object(
 Bucket="data-lake",
 Key=f"pipeline/claims-etl-{date.today().isoformat()}/bronze/claims/part0.parquet",
 Body=parquet_bytes
)
# Commit the changes with metadata
client.commits.commit(
 repository="data-lake",
 branch=f"pipeline/claims-etl-{date.today().isoformat()}",
 commit_creation={
 "message": f"Claims ETL for {date.today().isoformat()}",
 "metadata": {
 "rows_processed": "500000",
 "source": "claims-db-prod",
 "pipeline_run_id": "run-abc123"
 }
 }
)
# After validation, merge to main
client.refs.merge_into_branch(
 repository="data-lake",
 source_ref=f"pipeline/claims-etl-{date.today().isoformat()}",
 destination_branch="main"
)
```
**Why lakeFS matters:**
- **Atomic multi-file commits:** All files in a pipeline run are committed
as one atomic change
- **Rollback:** One command rolls back to any previous commit
- **Isolated experiments:** Test schema changes in a branch without
affecting production
- **CI/CD for data:** Run data quality checks on a branch before merging to
main
This PDF document was created using the online html editor powered by CKEditor.
---
## 11.6 Semantic / Metrics Layer
### The Problem Without a Metrics Layer
```mermaid
flowchart TD
 subgraph Without Metrics Layer — Metrics defined in 17 places
 DA[Data Analyst\\nSELECT COUNT DISTINCT member_id...] -->
BI[Tableau]
 DE[Data Engineer\\nCOUNT DISTINCT member_id in Airflow] -->
R[Report]
 DS[Data Scientist\\nlen df member_id unique] --> NB[Notebook]
 PM[Product Manager\\nSQL in Mode with slightly different filter] -->
M[Metric]
 end
 subgraph Problem
 CONF[4 different numbers\\nfor "active members"\\nWho is right?]
 end
 BI --> CONF
 R --> CONF
 NB --> CONF
 M --> CONF
```
### dbt Semantic Layer (MetricFlow)
```yaml
# models/metrics/member_metrics.yml
metrics:
 - name: active_member_count
 description: "Count of distinct active members in measurement period"
 label: "Active Members"
 type: simple
 type_params:
 measure:
 name: member_count
This PDF document was created using the online html editor powered by CKEditor.
 fill_nulls_with: 0
 filter: |
 {{ Dimension('member__status') }} = 'ACTIVE'
 AND {{ Dimension('member__effective_date') }} <= {{
TimeDimension('metric_time', 'day') }}
 AND {{ Dimension('member__termination_date') }} > {{
TimeDimension('metric_time', 'day') }}
 - name: hedis_cdc_rate
 description: "HEDIS Comprehensive Diabetes Care rate — HbA1c tested"
 label: "HEDIS CDC Rate"
 type: ratio
 type_params:
 numerator: hba1c_tested_count
 denominator: diabetes_denominator_count
```
```python
# Query metrics via dbt Semantic Layer API (not raw SQL)
# Same metric definition → consistent number everywhere
from dbt_semantic_interfaces.references import MetricReference
from dbt.adapters.semantic_layer.client import SemanticLayerClient
client = SemanticLayerClient(server_url="grpc://semantic-layer:8080")
result = client.create_query(
 metrics=[MetricReference("active_member_count")],
 group_by=["member__plan_type", "metric_time__month"],
 where="metric_time__month between '2024-01-01' and '2024-12-31'"
)
```
### Cube.dev — Dedicated Metrics Layer
```javascript
// cube.js schema
cube(`Members`, {
 sql: `SELECT * FROM analytics.dim_member`,
This PDF document was created using the online html editor powered by CKEditor.

 measures: {
 count: { type: `count`, drillMembers: [memberId, name] },
 activeCount: {
 type: `count`,
 filters: [{ sql: `${CUBE}.status = 'ACTIVE'` }]
 }
 },

 dimensions: {
 memberId: { sql: `member_id`, type: `string`, primaryKey: true },
 planType: { sql: `plan_type`, type: `string` },
 state: { sql: `state`, type: `string` },
 joinedDate: { sql: `effective_date`, type: `time` }
 },

 segments: {
 diabeticMembers: { sql: `${CUBE}.chronic_condition = 'DIABETES'` }
 }
});
```
---
## 11.7 Real-World Architecture Decisions
### Decision: When to Use Each Modern Technology
```mermaid
flowchart TD
 Q1{Do you need\\nsemantic/similarity\\nsearch?}
 Q1 -->|Yes| Q2{Scale of vectors?}
 Q2 -->|< 1M, existing Postgres| PGV[pgvector\\nSimplest option]
 Q2 -->|1M - 100M, fully managed| PIN[Pinecone or\\nAzure AI Search]
 Q2 -->|> 100M, open source| MIL[Milvus or\\nWeaviate]
 Q1 -->|No| Q3{Need AI-augmented\\nanswers from docs?}
 Q3 -->|Yes| RAG[Build RAG pipeline\\nChunking + Embeddings\\n+ LLM]
 Q3 -->|No| Q4{Need low-latency\\ncaching?}
This PDF document was created using the online html editor powered by CKEditor.
 Q4 -->|Yes| RED[Redis\\nCache-Aside + TTL\\nfor API/feature serving]
 Q4 -->|No| Q5{Need reproducible\\ninfrastructure?}
 Q5 -->|Yes| TF[Terraform\\nState in S3\\nWorkspaces per env]
 Q5 -->|No| Q6{Need data\\nversion control?}
 Q6 -->|Yes| LFS[lakeFS\\nGit for data lake]
 Q6 -->|No| Q7{Need consistent\\nbusiness metrics?}
 Q7 -->|Yes| SL[dbt Semantic Layer\\nor Cube.dev]
```
### Company Examples
| Company | Technology | Use Case | Outcome |
|---|---|---|---|
| **Notion** | pgvector | Semantic search across millions of pages | Ship in
days, no new infrastructure |
| **Shopify** | Pinecone | Product recommendation search | 40ms P99 latency
at 100M+ products |
| **Grab (Ride-sharing)** | Redis Cache-Aside | Driver location cache (1M
updates/sec) | DB load reduced 95% |
| **Airbnb** | Kubernetes (Spark) | Isolate data science experiments | 10×
faster iteration, no cluster conflicts |
| **Lyft** | Terraform | 50+ microservices infra | Reproducible environments
in minutes |
| **Atlan / dbt Cloud** | Semantic Layer | Single definition of "revenue",
"active user" | Eliminated metric discrepancies across teams |
---
## 11.8 Cross Questions — Modern Data Engineering
**Q1: What is a vector database and when would you use one?**
> A vector database stores high-dimensional numerical vectors (embeddings)
and enables similarity search — finding vectors closest to a query vector.
Use it when: building semantic search (match meaning, not keywords),
implementing RAG (give LLMs access to private documents), building
recommendation systems based on content similarity, or detecting anomalies
by finding records dissimilar from normal patterns.
This PDF document was created using the online html editor powered by CKEditor.
**Q2: What is the difference between Cache-Aside and Write-Through caching?
**
> Cache-Aside: the application manages the cache manually. On read miss, app
queries DB, then populates cache. On write, app updates DB and optionally
invalidates cache. Most flexible, but requires app code to handle cache
logic. Write-Through: every write goes simultaneously to cache AND DB
synchronously. Always consistent (cache never has stale data), but writes
are slower. Use Write-Through when you can't tolerate stale cache reads.
**Q3: How does Kubernetes help with Spark jobs?**
> Kubernetes runs Spark in container pods — each job gets its own isolated
pod with defined resource limits (CPU, memory). Executor pods are created on
demand and destroyed when done, so you pay only for active computation. It
eliminates the problem of shared clusters where one job's memory pressure
affects another. Spark-on-K8s enables fine-grained resource management and
portability across cloud providers.
**Q4: What is Terraform state and why must it be stored remotely?**
> Terraform state (`terraform.tfstate`) maps your HCL code to actual cloud
resources — without it, Terraform doesn't know what exists. Stored remotely
in S3 (with DynamoDB lock) so: (1) team members share the same state, (2)
concurrent applies are prevented by the lock, (3) state is not lost if a
developer's laptop dies, (4) state history is preserved. Never store state
locally for team projects.
**Q5: What is a RAG pipeline and how does it work?**
> RAG (Retrieval-Augmented Generation) gives an LLM access to your specific
documents without retraining it. Two phases: (1) Indexing — chunk documents,
embed each chunk into vectors, store in vector DB. (2) Query — embed user's
question, find top-K similar chunks via similarity search, pass those chunks
as context to the LLM along with the question. The LLM generates an answer
grounded in your documents, not just its training data.
**Q6: What is lakeFS and how is it different from Delta Lake time travel?**
> Delta Lake time travel lets you read historical snapshots of a single
table at a specific version/timestamp. lakeFS provides Git-like branching
across your entire data lake (all files). You can create a branch, write a
full pipeline run's output there, validate quality, then merge to main
This PDF document was created using the online html editor powered by CKEditor.
atomically. If something fails, rollback is one command. Delta time travel
is for point-in-time reads; lakeFS is for full workflow isolation and atomic
multi-file commits.
**Q7: Why do organizations need a semantic/metrics layer?**
> Without it, "monthly active members" gets computed differently in every
tool — analysts write their own SQL in BI tools, engineers hardcode logic in
pipelines, data scientists compute it differently in notebooks. This creates
inconsistent numbers and trust issues. A semantic layer defines metrics once
(in code), and all tools query that definition — ensuring every stakeholder
sees the same number for every business metric.
**Q8: What is HNSW and why is it used in vector search?**
> HNSW (Hierarchical Navigable Small World) is an approximate nearest
neighbor (ANN) algorithm used in most vector databases. It builds a multilayer graph where upper layers have long-range connections (for fast
traversal) and lower layers have fine-grained connections (for accuracy).
Search starts at the top layer, navigates to the approximate neighborhood,
then refines at the bottom layer. It achieves ~95-99% recall with O(log n)
query time, making it practical for millions of vectors.
---
This PDF document was created using the online html editor powered by CKEditor.
