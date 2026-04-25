
# Microsoft Fabric - Comprehensive Learning & Interview Preparation Guide

**Date Started:** April 24, 2026  
**Last Updated:** April 25, 2026  
**Status:** Complete Interview-Ready Reference

---

## 📑 Table of Contents

### 📘 1. Foundation & Core Concepts
- [Overview](#overview)
- [What is Microsoft Fabric?](#what-is-microsoft-fabric)
- [Core Concepts](#core-concepts)

### 📚 2. Storage & Data Lake
- [ADLS Gen 2 (Azure Data Lake Storage Gen 2)](#adls-gen-2-azure-data-lake-storage-gen-2)
- [OneLake](#onelake)
- [Parquet Format](#parquet-format)
- [Delta Parquet Format (Delta Lake)](#delta-parquet-format-delta-lake)

### 🏗️ 3. Data Architecture & Serving
- [Data Serving in Fabric](#data-serving-in-fabric)
- [KQL Database (Kusto/Real-Time Analytics)](#kql-database-kustoreal-time-analytics)

### 📥 4. Data Ingestion
- [Data Ingestion in Fabric](#data-ingestion-in-fabric)

### ⚙️ 5. Data Engineering
- [Fabric Data Engineering](#fabric-data-engineering)

### 🔄 6. Data Integration & Orchestration
- [Fabric Data Factory](#fabric-data-factory)
- **Pipeline Activities (Detailed):**
  - [Copy Activity](#copy-activity)
  - [ForEach Activity](#foreach-activity)
  - [Lookup Activity](#lookup-activity)
  - [Invoke Pipeline Activity](#invoke-pipeline-activity)
  - [If Condition Activity](#if-condition-activity)
  - [Set Variable Activity](#set-variable-activity)
  - [Web Activity](#web-activity)
  - [Wait Activity](#wait-activity)
  - [Get Metadata Activity](#get-metadata-activity)
  - [Until Activity](#until-activity)
- [Static vs Dynamic Pipelines](#static-vs-dynamic-pipelines)
- [Dataflow Gen1 vs Dataflow Gen2](#dataflow-gen1-vs-dataflow-gen2)
- [Dynamic Content & Pipeline Expression Builder](#dynamic-content--pipeline-expression-builder)

### 🚀 7. Performance Optimization
- [Fabric Data Optimization](#9-fabric-data-optimization)
  - [V-Order (Write-Time Optimization)](#1-v-order-write-time-optimization)
  - [Optimize Write (Small File Prevention)](#2-optimize-write-small-file-prevention)
  - [Z-Order (Data Clustering)](#3-z-order-data-clustering)
  - [OPTIMIZE Command (Table Compaction)](#4-optimize-command-table-compaction)
  - [VACUUM Command (Storage Cleanup)](#5-vacuum-command-storage-cleanup)
  - [Merge Optimization (Low Shuffle Merge)](#6-merge-optimization-low-shuffle-merge)

### 🏢 8. Data Warehousing
- [Fabric Data Warehouse](#10-fabric-data-warehouse)
  - [Architecture Foundation](#1-architecture-foundation)
  - [T-SQL Development Experience](#2-t-sql-development-experience)
  - [Table Types and Design](#3-table-types-and-design)
  - [Data Loading Methods](#4-data-loading-methods)
  - [Temporary Tables (#temp)](#5-temporary-tables-temp)
  - [Cross-Database Queries](#6-cross-database-queries)
  - [Statistics and Performance](#7-statistics-and-performance)
  - [SQL Analytics Endpoint (Read-Only)](#8-sql-analytics-endpoint-read-only)
  - [Table Constraints](#9-table-constraints)
  - [Collation](#10-collation)
  - [Warehouse vs Lakehouse Comparison](#warehouse-vs-lakehouse)
  - [Limitations & Considerations](#limitations--considerations)

### 🎯 9. Interview Questions (Conceptual)
- [Interview Questions by Topic](#interview-questions-by-topic)
  - **Question 1:** [What is Fabric Data Factory and how does it differ from Azure Data Factory?](#1-q-what-is-fabric-data-factory-and-how-does-it-differ-from-azure-data-factory)
  - **Question 2:** [Explain Medallion Architecture and how to implement it in Fabric](#2-q-explain-the-medallion-architecture-bronze-silver-gold-and-how-to-implement-it-in-fabric)
  - **Question 3:** [What are Slowly Changing Dimensions (SCD) and implementation in Fabric?](#3-q-what-are-slowly-changing-dimensions-scd-and-how-do-you-implement-them-in-fabric)
  - **Question 4:** [What is OneLake and how does it differ from ADLS Gen2?](#4-q-what-is-onelake-and-how-does-it-differ-from-adls-gen2)
  - **Question 5:** [Explain Delta Lake architecture and ACID transactions](#5-q-explain-delta-lake-architecture-and-how-it-enables-acid-transactions)
  - **Question 6:** [What are Cross-Database Queries in Fabric?](#6-q-what-are-cross-database-queries-in-fabric-warehouse-and-when-would-you-use-them)
  - **Question 7:** [What is CTAS and how does it improve performance?](#7-q-what-is-ctas-create-table-as-select-and-how-does-it-improve-performance-compared-to-traditional-methods)
  - **Question 8:** [Explain Dimensional Data Modeling (Star & Snowflake schemas)](#8-q-what-is-dimensional-data-modeling-and-how-do-you-implement-star-and-snowflake-schemas-in-fabric-warehouse)
  - **Question 9:** [What are Views in Fabric and their use cases?](#9-q-what-are-views-in-fabric-data-warehouse-and-how-do-you-use-them-effectively)
  - **Question 10:** [How is data governance maintained in Microsoft Fabric?](#10-q-how-is-data-governance-maintained-in-microsoft-fabric)

### 🎬 10. Scenario-Based Interview Questions
- [Scenario-Based Interview Questions](#scenario-based-interview-questions)
  - [Scenario 1: Performance Optimization](#scenario-1-performance-optimization)
  - [Scenario 2: Data Quality & Validation](#scenario-2-data-quality--validation)
  - [Scenario 3: Incremental Data Loading](#scenario-3-incremental-data-loading)
  - [Scenario 4: Cross-Domain Data Integration](#scenario-4-cross-domain-data-integration)
  - [Scenario 5: Migration from Azure Synapse to Fabric](#scenario-5-migration-from-azure-synapse-to-fabric)
  - [Scenario 6: Real-Time Data Pipeline](#scenario-6-real-time-data-pipeline)

### 💡 11. Best Practices & Reference
- [Components & Services](#components--services)
- [Best Practices](#best-practices)
- [Examples & Use Cases](#examples--use-cases)

---

## 📘 Overview

Microsoft Fabric is a unified analytics platform that brings together various data and analytics tools into a single integrated solution. It combines data integration, data engineering, data warehousing, data science, real-time analytics, and business intelligence.

**Key Features:**
- **Unified Analytics Platform:** One platform for all data and analytics needs
- **End-to-End Solution:** Data movement → transformation → analytics → visualization
- **SaaS Foundation:** Fully managed, no infrastructure to maintain
- **Integrated Services:** Power BI, Azure Synapse, Azure Data Factory capabilities
- **OneLake:** Unified data lake automatically included
- **Pay-as-you-go:** Capacity-based pricing model

**Core Value Proposition:**
- **Reduce Complexity:** Eliminate data silos and tool sprawl
- **Accelerate Time-to-Insight:** Pre-integrated components
- **Lower TCO:** Simplified architecture and management
- **Enterprise Security:** Built-in governance and compliance

---

## 📘 Core Concepts

### What is Microsoft Fabric?

Microsoft Fabric is an all-in-one analytics solution for enterprises that covers everything from data movement to data science, real-time analytics, and business intelligence.

**Key Capabilities:**
1. **Data Integration** - Move and transform data
2. **Data Engineering** - Build data pipelines at scale
3. **Data Warehousing** - SQL-based analytical queries
4. **Data Science** - ML model development and deployment
5. **Real-Time Analytics** - Streaming data processing
6. **Business Intelligence** - Interactive reports and dashboards

**Architecture Layers:**
```
┌────────────────────────────────────────────────────────────┐
│                     POWER BI (Reporting)                    │
└────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────┐
│  Data Warehouse │ Data Science │ Real-Time Analytics │ ...│
└────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────┐
│         Data Factory (Pipelines & Dataflows)                │
└────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────┐
│              OneLake (Unified Data Lake)                    │
└────────────────────────────────────────────────────────────┘
```

---

## 📚 Terminologies

> **Note:** Each terminology includes definition, key concepts, use cases, and interview questions.

### ADLS Gen 2 (Azure Data Lake Storage Gen 2)

**Definition:**
Azure Data Lake Storage Gen 2 is Microsoft's cloud-based, highly scalable data lake solution that combines the capabilities of Azure Data Lake Storage Gen 1 with Azure Blob Storage. It provides hierarchical namespace, fine-grained access control, and enterprise-grade security for big data analytics workloads.

**Key Concepts:**
- **Hierarchical Namespace (HNS):** Organizes data in a directory structure (folders/subfolders) instead of flat blob storage, enabling faster operations on directories
- **Multi-protocol support:** Supports both Blob API and Data Lake File System (DFS) API
- **Cost-effective:** Offers tiered storage (Hot, Cool, Archive) for optimizing costs
- **Security:** Supports Azure AD integration, ACLs (Access Control Lists), and RBAC (Role-Based Access Control)
- **Performance:** Optimized for analytics workloads with high throughput and low latency
- **Integration:** Works seamlessly with Azure Synapse, Databricks, HDInsight, and other analytics services

**Use Cases:**
- Big data analytics and data warehousing
- Data lake architecture for enterprise data platforms
- Machine learning model training on large datasets
- Log analytics and IoT data storage
- Real-time and batch data processing

**Interview Questions:**

1. **Q:** What are the key differences between ADLS Gen 1 and ADLS Gen 2?
   **A:** 
   - **Gen 2** is built on Azure Blob Storage, while Gen 1 was a separate service
   - Gen 2 supports both Blob and ADLS APIs, Gen 1 only supported ADLS API
   - Gen 2 offers better cost optimization with tiered storage (Hot/Cool/Archive)
   - Gen 2 has better availability and disaster recovery options
   - Gen 2 provides better integration with the broader Azure ecosystem
   - Gen 2 has hierarchical namespace as an optional feature, making it more flexible

2. **Q:** What is a Hierarchical Namespace and why is it important in ADLS Gen 2?
   **A:** 
   Hierarchical Namespace (HNS) organizes data in a directory/folder structure similar to a traditional file system, rather than a flat structure. This is important because:
   - **Performance:** Directory operations (rename, delete) are atomic and much faster - O(1) instead of O(n)
   - **Organization:** Makes data easier to organize and manage logically
   - **Access Control:** Enables granular security with ACLs at folder and file levels
   - **Compatibility:** Works better with big data tools that expect file system semantics
   - **Example:** Renaming a folder with 1 million files is instant with HNS vs hours without it

3. **Q:** How do you implement security in ADLS Gen 2?
   **A:** 
   ADLS Gen 2 provides multi-layered security:
   - **RBAC (Role-Based Access Control):** Assign Azure roles like Storage Blob Data Owner, Contributor, Reader at subscription/resource group/storage account level
   - **ACLs (Access Control Lists):** Granular permissions (Read, Write, Execute) at directory and file level
   - **Azure AD Integration:** Use Azure Active Directory for authentication
   - **Encryption:** Data encrypted at rest (Microsoft-managed or customer-managed keys) and in transit (HTTPS/TLS)
   - **Network Security:** Virtual Network service endpoints, private endpoints, and firewall rules
   - **Example:** You might use RBAC to give a team Storage Blob Data Contributor access, but use ACLs to restrict specific sensitive folders to only certain users

4. **Q:** When would you use ADLS Gen 2 vs Azure Blob Storage?
   **A:** 
   Use **ADLS Gen 2** when:
   - You need hierarchical namespace for big data analytics
   - Working with Hadoop/Spark ecosystems
   - Require fine-grained ACL-based security
   - Need optimized directory operations
   
   Use **standard Blob Storage** when:
   - Simple object storage for unstructured data
   - Web content, backups, or media files
   - Don't need hierarchical organization
   - Want lowest cost for basic storage needs

---

### OneLake

**Definition:**
OneLake is Microsoft Fabric's built-in, unified data lake that provides a single SaaS data lake for the entire organization. It's automatically provisioned with every Fabric tenant and serves as the central storage foundation for all Fabric workloads, eliminating data silos and duplication.

**Key Concepts:**
- **Single Data Lake:** One centralized data lake per organization, no need to provision multiple storage accounts
- **Built on ADLS Gen 2:** Leverages ADLS Gen 2 architecture but with a SaaS model
- **Automatic Provisioning:** Created automatically with Fabric tenant, no manual setup required
- **Delta Lake Format:** Stores data in open Delta-Parquet format by default
- **Unified Namespace:** All Fabric experiences (Data Factory, Synapse, Power BI) work with the same data
- **Shortcuts:** Virtualize data from other sources without physically copying it
- **Multi-cloud Support:** Can create shortcuts to AWS S3, Google Cloud Storage, and other ADLS accounts
- **Governance:** Centralized governance with Microsoft Purview integration

**Use Cases:**
- Enterprise-wide data lake strategy
- Eliminating data silos across departments
- Reducing data duplication and storage costs
- Enabling seamless collaboration across teams
- Building lakehouse architectures

**Interview Questions:**

1. **Q:** How is OneLake different from traditional data lakes like ADLS Gen 2?
   **A:** 
   Key differences:
   
   **OneLake (Fabric's approach):**
   - **SaaS Model:** Fully managed, no infrastructure to provision
   - **Single Instance:** One lake per organization, automatic for all users
   - **Built-in:** Integrated with all Fabric workloads automatically
   - **No Boundaries:** Seamless access across all Fabric services
   - **Governance by Default:** Unified security and compliance
   
   **Traditional Data Lakes (ADLS Gen 2):**
   - **IaaS/PaaS Model:** You provision and manage storage accounts
   - **Multiple Instances:** Teams create separate storage accounts, leading to silos
   - **Manual Integration:** Need to configure connections to each service
   - **Data Movement:** Often requires copying data between systems
   - **Complex Governance:** Need to implement governance across multiple stores

2. **Q:** What are OneLake Shortcuts and why are they important?
   **A:** 
   OneLake Shortcuts are pointer/reference objects that virtualize data from external sources into OneLake without physically copying it. 
   
   **How they work:**
   - Create a shortcut to data in S3, ADLS Gen 2, or another OneLake location
   - Data appears in OneLake as if it's stored there
   - Queries execute directly against source location
   - No data duplication or movement
   
   **Benefits:**
   - **Cost Savings:** Avoid duplicate storage costs
   - **Real-time Access:** Always access latest data from source
   - **Multi-cloud Strategy:** Unify data from AWS, Azure, GCP
   - **Data Mesh:** Enable federated data ownership while maintaining central access
   - **Example:** Finance team has data in S3, Marketing in ADLS, both accessible in OneLake without copying

3. **Q:** What is the "One Copy" principle in OneLake?
   **A:** 
   The "One Copy" principle means data is stored once in OneLake and reused by all Fabric workloads without creating physical copies. 
   
   **How it works:**
   - Data written by Data Factory is immediately available in Synapse, Power BI, etc.
   - All services read from the same Delta-Parquet files
   - No ETL needed to move data between services
   - Compute and storage are decoupled
   
   **Benefits:**
   - **Reduced Costs:** No duplicate storage
   - **Consistency:** Everyone sees the same data
   - **Freshness:** No delays from data movement
   - **Simplified Architecture:** No complex ETL pipelines between services
   - **Example:** Data engineer writes data, data scientist analyzes it, analyst visualizes it - all from same files without copies

4. **Q:** How does OneLake ensure security and compliance?
   **A:** 
   - **Workspace-based Security:** Data organized in workspaces with role-based permissions
   - **Item-level Permissions:** Control access at individual lakehouse, warehouse, or dataset level
   - **Microsoft Purview Integration:** Automatic data cataloging, lineage tracking, and sensitivity labeling
   - **Encryption:** Data encrypted at rest and in transit automatically
   - **Compliance Certifications:** Inherits Azure compliance (ISO, SOC, HIPAA, etc.)
   - **Auditing:** Built-in audit logs for all data access and operations
   - **Data Residency:** Control where data is stored geographically

---

### Parquet Format

**Definition:**
Apache Parquet is an open-source, columnar storage file format designed for efficient data storage and retrieval. It's optimized for analytics workloads where you typically query a subset of columns across many rows, making it ideal for big data processing frameworks like Spark, Hive, and analytics platforms.

**Key Concepts:**
- **Columnar Storage:** Data stored by columns rather than rows, enabling better compression and query performance
- **Compression:** Highly efficient compression (Snappy, Gzip, LZO) as similar data types are stored together
- **Schema Evolution:** Supports adding, removing, or modifying columns without rewriting all data
- **Predicate Pushdown:** Can skip reading unnecessary data based on query filters
- **Platform Independent:** Works across Hadoop, Spark, Pandas, etc.
- **Self-describing:** Schema metadata embedded in the file
- **Nested Data Support:** Handles complex nested structures (arrays, maps, structs)

**Use Cases:**
- Big data analytics and data warehousing
- Data lake storage format (especially with Delta Lake)
- Machine learning feature stores
- Long-term archival of analytical data
- ETL pipeline intermediate storage

**Interview Questions:**

1. **Q:** What are the advantages of Parquet over CSV or JSON for analytics?
   **A:** 
   
   **Parquet vs CSV:**
   - **Storage:** Parquet 10-100x smaller due to columnar storage and compression
   - **Query Performance:** Read only needed columns; CSV must read entire row
   - **Type Safety:** Parquet preserves data types; CSV everything is string
   - **Schema:** Parquet includes schema; CSV requires external schema definition
   - **Example:** Query "SELECT AVG(salary) FROM employees" - Parquet reads only salary column, CSV reads all columns
   
   **Parquet vs JSON:**
   - **Size:** Parquet typically 5-10x smaller
   - **Performance:** Columnar access much faster than nested JSON parsing
   - **Analytics:** Parquet optimized for OLAP; JSON for OLTP/document storage
   - **Compression:** Better compression ratios in Parquet
   - **When to use JSON:** APIs, document stores, semi-structured data with varying schemas

2. **Q:** Explain how columnar storage in Parquet improves query performance?
   **A:** 
   
   **Columnar Storage Benefits:**
   - **I/O Reduction:** Query "SELECT name, salary" only reads those 2 columns, not all 50 columns
   - **Better Compression:** Same data types stored together compress better (all integers, all strings)
   - **Predicate Pushdown:** Filter "WHERE department='Sales'" can skip entire column chunks without reading them
   - **Cache Efficiency:** Similar data types have better CPU cache locality
   - **Vectorization:** Modern CPUs can process columnar data in batches (SIMD operations)
   
   **Example Scenario:**
   Table with 100 million rows, 50 columns (5GB total)
   - Query: `SELECT AVG(age) WHERE city='NYC'`
   - **Row-based (CSV):** Read all 5GB
   - **Column-based (Parquet):** Read ~100MB (age + city columns) + skip non-NYC data
   - **Result:** 50x less I/O, 10-50x faster query

3. **Q:** What is the difference between Parquet and Delta Lake format?
   **A:** 
   
   **Parquet:**
   - **Format:** Just a file format specification
   - **Capabilities:** Storage and compression
   - **Limitations:** No ACID transactions, no time travel, difficult deletes/updates
   
   **Delta Lake:**
   - **Built on Parquet:** Uses Parquet files for storage
   - **Transaction Log:** Adds JSON transaction log for ACID guarantees
   - **ACID Support:** Atomic commits, isolation, consistency, durability
   - **Time Travel:** Query historical versions of data
   - **Schema Enforcement:** Prevents bad data writes
   - **Upserts/Deletes:** Efficient MERGE, UPDATE, DELETE operations
   - **Example:** Delta Lake = Parquet files + transaction log + metadata + ACID properties
   
   **When to use:**
   - **Parquet:** Read-only archives, ETL intermediate files, simple analytics
   - **Delta Lake:** Production data lakes, frequent updates, need ACID, collaborative environments

4. **Q:** How does Parquet handle nested data structures?
   **A:** 
   Parquet uses **Dremel encoding** (repetition and definition levels) to efficiently store nested structures:
   
   **Example JSON:**
   ```json
   {
     "name": "John",
     "addresses": [
       {"city": "NYC", "zip": "10001"},
       {"city": "LA", "zip": "90001"}
     ]
   }
   ```
   
   **How Parquet stores it:**
   - Flattens nested structure into columns
   - Uses repetition levels (which repetition of array/map)
   - Uses definition levels (null handling)
   - Columns: `name`, `addresses.city`, `addresses.zip`
   - **Benefits:** Can query `addresses.city` without reading entire nested structure
   - **Performance:** Much faster than parsing JSON for analytics
   
   **Use Case:** Query "Find all customers with addresses in NYC" - only read addresses.city column, not entire nested object

5. **Q:** What are Row Groups and Column Chunks in Parquet?
   **A:** 
   
   **Row Group:**
   - Horizontal partition of data (e.g., 128MB or 1 million rows)
   - Contains column chunks for each column
   - Unit of parallelization (each Spark task processes one row group)
   
   **Column Chunk:**
   - All values for one column within a row group
   - Contains one or more pages
   - Enables columnar reads
   
   **Page:**
   - Smallest unit of reading (typically 1MB)
   - Compressed and encoded together
   
   **Structure:**
   ```
   Parquet File
   ├── Row Group 1 (128MB)
   │   ├── Column Chunk: name
   │   ├── Column Chunk: age
   │   └── Column Chunk: salary
   ├── Row Group 2 (128MB)
   │   ├── Column Chunk: name
   │   ├── Column Chunk: age
   │   └── Column Chunk: salary
   └── Footer (metadata, schema, row group locations)
   ```
   
   **Benefits:**
   - Parallel processing of row groups
   - Skip entire row groups based on min/max statistics
   - Read only needed column chunks

---

### Delta Parquet Format (Delta Lake)

**Definition:**
Delta Parquet (commonly called Delta Lake) is an open-source storage layer that brings ACID transactions, scalable metadata handling, and time travel capabilities to data lakes. It uses Parquet files for data storage and adds a transaction log (JSON files) to provide database-like reliability on top of cloud object storage. Delta Lake is the default storage format in Microsoft Fabric's OneLake.

**Key Concepts:**
- **Transaction Log:** Delta log (_delta_log folder) tracks all changes as JSON files, enabling ACID properties
- **ACID Transactions:** Atomicity, Consistency, Isolation, Durability for data lake operations
- **Time Travel:** Query historical versions of data using version numbers or timestamps
- **Schema Enforcement:** Prevents incompatible data writes
- **Schema Evolution:** Safe schema changes (add/rename columns) without breaking queries
- **Optimized Writes:** Automatic file sizing, compaction, and Z-ordering for performance
- **MERGE/UPSERT Support:** Efficient updates and deletes (unlike plain Parquet)
- **Vacuum:** Clean up old data files to save storage
- **Open Format:** Based on Parquet, works with Spark, Databricks, Fabric, and other engines

**File Structure:**
```
my_delta_table/
├── _delta_log/
│   ├── 00000000000000000000.json  (transaction 0)
│   ├── 00000000000000000001.json  (transaction 1)
│   ├── 00000000000000000010.checkpoint.parquet
│   └── _last_checkpoint
├── part-00000-xxx.snappy.parquet  (data file 1)
├── part-00001-xxx.snappy.parquet  (data file 2)
└── part-00002-xxx.snappy.parquet  (data file 3)
```

**Use Cases:**
- Production data lakes requiring reliability
- Streaming data pipelines with updates
- Lakehouse architectures (OneLake, Databricks)
- CDC (Change Data Capture) implementations
- Data versioning and audit trails
- Replacing traditional data warehouses with lake-based solutions

**Interview Questions:**

1. **Q:** What exactly is Delta Parquet format and how does it differ from regular Parquet?
   **A:** 
   Delta Parquet (Delta Lake) is **Parquet files + transaction log + metadata management**.
   
   **Regular Parquet:**
   - Just .parquet files containing data
   - No transaction support
   - Append-only (can't update/delete efficiently)
   - No versioning or time travel
   - Schema changes are risky
   
   **Delta Parquet (Delta Lake):**
   - Parquet files for data PLUS _delta_log folder
   - ACID transactions via transaction log
   - Efficient MERGE, UPDATE, DELETE operations
   - Time travel to any previous version
   - Schema enforcement and evolution
   - Metadata for optimization (statistics, min/max values)
   
   **Example:** 
   - **Parquet:** Update a row = read entire file, modify, write new file, delete old (error-prone)
   - **Delta:** Update tracked in log, old versions kept, atomic operation, can rollback

2. **Q:** How does Delta Lake provide ACID transactions on cloud object storage?
   **A:** 
   Delta Lake uses **optimistic concurrency control** with a transaction log:
   
   **Write Process:**
   1. **Read:** Check current version in _delta_log
   2. **Prepare:** Write new Parquet files to storage
   3. **Commit:** Write atomic JSON log entry (version N+1)
   4. **Verify:** Check if version N+1 was successfully written
   5. **Conflict:** If another process wrote N+1, retry with new version
   
   **ACID Properties:**
   - **Atomicity:** Either entire log entry commits or nothing does
   - **Consistency:** Schema validation before commit
   - **Isolation:** Readers see consistent snapshots, writers use versioning
   - **Durability:** Cloud storage guarantees once log entry is written
   
   **Example Scenario:**
   - User A writes update at 2:00 PM → creates version 15
   - User B writes simultaneously → creates version 16
   - Both succeed because writes are to different versions
   - Readers can choose to read v15 or v16

3. **Q:** What is Time Travel in Delta Lake and what are its use cases?
   **A:** 
   Time Travel allows querying previous versions of data using version number or timestamp.
   
   **How it works:**
   - Every change creates a new version in transaction log
   - Old Parquet files are retained (until VACUUM)
   - Query syntax: `SELECT * FROM table VERSION AS OF 10` or `TIMESTAMP AS OF '2026-04-01'`
   
   **Use Cases:**
   1. **Audit & Compliance:** "Show me data as it was on Dec 31 for year-end reporting"
   2. **Reproduce ML Models:** "Train model with exact data version used in production"
   3. **Debugging:** "What did the data look like before the bad update?"
   4. **Rollback:** "Restore table to version before the mistake"
   5. **A/B Testing:** "Compare metrics between old and new data versions"
   
   **Example:**
   ```sql
   -- Current data
   SELECT COUNT(*) FROM sales;  -- 1000 rows
   
   -- Data as of yesterday
   SELECT COUNT(*) FROM sales VERSION AS OF 25;  -- 950 rows
   
   -- Data from specific date
   SELECT * FROM sales TIMESTAMP AS OF '2026-04-20';
   ```

4. **Q:** What are MERGE operations in Delta Lake and why are they important?
   **A:** 
   MERGE (UPSERT) allows combining INSERT, UPDATE, and DELETE in a single atomic operation.
   
   **SQL Syntax:**
   ```sql
   MERGE INTO target_table AS target
   USING source_table AS source
   ON target.id = source.id
   WHEN MATCHED THEN 
     UPDATE SET target.value = source.value
   WHEN NOT MATCHED THEN
     INSERT (id, value) VALUES (source.id, source.value)
   ```
   
   **Why Important:**
   - **CDC Implementation:** Apply change streams from databases
   - **SCD Type 2:** Slowly Changing Dimensions updates
   - **Deduplication:** Remove duplicates while preserving latest records
   - **Incremental Loads:** Efficiently update only changed records
   
   **Example Use Case - Daily Updates:**
   - Source system has 10 million records
   - Only 50,000 changed today
   - **Without MERGE:** Rewrite entire 10M table
   - **With MERGE:** Update only 50K, takes seconds instead of hours

5. **Q:** What is the difference between Checkpoints and Transaction Log files in Delta Lake?
   **A:** 
   
   **Transaction Log Files (.json):**
   - One file per transaction (version)
   - Contains: added files, removed files, metadata changes, schema
   - Small files (few KB each)
   - **Problem:** Reading 10,000 transactions = reading 10,000 JSON files (slow!)
   
   **Checkpoint Files (.checkpoint.parquet):**
   - Aggregate state created every 10 versions (configurable)
   - Single Parquet file containing entire table state at that version
   - Faster to read than thousands of JSON files
   - Includes: all active files, schema, metadata, statistics
   
   **How They Work Together:**
   ```
   _delta_log/
   ├── 00000.json         (version 0)
   ├── 00001.json         (version 1)
   ├── ...
   ├── 00009.json         (version 9)
   ├── 00010.checkpoint.parquet  (checkpoint at v10)
   ├── 00011.json         (version 11)
   └── 00012.json         (version 12)
   ```
   
   **Reading Version 12:**
   - Read checkpoint at v10 (fast, one file)
   - Read json files 11, 12 (just 2 files)
   - **Result:** Much faster than reading 13 JSON files
   
   **Benefits:**
   - Faster query planning
   - Improved metadata scalability
   - Reduced latency for file listing

6. **Q:** What is OPTIMIZE and Z-ORDERING in Delta Lake?
   **A:** 
   
   **OPTIMIZE (Compaction):**
   - Combines small files into larger, optimally-sized files
   - **Problem:** Many small files = slow queries (metadata overhead)
   - **Solution:** Compact into fewer, larger files (128MB - 1GB each)
   
   ```sql
   OPTIMIZE my_table;
   ```
   
   **Z-ORDERING (Data Clustering):**
   - Co-locates related data in same files using multi-dimensional clustering
   - Improves data skipping (skip entire files based on filters)
   
   ```sql
   OPTIMIZE my_table ZORDER BY (date, customer_id);
   ```
   
   **How Z-Order Works:**
   - Uses space-filling curves to cluster multi-dimensional data
   - Data with similar values in multiple columns stored together
   - Maximizes data skipping for common filter patterns
   
   **Example Scenario:**
   - Table: sales with 1 billion rows
   - Query: `WHERE date = '2026-04-24' AND region = 'West'`
   - **Without Z-Order:** Scan many files
   - **With Z-Order on (date, region):** Skip 95% of files, read only relevant ones
   - **Result:** 10-100x faster queries
   
   **Best Practices:**
   - Run OPTIMIZE regularly (weekly/monthly)
   - Z-Order by frequently filtered columns (2-4 columns max)
   - Monitor small file count and optimize when too many

7. **Q:** How does VACUUM work in Delta Lake and when should you use it?
   **A:** 
   VACUUM removes old data files that are no longer referenced by the transaction log.
   
   **Why Needed:**
   - Time Travel and versioning keep old files
   - Updates/Deletes create new files, old ones remain
   - Storage costs increase over time
   
   **Syntax:**
   ```sql
   VACUUM my_table RETAIN 168 HOURS;  -- Keep 7 days
   ```
   
   **How it Works:**
   1. Reads transaction log to find active files
   2. Lists all Parquet files in directory
   3. Deletes files not in active set AND older than retention period
   4. **Default retention:** 7 days (168 hours)
   
   **Important Considerations:**
   - **Time Travel Impact:** Can't query versions older than retention period after VACUUM
   - **Running Jobs:** Set retention > longest running query duration
   - **Compliance:** Consider regulatory requirements for data retention
   
   **Best Practices:**
   - Run VACUUM monthly/quarterly
   - Set retention based on:
     * Longest query runtime (streaming jobs might run days)
     * Time Travel requirements
     * Compliance needs
   - **Example:** If monthly reports query last 30 days, set retention ≥ 30 days
   
   **Safety:**
   - Delta Lake prevents VACUUM with retention < 7 days by default
   - Can override but risky (might delete files from active queries)

---

### Data Serving in Fabric

**Definition:**
Data Serving in Microsoft Fabric refers to the layer and mechanisms that expose data to end users, applications, and BI tools for consumption and analysis. It encompasses how data is made accessible through various endpoints like SQL, APIs, Power BI semantic models, and DirectLake, enabling different consumption patterns from interactive queries to real-time dashboards.

**Key Concepts:**
- **Multiple Serving Endpoints:** SQL endpoint, Power BI, GraphQL API, OneLake API
- **DirectLake Mode:** Power BI reads directly from Delta Parquet files without importing or caching
- **SQL Analytics Endpoint:** Auto-generated T-SQL endpoint for every Lakehouse and Warehouse
- **Semantic Models (Datasets):** Business logic layer for consistent reporting
- **Row-Level Security (RLS):** Data security at serving layer
- **Query Performance:** Optimizations like caching, materialized views, aggregations
- **Compute Separation:** Serving layer compute separate from storage (OneLake)
- **Auto-scaling:** Automatic resource scaling based on query demand

**Serving Patterns in Fabric:**

1. **Lakehouse SQL Endpoint**
   - Read-only SQL endpoint auto-created with every Lakehouse
   - Queries Delta tables using T-SQL
   - Connects via standard SQL clients (SSMS, Azure Data Studio)

2. **Data Warehouse**
   - Dedicated SQL engine optimized for analytics
   - Supports read/write operations, transactions
   - T-SQL compatibility with SQL Server

3. **Power BI DirectLake**
   - Reads Parquet files directly from OneLake
   - No import or duplication needed
   - Near real-time performance
   - Automatic query optimization

4. **KQL Database (Real-Time Analytics)**
   - Serves streaming and time-series data
   - Kusto Query Language interface
   - Sub-second query latency

5. **API/GraphQL**
   - REST and GraphQL APIs for application integration
   - Programmatic data access

**Use Cases:**
- Self-service BI with Power BI reports
- Ad-hoc SQL queries from analysts
- Application integration via APIs
- Real-time dashboards and monitoring
- Data sharing across organizations
- Embedded analytics in custom applications

**Interview Questions:**

1. **Q:** What is Data Serving in Microsoft Fabric and why is it important?
   **A:** 
   Data Serving is the consumption layer that makes data accessible to end users and applications after it's been ingested and transformed. It's the "last mile" of the analytics pipeline.
   
   **Importance:**
   - **Accessibility:** Makes data available in formats users understand (SQL, Power BI, APIs)
   - **Performance:** Optimizes query execution for different workload patterns
   - **Security:** Enforces access controls and data governance
   - **Flexibility:** Supports multiple tools and consumption patterns
   - **Scalability:** Handles concurrent users without performance degradation
   
   **Analogy:** 
   - Data Engineering = Kitchen (data preparation)
   - Data Serving = Restaurant (how customers consume the food)
   - Different customers want different experiences (SQL analysts, Power BI users, applications)
   
   **Example:**
   - Raw sales data → Lakehouse (stored)
   - Transformed into aggregated tables → Data Engineering
   - Served via:
     * Power BI reports for executives
     * SQL queries for analysts
     * REST API for mobile app
     * Real-time dashboard for operations

2. **Q:** What is DirectLake in Power BI and how does it differ from Import and DirectQuery modes?
   **A:** 
   DirectLake is a revolutionary Power BI mode that reads directly from Delta Parquet files in OneLake without copying data.
   
   **Comparison:**
   
   | Feature | Import | DirectQuery | DirectLake |
   |---------|--------|-------------|------------|
   | **Data Location** | Copied to Power BI | Source database | OneLake Delta files |
   | **Refresh Needed** | Yes (scheduled) | No (live) | No (always current) |
   | **Performance** | Fastest | Slower | Very Fast |
   | **Data Freshness** | Refresh interval | Real-time | Near real-time |
   | **Size Limits** | Limited by capacity | Unlimited | Very large datasets |
   | **Query Engine** | Vertipaq (in-memory) | Source SQL | Fabric SQL engine |
   
   **How DirectLake Works:**
   1. Power BI reads metadata from Delta Lake transaction log
   2. Queries optimized and pushed to Fabric SQL engine
   3. SQL engine reads only needed Parquet files
   4. Results returned to Power BI
   5. Frequently accessed data cached in memory
   
   **Benefits:**
   - **No Duplication:** Single copy in OneLake
   - **Always Fresh:** See latest data without refresh
   - **Fast:** Columnar Parquet + caching + optimization
   - **Large Scale:** Handle billion-row tables
   
   **When to Use:**
   - **Import:** Small datasets (<1GB), need offline access
   - **DirectQuery:** Live connections to external databases, very fresh data required
   - **DirectLake:** Large Fabric datasets, need balance of performance and freshness

3. **Q:** What is the SQL Analytics Endpoint in Lakehouse and how does it work?
   **A:** 
   SQL Analytics Endpoint is an automatically generated, read-only T-SQL endpoint created for every Lakehouse, allowing SQL-based querying of Delta tables.
   
   **How It Works:**
   - **Automatic Creation:** Created when you create a Lakehouse
   - **Metadata Sync:** Automatically discovers tables in Lakehouse
   - **Read-Only:** SELECT queries only (INSERT/UPDATE/DELETE not supported)
   - **T-SQL Compatible:** Works with SQL Server Management Studio, Azure Data Studio, etc.
   - **Delta Table Access:** Queries Delta tables directly
   - **Views & Functions:** Can create SQL views and functions
   
   **Architecture:**
   ```
   OneLake (Delta Tables)
        ↓
   SQL Analytics Endpoint (Auto-generated)
        ↓
   SQL Clients (SSMS, Power BI, Excel, etc.)
   ```
   
   **Features:**
   - **Auto-discovery:** New tables appear automatically
   - **Schema inference:** Reads schema from Delta metadata
   - **Performance:** Optimized query execution
   - **Security:** Inherits workspace permissions
   
   **Example Usage:**
   ```sql
   -- Connect via SQL client
   SELECT 
       product_category,
       SUM(sales_amount) as total_sales
   FROM sales_fact
   WHERE order_date >= '2026-01-01'
   GROUP BY product_category
   ORDER BY total_sales DESC;
   ```
   
   **Use Cases:**
   - Ad-hoc analysis by SQL-savvy analysts
   - Connecting BI tools (Tableau, Qlik, etc.)
   - Data validation and quality checks
   - Creating reusable SQL views for reporting

4. **Q:** What is the difference between a Lakehouse and a Data Warehouse in Fabric for data serving?
   **A:** 
   Both serve data, but they're optimized for different use cases:
   
   **Lakehouse:**
   - **Storage Format:** Delta Parquet in OneLake
   - **SQL Access:** Read-only SQL Analytics Endpoint
   - **Data Types:** Structured, semi-structured, unstructured
   - **Write Operations:** Via Spark, Dataflow, Pipeline (not via SQL)
   - **Use Case:** Data lake + SQL analytics, data science, ML
   - **Flexibility:** Schema-on-read, flexible schema evolution
   - **Performance:** Optimized for large-scale scans
   
   **Data Warehouse:**
   - **Storage Format:** Managed SQL database (on Delta)
   - **SQL Access:** Full T-SQL read/write support
   - **Data Types:** Structured data
   - **Write Operations:** INSERT, UPDATE, DELETE via SQL
   - **Use Case:** Traditional BI, reporting, dashboards
   - **Structure:** Schema-on-write, enforced schema
   - **Performance:** Optimized for joins, aggregations
   
   **When to Use:**
   
   **Choose Lakehouse if:**
   - Working with diverse data types (JSON, logs, images)
   - Need data science/ML capabilities
   - Prefer Spark-based transformations
   - Want storage flexibility
   - Building medallion architecture (Bronze/Silver/Gold)
   
   **Choose Warehouse if:**
   - Traditional SQL-based workflows
   - Need SQL write operations
   - Serving structured reports/dashboards
   - Team familiar with SQL Server/Synapse
   - Need stored procedures, triggers
   
   **Common Pattern:**
   - Lakehouse for data engineering (Bronze → Silver)
   - Warehouse for serving layer (Gold/consumption)

5. **Q:** How does Row-Level Security (RLS) work in Fabric's data serving layer?
   **A:** 
   Row-Level Security restricts data access at the row level based on user identity, ensuring users only see data they're authorized to view.
   
   **Implementation Levels:**
   
   **1. Power BI Semantic Model RLS:**
   ```DAX
   -- Define role in Power BI
   [Region] = USERPRINCIPALNAME()
   
   -- Or using static filter
   [SalesRegion] = "West"
   ```
   
   **2. Warehouse/Lakehouse RLS (SQL):**
   ```sql
   -- Create security predicate function
   CREATE FUNCTION dbo.fn_securitypredicate(@Region AS nvarchar(100))
       RETURNS TABLE
   WITH SCHEMABINDING
   AS
       RETURN SELECT 1 AS fn_securitypredicate_result
       WHERE @Region = USER_NAME() 
          OR IS_MEMBER('db_owner') = 1;
   
   -- Apply security policy
   CREATE SECURITY POLICY RegionPolicy
   ADD FILTER PREDICATE dbo.fn_securitypredicate(Region)
   ON dbo.Sales;
   ```
   
   **How It Works:**
   1. User queries data
   2. System identifies user (Azure AD)
   3. Security policy applies filter automatically
   4. User only sees filtered results
   5. Transparent to user (looks like normal query)
   
   **Best Practices:**
   - **Consistent Implementation:** Apply RLS at serving layer (Power BI or SQL)
   - **Test Thoroughly:** Verify with different user accounts
   - **Performance:** Index columns used in RLS filters
   - **Documentation:** Document security rules clearly
   - **Audit:** Log and monitor RLS policy usage
   
   **Example Scenario:**
   - Sales table has 1M rows
   - RLS policy: `[Region] = USERPRINCIPALNAME()`
   - User "west@company.com" queries: sees only West region rows
   - User "admin@company.com" in Admin role: sees all rows

6. **Q:** What performance optimization techniques are used in Fabric's data serving layer?
   **A:** 
   
   **1. Columnar Storage (Parquet):**
   - Read only required columns
   - Better compression ratios
   - Optimized for analytics
   
   **2. Data Skipping:**
   - Min/Max statistics per file
   - Skip entire files based on filters
   - Parquet metadata enables predicate pushdown
   
   **3. Caching:**
   - **Result Cache:** Repeated queries return cached results
   - **DirectLake Cache:** Frequently accessed data in memory
   - **Metadata Cache:** Schema and statistics cached
   
   **4. Partitioning:**
   ```python
   # Partition by date for better query performance
   df.write.partitionBy("order_date").format("delta").save("sales")
   ```
   - Skip entire partitions based on filters
   - `WHERE order_date = '2026-04-24'` reads only that partition
   
   **5. Z-Ordering/Clustering:**
   ```sql
   OPTIMIZE sales_table ZORDER BY (customer_id, product_id);
   ```
   - Co-locate related data
   - Maximize data skipping effectiveness
   
   **6. Materialized Views:**
   ```sql
   -- Pre-compute expensive aggregations
   CREATE MATERIALIZED VIEW sales_summary AS
   SELECT 
       product_id,
       SUM(amount) as total_sales,
       COUNT(*) as order_count
   FROM sales
   GROUP BY product_id;
   ```
   
   **7. Aggregations (Power BI):**
   - Pre-aggregated tables for common queries
   - Automatic aggregation routing
   - Reduces data scanned
   
   **8. Query Folding:**
   - Push filters/transformations to source
   - Reduce data movement
   - Let SQL engine do heavy lifting
   
   **Real-World Example:**
   - **Original Query:** 1 billion rows, 60 seconds
   - **After Partitioning:** Scan 100M rows (date filter), 15 seconds
   - **After Z-Ordering:** Skip 80% of partitioned data, 5 seconds
   - **With Caching:** Subsequent runs, <1 second
   - **Result:** 60x improvement

7. **Q:** How do you choose the right data serving option in Fabric for different scenarios?
   **A:** 
   
   **Decision Matrix:**
   
   **Use Lakehouse SQL Endpoint when:**
   - Ad-hoc SQL analysis by data analysts
   - Read-only querying of Delta tables
   - Connecting third-party BI tools
   - Data exploration and validation
   - **Example:** Analyst running exploratory queries in Azure Data Studio
   
   **Use Data Warehouse when:**
   - Need SQL write operations (INSERT/UPDATE/DELETE)
   - Building traditional star schema
   - SQL Server expertise in team
   - Stored procedures and T-SQL logic required
   - **Example:** ETL loading dimension tables via SQL scripts
   
   **Use Power BI DirectLake when:**
   - Interactive dashboards and reports
   - Large datasets (billions of rows)
   - Need near real-time data freshness
   - Self-service BI scenarios
   - **Example:** Executive dashboard showing today's sales
   
   **Use KQL Database when:**
   - Time-series or log data
   - Sub-second query latency required
   - Streaming analytics
   - IoT or telemetry data
   - **Example:** Real-time application monitoring dashboard
   
   **Use REST/GraphQL API when:**
   - Application integration
   - Mobile app data access
   - Microservices architecture
   - External system integration
   - **Example:** Mobile sales app fetching customer data
   
   **Combined Pattern Example:**
   ```
   IoT Sensors → KQL Database (real-time serving)
        ↓
   Batch Processing → Lakehouse (data engineering)
        ↓
   Aggregations → Warehouse (star schema)
        ↓
   Power BI DirectLake (executive dashboards)
        ↓
   REST API (mobile app integration)
   ```

---

### KQL Database (Kusto/Real-Time Analytics)

**Definition:**
KQL Database (Kusto Query Language Database) is Microsoft Fabric's real-time analytics engine optimized for high-velocity streaming data, time-series data, and log analytics. Based on Azure Data Explorer technology, it provides sub-second query performance on petabytes of data with automatic ingestion, indexing, and retention management. It's specifically designed for telemetry, IoT, logs, and observability scenarios.

**Key Concepts:**

**1. Architecture:**
- **Columnar Storage:** Data stored in compressed columnar format for fast analytics
- **Auto-Indexing:** Automatic indexing of all data for optimal query performance
- **Hot/Cold Tiering:** Frequently accessed data in SSD (hot), older data in blob storage (cold)
- **Streaming Ingestion:** Real-time data ingestion with <1 second latency
- **Query Engine:** Highly optimized for time-series and log analytics

**2. KQL (Kusto Query Language):**
- **Pipe-based Syntax:** Query operations chained with pipe `|` operator
- **Rich Functions:** Time-series functions, string manipulation, statistical functions
- **Aggregation-first:** Optimized for summarization and aggregations
- **Intuitive:** Easy to learn, similar to SQL but more concise for analytics

**3. Data Organization:**
- **Database:** Container for tables and functions
- **Tables:** Store data in columnar format
- **Materialized Views:** Pre-computed aggregations
- **Functions:** Reusable KQL query logic
- **External Tables:** Query data from OneLake/ADLS without ingestion

**4. Ingestion Methods:**
- **Streaming:** Event Hubs, IoT Hub, Kafka
- **Batch:** Blob storage, OneLake files
- **One-click:** Direct from applications via SDK
- **Eventstream:** Fabric's streaming connector

**5. Performance Features:**
- **Extent (Shard) Management:** Data automatically organized into extents (row groups)
- **Data Sharding Policy:** Distributes data across cluster nodes
- **Caching Policy:** Control hot cache duration
- **Retention Policy:** Automatic data lifecycle management
- **Partitioning:** Logical data organization for query optimization

**Use Cases:**
- Real-time application monitoring and observability
- IoT telemetry analytics (sensors, devices)
- Log analytics and security monitoring (SIEM)
- Time-series analytics (metrics, performance data)
- Click-stream analysis for web analytics
- Financial market data analysis
- Network traffic monitoring
- Gaming telemetry and player analytics

**KQL Query Examples:**

```kql
// Basic query - show recent logs
ApplicationLogs
| where Timestamp > ago(1h)
| where Level == "Error"
| project Timestamp, Message, UserID
| take 100

// Time series aggregation
SensorData
| where Timestamp > ago(24h)
| summarize AvgTemp = avg(Temperature), MaxTemp = max(Temperature) 
  by bin(Timestamp, 5m), SensorID
| render timechart

// Join and complex analysis
WebLogs
| where Timestamp > ago(7d)
| join kind=inner (
    Users
    | project UserID, Country, Tier
  ) on UserID
| summarize PageViews = count() by Country, Tier
| top 10 by PageViews desc
```

**Interview Questions:**

1. **Q:** What is a KQL Database and how does it differ from traditional SQL databases?
   **A:** 
   KQL Database is a real-time analytics engine optimized for append-only, time-series data, whereas traditional SQL databases are designed for transactional workloads.
   
   **KQL Database:**
   - **Workload:** Analytics on streaming/time-series data
   - **Write Pattern:** Append-only (immutable data)
   - **Query Pattern:** Aggregations, time-series analysis, log searches
   - **Performance:** Sub-second queries on billions of rows
   - **Data Volume:** Optimized for petabytes
   - **Schema:** Schema-on-write with dynamic columns support
   - **Indexing:** Automatic full-text indexing on all columns
   - **Latency:** <1 second ingestion latency
   
   **Traditional SQL Database:**
   - **Workload:** OLTP (transactions) or OLAP (warehouse)
   - **Write Pattern:** CRUD operations (insert, update, delete)
   - **Query Pattern:** Joins, lookups, transactions
   - **Performance:** Optimized for row lookups and updates
   - **Data Volume:** Typically gigabytes to terabytes
   - **Schema:** Strict schema enforcement
   - **Indexing:** Manual index creation
   - **Latency:** Batch or micro-batch ingestion
   
   **When to Choose KQL Database:**
   - Real-time monitoring dashboards
   - Log aggregation from thousands of sources
   - IoT data from millions of devices
   - Time-series analytics
   - High-velocity data ingestion (millions of events/sec)
   
   **Example Scenario:**
   - **Need:** Monitor 10,000 servers, each sending metrics every 10 seconds
   - **Data Rate:** 1 million events per second
   - **Query:** "Show CPU spike patterns in the last hour"
   - **KQL Database:** Perfect fit, <1 sec query time
   - **SQL Database:** Would struggle with ingestion rate and query performance

2. **Q:** Explain the Hot/Cold data tiering in KQL Database and its benefits?
   **A:** 
   Hot/Cold tiering automatically manages data placement based on age and access patterns to optimize cost and performance.
   
   **Hot Cache (SSD Storage):**
   - **Location:** Local SSD on cluster nodes
   - **Access Speed:** Extremely fast (<10ms latency)
   - **Size:** Limited (user-configured, e.g., 30 days)
   - **Cost:** Higher (premium storage)
   - **Use:** Recent, frequently queried data
   
   **Cold Storage (Blob Storage):**
   - **Location:** Azure Blob Storage / OneLake
   - **Access Speed:** Slower (~100ms+ latency)
   - **Size:** Unlimited (retention policy, e.g., 365 days)
   - **Cost:** Much lower
   - **Use:** Historical data, infrequent queries
   
   **How It Works:**
   ```kql
   // Set caching policy - keep 30 days in hot cache
   .alter table SensorData policy caching hot = 30d
   
   // Set retention policy - keep 365 days total
   .alter table SensorData policy retention softdelete = 365d
   ```
   
   **Automatic Behavior:**
   1. Data ingested → Immediately in hot cache
   2. After 30 days → Moves to cold storage (still queryable)
   3. After 365 days → Automatically deleted
   4. **Query Transparency:** Queries work seamlessly across both tiers
   
   **Benefits:**
   - **Cost Optimization:** 90% of storage in cheap cold tier
   - **Performance:** 90% of queries hit recent data in hot cache (fast)
   - **Automatic:** No manual data movement required
   - **Lifecycle Management:** Set-and-forget retention
   
   **Example:**
   - **Hot cache:** 30 days = 100 GB on SSD = $500/month
   - **Cold storage:** 335 days = 1.1 TB on blob = $30/month
   - **Total cost:** $530/month vs $5000/month if all on SSD
   - **Query performance:** 95% of queries hit hot cache (fast)

3. **Q:** What is KQL (Kusto Query Language) and how is it different from SQL?
   **A:** 
   KQL is a read-only, pipe-based query language optimized for analytics, designed to be more intuitive for data exploration than SQL.
   
   **Key Differences:**
   
   **Syntax Style:**
   ```kql
   // KQL - Pipe-based, top-down flow
   SalesData
   | where Country == "USA"
   | where Amount > 1000
   | summarize TotalSales = sum(Amount) by ProductID
   | order by TotalSales desc
   | take 10
   ```
   
   ```sql
   -- SQL - Clause-based
   SELECT TOP 10 
       ProductID, 
       SUM(Amount) AS TotalSales
   FROM SalesData
   WHERE Country = 'USA' 
       AND Amount > 1000
   GROUP BY ProductID
   ORDER BY TotalSales DESC
   ```
   
   **Differences:**
   
   | Aspect | KQL | SQL |
   |--------|-----|-----|
   | **Read/Write** | Read-only | Read & Write |
   | **Flow** | Top-down (pipes) | Declarative |
   | **Learning Curve** | Easier for analysts | Requires SQL knowledge |
   | **Time-series** | Built-in functions | Complex queries |
   | **String Search** | Native full-text | Limited (LIKE) |
   | **Aggregations** | Optimized | Standard |
   | **Visualization** | Built-in rendering | External tools |
   
   **KQL Strengths:**
   
   1. **Time-series Functions:**
   ```kql
   // Find anomalies in CPU usage
   CPUMetrics
   | make-series AvgCPU = avg(CPU) on Timestamp step 5m
   | extend anomalies = series_decompose_anomalies(AvgCPU, 1.5)
   ```
   
   2. **String Operations:**
   ```kql
   // Full-text search
   Logs
   | where * has "error"  // Searches all columns
   | parse Message with "User: " UserID " failed at " FailedStep
   ```
   
   3. **Statistical Functions:**
   ```kql
   // Calculate percentiles
   ResponseTimes
   | summarize percentiles(Duration, 50, 90, 95, 99) by APIEndpoint
   ```
   
   4. **Built-in Visualization:**
   ```kql
   // Render chart directly
   | render timechart
   | render piechart
   | render columnchart
   ```
   
   **When to Use Each:**
   - **KQL:** Log analysis, time-series, real-time monitoring, data exploration
   - **SQL:** Business reporting, data warehousing, transactional queries

4. **Q:** How does data ingestion work in KQL Database?
   **A:** 
   KQL Database supports multiple ingestion methods optimized for different scenarios:
   
   **1. Streaming Ingestion (Real-time):**
   ```
   Event Source (IoT Hub, Event Hubs, Kafka)
        ↓
   KQL Database (Streaming endpoint)
        ↓
   <1 second latency
        ↓
   Data available for querying
   ```
   
   **Characteristics:**
   - **Latency:** <1 second from ingestion to query
   - **Use Case:** Real-time dashboards, alerts
   - **Data Volume:** Millions of events per second
   - **Batching:** Micro-batches (1-2 seconds) automatically
   
   **2. Batch Ingestion (High-throughput):**
   ```kql
   // Ingest from blob storage
   .ingest into table SensorData 
   ('https://storage.blob.core.windows.net/data/sensors.csv')
   with (format='csv')
   ```
   
   **Characteristics:**
   - **Latency:** Few seconds to minutes
   - **Use Case:** Historical data loads, backfills
   - **Optimization:** Automatically batches for optimal extent size
   
   **3. Queued Ingestion (Default):**
   - Ingestion queued and batched (optimal for throughput)
   - Latency: 30 seconds - 5 minutes
   - Automatically optimizes extent size (1GB recommended)
   - **Best for:** Most scenarios balancing latency and performance
   
   **4. Eventstream (Fabric Integration):**
   ```
   Fabric Eventstream
        ↓ (Configure transformation)
   KQL Database (Destination)
        ↓
   Real-time processing
   ```
   
   **Data Flow:**
   1. **Receive:** Data arrives at ingestion endpoint
   2. **Queue:** Data queued in ingestion queue
   3. **Batch:** System batches data optimally (~1GB extents)
   4. **Compress:** Data compressed (typically 10:1 ratio)
   5. **Index:** Full-text indexing on all columns
   6. **Seal:** Extent sealed and replicated
   7. **Query:** Data immediately available
   
   **Ingestion Mapping:**
   ```kql
   // Define how to map JSON to table
   .create table Logs ingestion json mapping 'LogMapping'
   ```
   [
       {"column": "Timestamp", "path": "$.time"},
       {"column": "Level", "path": "$.severity"},
       {"column": "Message", "path": "$.msg"}
   ]
   ```
   
   **Best Practices:**
   - Use streaming for <1 sec latency needs
   - Use queued for optimal throughput (default)
   - Batch size: Aim for 100MB-1GB per batch
   - Compression: Use compressed format (Parquet, JSON.gz)
   - Partitioning: Partition large batches by time

5. **Q:** What are Extents in KQL Database and why are they important?
   **A:** 
   Extents (also called shards or data shards) are immutable, compressed data chunks that form the fundamental storage and processing unit in KQL Database.
   
   **What is an Extent:**
   - **Definition:** Horizontal partition of table data (like row groups in Parquet)
   - **Size:** Typically 1GB uncompressed (~100MB compressed)
   - **Content:** Columnar data + indexes + metadata
   - **Immutability:** Once sealed, never modified
   - **Lifecycle:** Created → Indexed → Merged → Eventually deleted
   
   **Extent Structure:**
   ```
   Extent (Shard)
   ├── Columnar Data (compressed)
   ├── Full-text Inverted Index
   ├── Column Indexes
   ├── Metadata (min/max, statistics)
   ├── Extent Tags (for data management)
   └── Creation Time & ID
   ```
   
   **Why Extents Matter:**
   
   **1. Query Performance:**
   - **Data Skipping:** Query engine reads extent metadata first
   - **Pruning:** Skips extents that don't match filter criteria
   - **Parallelization:** Each extent processed independently
   
   **Example:**
   ```kql
   Logs
   | where Timestamp between (datetime(2026-04-24) .. datetime(2026-04-25))
   | where Level == "Error"
   ```
   - 1000 extents in table
   - 980 extents outside date range → **skipped** (metadata check)
   - 20 extents in range → **scanned**
   - Result: 50x faster query
   
   **2. Data Organization:**
   - **Merge Policy:** Small extents automatically merged into larger ones
   - **Optimal Size:** Maintains ~1GB extent size for best performance
   - **Partitioning:** Data naturally partitioned by ingestion time
   
   **3. Management Operations:**
   ```kql
   // View extent information
   .show table MyTable extents
   
   // Manually merge extents
   .merge extents from MyTable
   
   // Add tags for data management
   .alter-merge table MyTable extent tags 
       ('drop-by:2026-12-31') 
   <| MyTable | where CreatedDate < datetime(2026-01-01)
   ```
   
   **Extent Lifecycle:**
   ```
   Ingestion → Small Extents (1MB-100MB)
        ↓
   Merge Policy → Merged to ~1GB extents
        ↓
   Hot Cache (if within cache period)
        ↓
   Cold Storage (after cache expiry)
        ↓
   Soft Delete (after retention period)
   ```
   
   **Best Practices:**
   - Monitor extent count (too many = poor performance)
   - Optimal extent size: 100MB-1GB compressed
   - Use extent tags for granular data management
   - Let auto-merge handle extent optimization

6. **Q:** How do you implement real-time alerting and monitoring with KQL Database?
   **A:** 
   
   **Method 1: Scheduled Query (Update Policy)**
   
   Update policies automatically process data as it's ingested:
   
   ```kql
   // Create aggregation table
   .create table ErrorSummary (
       Timestamp: datetime,
       ErrorCount: long,
       ErrorRate: double
   )
   
   // Create update policy - runs on every ingestion
   .alter table ApplicationLogs policy update
   ```
   [
       {
           "IsEnabled": true,
           "Source": "ApplicationLogs",
           "Query": 
               "ApplicationLogs
                | where Level == 'Error'
                | summarize 
                    ErrorCount = count(),
                    ErrorRate = count() * 100.0 / toscalar(ApplicationLogs | count())
                  by bin(Timestamp, 5m)",
           "IsTransactional": false
       }
   ]
   ```
   
   **Method 2: Data Explorer Alerts (Azure)**
   
   ```kql
   // Alert query - runs every 5 minutes
   ApplicationLogs
   | where Timestamp > ago(5m)
   | where Level == "Error"
   | summarize ErrorCount = count() by Application
   | where ErrorCount > 100  // Alert threshold
   ```
   
   **Configuration:**
   - Frequency: Every 5 minutes
   - Threshold: ErrorCount > 100
   - Action: Email, Teams, Logic App
   
   **Method 3: Fabric Eventstream + Reflex (Data Activator)**
   
   ```
   KQL Database
        ↓ (Continuous query)
   Eventstream
        ↓ (Pattern detection)
   Reflex (Data Activator)
        ↓ (Condition met)
   Action (Email, Teams, Power Automate)
   ```
   
   **Real-World Example - Application Monitoring:**
   
   ```kql
   // Monitor API response times
   .create table APIMetrics (
       Timestamp: datetime,
       Endpoint: string,
       Duration: long,
       Status: int
   )
   
   // Real-time aggregation
   .create table APIAlerts (
       Timestamp: datetime,
       Endpoint: string,
       AvgDuration: double,
       P95Duration: long,
       ErrorRate: double
   )
   
   // Update policy for real-time monitoring
   .alter table APIMetrics policy update
   ```
   [
       {
           "Source": "APIMetrics",
           "Query": 
               "APIMetrics
                | where Timestamp > ago(1m)
                | summarize 
                    AvgDuration = avg(Duration),
                    P95Duration = percentile(Duration, 95),
                    ErrorRate = countif(Status >= 500) * 100.0 / count()
                  by bin(Timestamp, 1m), Endpoint
                | where P95Duration > 1000 or ErrorRate > 5"
       }
   ]
   ```
   
   **Dashboard Integration:**
   - Query APIAlerts table in Power BI
   - Set refresh to 1 minute (DirectQuery)
   - Visual alerts when thresholds exceeded
   
   **Best Practices:**
   - Keep alert queries simple and fast
   - Use materialized views for complex aggregations
   - Set appropriate thresholds to avoid alert fatigue
   - Test alert logic before production
   - Include context in alert messages (which server, what metric, etc.)

7. **Q:** What are Materialized Views in KQL Database and when should you use them?
   **A:** 
   Materialized Views are pre-computed aggregations that automatically update as new data arrives, dramatically improving query performance for common analytical patterns.
   
   **Definition:**
   Like Update Policies, but simpler syntax and optimized specifically for aggregations.
   
   **Creating a Materialized View:**
   ```kql
   // Create materialized view
   .create materialized-view HourlyWebStats on table WebLogs
   {
       WebLogs
       | summarize 
           PageViews = count(),
           UniqueUsers = dcount(UserID),
           AvgDuration = avg(Duration)
         by bin(Timestamp, 1h), Page
   }
   ```
   
   **How It Works:**
   1. As data ingested into WebLogs
   2. Aggregation automatically applied
   3. Results stored in HourlyWebStats view
   4. Queries against view return pre-computed results instantly
   
   **Querying:**
   ```kql
   // Query runs against pre-aggregated data (instant)
   HourlyWebStats
   | where Timestamp > ago(7d)
   | summarize TotalPageViews = sum(PageViews) by Page
   | top 10 by TotalPageViews
   
   // Instead of scanning billions of raw rows
   WebLogs  // This would be slow!
   | where Timestamp > ago(7d)
   | summarize PageViews = count() by Page
   | top 10 by PageViews
   ```
   
   **Benefits:**
   - **Performance:** 10-1000x faster queries
   - **Automatic:** Updates happen automatically
   - **Cost:** Reduced compute for repeated queries
   - **Freshness:** Always up-to-date (within seconds)
   
   **When to Use Materialized Views:**
   
   ✅ **Good Use Cases:**
   - Repeated dashboard queries
   - Known aggregation patterns (hourly, daily summaries)
   - Expensive aggregations (dcount, percentiles)
   - High-frequency queries on large datasets
   
   ❌ **Not Recommended:**
   - Ad-hoc, changing query patterns
   - Very granular aggregations (per-second might be too much)
   - Queries on very fresh data (<1 min latency needed)
   - Simple filters without aggregation
   
   **Example - IoT Sensor Analytics:**
   
   ```kql
   // Raw data: 1 billion rows/day, 100 sensors, reading every 10 sec
   .create table SensorReadings (
       Timestamp: datetime,
       SensorID: string,
       Temperature: double,
       Humidity: double
   )
   
   // Materialized view: 5-minute aggregations
   .create materialized-view SensorStats5m on table SensorReadings
   {
       SensorReadings
       | summarize 
           AvgTemp = avg(Temperature),
           MinTemp = min(Temperature),
           MaxTemp = max(Temperature),
           AvgHumidity = avg(Humidity)
         by bin(Timestamp, 5m), SensorID
   }
   
   // Dashboard query (fast - reads aggregated data)
   SensorStats5m
   | where Timestamp > ago(24h)
   | where SensorID == "Sensor001"
   | render timechart with (ytitle="Temperature")
   
   // Original data: 8,640,000 rows → Materialized view: 28,800 rows
   // Query time: 10 seconds → <0.1 seconds (100x faster)
   ```
   
   **Materialized View Management:**
   ```kql
   // Check materialization status
   .show materialized-view HourlyWebStats
   
   // Disable/enable
   .alter materialized-view HourlyWebStats disable
   .alter materialized-view HourlyWebStats enable
   
   // Drop materialized view
   .drop materialized-view HourlyWebStats
   ```
   
   **Best Practices:**
   - Use for queries running >100 times/day
   - Aggregate to reasonable granularity (5min, 1hour, 1day)
   - Monitor materialization lag (should be <1 min)
   - Limit to 3-5 views per table
   - Consider storage cost (views store additional data)

---

### Data Ingestion in Fabric

**Definition:**
Data Ingestion in Microsoft Fabric is the process of bringing data from various sources (databases, files, APIs, streams) into OneLake for storage, transformation, and analysis. Fabric provides multiple ingestion methods optimized for different scenarios - from batch data loads to real-time streaming, from code-based ingestion to low-code visual tools.

**Key Ingestion Methods:**

**1. Data Pipeline (Copy Activity)**
- Visual, low-code ETL/ELT tool
- Based on Azure Data Factory
- Drag-and-drop interface
- 100+ built-in connectors

**2. Dataflow Gen2**
- Power Query-based data transformation
- Self-service data preparation
- Visual transformation interface
- Mashup and shape data

**3. Eventstream**
- Real-time data ingestion
- Streaming data from Event Hubs, IoT Hub, Kafka
- No-code stream processing
- Route to Lakehouse, KQL Database, or custom destinations

**4. Notebooks (Spark)**
- Code-based ingestion with PySpark/Scala
- Complex transformations
- ML feature engineering
- Programmatic control

**5. OneLake Shortcuts**
- Virtualize external data without copying
- Connect to ADLS Gen2, S3, Dataverse
- Zero data movement
- Unified namespace

**6. Direct Upload**
- Upload files to Lakehouse
- Manual file drops
- Small datasets

**7. COPY INTO (SQL)**
- T-SQL command for bulk load
- Load from OneLake or external storage
- Warehouse ingestion

**Data Sources Supported:**
- **Databases:** SQL Server, Oracle, PostgreSQL, MySQL, Cosmos DB, Snowflake, etc.
- **Cloud Storage:** Azure Blob, ADLS Gen2, AWS S3, Google Cloud Storage
- **SaaS Applications:** Salesforce, Dynamics 365, SharePoint, ServiceNow
- **Files:** CSV, JSON, Parquet, Excel, XML, Avro, ORC
- **Streaming:** Event Hubs, IoT Hub, Kafka, Azure Stream Analytics
- **APIs:** REST APIs, OData, Web APIs

**Interview Questions:**

1. **Q:** What are the main data ingestion methods in Microsoft Fabric and when should you use each?
   **A:** 
   Fabric provides multiple ingestion paths optimized for different scenarios:
   
   **Data Pipeline (Copy Activity) - Use When:**
   - **Scenario:** Batch data movement from databases or files
   - **Complexity:** Simple extract and load (minimal transformation)
   - **Volume:** Large datasets (GBs to TBs)
   - **Schedule:** Regular, scheduled batch loads
   - **Skill Level:** Low-code, citizen integrators
   - **Example:** "Copy daily sales from SQL Server to Lakehouse every night at 2 AM"
   
   **Dataflow Gen2 - Use When:**
   - **Scenario:** Self-service data preparation with transformations
   - **Complexity:** Moderate transformations (filter, join, aggregate, pivot)
   - **Volume:** Small to medium datasets (MBs to GBs)
   - **Schedule:** Refresh-based or triggered
   - **Skill Level:** Business analysts familiar with Power Query
   - **Example:** "Pull data from Excel and SharePoint, clean and combine, load to Lakehouse"
   
   **Eventstream - Use When:**
   - **Scenario:** Real-time streaming data ingestion
   - **Complexity:** No-code stream routing and filtering
   - **Volume:** High-velocity continuous data streams
   - **Latency:** <1 second required
   - **Skill Level:** Low-code, stream processing
   - **Example:** "Ingest IoT sensor data from Event Hub to KQL Database in real-time"
   
   **Notebooks (Spark) - Use When:**
   - **Scenario:** Complex transformations, ML feature engineering
   - **Complexity:** Advanced logic, custom algorithms
   - **Volume:** Very large datasets (TBs to PBs)
   - **Schedule:** Programmatic control
   - **Skill Level:** Data engineers/scientists with Python/Scala
   - **Example:** "Read JSON from ADLS, apply complex business logic, write to Delta tables"
   
   **OneLake Shortcuts - Use When:**
   - **Scenario:** Access external data without copying
   - **Complexity:** Read-only access to external sources
   - **Volume:** Any size (no data movement)
   - **Cost:** Minimize storage duplication
   - **Skill Level:** Any (simple setup)
   - **Example:** "Access existing ADLS Gen2 data lake without migrating data"
   
   **Decision Matrix:**
   ```
   Need real-time? → Eventstream
   Complex code-based transformations? → Notebooks
   Self-service data prep? → Dataflow Gen2
   Simple scheduled copy? → Data Pipeline
   Virtualize external data? → Shortcuts
   ```

2. **Q:** Explain Data Pipeline in Fabric and how it differs from Azure Data Factory?
   **A:** 
   Data Pipeline in Fabric is the evolution of Azure Data Factory (ADF), deeply integrated into the Fabric ecosystem.
   
   **Key Components:**
   
   **1. Copy Activity:**
   - Extract data from source
   - Transform (basic column mapping, data type conversion)
   - Load to destination
   
   **2. Data Flow (Mapping Data Flow):**
   - Visual transformation designer
   - Complex transformations using Spark
   - Data quality and cleansing
   
   **3. Activities:**
   - Execute Notebook
   - Stored Procedure
   - Web Activity (call APIs)
   - Script Activity (run SQL)
   - Lookup, ForEach, If Condition
   
   **Example Pipeline:**
   ```
   1. Copy Activity: SQL Server → Bronze Lakehouse (raw data)
        ↓
   2. Notebook Activity: Transform and clean data
        ↓
   3. Copy Activity: Write to Silver Lakehouse (curated)
        ↓
   4. Script Activity: Refresh Power BI dataset
   ```
   
   **Fabric Data Pipeline vs Azure Data Factory:**
   
   | Feature | Fabric Data Pipeline | Azure Data Factory |
   |---------|---------------------|-------------------|
   | **Infrastructure** | SaaS (fully managed) | PaaS (some config needed) |
   | **Storage** | OneLake (automatic) | Must provision storage |
   | **Integration** | Native Fabric integration | Azure services |
   | **Licensing** | Fabric capacity | ADF pricing model |
   | **Compute** | Fabric capacity pools | Integration Runtime |
   | **Monitoring** | Fabric monitoring center | ADF monitoring |
   | **Security** | Workspace-based | Azure AD + separate config |
   
   **Similarities:**
   - Same visual designer
   - Same connectors (100+)
   - Same activity types
   - Can migrate ADF pipelines to Fabric
   
   **When to Use Fabric Pipelines:**
   - Building new Fabric-native solutions
   - Ingesting to Lakehouse/Warehouse
   - Unified Fabric experience
   
   **When to Use ADF:**
   - Existing ADF investments
   - Azure-only scenarios (no Fabric)
   - Need IR (Integration Runtime) customization
   
   **Example - Daily Data Refresh:**
   ```json
   {
     "name": "DailySalesRefresh",
     "activities": [
       {
         "type": "Copy",
         "source": {
           "type": "SqlServer",
           "query": "SELECT * FROM Sales WHERE OrderDate >= DATEADD(day, -1, GETDATE())"
         },
         "sink": {
           "type": "Lakehouse",
           "table": "sales_incremental"
         }
       },
       {
         "type": "Notebook",
         "notebookPath": "Transform_Sales",
         "dependsOn": ["CopyActivity"]
       }
     ],
     "schedule": {
       "frequency": "Day",
       "interval": 1,
       "startTime": "2026-01-01T02:00:00"
     }
   }
   ```

3. **Q:** What is Dataflow Gen2 and how does it differ from Dataflow Gen1 (Power BI Dataflows)?
   **A:** 
   Dataflow Gen2 is Fabric's enhanced version of Power Query-based data integration, with new capabilities and OneLake integration.
   
   **What is Dataflow Gen2:**
   - Visual data transformation tool using Power Query (M language)
   - Extract, Transform, Load (ETL) or Extract, Load, Transform (ELT)
   - Self-service data preparation
   - Outputs to OneLake, Warehouse, or Lakehouse
   
   **Power Query Transformations:**
   - Filter rows
   - Remove/rename columns
   - Merge queries (joins)
   - Append queries (union)
   - Pivot/unpivot
   - Split columns
   - Data type conversion
   - Custom functions (M language)
   
   **Gen2 vs Gen1 Differences:**
   
   | Feature | Dataflow Gen2 (Fabric) | Dataflow Gen1 (Power BI) |
   |---------|----------------------|-------------------------|
   | **Output** | Lakehouse, Warehouse, OneLake | Power BI workspace |
   | **Storage** | OneLake (Delta/Parquet) | Azure Data Lake |
   | **Compute** | Fabric Spark engine | Power BI Premium capacity |
   | **Reusability** | Used by any Fabric item | Power BI datasets only |
   | **Fast Copy** | Yes (direct parquet write) | No |
   | **Staging** | Optional | Required |
   | **Incremental Refresh** | Built-in | Manual configuration |
   
   **Dataflow Gen2 Architecture:**
   ```
   Data Sources (SQL, Excel, APIs)
        ↓
   Power Query Transformations
        ↓
   [Optional: Staging in OneLake]
        ↓
   Output Destinations:
   - Lakehouse Table
   - Warehouse Table  
   - OneLake Files
   ```
   
   **Example Use Case:**
   
   **Scenario:** Combine customer data from multiple sources
   
   ```
   Step 1: Connect to Sources
   - Query1: SQL Server customer data
   - Query2: Excel customer contacts
   - Query3: Salesforce accounts
   
   Step 2: Transform
   - Standardize column names
   - Remove duplicates
   - Merge on customer ID
   - Filter active customers
   - Add calculated columns
   
   Step 3: Output
   - Load to Lakehouse "CustomerMaster" table
   - Delta format
   - Incremental refresh enabled
   ```
   
   **When to Use Dataflow Gen2:**
   - ✅ Self-service data preparation
   - ✅ Combining multiple small sources
   - ✅ Users familiar with Power Query/Excel
   - ✅ Moderate complexity transformations
   - ✅ Datasets < 1 GB
   
   **When to Use Alternatives:**
   - Large datasets (>1 GB) → Notebooks or Data Pipeline
   - Complex logic → Notebooks (Python/Spark)
   - Simple copy → Data Pipeline Copy Activity
   - Real-time → Eventstream
   
   **Best Practices:**
   - Enable Fast Copy for better performance
   - Use query folding (push transformations to source)
   - Limit data preview during development
   - Use incremental refresh for large tables
   - Parameterize connection strings

4. **Q:** How does Eventstream work for real-time data ingestion in Fabric?
   **A:** 
   Eventstream is Fabric's no-code, real-time data ingestion and routing service for streaming data.
   
   **What is Eventstream:**
   - Real-time data streaming service
   - No-code visual designer
   - Connect streaming sources to destinations
   - Built-in transformations and routing
   - <1 second latency
   
   **Architecture:**
   ```
   Sources                Eventstream              Destinations
   --------              ------------             -------------
   Event Hub    →                        →        Lakehouse
   IoT Hub      →    [Transform/Route]   →        KQL Database
   Kafka        →                        →        Eventhouse
   Custom App   →                        →        Reflex
                                         →        Another Eventstream
   ```
   
   **Key Capabilities:**
   
   **1. Sources:**
   - Azure Event Hubs
   - Azure IoT Hub
   - Apache Kafka
   - Sample data (testing)
   - Custom applications (SDK)
   
   **2. Transformations (In-Stream):**
   - Filter events
   - Manage fields (add, remove, rename)
   - Aggregate (tumbling window)
   - Expand (unnest JSON)
   
   **3. Destinations:**
   - Lakehouse (real-time tables)
   - KQL Database
   - Custom App (push to external systems)
   - Reflex (Data Activator for alerts)
   
   **Real-World Example - IoT Sensor Pipeline:**
   
   ```
   IoT Device Sensors (10,000 devices)
        ↓ (MQTT → IoT Hub)
   Eventstream: "SensorDataStream"
        ↓
   Transformation 1: Filter (remove invalid readings)
        | where Temperature > -40 and Temperature < 100
        ↓
   Transformation 2: Aggregate (5-minute windows)
        | summarize AvgTemp = avg(Temperature) by DeviceID, bin(Timestamp, 5m)
        ↓
   Split Routing:
        ├─→ Hot data (last 24h) → KQL Database (real-time dashboard)
        ├─→ All data → Lakehouse (historical analysis)
        └─→ Anomalies (Temp > 80) → Reflex (send alerts)
   ```
   
   **Setup Example:**
   
   **Step 1: Create Eventstream**
   - Name: "SensorDataStream"
   - Add source: Azure IoT Hub
   - Configure connection string
   
   **Step 2: Add Transformation**
   ```kql
   // Filter transformation
   | where isnotnull(Temperature)
   | where Temperature between (-40 .. 100)
   | extend Location = tostring(properties.location)
   ```
   
   **Step 3: Add Destinations**
   - Destination 1: KQL Database → table "SensorReadings"
   - Destination 2: Lakehouse → table "SensorHistory"
   - Destination 3: Reflex → alert when temp > threshold
   
   **Benefits:**
   - **No Code:** Visual designer, no coding required
   - **Real-time:** <1 second latency
   - **Scalable:** Handles millions of events/second
   - **Integrated:** Native Fabric integration
   - **Monitoring:** Built-in monitoring and metrics
   
   **When to Use Eventstream:**
   - ✅ Real-time dashboards
   - ✅ IoT telemetry ingestion
   - ✅ Clickstream analytics
   - ✅ Application logging
   - ✅ Live monitoring and alerting
   - ✅ No-code requirement
   
   **When NOT to Use:**
   - ❌ Batch data (use Pipeline)
   - ❌ Complex transformations (use Spark notebooks)
   - ❌ Need custom business logic (use code-based approach)

5. **Q:** What are OneLake Shortcuts and how do they enable data virtualization?
   **A:** 
   OneLake Shortcuts create virtual pointers to data stored in external locations, making it accessible through OneLake without physically copying it.
   
   **What is a Shortcut:**
   - Virtual reference to external data
   - No data duplication or movement
   - Data remains in source location
   - Appears as native OneLake data
   - Read-only access
   
   **Supported Sources:**
   - **Azure Data Lake Storage Gen2** (ADLS Gen2)
   - **AWS S3** (Amazon S3)
   - **Google Cloud Storage**
   - **Dataverse** (Dynamics 365, Power Apps)
   - **Other OneLake locations** (cross-workspace)
   
   **How Shortcuts Work:**
   ```
   External Storage (ADLS Gen2)
   └── /data/sales/
       ├── 2026-01/sales.parquet
       ├── 2026-02/sales.parquet
       └── 2026-03/sales.parquet
             ↑
             | (Shortcut - no copy)
             ↓
   OneLake (Lakehouse)
   └── Files/
       └── Sales/ [SHORTCUT]
           ├── 2026-01/sales.parquet (virtual)
           ├── 2026-02/sales.parquet (virtual)
           └── 2026-03/sales.parquet (virtual)
   ```
   
   **Reading Data:**
   ```python
   # Spark notebook - reads via shortcut (transparently)
   df = spark.read.parquet("Files/Sales/")
   
   # SQL - queries shortcut data
   SELECT * FROM sales_shortcut_table
   
   # User doesn't know it's a shortcut - looks like local data
   ```
   
   **Types of Shortcuts:**
   
   **1. ADLS Gen2 Shortcut:**
   ```
   Source: abfss://container@storage.dfs.core.windows.net/sales/
   Target: OneLake Lakehouse → Files/ExternalSales
   Authentication: SAS token or Service Principal
   ```
   
   **2. S3 Shortcut:**
   ```
   Source: s3://my-bucket/analytics-data/
   Target: OneLake Lakehouse → Files/S3Data
   Authentication: AWS credentials
   ```
   
   **3. OneLake Shortcut (Cross-workspace):**
   ```
   Source: Workspace A → Lakehouse1 → Files/SharedData
   Target: Workspace B → Lakehouse2 → Files/SharedDataShortcut
   Authentication: Workspace permissions
   ```
   
   **Benefits:**
   
   **1. Cost Savings:**
   - No duplicate storage
   - Keep data in original location
   - Pay only for storage once
   
   **2. Centralized Access:**
   - Unified namespace across clouds
   - Access S3, ADLS, GCS from OneLake
   - Single interface for all data
   
   **3. Data Mesh:**
   - Departments own their data
   - Share via shortcuts
   - No data movement between teams
   
   **4. Gradual Migration:**
   - Access legacy data without migration
   - Migrate incrementally
   - Maintain business continuity
   
   **Real-World Use Cases:**
   
   **Use Case 1: Multi-Cloud Data Strategy**
   ```
   Marketing Data (AWS S3)
       ↓ [Shortcut]
   OneLake Lakehouse
       ↑ [Shortcut]
   Sales Data (Azure ADLS Gen2)
   
   Analysis:
   - Combine marketing + sales data
   - No data movement
   - Query both sources via Spark
   ```
   
   **Use Case 2: Data Mesh Architecture**
   ```
   Finance Workspace (owns financial data)
       ↓ [Shortcut shared]
   Central Analytics Workspace
       ↑ [Shortcut shared]
   HR Workspace (owns employee data)
   
   Benefits:
   - Federated ownership
   - Centralized analysis
   - No duplication
   ```
   
   **Use Case 3: Legacy System Integration**
   ```
   Legacy ADLS Gen2 Data Lake (10 TB)
       ↓ [Shortcuts - no migration needed]
   Fabric Lakehouse
       ↓
   Power BI Reports (DirectLake)
   
   Migration Path:
   - Phase 1: Access via shortcuts (immediate value)
   - Phase 2: Gradually copy critical data
   - Phase 3: Decommission legacy when ready
   ```
   
   **Limitations:**
   - **Read-only:** Cannot write to shortcut sources
   - **Performance:** Slightly slower than local data (network latency)
   - **Format Support:** Best with Parquet/Delta; CSV works but slower
   - **Authentication:** Need to manage credentials for external sources
   
   **Best Practices:**
   - Use for large, infrequently changing datasets
   - Prefer Delta/Parquet formats for performance
   - Copy frequently accessed data locally
   - Monitor data access patterns
   - Use for cross-workspace collaboration
   - Test performance before production use

6. **Q:** How do you implement incremental data loading in Fabric?
   **A:** 
   Incremental loading loads only new or changed data instead of full refresh, improving performance and reducing costs.
   
   **Methods for Incremental Load:**
   
   **1. Watermark Pattern (Pipeline):**
   
   Track last loaded timestamp/ID and load only newer records:
   
   ```sql
   -- Store last watermark in control table
   CREATE TABLE ControlTable (
       TableName VARCHAR(100),
       LastWatermark DATETIME
   )
   
   -- Pipeline lookup activity
   SELECT LastWatermark FROM ControlTable WHERE TableName = 'Sales'
   
   -- Pipeline copy activity query
   SELECT * FROM SourceSales 
   WHERE ModifiedDate > '@{activity('Lookup').output.firstRow.LastWatermark}'
   
   -- Update watermark after successful load
   UPDATE ControlTable 
   SET LastWatermark = GETDATE()
   WHERE TableName = 'Sales'
   ```
   
   **Pipeline Flow:**
   ```
   1. Lookup Activity → Get last watermark
   2. Copy Activity → Load new data (WHERE ModifiedDate > watermark)
   3. Notebook Activity → Merge into Delta table
   4. Stored Procedure → Update watermark
   ```
   
   **2. Delta Lake MERGE (Upsert):**
   
   ```python
   # Spark notebook - incremental load with merge
   from delta.tables import DeltaTable
   
   # Read incremental data
   incremental_df = spark.read.jdbc(
       url="jdbc:sqlserver://server.database.windows.net:1433",
       table="(SELECT * FROM Sales WHERE ModifiedDate > '2026-04-24') as src"
   )
   
   # Get existing Delta table
   target_table = DeltaTable.forPath(spark, "Tables/sales")
   
   # Merge (upsert) - update existing, insert new
   target_table.alias("target").merge(
       incremental_df.alias("source"),
       "target.SalesID = source.SalesID"
   ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
   ```
   
   **3. Dataflow Gen2 Incremental Refresh:**
   
   ```
   Dataflow Gen2 Settings:
   - Enable Incremental Refresh
   - Partition column: OrderDate
   - Refresh window: Last 7 days
   - Historical data: Archive (no refresh)
   
   Result:
   - Only last 7 days refreshed
   - Older data remains unchanged
   - Much faster refresh
   ```
   
   **4. Change Data Capture (CDC):**
   
   ```sql
   -- Enable CDC on source (SQL Server)
   EXEC sys.sp_cdc_enable_table
       @source_schema = 'dbo',
       @source_name = 'Sales',
       @role_name = NULL
   
   -- Pipeline reads only changes
   SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_Sales(
       @from_lsn, @to_lsn, 'all'
   )
   
   -- Fabric processes: Insert, Update, Delete
   ```
   
   **5. Partition-based Loading:**
   
   ```python
   # Load only today's partition
   current_date = datetime.now().strftime('%Y-%m-%d')
   
   df = spark.read.parquet(f"Files/sales/date={current_date}/")
   
   df.write.format("delta") \
       .mode("overwrite") \
       .option("replaceWhere", f"date = '{current_date}'") \
       .save("Tables/sales")
   ```
   
   **Comparison:**
   
   | Method | Best For | Pros | Cons |
   |--------|----------|------|------|
   | **Watermark** | Timestamp tracking | Simple, reliable | Needs tracking table |
   | **Delta MERGE** | Upsert scenarios | Handles updates/deletes | More complex |
   | **Dataflow Incremental** | Self-service BI | Easy configuration | Limited to Dataflow |
   | **CDC** | Real-time changes | Captures all changes | Source must support CDC |
   | **Partition-based** | Date-partitioned data | Very fast | Only for partitioned tables |
   
   **Best Practices:**
   - Always have a ModifiedDate/Timestamp column
   - Test incremental logic thoroughly
   - Handle late-arriving data
   - Monitor for missed records
   - Use Delta MERGE for upsert scenarios
   - Implement error handling and retry logic
   - Log incremental load metrics

7. **Q:** What are the best practices for data ingestion performance optimization in Fabric?
   **A:** 
   
   **1. Choose the Right Ingestion Method:**
   - **Small datasets (<1GB):** Dataflow Gen2
   - **Medium datasets (1-100GB):** Data Pipeline
   - **Large datasets (>100GB):** Notebooks with Spark
   - **Real-time:** Eventstream
   - **External data access:** Shortcuts
   
   **2. Optimize Data Pipeline:**
   
   **Parallelism:**
   ```json
   {
     "parallelCopies": 8,  // Parallel copy operations
     "dataIntegrationUnits": 16  // Compute units
   }
   ```
   
   **File Patterns:**
   - Use partitioned source data (folder structure)
   - Wildcard patterns for multiple files
   - Binary copy when no transformation needed
   
   **Staged Copy:**
   ```
   Source → Staging (OneLake) → Transform → Destination
   - Break large operations into stages
   - Enable recovery on failure
   ```
   
   **3. Optimize Spark Notebooks:**
   
   **Partitioning:**
   ```python
   # Read with partitioning
   df = spark.read.parquet("Files/sales/") \
       .repartition(200)  # Optimize for cluster size
   
   # Write with partitioning
   df.write.partitionBy("year", "month") \
       .format("delta") \
       .mode("append") \
       .save("Tables/sales")
   ```
   
   **Optimize Spark Config:**
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", 200)
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
   ```
   
   **Broadcast Small Tables:**
   ```python
   from pyspark.sql.functions import broadcast
   
   # Join large table with small lookup table
   result = large_df.join(
       broadcast(small_df),  # Broadcast to all nodes
       "CustomerID"
   )
   ```
   
   **4. File Format Optimization:**
   
   **Choose Right Format:**
   - **CSV:** Avoid for large datasets (slow, no compression)
   - **JSON:** Good for semi-structured, but larger files
   - **Parquet:** Best for analytics (columnar, compressed)
   - **Delta:** Parquet + ACID + performance features
   
   **Compression:**
   ```python
   # Use Snappy compression (good balance)
   df.write.option("compression", "snappy") \
       .parquet("Files/output/")
   ```
   
   **5. Incremental Loading:**
   - Load only changed data (watermark pattern)
   - Use Delta MERGE for upserts
   - Partition by date for easy incremental loads
   
   **6. Network and Connectivity:**
   - Use VNet integration for on-premises sources
   - Enable ExpressRoute for large volumes
   - Co-locate source and Fabric in same region
   - Use compression during transfer
   
   **7. Monitoring and Tuning:**
   
   **Pipeline Monitoring:**
   ```
   Fabric Monitoring Hub
   - View pipeline run history
   - Check activity duration
   - Identify bottlenecks
   - Monitor data movement volume
   ```
   
   **Spark Monitoring:**
   ```python
   # Track Spark job metrics
   - Stage execution time
   - Shuffle operations (expensive)
   - Data skew in partitions
   - Memory usage
   ```
   
   **8. Error Handling:**
   ```python
   # Notebook error handling
   try:
       df = spark.read.parquet("Files/source/")
       df.write.format("delta").save("Tables/target")
   except Exception as e:
       # Log error
       print(f"Ingestion failed: {str(e)}")
       # Send alert
       # Retry logic
       raise
   ```
   
   **Performance Checklist:**
   - ✅ Use appropriate ingestion method
   - ✅ Enable parallelism where possible
   - ✅ Use columnar formats (Parquet/Delta)
   - ✅ Implement incremental loading
   - ✅ Partition large datasets
   - ✅ Optimize Spark configurations
   - ✅ Monitor and tune regularly
   - ✅ Use compression
   - ✅ Co-locate data when possible
   - ✅ Implement error handling and retry logic

---

### Fabric Data Engineering

**Definition:**
Fabric Data Engineering is a comprehensive workload in Microsoft Fabric designed for data engineers to build, manage, and scale big data solutions using Apache Spark. It provides a unified development environment for data transformation, cleansing, enrichment, and processing at scale. Data Engineering in Fabric combines the power of Spark with OneLake's lakehouse architecture, enabling engineers to process petabyte-scale data using familiar tools like Notebooks, Spark Job Definitions, and Lakehouses.

**Purpose:**
- Process large-scale data transformations (TB to PB scale)
- Build ETL/ELT pipelines using Spark (PySpark, Scala, R, SQL)
- Implement data cleansing and quality checks
- Create reusable data transformation jobs
- Perform complex analytics and aggregations
- Enable batch and streaming data processing
- Support data science and machine learning workflows

**Key Components:**

**1. Lakehouse:**
The central storage and compute engine for data engineering workloads.

**What It Is:**
- Unified data storage (files + tables)
- Built on Delta Lake format (ACID transactions)
- Automatic OneLake integration
- SQL and Spark access to same data
- Schema enforcement and evolution

**Capabilities:**
```
Lakehouse Structure:
├── Files/ (Unstructured data)
│   ├── Bronze/ (Raw ingested data)
│   ├── Silver/ (Cleansed data)
│   └── Gold/ (Business-ready data)
└── Tables/ (Structured Delta tables)
    ├── Bronze.Sales_Raw
    ├── Silver.Sales_Clean
    └── Gold.Sales_Aggregated
```

**Use Cases:**
- Store raw, cleansed, and curated data layers
- Enable SQL queries on Spark-processed data
- Support DirectLake for Power BI (zero-copy)
- Provide schema-on-read for data exploration
- Version control with Delta time travel

**Example:**
```python
# Write to Lakehouse table
df = spark.read.parquet("Files/raw/sales/")

# Transform
df_clean = df.filter(col("Amount") > 0) \
    .withColumn("Year", year(col("OrderDate"))) \
    .dropDuplicates(["OrderID"])

# Save to Delta table
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.Sales")

# Query with SQL
spark.sql("SELECT Year, SUM(Amount) FROM Silver.Sales GROUP BY Year").show()
```

---

**2. Notebooks:**
Interactive development environment for Spark code.

**What It Is:**
- Browser-based Jupyter-style notebooks
- Support PySpark, Spark SQL, Scala, R
- Cell-by-cell execution
- Rich visualizations (charts, graphs)
- Markdown documentation
- Git integration

**Capabilities:**
```
Notebook Features:
✅ Multiple language support (%%pyspark, %%sql, %%scala)
✅ IntelliSense and code completion
✅ Built-in data visualization
✅ Parameter cells for orchestration
✅ Inline comments and documentation
✅ Debug and profile code
✅ Schedule execution via pipelines
✅ Share and collaborate
```

**Common Development Patterns:**

**Pattern 1: Medallion Architecture (Bronze-Silver-Gold)**
```python
# Bronze: Ingest raw data
bronze_df = spark.read.csv("Files/raw/sales.csv", header=True)
bronze_df.write.format("delta").mode("append").saveAsTable("Bronze.Sales")

# Silver: Cleanse and standardize
silver_df = spark.table("Bronze.Sales") \
    .filter(col("Amount").isNotNull()) \
    .withColumn("Amount", col("Amount").cast("decimal(10,2)")) \
    .withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd")) \
    .dropDuplicates(["OrderID"])

silver_df.write.format("delta").mode("overwrite").saveAsTable("Silver.Sales")

# Gold: Business aggregations
gold_df = spark.table("Silver.Sales") \
    .groupBy("Year", "Month", "ProductCategory") \
    .agg(
        sum("Amount").alias("TotalRevenue"),
        count("OrderID").alias("OrderCount"),
        avg("Amount").alias("AvgOrderValue")
    )

gold_df.write.format("delta").mode("overwrite").saveAsTable("Gold.SalesSummary")
```

**Pattern 2: Incremental Processing**
```python
from delta.tables import DeltaTable

# Read only new data (watermark pattern)
last_watermark = spark.sql("SELECT MAX(ModifiedDate) FROM Silver.Customers").collect()[0][0]

new_data = spark.read.parquet("Files/raw/customers/") \
    .filter(col("ModifiedDate") > last_watermark)

# Upsert into existing Delta table
target_table = DeltaTable.forName(spark, "Silver.Customers")

target_table.alias("target").merge(
    new_data.alias("source"),
    "target.CustomerID = source.CustomerID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

**Pattern 3: Data Quality Checks**
```python
# Validation framework
def validate_data(df, table_name):
    """Run data quality checks"""
    issues = []
    
    # Check for nulls in key columns
    null_count = df.filter(col("CustomerID").isNull()).count()
    if null_count > 0:
        issues.append(f"Found {null_count} null CustomerIDs")
    
    # Check for duplicates
    dup_count = df.count() - df.dropDuplicates(["CustomerID"]).count()
    if dup_count > 0:
        issues.append(f"Found {dup_count} duplicate CustomerIDs")
    
    # Check date ranges
    invalid_dates = df.filter(col("OrderDate") > current_date()).count()
    if invalid_dates > 0:
        issues.append(f"Found {invalid_dates} future-dated orders")
    
    # Log results
    if issues:
        print(f"❌ Validation FAILED for {table_name}:")
        for issue in issues:
            print(f"  - {issue}")
        raise ValueError(f"Data quality issues in {table_name}")
    else:
        print(f"✅ Validation PASSED for {table_name}")
    
    return df

# Use in pipeline
validated_df = validate_data(raw_df, "Sales")
validated_df.write.format("delta").saveAsTable("Silver.Sales")
```

**Use Cases:**
- Exploratory data analysis (EDA)
- Prototyping transformations
- Ad-hoc data processing
- Data quality investigations
- Complex business logic implementation
- Documentation with code

---

**3. Spark Job Definitions:**
Productionized, scheduled Spark jobs for automated processing.

**What It Is:**
- Standalone Spark application
- Upload JAR/Python files or reference notebooks
- Schedule via Data Factory pipelines
- Parameterized execution
- Production-grade job management

**Differences from Notebooks:**

| Aspect | Notebook | Spark Job Definition |
|--------|----------|---------------------|
| **Purpose** | Interactive development | Production automation |
| **Execution** | Cell-by-cell | Entire script |
| **UI** | Browser-based editor | Code upload |
| **Scheduling** | Via pipeline (notebook activity) | Via pipeline (spark job activity) |
| **Best For** | Development, exploration | Scheduled batch jobs |
| **Output** | Interactive results | Logs and data output |

**Configuration Example:**
```json
{
  "name": "SparkJobDefinition_DailySalesProcessing",
  "type": "SparkJobDefinition",
  "properties": {
    "mainFile": "process_sales.py",
    "commandLineArguments": [
      "--load-date", "2026-04-25",
      "--environment", "prod"
    ],
    "defaultLakehouse": "SalesLakehouse",
    "sparkConf": {
      "spark.executor.memory": "8g",
      "spark.executor.cores": "4",
      "spark.dynamicAllocation.enabled": "true"
    }
  }
}
```

**Python Script (process_sales.py):**
```python
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True)
parser.add_argument("--environment", default="dev")
args = parser.parse_args()

# Initialize Spark
spark = SparkSession.builder.appName("DailySalesProcessing").getOrCreate()

print(f"Processing sales for {args.load_date} in {args.environment} environment")

# Load data
df = spark.read.parquet(f"Files/raw/sales/date={args.load_date}/")

# Transform
df_processed = df.filter(col("Amount") > 0) \
    .withColumn("ProcessedDate", current_timestamp())

# Save
df_processed.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("Silver.Sales")

print(f"✅ Processed {df_processed.count()} records")
```

**Use Cases:**
- Scheduled daily/hourly batch processing
- Automated ETL jobs
- Repeatable transformation workflows
- Parameter-driven processing
- Production data pipelines

---

**4. Environments:**
Custom Spark runtime configurations with libraries and settings.

**What It Is:**
- Pre-configured Spark runtime environments
- Custom Python/R package installations
- Spark configuration templates
- Reusable across notebooks and jobs

**Configuration:**
```yaml
name: DataEngineering_Production
description: Production environment with ML libraries

spark_version: 3.4
runtime: Fabric Runtime 1.2

libraries:
  pypi:
    - pandas==2.0.0
    - scikit-learn==1.3.0
    - xgboost==2.0.0
    - great-expectations==0.18.0
  conda:
    - numpy=1.24.0

spark_conf:
  spark.executor.memory: "16g"
  spark.executor.cores: 8
  spark.dynamicAllocation.enabled: true
  spark.sql.adaptive.enabled: true
  spark.databricks.delta.optimizeWrite.enabled: true
```

**Use Cases:**
- Consistent runtime across team
- ML library management
- Performance optimization settings
- Environment promotion (dev → test → prod)

---

**5. Apache Spark Pools (Serverless Compute):**

**What It Is:**
- Serverless Spark compute clusters
- Auto-scaling based on workload
- Pay-per-use (Fabric capacity units)
- No cluster management needed

**Features:**
```
Spark Pool Capabilities:
✅ Auto-start: Cluster starts on-demand
✅ Auto-scale: Add/remove nodes based on load
✅ Auto-terminate: Stop after idle period
✅ Multiple pool sizes: Small, Medium, Large
✅ Optimized for Fabric workloads
```

**Compute Sizing:**

| Pool Size | Executor Cores | Executor Memory | Use Case |
|-----------|---------------|-----------------|----------|
| **Small** | 4 cores | 8 GB | Development, small datasets (<10GB) |
| **Medium** | 8 cores | 16 GB | Production, medium datasets (10-100GB) |
| **Large** | 16 cores | 32 GB | Large-scale processing (100GB-1TB) |
| **X-Large** | 32 cores | 64 GB | Massive datasets (>1TB) |

---

**Data Engineering Workflow:**

**End-to-End Example: E-Commerce Data Pipeline**

**Scenario:**
- Daily load of 10GB transaction data from various sources
- Cleanse, validate, and enrich data
- Create aggregated business metrics
- Load to Data Warehouse for reporting

**Step 1: Ingestion (Bronze Layer)**
```python
# Notebook: 01_Ingest_Raw_Data.ipynb

from pyspark.sql.functions import *

# Read from multiple sources
df_orders = spark.read.json("Files/external/shopify/orders/")
df_customers = spark.read.parquet("Files/external/crm/customers/")
df_products = spark.read.csv("Files/external/products.csv", header=True)

# Add ingestion metadata
df_orders = df_orders \
    .withColumn("IngestionDate", current_timestamp()) \
    .withColumn("SourceSystem", lit("Shopify"))

# Write to Bronze (raw)
df_orders.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("OrderDate") \
    .saveAsTable("Bronze.Orders")

df_customers.write.format("delta").mode("overwrite").saveAsTable("Bronze.Customers")
df_products.write.format("delta").mode("overwrite").saveAsTable("Bronze.Products")

print("✅ Bronze layer ingestion complete")
```

**Step 2: Cleansing (Silver Layer)**
```python
# Notebook: 02_Cleanse_Data.ipynb

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read Bronze
df_orders_raw = spark.table("Bronze.Orders")

# Data cleansing
df_orders_clean = df_orders_raw \
    .filter(col("OrderID").isNotNull()) \
    .filter(col("TotalAmount") > 0) \
    .filter(col("OrderDate") <= current_date()) \
    .dropDuplicates(["OrderID"]) \
    .withColumn("TotalAmount", col("TotalAmount").cast(DecimalType(10,2))) \
    .withColumn("OrderDate", to_date(col("OrderDate"))) \
    .withColumn("Year", year(col("OrderDate"))) \
    .withColumn("Month", month(col("OrderDate"))) \
    .withColumn("Quarter", quarter(col("OrderDate")))

# Data validation
total_raw = df_orders_raw.count()
total_clean = df_orders_clean.count()
dropped = total_raw - total_clean

print(f"Raw records: {total_raw:,}")
print(f"Clean records: {total_clean:,}")
print(f"Dropped: {dropped:,} ({dropped/total_raw*100:.2f}%)")

if dropped/total_raw > 0.05:  # More than 5% dropped
    raise ValueError("❌ Too many records dropped during cleansing!")

# Write to Silver
df_orders_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Silver.Orders")

print("✅ Silver layer cleansing complete")
```

**Step 3: Enrichment (Silver Layer)**
```python
# Notebook: 03_Enrich_Data.ipynb

# Read clean data
df_orders = spark.table("Silver.Orders")
df_customers = spark.table("Bronze.Customers")
df_products = spark.table("Bronze.Products")

# Join and enrich
df_enriched = df_orders \
    .join(df_customers, "CustomerID", "left") \
    .join(df_products, "ProductID", "left") \
    .select(
        df_orders["*"],
        df_customers["CustomerName"],
        df_customers["CustomerSegment"],
        df_customers["CustomerCountry"],
        df_products["ProductName"],
        df_products["ProductCategory"],
        df_products["ProductBrand"]
    ) \
    .withColumn("IsHighValue", when(col("TotalAmount") > 1000, True).otherwise(False)) \
    .withColumn("DayOfWeek", dayofweek(col("OrderDate"))) \
    .withColumn("IsWeekend", when(col("DayOfWeek").isin([1,7]), True).otherwise(False))

# Write enriched data
df_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Silver.OrdersEnriched")

print("✅ Data enrichment complete")
```

**Step 4: Aggregation (Gold Layer)**
```python
# Notebook: 04_Create_Gold_Tables.ipynb

# Read enriched data
df = spark.table("Silver.OrdersEnriched")

# Daily sales summary
df_daily = df.groupBy("Year", "Month", "OrderDate") \
    .agg(
        count("OrderID").alias("TotalOrders"),
        sum("TotalAmount").alias("TotalRevenue"),
        avg("TotalAmount").alias("AvgOrderValue"),
        countDistinct("CustomerID").alias("UniqueCustomers")
    )

df_daily.write.format("delta").mode("overwrite").saveAsTable("Gold.DailySales")

# Product performance
df_product = df.groupBy("ProductCategory", "ProductBrand") \
    .agg(
        count("OrderID").alias("OrderCount"),
        sum("TotalAmount").alias("Revenue"),
        countDistinct("CustomerID").alias("CustomerCount")
    ) \
    .orderBy(col("Revenue").desc())

df_product.write.format("delta").mode("overwrite").saveAsTable("Gold.ProductPerformance")

# Customer segmentation
df_customer = df.groupBy("CustomerID", "CustomerSegment", "CustomerCountry") \
    .agg(
        count("OrderID").alias("TotalOrders"),
        sum("TotalAmount").alias("LifetimeValue"),
        max("OrderDate").alias("LastOrderDate")
    ) \
    .withColumn("DaysSinceLastOrder", datediff(current_date(), col("LastOrderDate")))

df_customer.write.format("delta").mode("overwrite").saveAsTable("Gold.CustomerMetrics")

print("✅ Gold layer aggregations complete")
```

**Step 5: Orchestration (Data Factory Pipeline)**
```json
{
  "name": "Pipeline_Daily_ECommerce_Processing",
  "activities": [
    {
      "name": "01_IngestRawData",
      "type": "SynapseNotebook",
      "typeProperties": {
        "notebook": {"referenceName": "01_Ingest_Raw_Data"}
      }
    },
    {
      "name": "02_CleanseData",
      "type": "SynapseNotebook",
      "dependsOn": [{"activity": "01_IngestRawData"}],
      "typeProperties": {
        "notebook": {"referenceName": "02_Cleanse_Data"}
      }
    },
    {
      "name": "03_EnrichData",
      "type": "SynapseNotebook",
      "dependsOn": [{"activity": "02_CleanseData"}],
      "typeProperties": {
        "notebook": {"referenceName": "03_Enrich_Data"}
      }
    },
    {
      "name": "04_CreateGoldTables",
      "type": "SynapseNotebook",
      "dependsOn": [{"activity": "03_EnrichData"}],
      "typeProperties": {
        "notebook": {"referenceName": "04_Create_Gold_Tables"}
      }
    },
    {
      "name": "05_RefreshPowerBI",
      "type": "PowerBIRefresh",
      "dependsOn": [{"activity": "04_CreateGoldTables"}]
    }
  ]
}
```

---

**Data Engineering Best Practices:**

**1. Medallion Architecture:**
```
Bronze (Raw) → Silver (Clean) → Gold (Business)

Benefits:
✅ Clear data quality progression
✅ Auditable data lineage
✅ Reprocess any layer independently
✅ Different access controls per layer
```

**2. Delta Lake Optimization:**
```python
# Optimize table layout
spark.sql("OPTIMIZE Gold.Sales")

# Z-Ordering for better query performance
spark.sql("OPTIMIZE Gold.Sales ZORDER BY (Year, ProductCategory)")

# Vacuum old files (retention period)
spark.sql("VACUUM Gold.Sales RETAIN 168 HOURS")  # 7 days

# Time travel (query historical data)
df_yesterday = spark.read \
    .format("delta") \
    .option("versionAsOf", 1) \
    .table("Gold.Sales")
```

**3. Partitioning Strategy:**
```python
# Partition by date for time-series data
df.write \
    .format("delta") \
    .partitionBy("Year", "Month") \
    .saveAsTable("Sales")

# Query specific partition (much faster)
df = spark.read.table("Sales").filter(col("Year") == 2026)
```

**4. Performance Tuning:**
```python
# Configure Spark for optimal performance
spark.conf.set("spark.sql.shuffle.partitions", 200)  # Adjust based on data size
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Adaptive query execution
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")  # Auto-optimize writes
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Cache frequently used DataFrames
df_products = spark.table("Silver.Products").cache()
```

**5. Error Handling:**
```python
# Robust error handling pattern
def process_with_retry(func, max_retries=3):
    """Execute function with retry logic"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️ Attempt {attempt + 1} failed: {str(e)}")
                print(f"Retrying in {2**attempt} seconds...")
                time.sleep(2**attempt)
            else:
                print(f"❌ All {max_retries} attempts failed!")
                raise

# Use in pipeline
process_with_retry(lambda: df.write.format("delta").saveAsTable("Sales"))
```

---

**Comparison: Data Engineering vs Data Factory vs Data Science**

| Aspect | Data Engineering | Data Factory | Data Science |
|--------|-----------------|--------------|--------------|
| **Primary Tool** | Notebooks (Spark) | Pipelines (visual) | Notebooks (ML) |
| **Language** | PySpark, SQL, Scala | Low-code/no-code | Python, R |
| **Data Volume** | Large (TB-PB) | Any | Medium (GB-TB) |
| **Use Case** | Transform, cleanse | Orchestrate, move | Model, predict |
| **User** | Data Engineer | Data Integrator | Data Scientist |
| **Complexity** | Code-heavy | Visual | Algorithm-heavy |
| **Output** | Curated data tables | Data movement | ML models |

---

**When to Use Fabric Data Engineering:**

| Scenario | Use Data Engineering | Alternative |
|----------|---------------------|-------------|
| **Large datasets (>10GB)** | ✅ Yes - Spark scales | Dataflow Gen2 (limited) |
| **Complex transformations** | ✅ Yes - full Spark capabilities | Data Factory (limited) |
| **Custom business logic** | ✅ Yes - write custom code | Data Factory expressions |
| **Machine learning** | ✅ Yes - MLlib, scikit-learn | Data Science (specialized) |
| **Batch processing** | ✅ Yes - optimized for batch | Eventstream (streaming) |
| **Data quality checks** | ✅ Yes - custom validation | Limited in other tools |
| **Small datasets (<1GB)** | ❌ Overkill - use Dataflow Gen2 | Dataflow Gen2 |
| **Simple copy** | ❌ Overkill - use pipeline Copy | Data Factory |
| **Real-time streaming** | ❌ Use Eventstream | Eventstream + KQL |

---

**Interview Questions:**

**1. Q: What is Fabric Data Engineering and what are its core components?**

**A:** Fabric Data Engineering is Microsoft Fabric's workload dedicated to building large-scale data transformation and processing solutions using Apache Spark. It's designed for data engineers to implement ETL/ELT pipelines, data cleansing, and complex transformations at petabyte scale.

**Core Components:**

**1. Lakehouse:**
- Unified storage for files and tables
- Built on Delta Lake (ACID transactions, time travel)
- Stores Bronze, Silver, and Gold data layers
- Enables both Spark and SQL access
- DirectLake support for Power BI

**Example:**
```python
# Lakehouse stores both files and tables
spark.write.parquet("Files/raw/sales.parquet")  # File storage
df.write.format("delta").saveAsTable("Sales")   # Table storage
```

**2. Notebooks:**
- Interactive Spark development environment
- Supports PySpark, SQL, Scala, R
- Cell-by-cell execution for exploration
- Rich visualizations
- Git integration for version control

**Example Use:**
```python
# Exploratory analysis
df = spark.table("Sales")
df.groupBy("ProductCategory").sum("Revenue").show()

# Visualize results inline
display(df)  # Interactive charts
```

**3. Spark Job Definitions:**
- Production-grade scheduled Spark applications
- Parameterized execution
- Orchestrated via Data Factory pipelines
- JAR/Python file uploads

**Example:**
```python
# Scheduled job runs daily
python process_sales.py --date=2026-04-25 --env=prod
```

**4. Environments:**
- Custom Spark runtime configurations
- Managed Python/R libraries
- Reusable across notebooks and jobs

**5. Apache Spark Pools:**
- Serverless compute (auto-scaling)
- Pay-per-use model
- No cluster management

**Architecture:**
```
Data Sources
    ↓
Lakehouse (OneLake)
    ├── Files/ (unstructured)
    └── Tables/ (Delta)
        ├── Bronze (raw)
        ├── Silver (cleansed)
        └── Gold (curated)
    ↓
Notebooks/Jobs (Spark processing)
    ↓
Data Warehouse or Power BI
```

**Key Differentiators:**
- **Scale:** Process TB-PB datasets
- **Flexibility:** Full Spark capabilities
- **Performance:** Optimized for big data
- **Integration:** Native Fabric ecosystem

---

**2. Q: Explain the Medallion Architecture (Bronze-Silver-Gold) in Fabric Data Engineering?**

**A:** The Medallion Architecture is a data design pattern that organizes data into three progressive quality layers, each serving different purposes and user groups. It's the industry-standard approach for data lakehouses.

**Layer 1: Bronze (Raw/Landing Zone)**

**Purpose:** Store raw, unprocessed data exactly as received from source

**Characteristics:**
- Immutable (append-only, never delete)
- Complete history (full audit trail)
- Schema-on-read (no enforcement)
- Minimal transformations (maybe add ingestion timestamp)
- All data retained (even "bad" records)

**Example:**
```python
# Bronze: Ingest exactly as received
df_raw = spark.read.json("Files/external/api/orders.json")

df_raw = df_raw.withColumn("IngestionTimestamp", current_timestamp()) \
               .withColumn("SourceFile", input_file_name())

# Append to Bronze (never overwrite)
df_raw.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("IngestionDate") \
    .saveAsTable("Bronze.Orders")
```

**Who Uses:** Data engineers for troubleshooting, reprocessing

**Benefits:**
- Can always go back to original data
- Reprocess if transformation logic changes
- Complete audit trail
- Source of truth

---

**Layer 2: Silver (Cleansed/Validated)**

**Purpose:** Cleaned, validated, and conformed data ready for analysis

**Characteristics:**
- Data quality applied (nulls removed, types validated)
- Deduplicated
- Standardized formats (dates, decimals)
- Schema enforcement
- Business rules applied
- Joins with reference data

**Example:**
```python
# Silver: Cleanse and validate
df_bronze = spark.table("Bronze.Orders")

df_silver = df_bronze \
    .filter(col("OrderID").isNotNull()) \
    .filter(col("TotalAmount") > 0) \
    .filter(col("OrderDate") <= current_date()) \
    .dropDuplicates(["OrderID"]) \
    .withColumn("TotalAmount", col("TotalAmount").cast("decimal(10,2)")) \
    .withColumn("OrderDate", to_date(col("OrderDate"))) \
    .withColumn("ProcessedTimestamp", current_timestamp())

# Data quality validation
if df_silver.filter(col("CustomerID").isNull()).count() > 0:
    raise ValueError("Invalid data: NULL CustomerIDs found")

# Overwrite Silver (idempotent)
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Silver.Orders")
```

**Who Uses:** Data engineers, advanced analysts

**Benefits:**
- High-quality, trusted data
- Faster queries (clean, validated)
- Consistent across use cases
- Foundation for business logic

---

**Layer 3: Gold (Business/Aggregated)**

**Purpose:** Business-ready datasets optimized for specific use cases

**Characteristics:**
- Aggregated metrics
- Business logic applied
- Denormalized for performance
- Department/use-case specific
- Optimized for BI tools
- Often dimensional models (facts, dimensions)

**Example:**
```python
# Gold: Business aggregations
df_silver = spark.table("Silver.Orders")

# Daily sales summary for executives
df_daily_sales = df_silver \
    .groupBy("OrderDate", "ProductCategory", "Region") \
    .agg(
        count("OrderID").alias("TotalOrders"),
        sum("TotalAmount").alias("TotalRevenue"),
        avg("TotalAmount").alias("AvgOrderValue"),
        countDistinct("CustomerID").alias("UniqueCustomers")
    ) \
    .withColumn("RunDate", current_date())

df_daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Gold.DailySalesSummary")

# Customer lifetime value for marketing
df_customer_ltv = df_silver \
    .groupBy("CustomerID") \
    .agg(
        sum("TotalAmount").alias("LifetimeValue"),
        count("OrderID").alias("TotalOrders"),
        max("OrderDate").alias("LastOrderDate"),
        min("OrderDate").alias("FirstOrderDate")
    ) \
    .withColumn("DaysSinceLastOrder", 
        datediff(current_date(), col("LastOrderDate"))) \
    .withColumn("CustomerTenure", 
        datediff(current_date(), col("FirstOrderDate")))

df_customer_ltv.write.format("delta").mode("overwrite").saveAsTable("Gold.CustomerLTV")
```

**Who Uses:** Business users, analysts, Power BI reports

**Benefits:**
- Fast queries (pre-aggregated)
- Business-friendly naming
- Optimized for specific questions
- Ready for DirectLake in Power BI

---

**Complete Flow Example:**

```python
# BRONZE: Raw ingestion
spark.read.json("Files/raw/") \
    .write.format("delta").mode("append").saveAsTable("Bronze.Sales")

# SILVER: Cleansing
spark.table("Bronze.Sales") \
    .filter(col("Amount") > 0) \
    .dropDuplicates(["SaleID"]) \
    .write.format("delta").mode("overwrite").saveAsTable("Silver.Sales")

# GOLD: Business metrics
spark.table("Silver.Sales") \
    .groupBy("Year", "Month").sum("Amount") \
    .write.format("delta").mode("overwrite").saveAsTable("Gold.MonthlySales")
```

**Medallion Benefits:**

| Benefit | Description |
|---------|-------------|
| **Separation of Concerns** | Each layer has specific purpose |
| **Incremental Processing** | Process only changed data in each layer |
| **Data Quality** | Progressive improvement from Bronze → Gold |
| **Flexibility** | Reprocess any layer without affecting others |
| **Performance** | Gold tables optimized for queries |
| **Governance** | Different access controls per layer |
| **Auditability** | Full lineage from raw to business |

**Access Control Pattern:**
```
Bronze: Data Engineers only (raw, sensitive)
Silver: Data Engineers + Advanced Analysts
Gold: All users (business-ready, governed)
```

**Reprocessing Pattern:**
```
Bug found in transformation logic:
1. Fix Silver transformation code
2. Reprocess: Bronze → Silver (source still intact)
3. Reprocess: Silver → Gold
4. Bronze untouched (original data preserved)
```

**Real-World Timeline:**
```
Day 1: Ingest 100GB to Bronze (30 min)
Day 1: Process Bronze → Silver (1 hour)
Day 1: Process Silver → Gold (30 min)
Total: 2 hours

Day 2: Incremental
- Bronze: +5GB new data (2 min)
- Silver: Process only new 5GB (5 min)
- Gold: Reaggregate affected partitions (3 min)
Total: 10 minutes
```

The Medallion Architecture is **essential** for scalable, maintainable, and high-quality data engineering in Fabric!

---

**3. Q: When would you use Fabric Data Engineering (Notebooks) vs Dataflow Gen2 vs Data Factory Pipelines?**

**A:** Each tool serves different purposes based on data volume, complexity, and user skills:

**Use Fabric Data Engineering (Notebooks) When:**

✅ **Large Data Volumes (>10GB)**
```python
# Process 500GB daily logs
df = spark.read.parquet("Files/logs/")  # Handles TB-PB scale
df_processed = df.filter(...).groupBy(...).agg(...)
df_processed.write.format("delta").saveAsTable("ProcessedLogs")
```

✅ **Complex Transformations**
```python
# Window functions, complex joins, custom logic
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("CustomerID").orderBy("OrderDate")
df = df.withColumn("PreviousOrderDate", lag("OrderDate").over(windowSpec)) \
       .withColumn("DaysBetweenOrders", datediff(col("OrderDate"), col("PreviousOrderDate")))
```

✅ **Custom Business Logic**
```python
# Complex calculation not possible in Dataflow
def calculate_customer_risk_score(df):
    # Multi-step custom algorithm
    return df.withColumn("RiskScore", custom_udf(col("metrics")))
```

✅ **Machine Learning**
```python
from pyspark.ml.regression import LinearRegression

# Train model on large dataset
model = LinearRegression().fit(training_data)
```

✅ **Batch Processing at Scale**
- Daily processing of hundreds of GB
- Historical data reprocessing
- Complex ETL workflows

**Notebook Strengths:**
- 🟢 Unlimited scale (TB-PB)
- 🟢 Full Spark capabilities
- 🟢 Custom Python/Scala/R code
- 🟢 ML libraries
- 🟢 Complex algorithms

**Notebook Limitations:**
- 🔴 Requires coding skills
- 🔴 Longer development time
- 🔴 Not self-service for business users

---

**Use Dataflow Gen2 When:**

✅ **Small-Medium Data (<10GB)**
```
Power Query handles:
- Excel/CSV files (<1GB)
- API responses (thousands of records)
- Small database tables (<10M rows)
```

✅ **Self-Service Users (No Coding)**
```
Business Analyst workflow:
1. Visual drag-and-drop interface
2. Filter, merge, group operations
3. No Python/Spark knowledge needed
```

✅ **Simple Transformations**
```
Power Query operations:
- Remove duplicates
- Filter rows
- Change data types
- Merge tables
- Pivot/unpivot
- Basic calculations
```

✅ **Quick Development**
```
Dataflow Gen2:
- Build in 30 minutes
- Point-and-click
- 300+ built-in transformations

vs Notebook:
- Build in 2-3 hours
- Write code
- Test and debug
```

**Example Use Case:**
```
Sales Rep Dashboard:
- Connect to Salesforce API (1000 opportunities)
- Filter closed deals
- Merge with product pricing
- Calculate commission
- Load to Lakehouse table
- Refresh daily

Perfect for Dataflow Gen2!
```

**Dataflow Gen2 Strengths:**
- 🟢 No coding required
- 🟢 Fast development
- 🟢 Business user friendly
- 🟢 Visual interface
- 🟢 300+ transformations

**Dataflow Gen2 Limitations:**
- 🔴 Limited to ~10GB
- 🔴 No complex algorithms
- 🔴 No ML capabilities
- 🔴 Performance ceiling

---

**Use Data Factory Pipelines When:**

✅ **Orchestration (Not Transformation)**
```json
// Pipeline orchestrates, doesn't transform
Pipeline {
  1. Trigger Dataflow Gen2 → Clean data
  2. Trigger Notebook → Complex transform
  3. Copy Activity → Move results
  4. Refresh Power BI
}
```

✅ **Simple Data Movement**
```json
// Copy 100 tables from SQL to Lakehouse
{
  "type": "Copy",
  "source": "SQLServer.Sales",
  "sink": "Lakehouse.Bronze.Sales"
}
// No transformation needed
```

✅ **Metadata-Driven Patterns**
```json
// Lookup control table, process dynamically
ForEach table in control_table {
  Copy table from source to destination
}
```

✅ **Error Handling & Retry**
```json
{
  "activities": [
    {"name": "CopyData", "type": "Copy"},
    {"name": "OnSuccess", "dependsOn": ["CopyData"]},
    {"name": "OnFailure", "dependsOn": ["CopyData"], "retry": 3}
  ]
}
```

**Pipeline Strengths:**
- 🟢 Excellent orchestration
- 🟢 Error handling
- 🟢 Scheduling/triggers
- 🟢 Visual workflow
- 🟢 100+ connectors

**Pipeline Limitations:**
- 🔴 Not for transformations
- 🔴 Limited data processing
- 🔴 Should call Notebook/Dataflow for transforms

---

**Decision Matrix:**

| Factor | Notebook | Dataflow Gen2 | Pipeline |
|--------|----------|--------------|----------|
| **Data Volume** | >10GB (unlimited) | <10GB | Any (orchestration) |
| **User Skill** | Data engineer (code) | Business analyst (no code) | Data engineer |
| **Complexity** | Any | Simple-Moderate | N/A |
| **Development Speed** | Slower (hours) | Fast (minutes) | Fast |
| **Use Case** | Transform large data | Self-service ETL | Orchestrate workflow |
| **ML Support** | Yes | No | No |
| **Custom Logic** | Full control | Limited | Very limited |

**Real-World Decision Tree:**

```
Question 1: Is this orchestration or transformation?
  Orchestration → Use Pipeline
  Transformation → Continue to Q2

Question 2: What's the data volume?
  >10GB → Use Notebook
  <10GB → Continue to Q3

Question 3: Who will maintain this?
  Data Engineer → Use Notebook (more control)
  Business Analyst → Use Dataflow Gen2

Question 4: How complex is the transformation?
  Complex (ML, window functions, custom algorithms) → Notebook
  Simple (filter, join, aggregate) → Dataflow Gen2
```

**Best Practice: Use All Three Together!**

```
Complete Solution:
1. PIPELINE: Orchestrate entire workflow
   ├── 2. DATAFLOW GEN2: Business users prep their datasets
   ├── 3. NOTEBOOK: Data engineers handle heavy lifting
   └── 4. PIPELINE: Continue orchestration

Example:
Pipeline {
  → Dataflow Gen2: Extract API data, basic cleaning
  → Notebook: Join with 100GB historical data, ML scoring
  → Dataflow Gen2: Create business-friendly summary
  → Copy to Warehouse
  → Refresh Power BI
}
```

Choose the right tool for each step, don't force one tool to do everything!

---

### Fabric Data Factory

**Definition:**
Fabric Data Factory is Microsoft Fabric's fully managed, cloud-based data integration service that enables you to create, schedule, and orchestrate data workflows at scale. It's the evolution of Azure Data Factory (ADF) integrated natively into Fabric, providing visual, low-code/no-code tools for building ETL (Extract, Transform, Load) and ELT (Extract, Load, Transform) pipelines that move and transform data across various sources and destinations.

**Key Components:**

**1. Data Pipelines:**
- Visual workflow orchestration
- Drag-and-drop activity designer
- Control flow (loops, conditions, dependencies)
- Data flow (transformations)

**2. Dataflows Gen2:**
- Power Query-based transformations
- Self-service data preparation
- M language expressions
- Visual transformation designer

**3. Activities:**
- **Data Movement:** Copy Activity, Data Flow
- **Data Transformation:** Notebook, Stored Procedure, Script
- **Control Flow:** ForEach, If Condition, Until, Wait
- **External:** Web Activity, Azure Function, Logic Apps
- **Fabric-specific:** Warehouse, Lakehouse operations

**4. Connectors:**
- 100+ built-in connectors
- Databases, files, SaaS apps, APIs
- On-premises and cloud sources
- Custom connectors via REST API

**5. Integration Runtime:**
- **Fabric Runtime:** Fully managed, serverless compute
- **Self-hosted IR:** For on-premises/VNet sources
- **Auto-scaling:** Dynamic resource allocation

**Architecture:**
```
Data Sources (SQL, Files, APIs, etc.)
        ↓
Data Factory Pipelines
├── Copy Activity (data movement)
├── Data Flow (transformations)
├── Notebook Activity (Spark processing)
└── Control Activities (orchestration)
        ↓
OneLake Destinations
├── Lakehouse
├── Warehouse
└── KQL Database
```

**Common Patterns:**

**1. Medallion Architecture (Bronze-Silver-Gold):**
```
Source Systems
    ↓ Pipeline 1: Copy Activity
Bronze Layer (Raw Data in Lakehouse)
    ↓ Pipeline 2: Notebook Activity
Silver Layer (Cleansed, Validated)
    ↓ Pipeline 3: Data Flow
Gold Layer (Business-ready, aggregated)
    ↓
Power BI DirectLake
```

**2. Incremental Load Pattern:**
```
Pipeline Activities:
1. Lookup: Get last watermark
2. Copy: Load incremental data
3. Notebook: Transform and merge
4. Stored Procedure: Update watermark
5. Refresh: Update semantic model
```

**3. Parallel Processing:**
```
ForEach Activity (Parallel = TRUE)
├── Copy Activity (Table 1)
├── Copy Activity (Table 2)
├── Copy Activity (Table 3)
└── Copy Activity (Table N)
```

**Use Cases:**
- ETL/ELT data integration
- Data migration to OneLake
- Scheduled batch processing
- Data warehouse loading
- Multi-source data consolidation
- Hybrid cloud data integration
- Data lake population

---

## Activities in Detail

### Copy Activity

**Definition:**
Copy Activity is the primary data movement activity in Fabric Data Factory that copies data from a source to a destination (sink). It's the workhorse of ETL/ELT pipelines, supporting 100+ built-in connectors and handling data movement at scale with built-in fault tolerance, retry logic, and performance optimization.

**Purpose:**
- Move data between heterogeneous systems
- Extract data from sources and load into OneLake
- Bulk data ingestion
- File-to-table or table-to-file conversions
- Cross-cloud data movement

**Key Features:**

**1. 100+ Connectors:**
- **Databases:** SQL Server, Oracle, PostgreSQL, MySQL, Cosmos DB, Snowflake, Databricks
- **Cloud Storage:** Azure Blob, ADLS Gen2, AWS S3, Google Cloud Storage
- **SaaS Applications:** Salesforce, Dynamics 365, SharePoint, ServiceNow, SAP
- **Files:** CSV, JSON, Parquet, Avro, ORC, Excel, XML
- **APIs:** REST API, OData, HTTP

**2. Performance Features:**
- **Parallel Copies:** Distribute data movement across multiple threads
- **Data Integration Units (DIU):** Scalable compute resources (2, 4, 8, 16, 32 DIUs)
- **Staged Copy:** Intermediate staging for complex transformations
- **Binary Copy:** Direct file copy without parsing (fastest)
- **Partitioning:** Partition large datasets for parallel processing

**3. Transformation Capabilities:**
- Column mapping (rename, reorder)
- Data type conversion
- Basic filtering (SQL WHERE clause)
- Format conversion (CSV → Parquet, JSON → Delta)
- Compression/decompression

**4. Fault Tolerance:**
- Automatic retry on transient failures
- Skip incompatible rows
- Fault tolerance settings
- Session logs for troubleshooting

**Configuration:**

```json
{
  "name": "CopyActivity_Example",
  "type": "Copy",
  "inputs": [{
    "referenceName": "SourceDataset",
    "type": "DatasetReference",
    "parameters": {
      "SchemaName": "dbo",
      "TableName": "Sales"
    }
  }],
  "outputs": [{
    "referenceName": "LakehouseDataset",
    "type": "DatasetReference",
    "parameters": {
      "TableName": "Sales"
    }
  }],
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "SELECT * FROM dbo.Sales WHERE OrderDate >= '2026-01-01'",
      "queryTimeout": "02:00:00"
    },
    "sink": {
      "type": "LakehouseSink",
      "tableOption": "autoCreate",
      "writeBehavior": "append"
    },
    "enableStaging": false,
    "parallelCopies": 8,
    "dataIntegrationUnits": 16,
    "enableSkipIncompatibleRow": true
  },
  "policy": {
    "retry": 3,
    "retryIntervalInSeconds": 30,
    "timeout": "01:00:00"
  }
}
```

**Common Patterns:**

**1. Full Table Copy:**
```json
{
  "source": {
    "type": "SqlSource",
    "sqlReaderQuery": "SELECT * FROM dbo.Customers"
  },
  "sink": {
    "type": "LakehouseSink",
    "tableOption": "autoCreate",
    "writeBehavior": "overwrite"
  }
}
```

**2. Incremental Copy (Watermark Pattern):**
```json
{
  "source": {
    "type": "SqlSource",
    "sqlReaderQuery": "SELECT * FROM dbo.Sales WHERE ModifiedDate > '@{activity('GetWatermark').output.firstRow.LastWatermark}'"
  },
  "sink": {
    "type": "LakehouseSink",
    "writeBehavior": "append"
  }
}
```

**3. Multiple Files to Single Table:**
```json
{
  "source": {
    "type": "BinarySource",
    "storeSettings": {
      "type": "AzureBlobStorageReadSettings",
      "recursive": true,
      "wildcardFileName": "sales_*.csv"
    }
  },
  "sink": {
    "type": "LakehouseSink",
    "writeBehavior": "append"
  }
}
```

**4. Format Conversion (CSV to Parquet):**
```json
{
  "source": {
    "type": "DelimitedTextSource",
    "storeSettings": {
      "type": "AzureBlobFSReadSettings"
    },
    "formatSettings": {
      "type": "DelimitedTextReadSettings"
    }
  },
  "sink": {
    "type": "ParquetSink",
    "storeSettings": {
      "type": "LakehouseWriteSettings"
    },
    "formatSettings": {
      "type": "ParquetWriteSettings"
    }
  }
}
```

**Performance Optimization:**

**1. Data Integration Units (DIU):**
- Default: 4 DIU
- Increase for large datasets: 8, 16, 32 DIU
- Monitor actual DIU usage in activity output

**2. Parallel Copies:**
```json
{
  "parallelCopies": 8,  // Run 8 parallel threads
  "enablePartitionDiscovery": true
}
```

**3. Staged Copy (for complex transformations):**
```json
{
  "enableStaging": true,
  "stagingSettings": {
    "linkedServiceName": {
      "referenceName": "OneLakeStaging"
    },
    "path": "staging/temp"
  }
}
```

**4. Binary Copy (fastest - no parsing):**
```json
{
  "source": {
    "type": "BinarySource"
  },
  "sink": {
    "type": "BinarySink"
  },
  "preserveHierarchy": true
}
```

**Monitoring Copy Activity Output:**

```json
{
  "executionDetails": {
    "status": "Succeeded",
    "start": "2026-04-25T10:00:00Z",
    "end": "2026-04-25T10:05:23Z",
    "rowsRead": 1234567,
    "rowsCopied": 1234567,
    "rowsSkipped": 0,
    "copyDuration": 323,
    "throughput": 3821.45,  // KB/s
    "errors": [],
    "effectiveIntegrationRuntime": "FabricRuntime",
    "usedDataIntegrationUnits": 8,
    "billedDuration": 323,
    "usedParallelCopies": 8,
    "dataRead": 1234567890,  // bytes
    "dataWritten": 987654321,  // bytes (compressed)
    "sourcePeakConnections": 4,
    "sinkPeakConnections": 4
  }
}
```

**Best Practices:**

**Performance:**
- ✅ Use binary copy for same-format transfers
- ✅ Enable parallel copies for large datasets
- ✅ Increase DIU for performance-critical loads
- ✅ Use staged copy only when necessary
- ✅ Optimize source queries (SELECT specific columns, add WHERE clauses)
- ✅ Use appropriate file formats (Parquet > CSV for analytics)

**Reliability:**
- ✅ Set appropriate timeout values
- ✅ Enable retry logic (3 retries recommended)
- ✅ Use skip incompatible rows for data quality issues
- ✅ Monitor session logs for errors
- ✅ Implement watermark pattern for incremental loads

**Security:**
- ✅ Use managed identities for authentication
- ✅ Enable secure input/output for sensitive data
- ✅ Encrypt data in transit
- ✅ Use private endpoints for on-premises sources

**When to Use Copy Activity:**
- ✅ Simple data movement with minimal transformation
- ✅ Bulk data loading
- ✅ File format conversions
- ✅ Scheduled data extracts
- ✅ Data migrations

**When NOT to Use (use alternatives):**
- ❌ Complex transformations → Use Data Flow or Notebook
- ❌ Real-time streaming → Use Eventstream
- ❌ Row-by-row processing → Use Notebook
- ❌ Self-service data prep → Use Dataflow Gen2

---

### ForEach Activity

**Definition:**
ForEach Activity is a control flow activity that iterates over a collection (array) and executes one or more inner activities for each item in the collection. It enables dynamic, metadata-driven pipelines by allowing the same logic to be applied to multiple items (tables, files, parameters) without duplicating pipeline definitions.

**Purpose:**
- Process multiple files, tables, or objects with the same logic
- Implement dynamic, metadata-driven pipelines
- Iterate over arrays from parameters or lookup results
- Parallelize processing across multiple items
- Avoid pipeline duplication

**Key Features:**

**1. Sequential vs. Parallel Execution:**
- **Sequential:** Process items one by one (isSequential = true)
- **Parallel:** Process multiple items concurrently (isSequential = false)
- **Batch Count:** Control concurrency level (e.g., process 4 items at a time)

**2. Item Access:**
- Access current item with `@item()` expression
- Works with strings, objects, nested arrays
- Can access object properties: `@item().PropertyName`

**3. Inner Activities:**
- Can contain any activity type (Copy, Notebook, Web, etc.)
- Multiple activities can be nested
- Activities can depend on each other within the loop

**4. Dynamic Items:**
- Items from pipeline parameters
- Items from Lookup activity results
- Items from variables
- Hardcoded arrays

**Configuration:**

```json
{
  "name": "ForEachActivity_Example",
  "type": "ForEach",
  "typeProperties": {
    "items": {
      "value": "@pipeline().parameters.TableList",
      "type": "Expression"
    },
    "isSequential": false,
    "batchCount": 4,
    "activities": [
      {
        "name": "CopyTable",
        "type": "Copy",
        "inputs": [{
          "referenceName": "GenericSQLDataset",
          "parameters": {
            "TableName": "@item()"
          }
        }],
        "outputs": [{
          "referenceName": "GenericLakehouseDataset",
          "parameters": {
            "TableName": "@item()"
          }
        }]
      }
    ]
  }
}
```

**Common Patterns:**

**1. Simple Array Iteration:**
```json
{
  "items": {
    "value": ["Sales", "Customers", "Products", "Orders"]
  },
  "isSequential": false,
  "activities": [{
    "name": "ProcessTable",
    "type": "Copy",
    "typeProperties": {
      "source": {
        "query": "SELECT * FROM @{item()}"
      }
    }
  }]
}
```

**2. Lookup + ForEach (Metadata-Driven):**
```json
// Pipeline Flow:
// 1. Lookup Activity
{
  "name": "GetTableList",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "query": "SELECT TableName, Schema, DestinationPath FROM PipelineConfig WHERE IsActive = 1"
    },
    "firstRowOnly": false
  }
}

// 2. ForEach Activity
{
  "name": "ProcessTables",
  "type": "ForEach",
  "dependsOn": [{
    "activity": "GetTableList",
    "dependencyConditions": ["Succeeded"]
  }],
  "typeProperties": {
    "items": {
      "value": "@activity('GetTableList').output.value"
    },
    "isSequential": false,
    "batchCount": 5,
    "activities": [{
      "name": "CopyData",
      "type": "Copy",
      "typeProperties": {
        "source": {
          "query": "SELECT * FROM @{item().Schema}.@{item().TableName}"
        },
        "sink": {
          "tableName": "@{item().DestinationPath}"
        }
      }
    }]
  }
}
```

**3. Nested ForEach (Process Files in Multiple Folders):**
```json
{
  "name": "ForEachFolder",
  "type": "ForEach",
  "typeProperties": {
    "items": {
      "value": ["sales", "marketing", "finance"]
    },
    "activities": [{
      "name": "GetFilesInFolder",
      "type": "GetMetadata",
      "typeProperties": {
        "fieldList": ["childItems"]
      }
    }, {
      "name": "ForEachFile",
      "type": "ForEach",
      "typeProperties": {
        "items": {
          "value": "@activity('GetFilesInFolder').output.childItems"
        },
        "activities": [{
          "name": "ProcessFile",
          "type": "Copy"
        }]
      }
    }]
  }
}
```

**4. Parallel Processing with Error Handling:**
```json
{
  "name": "ProcessTablesWithErrorHandling",
  "type": "ForEach",
  "typeProperties": {
    "items": {
      "value": "@activity('GetTableList').output.value"
    },
    "isSequential": false,
    "batchCount": 10,
    "activities": [{
      "name": "TryCopyData",
      "type": "Copy",
      "policy": {
        "retry": 3,
        "retryIntervalInSeconds": 30
      }
    }, {
      "name": "LogSuccess",
      "type": "SqlServerStoredProcedure",
      "dependsOn": [{
        "activity": "TryCopyData",
        "dependencyConditions": ["Succeeded"]
      }],
      "typeProperties": {
        "storedProcedureName": "LogPipelineSuccess",
        "storedProcedureParameters": {
          "TableName": "@item().TableName",
          "RowsCopied": "@activity('TryCopyData').output.rowsCopied"
        }
      }
    }, {
      "name": "LogFailure",
      "type": "SqlServerStoredProcedure",
      "dependsOn": [{
        "activity": "TryCopyData",
        "dependencyConditions": ["Failed"]
      }],
      "typeProperties": {
        "storedProcedureName": "LogPipelineError",
        "storedProcedureParameters": {
          "TableName": "@item().TableName",
          "ErrorMessage": "@activity('TryCopyData').error.message"
        }
      }
    }]
  }
}
```

**Sequential vs. Parallel Execution:**

**Sequential (isSequential = true):**
```json
{
  "isSequential": true
}

// Execution:
// Item 1 → Complete → Item 2 → Complete → Item 3 → Complete
// Total Time: Sum of all item processing times
// Use when: Items have dependencies, need strict ordering
```

**Parallel (isSequential = false, batchCount = 4):**
```json
{
  "isSequential": false,
  "batchCount": 4
}

// Execution:
// Batch 1: Items 1,2,3,4 (parallel)
// Batch 2: Items 5,6,7,8 (parallel)
// Total Time: Max item time per batch × number of batches
// Use when: Items are independent, want faster execution
```

**Performance Considerations:**

**1. Optimal Batch Size:**
```
Total Items: 20 tables
Parallel Processing: 5 tables at a time (batchCount = 5)
Result: 4 batches, faster than sequential
```

**2. Resource Limits:**
- Fabric has concurrency limits
- Too high batchCount can overwhelm system
- Recommended: 4-10 concurrent operations
- Monitor and adjust based on performance

**3. Impact of Inner Activities:**
```
ForEach with Copy Activity:
- Each copy uses DIU (compute resources)
- batchCount = 10 → 10 parallel copies
- Monitor capacity usage
```

**Real-World Example:**

**Scenario: Load 50 tables from SQL Server to Lakehouse**

**Step 1: Configuration Table**
```sql
CREATE TABLE ETLConfig (
    TableID INT,
    SchemaName VARCHAR(50),
    TableName VARCHAR(100),
    DestinationPath VARCHAR(200),
    IsActive BIT,
    Priority INT
);

INSERT INTO ETLConfig VALUES
(1, 'dbo', 'Sales', 'Tables/Sales', 1, 1),
(2, 'dbo', 'Customers', 'Tables/Customers', 1, 1),
(3, 'dbo', 'Products', 'Tables/Products', 1, 2),
-- ... 47 more tables
(50, 'dbo', 'Audit', 'Tables/Audit', 1, 3);
```

**Step 2: Pipeline**
```json
{
  "activities": [
    {
      "name": "GetActiveTables",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "query": "SELECT * FROM ETLConfig WHERE IsActive = 1 ORDER BY Priority"
        },
        "firstRowOnly": false
      }
    },
    {
      "name": "LoadAllTables",
      "type": "ForEach",
      "dependsOn": [{"activity": "GetActiveTables"}],
      "typeProperties": {
        "items": "@activity('GetActiveTables').output.value",
        "isSequential": false,
        "batchCount": 5,
        "activities": [{
          "name": "CopyTable",
          "type": "Copy",
          "inputs": [{
            "referenceName": "SQLDataset",
            "parameters": {
              "SchemaName": "@item().SchemaName",
              "TableName": "@item().TableName"
            }
          }],
          "outputs": [{
            "referenceName": "LakehouseDataset",
            "parameters": {
              "Path": "@item().DestinationPath"
            }
          }]
        }]
      }
    }
  ]
}
```

**Result:**
- 1 pipeline handles all 50 tables
- Processes 5 tables in parallel (batchCount = 5)
- Total batches: 10
- Easy to add/remove tables (just update config table)

**Best Practices:**

**Design:**
- ✅ Use Lookup + ForEach for metadata-driven pipelines
- ✅ Prefer parallel execution when items are independent
- ✅ Set appropriate batchCount (4-10 recommended)
- ✅ Use meaningful item property names in Lookup results
- ✅ Implement error handling within ForEach
- ✅ Log successes and failures per item

**Performance:**
- ✅ Enable parallel processing (isSequential = false)
- ✅ Optimize batchCount based on capacity
- ✅ Monitor concurrent resource usage
- ✅ Consider priority-based ordering in Lookup
- ✅ Use Copy Activity settings (DIU, parallelCopies) carefully

**Debugging:**
- ✅ Test with small item count first
- ✅ Use pipeline debug mode to inspect @item() values
- ✅ Check individual activity outputs within ForEach
- ✅ Implement comprehensive logging

**When to Use ForEach:**
- ✅ Processing multiple files/tables with same logic
- ✅ Metadata-driven pipelines
- ✅ Avoiding pipeline duplication
- ✅ Parallel processing of independent items
- ✅ Dynamic pipeline execution

**When NOT to Use:**
- ❌ Single item processing (just use the activity directly)
- ❌ Very large arrays (>1000 items) - consider chunking
- ❌ Items with complex dependencies - use separate activities with dependency chains

---

### Lookup Activity

**Definition:**
Lookup Activity is a control flow activity that retrieves data from a source and returns it to the pipeline for use in subsequent activities. It acts as a bridge between external data sources and pipeline logic, enabling metadata-driven and dynamic pipeline execution. Unlike Copy Activity which moves data between systems, Lookup Activity reads data and makes it available within the pipeline scope.

**Purpose:**
- Read configuration/metadata from databases or files
- Get watermark values for incremental loads
- Retrieve lists of items for ForEach loops
- Check data existence before processing
- Fetch runtime parameters from external sources
- Query control tables for pipeline orchestration

**Key Features:**

**1. Return Modes:**
- **First Row Only (firstRowOnly = true):** Returns single row as object
- **All Rows (firstRowOnly = false):** Returns array of objects (max 5000 rows)

**2. Supported Sources:**
- All data sources supported by Copy Activity
- SQL databases, files (CSV, JSON, Parquet), REST APIs
- Can use queries or stored procedures
- Azure Blob, ADLS Gen2, OneLake

**3. Output Access:**
- `@activity('LookupName').output.firstRow` - Access first row
- `@activity('LookupName').output.firstRow.ColumnName` - Access specific column
- `@activity('LookupName').output.value` - Access all rows (array)
- `@activity('LookupName').output.count` - Number of rows returned

**4. Row Limit:**
- Maximum 5000 rows
- For larger datasets, use Copy Activity instead
- Intended for small configuration/metadata datasets

**Configuration:**

```json
{
  "name": "LookupActivity_Example",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "SELECT TableName, LastWatermark, IsActive FROM ETLConfig WHERE IsActive = 1"
    },
    "dataset": {
      "referenceName": "ConfigDatabase",
      "type": "DatasetReference"
    },
    "firstRowOnly": false
  }
}
```

**Common Patterns:**

**1. Get Single Value (Watermark Pattern):**
```json
{
  "name": "GetLastWatermark",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "SELECT MAX(ModifiedDate) AS LastWatermark FROM WatermarkTable WHERE TableName = 'Sales'"
    },
    "firstRowOnly": true
  }
}

// Usage in subsequent Copy Activity:
{
  "name": "IncrementalCopy",
  "type": "Copy",
  "typeProperties": {
    "source": {
      "query": "SELECT * FROM Sales WHERE ModifiedDate > '@{activity('GetLastWatermark').output.firstRow.LastWatermark}'"
    }
  }
}
```

**2. Get List for ForEach (Metadata-Driven Pipeline):**
```json
{
  "name": "GetTableList",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "SELECT TableName, SourceSchema, DestinationPath, LoadType FROM PipelineConfig WHERE IsActive = 1 ORDER BY Priority"
    },
    "firstRowOnly": false
  }
}

// Usage in ForEach:
{
  "name": "ProcessAllTables",
  "type": "ForEach",
  "typeProperties": {
    "items": {
      "value": "@activity('GetTableList').output.value",
      "type": "Expression"
    },
    "activities": [{
      "name": "CopyTable",
      "type": "Copy",
      "typeProperties": {
        "source": {
          "query": "SELECT * FROM @{item().SourceSchema}.@{item().TableName}"
        },
        "sink": {
          "tableName": "@{item().DestinationPath}"
        }
      }
    }]
  }
}
```

**3. Check Data Existence:**
```json
{
  "name": "CheckForNewFiles",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "SELECT COUNT(*) AS FileCount FROM FileMetadata WHERE ProcessedDate IS NULL"
    },
    "firstRowOnly": true
  }
}

// Usage in If Condition:
{
  "name": "ProcessIfFilesExist",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@greater(activity('CheckForNewFiles').output.firstRow.FileCount, 0)",
      "type": "Expression"
    },
    "ifTrueActivities": [
      {"name": "ProcessFiles", "type": "Copy"}
    ],
    "ifFalseActivities": [
      {"name": "SendNoFilesAlert", "type": "WebActivity"}
    ]
  }
}
```

**4. Get Latest File:**
```json
{
  "name": "GetLatestFile",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "SELECT TOP 1 FileName, FilePath, FileSize FROM FileLog WHERE ProcessedDate IS NULL ORDER BY CreatedDate DESC"
    },
    "firstRowOnly": true
  }
}

// Usage:
{
  "name": "ProcessFile",
  "type": "Copy",
  "inputs": [{
    "referenceName": "BlobDataset",
    "parameters": {
      "FilePath": "@activity('GetLatestFile').output.firstRow.FilePath",
      "FileName": "@activity('GetLatestFile').output.firstRow.FileName"
    }
  }]
}
```

**5. Lookup from File (JSON/CSV):**
```json
{
  "name": "LookupFromJSON",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "JsonSource",
      "storeSettings": {
        "type": "AzureBlobStorageReadSettings"
      }
    },
    "dataset": {
      "referenceName": "ConfigJSON",
      "type": "DatasetReference"
    },
    "firstRowOnly": false
  }
}

// config.json:
[
  {"tableName": "Sales", "schema": "dbo", "destination": "Tables/Sales"},
  {"tableName": "Customers", "schema": "dbo", "destination": "Tables/Customers"}
]
```

**6. Stored Procedure Lookup:**
```json
{
  "name": "LookupViaStoredProc",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderStoredProcedureName": "sp_GetActiveTables",
      "storedProcedureParameters": {
        "Environment": {
          "value": "@pipeline().parameters.Environment",
          "type": "String"
        }
      }
    },
    "firstRowOnly": false
  }
}
```

**Real-World Example: Incremental Load with Watermark**

**Step 1: Watermark Table**
```sql
CREATE TABLE Watermark (
    TableName VARCHAR(100) PRIMARY KEY,
    WatermarkValue DATETIME,
    LastUpdateTime DATETIME
);

INSERT INTO Watermark VALUES
('Sales', '2026-01-01 00:00:00', GETDATE()),
('Customers', '2026-01-01 00:00:00', GETDATE());
```

**Step 2: Pipeline Activities**
```json
{
  "activities": [
    {
      "name": "GetOldWatermark",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "query": "SELECT WatermarkValue FROM Watermark WHERE TableName = '@{pipeline().parameters.TableName}'"
        },
        "firstRowOnly": true
      }
    },
    {
      "name": "GetNewWatermark",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "query": "SELECT MAX(ModifiedDate) AS NewWatermark FROM @{pipeline().parameters.TableName}"
        },
        "firstRowOnly": true
      }
    },
    {
      "name": "IncrementalCopy",
      "type": "Copy",
      "dependsOn": [
        {"activity": "GetOldWatermark"},
        {"activity": "GetNewWatermark"}
      ],
      "typeProperties": {
        "source": {
          "query": "SELECT * FROM @{pipeline().parameters.TableName} WHERE ModifiedDate > '@{activity('GetOldWatermark').output.firstRow.WatermarkValue}' AND ModifiedDate <= '@{activity('GetNewWatermark').output.firstRow.NewWatermark}'"
        }
      }
    },
    {
      "name": "UpdateWatermark",
      "type": "SqlServerStoredProcedure",
      "dependsOn": [{"activity": "IncrementalCopy"}],
      "typeProperties": {
        "storedProcedureName": "sp_UpdateWatermark",
        "storedProcedureParameters": {
          "TableName": "@pipeline().parameters.TableName",
          "NewWatermark": "@activity('GetNewWatermark').output.firstRow.NewWatermark"
        }
      }
    }
  ]
}
```

**Lookup Activity Output Examples:**

**First Row Only (firstRowOnly = true):**
```json
{
  "count": 1,
  "value": [
    {
      "TableName": "Sales",
      "LastWatermark": "2026-04-24T10:00:00Z",
      "IsActive": true
    }
  ],
  "firstRow": {
    "TableName": "Sales",
    "LastWatermark": "2026-04-24T10:00:00Z",
    "IsActive": true
  }
}

// Access: @activity('Lookup1').output.firstRow.TableName
```

**All Rows (firstRowOnly = false):**
```json
{
  "count": 3,
  "value": [
    {"TableName": "Sales", "Priority": 1},
    {"TableName": "Customers", "Priority": 2},
    {"TableName": "Products", "Priority": 3}
  ]
}

// Access: @activity('Lookup1').output.value (returns array)
// Use in ForEach: items = @activity('Lookup1').output.value
```

**Best Practices:**

**Performance:**
- ✅ Keep result set small (<5000 rows)
- ✅ Use firstRowOnly = true when expecting single value
- ✅ Optimize queries (add WHERE clauses, indexes)
- ✅ Use stored procedures for complex logic
- ✅ Cache lookup results in variables if used multiple times

**Design:**
- ✅ Use Lookup for configuration/metadata, not large data movement
- ✅ Combine with ForEach for metadata-driven pipelines
- ✅ Use meaningful column names in queries
- ✅ Handle NULL values appropriately
- ✅ Validate lookup results before use

**Error Handling:**
- ✅ Handle empty result sets (check count)
- ✅ Use default values for missing data
- ✅ Implement validation logic
- ✅ Log lookup queries for debugging

**Security:**
- ✅ Use parameterized queries to prevent SQL injection
- ✅ Limit permissions on lookup sources
- ✅ Avoid exposing sensitive data in lookup results

**Comparison: Lookup vs Copy Activity:**

| Aspect | Lookup Activity | Copy Activity |
|--------|----------------|---------------|
| **Purpose** | Read data for pipeline logic | Move data between systems |
| **Output** | Returns to pipeline | Writes to destination |
| **Data Volume** | Small (<5000 rows) | Large (unlimited) |
| **Use Case** | Configuration, metadata | ETL, data loading |
| **Access** | Via expressions (@activity) | Not accessible in pipeline |
| **Performance** | Fast (small datasets) | Optimized for bulk |

**When to Use Lookup:**
- ✅ Get watermark values
- ✅ Retrieve configuration from control tables
- ✅ Get list of items for ForEach
- ✅ Check data existence
- ✅ Fetch runtime parameters
- ✅ Query metadata tables

**When NOT to Use:**
- ❌ Large datasets (>5000 rows) → Use Copy Activity
- ❌ Data movement → Use Copy Activity
- ❌ Complex transformations → Use Notebook or Data Flow
- ❌ Result not needed in pipeline → Use Copy Activity

**The Power Trio: Lookup + ForEach + Copy**

This combination enables metadata-driven pipelines:

```
1. LOOKUP → Get configuration ("what to process")
   Returns: List of tables from control table
   
2. FOREACH → Loop through items ("how many times")
   Iterates: Over each table from Lookup
   
3. COPY → Move the data ("actual work")
   Executes: For each table
```

This pattern transforms **50 static pipelines** into **1 dynamic pipeline** managed by a simple configuration table!

---

### Invoke Pipeline Activity

**Definition:**
The Invoke Pipeline Activity (also called Execute Pipeline) is a control flow activity that allows you to call and execute another pipeline from within a parent pipeline. It enables modular pipeline design by breaking complex workflows into smaller, reusable pipelines that can be orchestrated together. This promotes code reuse, simplifies maintenance, and creates a hierarchical pipeline architecture.

**Purpose:**
- Create modular, reusable pipeline components
- Break complex workflows into manageable pieces
- Implement separation of concerns
- Enable parallel execution of independent pipelines
- Pass parameters between parent and child pipelines
- Build pipeline templates for common patterns
- Simplify debugging and testing

**Key Features:**

**1. Parent-Child Relationship:**
- Parent pipeline invokes one or more child pipelines
- Child pipeline executes independently
- Parent waits for child completion (or can run async)
- Parent can access child pipeline outputs

**2. Parameter Passing:**
- Parent passes parameters to child
- Child returns values to parent
- Supports all data types (string, int, array, object)
- Dynamic parameter values using expressions

**3. Execution Modes:**
- **Synchronous (Wait):** Parent waits for child to complete
- **Asynchronous (Fire-and-forget):** Parent continues immediately

**4. Return Values:**
- Child pipeline can return output values
- Parent accesses via `@activity('InvokePipeline').output`
- Useful for passing computed values, status, counts, etc.

---

**Basic Configuration:**

```json
{
  "name": "InvokeChildPipeline",
  "type": "ExecutePipeline",
  "typeProperties": {
    "pipeline": {
      "referenceName": "Child_ProcessTable",
      "type": "PipelineReference"
    },
    "waitOnCompletion": true,  // Synchronous execution
    "parameters": {
      "TableName": {
        "value": "@pipeline().parameters.TableName",
        "type": "Expression"
      },
      "LoadDate": {
        "value": "@formatDateTime(utcnow(), 'yyyy-MM-dd')",
        "type": "Expression"
      }
    }
  }
}
```

**Child Pipeline Definition:**

```json
{
  "name": "Child_ProcessTable",
  "properties": {
    "parameters": {
      "TableName": {
        "type": "String"
      },
      "LoadDate": {
        "type": "String"
      }
    },
    "activities": [
      {
        "name": "CopyData",
        "type": "Copy",
        "typeProperties": {
          "source": {
            "query": "@concat(
              'SELECT * FROM ', 
              pipeline().parameters.TableName,
              ' WHERE Date = ''', 
              pipeline().parameters.LoadDate, 
              ''''
            )"
          }
        }
      }
    ]
  }
}
```

---

**Common Patterns:**

**1. Modular ETL Framework (Separation of Concerns):**

```
Architecture:
Master_ETL_Pipeline (Orchestrator)
├── Child_Extract (Source → Bronze)
├── Child_Transform (Bronze → Silver)
├── Child_Load (Silver → Gold)
└── Child_Notify (Send completion alerts)
```

**Parent Pipeline (Master_ETL_Pipeline):**
```json
{
  "name": "Master_ETL_Pipeline",
  "activities": [
    {
      "name": "InvokeExtract",
      "type": "ExecutePipeline",
      "typeProperties": {
        "pipeline": {
          "referenceName": "Child_Extract"
        },
        "waitOnCompletion": true,
        "parameters": {
          "SourceSystem": "SAP",
          "LoadDate": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
        }
      }
    },
    {
      "name": "InvokeTransform",
      "type": "ExecutePipeline",
      "dependsOn": [
        {"activity": "InvokeExtract", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "pipeline": {
          "referenceName": "Child_Transform"
        },
        "waitOnCompletion": true,
        "parameters": {
          "RowsExtracted": "@activity('InvokeExtract').output.pipelineReturnValue.RowCount"
        }
      }
    },
    {
      "name": "InvokeLoad",
      "type": "ExecutePipeline",
      "dependsOn": [
        {"activity": "InvokeTransform", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "pipeline": {
          "referenceName": "Child_Load"
        },
        "waitOnCompletion": true
      }
    },
    {
      "name": "InvokeNotify",
      "type": "ExecutePipeline",
      "dependsOn": [
        {"activity": "InvokeLoad", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "pipeline": {
          "referenceName": "Child_Notify"
        },
        "waitOnCompletion": false  // Async - don't wait for notification
      }
    }
  ]
}
```

**Benefits:**
- Each child pipeline has single responsibility
- Can test each component independently
- Reuse Child_Extract for different sources
- Easy to add new steps (e.g., Child_Archive)
- Clear failure isolation

---

**2. Parallel Pipeline Execution (Performance Optimization):**

```
Scenario: Process 5 independent data sources simultaneously

Master Pipeline:
├── Invoke_Process_Source1 (parallel)
├── Invoke_Process_Source2 (parallel)
├── Invoke_Process_Source3 (parallel)
├── Invoke_Process_Source4 (parallel)
└── Invoke_Process_Source5 (parallel)
    ↓ (all complete)
    Consolidation_Pipeline
```

**Parent Pipeline:**
```json
{
  "name": "Master_Parallel_Load",
  "activities": [
    {
      "name": "InvokeSourceSAP",
      "type": "ExecutePipeline",
      "typeProperties": {
        "pipeline": {"referenceName": "Child_LoadSource"},
        "waitOnCompletion": true,
        "parameters": {
          "SourceName": "SAP",
          "ConnectionString": "@pipeline().globalParameters.SAPConnection"
        }
      }
    },
    {
      "name": "InvokeSourceSalesforce",
      "type": "ExecutePipeline",
      "typeProperties": {
        "pipeline": {"referenceName": "Child_LoadSource"},
        "waitOnCompletion": true,
        "parameters": {
          "SourceName": "Salesforce",
          "ConnectionString": "@pipeline().globalParameters.SalesforceConnection"
        }
      }
    },
    {
      "name": "InvokeSourceSQL",
      "type": "ExecutePipeline",
      "typeProperties": {
        "pipeline": {"referenceName": "Child_LoadSource"},
        "waitOnCompletion": true,
        "parameters": {
          "SourceName": "SQLServer",
          "ConnectionString": "@pipeline().globalParameters.SQLConnection"
        }
      }
    },
    // Note: No dependencies between above 3 = parallel execution!
    
    {
      "name": "ConsolidateResults",
      "type": "ExecutePipeline",
      "dependsOn": [
        {"activity": "InvokeSourceSAP", "dependencyConditions": ["Succeeded"]},
        {"activity": "InvokeSourceSalesforce", "dependencyConditions": ["Succeeded"]},
        {"activity": "InvokeSourceSQL", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_Consolidate"},
        "waitOnCompletion": true,
        "parameters": {
          "SAPRowCount": "@activity('InvokeSourceSAP').output.pipelineReturnValue.RowCount",
          "SalesforceRowCount": "@activity('InvokeSourceSalesforce').output.pipelineReturnValue.RowCount",
          "SQLRowCount": "@activity('InvokeSourceSQL').output.pipelineReturnValue.RowCount"
        }
      }
    }
  ]
}
```

**Execution Timeline:**
```
Time 0:00 → Start all 3 source pipelines (parallel)
Time 0:15 → SAP completes (4M rows)
Time 0:18 → Salesforce completes (1M rows)
Time 0:20 → SQL completes (8M rows)
Time 0:20 → All complete, start Consolidate
Time 0:25 → Consolidate completes
Total Time: 25 minutes

Sequential would be: 15+18+20+5 = 58 minutes
Parallel saves: 33 minutes (57% faster!)
```

---

**3. Dynamic Pipeline Invocation (Metadata-Driven):**

```
Pattern: Lookup list of pipelines to run from control table

Control Table:
┌────────────┬──────────────────┬─────────┬──────────┐
│ PipelineID │ PipelineName     │ IsActive│ Priority │
├────────────┼──────────────────┼─────────┼──────────┤
│ 1          │ Load_Sales       │ 1       │ 1        │
│ 2          │ Load_Customer    │ 1       │ 1        │
│ 3          │ Load_Product     │ 1       │ 2        │
│ 4          │ Load_Inventory   │ 0       │ 3        │
└────────────┴──────────────────┴─────────┴──────────┘
```

**Master Pipeline:**
```json
{
  "name": "Dynamic_Pipeline_Executor",
  "activities": [
    {
      "name": "GetPipelineList",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "type": "SqlSource",
          "sqlReaderQuery": "SELECT PipelineName, Priority FROM dbo.PipelineControl WHERE IsActive = 1 ORDER BY Priority"
        },
        "firstRowOnly": false
      }
    },
    {
      "name": "ForEachPipeline",
      "type": "ForEach",
      "dependsOn": [
        {"activity": "GetPipelineList", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "items": {
          "value": "@activity('GetPipelineList').output.value",
          "type": "Expression"
        },
        "isSequential": false,  // Parallel execution within same priority
        "batchCount": 5,
        "activities": [
          {
            "name": "InvokeDynamicPipeline",
            "type": "ExecutePipeline",
            "typeProperties": {
              "pipeline": {
                "referenceName": {
                  "value": "@item().PipelineName",  // Dynamic pipeline name!
                  "type": "Expression"
                }
              },
              "waitOnCompletion": true,
              "parameters": {
                "LoadDate": "@formatDateTime(utcnow(), 'yyyy-MM-dd')",
                "Environment": "@pipeline().parameters.Environment"
              }
            }
          }
        ]
      }
    }
  ]
}
```

**Benefits:**
- Add new pipelines without changing master pipeline
- Enable/disable pipelines via control table
- Control execution order with Priority
- No code changes for pipeline additions

---

**4. Return Values Pattern (Pipeline Communication):**

**Child Pipeline with Return Values:**
```json
{
  "name": "Child_CountRecords",
  "properties": {
    "parameters": {
      "TableName": {"type": "String"}
    },
    "activities": [
      {
        "name": "GetRecordCount",
        "type": "Lookup",
        "typeProperties": {
          "source": {
            "query": "@concat('SELECT COUNT(*) AS RecordCount FROM ', pipeline().parameters.TableName)"
          },
          "firstRowOnly": true
        }
      },
      {
        "name": "SetReturnValue",
        "type": "SetVariable",
        "dependsOn": [{"activity": "GetRecordCount"}],
        "typeProperties": {
          "variableName": "OutputRecordCount",
          "value": "@activity('GetRecordCount').output.firstRow.RecordCount"
        }
      }
    ],
    "variables": {
      "OutputRecordCount": {"type": "Integer"}
    }
  }
}
```

**Parent Pipeline Accessing Return Values:**
```json
{
  "name": "Parent_ValidateRecordCount",
  "activities": [
    {
      "name": "InvokeCountRecords",
      "type": "ExecutePipeline",
      "typeProperties": {
        "pipeline": {"referenceName": "Child_CountRecords"},
        "waitOnCompletion": true,
        "parameters": {
          "TableName": "Sales"
        }
      }
    },
    {
      "name": "CheckRecordCount",
      "type": "IfCondition",
      "dependsOn": [{"activity": "InvokeCountRecords"}],
      "typeProperties": {
        "expression": {
          "value": "@greater(
            activity('InvokeCountRecords').output.pipelineReturnValue.OutputRecordCount,
            1000
          )",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "ProcessData",
            "type": "Copy"
          }
        ],
        "ifFalseActivities": [
          {
            "name": "SendLowVolumeAlert",
            "type": "WebActivity"
          }
        ]
      }
    }
  ]
}
```

---

**5. Error Handling & Retry Pattern:**

```json
{
  "name": "Master_WithErrorHandling",
  "activities": [
    {
      "name": "InvokeDataLoad",
      "type": "ExecutePipeline",
      "typeProperties": {
        "pipeline": {"referenceName": "Child_LoadData"},
        "waitOnCompletion": true
      }
    },
    {
      "name": "InvokeCleanupOnSuccess",
      "type": "ExecutePipeline",
      "dependsOn": [
        {"activity": "InvokeDataLoad", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_Cleanup"}
      }
    },
    {
      "name": "InvokeErrorHandling",
      "type": "ExecutePipeline",
      "dependsOn": [
        {"activity": "InvokeDataLoad", "dependencyConditions": ["Failed"]}
      ],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_ErrorHandler"},
        "waitOnCompletion": true,
        "parameters": {
          "ErrorMessage": "@activity('InvokeDataLoad').error.message",
          "PipelineRunId": "@pipeline().RunId",
          "FailedPipeline": "@activity('InvokeDataLoad').ExecutionStartTime"
        }
      }
    }
  ]
}
```

---

**Accessing Child Pipeline Outputs:**

**Child Pipeline Output Structure:**
```json
{
  "pipelineRunId": "abc-123-def",
  "pipelineReturnValue": {
    "RowCount": 12345,
    "Status": "Success",
    "ProcessedFiles": ["file1.csv", "file2.csv"]
  },
  "effectiveIntegrationRuntime": "Fabric-Runtime",
  "billingReference": {...}
}
```

**Parent Accessing Outputs:**
```json
// Access return values
@activity('InvokeChild').output.pipelineReturnValue.RowCount
@activity('InvokeChild').output.pipelineReturnValue.Status

// Access run metadata
@activity('InvokeChild').output.pipelineRunId
@activity('InvokeChild').output.effectiveIntegrationRuntime

// Use in expressions
{
  "expression": {
    "value": "@equals(
      activity('InvokeChild').output.pipelineReturnValue.Status,
      'Success'
    )"
  }
}
```

---

**Best Practices:**

**1. Pipeline Design:**

**✅ DO:**
- Keep child pipelines focused (single responsibility)
- Use descriptive pipeline names (Child_Extract_SAP, Child_Transform_Sales)
- Document expected parameters in pipeline description
- Return useful values from child pipelines
- Use consistent naming conventions

**❌ DON'T:**
- Create overly deep nesting (max 2-3 levels)
- Pass large datasets via parameters (use tables/files)
- Create circular dependencies
- Make child pipelines environment-specific

**2. Parameter Passing:**

```json
// ✅ GOOD: Pass configuration, not data
"parameters": {
  "TableName": "Sales",
  "LoadDate": "2026-04-25",
  "Environment": "prod"
}

// ❌ BAD: Don't pass large arrays/data
"parameters": {
  "AllRecords": "[{...1000s of records...}]"  // NO!
}

// ✅ BETTER: Pass reference to data
"parameters": {
  "LookupQuery": "SELECT * FROM StagingTable WHERE Batch = 123"
}
```

**3. Error Handling:**

```json
// Pattern: Always have error handling path
Master Pipeline {
  Invoke Child
  ├─ On Success → Invoke Success Handler
  └─ On Failure → Invoke Error Handler (logs, alerts, cleanup)
}

// Child pipelines should:
- Fail fast on critical errors
- Return error details in return values
- Clean up temporary resources
- Log failures appropriately
```

**4. Performance:**

```json
// ✅ Use parallel invocation for independent pipelines
No dependencies between Invoke activities = parallel execution

// ✅ Use async (waitOnCompletion = false) when appropriate
{
  "name": "InvokeAsyncNotification",
  "typeProperties": {
    "waitOnCompletion": false  // Don't wait for notification pipeline
  }
}

// ✅ Minimize nesting depth
1 level: Master → Child (preferred)
2 levels: Master → Child → Grandchild (OK)
3+ levels: Consider refactoring
```

**5. Monitoring:**

```
Fabric Monitoring Hub:
- Parent pipeline shows child pipeline invocations
- Click through to view child pipeline details
- Drill down to specific activity failures
- View entire execution tree

Logging Best Practice:
Master: Log orchestration decisions
Child: Log detailed processing steps
```

---

**Real-World Example: Enterprise Data Warehouse Load**

**Scenario:**
- Daily load of 100 tables from various sources
- Different load patterns (full, incremental, CDC)
- Complex transformations
- Error handling and notifications

**Pipeline Architecture:**

```
Master_Daily_DW_Load
├── Invoke_Pre_Load_Validation
│   └── Checks source availability, disk space
├── Invoke_Extract_Phase (Parallel)
│   ├── Child_Extract_SAP (50 tables)
│   ├── Child_Extract_Salesforce (20 tables)
│   └── Child_Extract_SQLServer (30 tables)
├── Invoke_Transform_Phase
│   ├── Child_Transform_Bronze_To_Silver
│   └── Child_Transform_Silver_To_Gold
├── Invoke_Load_Phase
│   └── Child_Load_To_Warehouse
├── Invoke_Post_Load_Tasks (Async)
│   ├── Child_Update_Statistics
│   ├── Child_Refresh_PowerBI
│   └── Child_Send_Success_Email
└── On Failure → Invoke_Error_Handler
```

**Master_Daily_DW_Load:**
```json
{
  "name": "Master_Daily_DW_Load",
  "activities": [
    {
      "name": "InvokePreLoadValidation",
      "type": "ExecutePipeline",
      "typeProperties": {
        "pipeline": {"referenceName": "Child_PreLoad_Validation"},
        "waitOnCompletion": true,
        "parameters": {
          "LoadDate": "@formatDateTime(utcnow(), 'yyyy-MM-dd')"
        }
      }
    },
    {
      "name": "InvokeExtractSAP",
      "type": "ExecutePipeline",
      "dependsOn": [{"activity": "InvokePreLoadValidation"}],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_Extract_SAP"},
        "waitOnCompletion": true
      }
    },
    {
      "name": "InvokeExtractSalesforce",
      "type": "ExecutePipeline",
      "dependsOn": [{"activity": "InvokePreLoadValidation"}],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_Extract_Salesforce"},
        "waitOnCompletion": true
      }
    },
    {
      "name": "InvokeExtractSQL",
      "type": "ExecutePipeline",
      "dependsOn": [{"activity": "InvokePreLoadValidation"}],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_Extract_SQLServer"},
        "waitOnCompletion": true
      }
    },
    // Extract pipelines run in parallel (no dependencies between them)
    
    {
      "name": "InvokeTransform",
      "type": "ExecutePipeline",
      "dependsOn": [
        {"activity": "InvokeExtractSAP"},
        {"activity": "InvokeExtractSalesforce"},
        {"activity": "InvokeExtractSQL"}
      ],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_Transform_All"},
        "waitOnCompletion": true,
        "parameters": {
          "SAPRowCount": "@activity('InvokeExtractSAP').output.pipelineReturnValue.RowCount",
          "SalesforceRowCount": "@activity('InvokeExtractSalesforce').output.pipelineReturnValue.RowCount",
          "SQLRowCount": "@activity('InvokeExtractSQL').output.pipelineReturnValue.RowCount"
        }
      }
    },
    {
      "name": "InvokeLoad",
      "type": "ExecutePipeline",
      "dependsOn": [{"activity": "InvokeTransform"}],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_Load_To_Warehouse"},
        "waitOnCompletion": true
      }
    },
    {
      "name": "InvokePostLoadTasks",
      "type": "ExecutePipeline",
      "dependsOn": [{"activity": "InvokeLoad"}],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_PostLoad_Tasks"},
        "waitOnCompletion": false  // Async - don't wait
      }
    },
    {
      "name": "InvokeErrorHandler",
      "type": "ExecutePipeline",
      "dependsOn": [
        {"activity": "InvokeExtractSAP", "dependencyConditions": ["Failed"]},
        {"activity": "InvokeExtractSalesforce", "dependencyConditions": ["Failed"]},
        {"activity": "InvokeExtractSQL", "dependencyConditions": ["Failed"]},
        {"activity": "InvokeTransform", "dependencyConditions": ["Failed"]},
        {"activity": "InvokeLoad", "dependencyConditions": ["Failed"]}
      ],
      "typeProperties": {
        "pipeline": {"referenceName": "Child_Error_Handler"},
        "parameters": {
          "MasterRunId": "@pipeline().RunId",
          "FailureTime": "@utcnow()"
        }
      }
    }
  ]
}
```

**Benefits:**
- ✅ Modular: Each child pipeline has single responsibility
- ✅ Reusable: Can invoke Child_Extract_SAP from other master pipelines
- ✅ Testable: Test each child independently
- ✅ Maintainable: Changes to SAP extraction isolated to one pipeline
- ✅ Parallel: Extract phase runs 3 pipelines simultaneously
- ✅ Monitored: Clear hierarchy in Monitoring Hub
- ✅ Scalable: Add new sources without changing master logic

**Execution Timeline:**
```
00:00 - Start Master
00:01 - PreLoad Validation (1 min)
00:02 - Start all 3 Extract pipelines (parallel)
00:12 - All extracts complete (10 min max)
00:12 - Transform phase (15 min)
00:27 - Load phase (8 min)
00:35 - PostLoad tasks start (async, don't wait)
00:35 - Master completes

Total: 35 minutes
Sequential would be: 1 + 25 + 15 + 8 = 49 minutes
Parallel saves: 14 minutes (29% faster!)
```

---

**When to Use Invoke Pipeline:**

| Scenario | Use Invoke Pipeline | Alternative |
|----------|-------------------|-------------|
| **Reusable logic** | ✅ Yes - create child pipeline | Copy activities (duplication) |
| **Modular design** | ✅ Yes - separation of concerns | Single large pipeline (messy) |
| **Parallel execution** | ✅ Yes - invoke multiple children | ForEach (limited use case) |
| **Error isolation** | ✅ Yes - contain failures | Try-catch in single pipeline |
| **Team collaboration** | ✅ Yes - teams own child pipelines | Monolithic pipeline (conflicts) |
| **Testing** | ✅ Yes - test components independently | Test entire pipeline (slow) |
| **Simple workflow** | ❌ No - overkill | Direct activities |
| **Single use logic** | ❌ No - unnecessary complexity | Inline activities |

**✅ Use Invoke Pipeline When:**
- Building enterprise-scale orchestration
- Need reusable components
- Multiple teams working on different parts
- Complex workflows with many steps
- Want parallel execution of independent processes
- Need clear separation of concerns
- Testing and debugging are priorities

**❌ Don't Use Invoke Pipeline When:**
- Simple, linear workflow
- Logic used only once
- Small POC or prototype
- Overhead not justified by benefits

---

**Key Takeaways:**

1. **Modular Design:** Break complex workflows into manageable, reusable pieces
2. **Parallel Execution:** Invoke multiple pipelines simultaneously for performance
3. **Parameter Passing:** Pass configuration, not data; use return values for communication
4. **Error Handling:** Always have error handling paths for invoked pipelines
5. **Monitoring:** Fabric Monitoring Hub shows entire execution tree
6. **Best Practice:** Use for enterprise patterns, avoid for simple workflows

The Invoke Pipeline Activity is **essential** for building scalable, maintainable enterprise data integration solutions!

---

### If Condition Activity

**Definition:**
The If Condition Activity is a control flow activity that implements conditional branching logic in pipelines, similar to if-else statements in programming languages. It evaluates a boolean expression and executes different sets of activities based on whether the condition evaluates to true or false. This enables dynamic pipeline behavior and decision-making at runtime.

**Purpose:**
- Implement branching logic based on runtime conditions
- Execute different activities based on data validation results
- Handle different scenarios in the same pipeline
- Skip or execute activities conditionally
- Implement business rules and logic gates
- Create adaptive pipelines that respond to data states

**Key Features:**

**1. Boolean Expression Evaluation:**
- Supports complex expressions using pipeline functions
- Can reference activity outputs, variables, parameters
- Evaluates to true or false at runtime
- Supports nested conditions

**2. Two Execution Paths:**
- **If True:** Activities to execute when condition is true
- **If False:** Activities to execute when condition is false
- Each path can contain multiple activities
- Activities within paths can have dependencies

**3. Expression Support:**
- Comparison operators: @equals(), @greater(), @less()
- Logical operators: @and(), @or(), @not()
- Access to activity outputs: @activity('name').output
- Variable/parameter access: @variables('name'), @pipeline().parameters.name

**4. Activity Chaining:**
- Can chain multiple If Conditions
- Can nest If Conditions (use sparingly)
- Can combine with other control flow activities

---

**Basic Configuration:**

```json
{
  "name": "CheckFileExists",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@greater(activity('GetMetadata').output.childItems.length, 0)",
      "type": "Expression"
    },
    "ifTrueActivities": [
      {
        "name": "ProcessFiles",
        "type": "Copy",
        "dependsOn": [],
        "typeProperties": {
          // Copy activity configuration
        }
      }
    ],
    "ifFalseActivities": [
      {
        "name": "LogNoFiles",
        "type": "Web",
        "dependsOn": [],
        "typeProperties": {
          "url": "https://logging-service.com/api/log",
          "method": "POST",
          "body": {
            "message": "No files found to process"
          }
        }
      }
    ]
  }
}
```

---

**Common Patterns:**

**1. Check Data Existence Before Processing:**

```json
{
  "activities": [
    {
      "name": "CheckForNewData",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "query": "SELECT COUNT(*) AS RecordCount FROM StagingTable WHERE ProcessedFlag = 0"
        },
        "firstRowOnly": true
      }
    },
    {
      "name": "IfDataExists",
      "type": "IfCondition",
      "dependsOn": [{"activity": "CheckForNewData", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "expression": {
          "value": "@greater(activity('CheckForNewData').output.firstRow.RecordCount, 0)",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "ProcessData",
            "type": "Copy",
            "typeProperties": {
              "source": {
                "query": "SELECT * FROM StagingTable WHERE ProcessedFlag = 0"
              },
              "sink": {
                "writeBehavior": "Insert"
              }
            }
          },
          {
            "name": "UpdateProcessedFlag",
            "type": "Script",
            "dependsOn": [{"activity": "ProcessData", "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {
              "scripts": [{
                "text": "UPDATE StagingTable SET ProcessedFlag = 1 WHERE ProcessedFlag = 0"
              }]
            }
          }
        ],
        "ifFalseActivities": [
          {
            "name": "LogNoDataFound",
            "type": "Web",
            "typeProperties": {
              "url": "@pipeline().parameters.LoggingEndpoint",
              "method": "POST",
              "body": {
                "message": "No new data to process",
                "timestamp": "@utcNow()",
                "pipeline": "@pipeline().Pipeline"
              }
            }
          }
        ]
      }
    }
  ]
}
```

**Result:**
- If RecordCount > 0: Copy data → Update flags
- If RecordCount = 0: Log "No data" message
- Prevents unnecessary processing when no data exists

---

**2. Environment-Based Execution (Dev/Prod):**

```json
{
  "name": "RouteByEnvironment",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@equals(pipeline().parameters.Environment, 'Production')",
      "type": "Expression"
    },
    "ifTrueActivities": [
      {
        "name": "LoadToProductionWarehouse",
        "type": "Copy",
        "typeProperties": {
          "sink": {
            "type": "WarehouseSink",
            "tableName": "Production.FactSales"
          }
        }
      },
      {
        "name": "SendProductionAlert",
        "type": "Web",
        "dependsOn": [{"activity": "LoadToProductionWarehouse", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
          "url": "@pipeline().parameters.ProductionWebhook",
          "method": "POST",
          "body": {
            "status": "Production load complete",
            "rowsLoaded": "@activity('LoadToProductionWarehouse').output.rowsCopied"
          }
        }
      }
    ],
    "ifFalseActivities": [
      {
        "name": "LoadToDevWarehouse",
        "type": "Copy",
        "typeProperties": {
          "sink": {
            "type": "WarehouseSink",
            "tableName": "Dev.FactSales"
          }
        }
      }
    ]
  }
}
```

**Use Case:**
- Same pipeline for multiple environments
- Different destinations based on parameter
- Different notification logic per environment
- Reduces pipeline duplication

---

**3. Data Quality Validation:**

```json
{
  "activities": [
    {
      "name": "ValidateDataQuality",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "query": "SELECT COUNT(*) AS NullCount FROM StagingTable WHERE CriticalColumn IS NULL"
        },
        "firstRowOnly": true
      }
    },
    {
      "name": "CheckDataQuality",
      "type": "IfCondition",
      "dependsOn": [{"activity": "ValidateDataQuality", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "expression": {
          "value": "@equals(activity('ValidateDataQuality').output.firstRow.NullCount, 0)",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "ProceedWithLoad",
            "type": "Copy",
            "typeProperties": {
              "source": {"query": "SELECT * FROM StagingTable"},
              "sink": {"tableName": "DWH.FactTable"}
            }
          }
        ],
        "ifFalseActivities": [
          {
            "name": "FailPipeline",
            "type": "Fail",
            "typeProperties": {
              "message": "Data quality check failed: NULL values found in critical column",
              "errorCode": "DataQualityError"
            }
          }
        ]
      }
    }
  ]
}
```

**Benefits:**
- Prevents loading bad data
- Explicit error handling for quality issues
- Pipeline fails fast on validation errors
- Clear error messaging

---

**4. File Count Threshold Logic:**

```json
{
  "name": "CheckFileCount",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@greater(activity('GetFileMetadata').output.childItems.length, 10)",
      "type": "Expression"
    },
    "ifTrueActivities": [
      {
        "name": "ProcessInParallel",
        "type": "ForEach",
        "typeProperties": {
          "items": "@activity('GetFileMetadata').output.childItems",
          "isSequential": false,
          "batchCount": 5,
          "activities": [
            {
              "name": "ProcessFile",
              "type": "Copy",
              "typeProperties": {
                "source": {
                  "type": "DelimitedTextSource",
                  "fileName": "@item().name"
                }
              }
            }
          ]
        }
      }
    ],
    "ifFalseActivities": [
      {
        "name": "ProcessSequentially",
        "type": "ForEach",
        "typeProperties": {
          "items": "@activity('GetFileMetadata').output.childItems",
          "isSequential": true,
          "activities": [
            {
              "name": "ProcessFile",
              "type": "Copy",
              "typeProperties": {
                "source": {
                  "type": "DelimitedTextSource",
                  "fileName": "@item().name"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
```

**Logic:**
- If more than 10 files: Process in parallel (faster)
- If 10 or fewer files: Process sequentially (less resource intensive)
- Adaptive performance based on workload

---

**5. Time-Based Routing (Business Hours vs. After Hours):**

```json
{
  "name": "CheckBusinessHours",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@and(greaterOrEquals(int(formatDateTime(utcNow(), 'HH')), 9), lessOrEquals(int(formatDateTime(utcNow(), 'HH')), 17))",
      "type": "Expression"
    },
    "ifTrueActivities": [
      {
        "name": "LoadWithLowPriority",
        "type": "Copy",
        "typeProperties": {
          "enableStaging": true,
          "parallelCopies": 2
        }
      }
    ],
    "ifFalseActivities": [
      {
        "name": "LoadWithHighPriority",
        "type": "Copy",
        "typeProperties": {
          "enableStaging": false,
          "parallelCopies": 10
        }
      }
    ]
  }
}
```

**Strategy:**
- Business hours (9 AM - 5 PM): Low resource usage
- After hours: Maximum performance
- Reduces impact on business operations

---

**Complex Expression Examples:**

**1. Multiple Conditions (AND Logic):**

```json
{
  "expression": {
    "value": "@and(
      equals(pipeline().parameters.Environment, 'Production'),
      greater(activity('Lookup1').output.firstRow.RowCount, 1000)
    )",
    "type": "Expression"
  }
}

// True only if BOTH:
// - Environment is Production
// - RowCount > 1000
```

**2. Multiple Conditions (OR Logic):**

```json
{
  "expression": {
    "value": "@or(
      equals(pipeline().parameters.ForceRun, 'true'),
      greater(activity('CheckNewData').output.firstRow.NewRecords, 0)
    )",
    "type": "Expression"
  }
}

// True if EITHER:
// - ForceRun parameter is 'true'
// - NewRecords > 0
```

**3. NOT Logic:**

```json
{
  "expression": {
    "value": "@not(equals(activity('Lookup1').output.firstRow.Status, 'Completed'))",
    "type": "Expression"
  }
}

// True if Status is NOT 'Completed'
```

**4. Nested Conditions:**

```json
{
  "expression": {
    "value": "@and(
      equals(pipeline().parameters.Environment, 'Production'),
      or(
        equals(dayOfWeek(utcNow()), 6),
        equals(dayOfWeek(utcNow()), 0)
      )
    )",
    "type": "Expression"
  }
}

// True if:
// - Environment is Production
// - AND (Day is Saturday OR Sunday)
```

**5. String Comparisons:**

```json
{
  "expression": {
    "value": "@contains(activity('Lookup1').output.firstRow.FileName, '.csv')",
    "type": "Expression"
  }
}

// True if FileName contains '.csv'
```

---

**Best Practices:**

**1. Expression Design:**

**✅ DO:**
- Keep expressions simple and readable
- Use meaningful variable/parameter names
- Document complex logic in pipeline description
- Test expressions in debug mode
- Use variables for reusable expressions

**❌ DON'T:**
- Create overly complex nested conditions (max 2-3 levels)
- Use magic numbers (use parameters instead)
- Duplicate expression logic
- Forget NULL handling

**2. Activity Organization:**

```json
// ✅ GOOD: Clear, organized paths
If Condition
├─ If True:
│  ├─ Validate
│  ├─ Process
│  └─ Notify Success
└─ If False:
   ├─ Log Error
   └─ Send Alert

// ❌ BAD: Too many activities in one path
If Condition
├─ If True: 15 activities (too complex!)
└─ If False: 1 activity
```

**3. Error Handling:**

```json
// ✅ GOOD: Both paths handled
{
  "ifTrueActivities": [
    {"name": "ProcessData", "type": "Copy"}
  ],
  "ifFalseActivities": [
    {"name": "LogSkipped", "type": "Web"}
  ]
}

// ❌ BAD: Empty false path (unclear intent)
{
  "ifTrueActivities": [
    {"name": "ProcessData", "type": "Copy"}
  ],
  "ifFalseActivities": []  // What happens here?
}
```

**4. Performance:**

```json
// ✅ GOOD: Lightweight condition check
Lookup (fast query) → If Condition → Process

// ❌ BAD: Heavy operation before condition
Copy entire table → If Condition (too late!)
```

**5. Testing:**

```
Debug Mode Tips:
- Test both True and False paths
- Verify expressions with different input values
- Check NULL handling
- Test boundary conditions (exactly equal, just below threshold, etc.)
- Use Set Variable to inspect expression results
```

---

**Common Use Cases:**

| Use Case | Condition Example | True Path | False Path |
|----------|------------------|-----------|------------|
| **Data Existence** | `@greater(rowCount, 0)` | Process data | Skip/log |
| **Environment** | `@equals(env, 'Prod')` | Prod destination | Dev destination |
| **File Type** | `@contains(fileName, '.csv')` | CSV processing | Other format |
| **Time-Based** | `@greater(hour, 18)` | Off-hours logic | Business hours logic |
| **Threshold** | `@greater(fileSize, 1000000)` | Large file handling | Standard processing |
| **Data Quality** | `@equals(nullCount, 0)` | Load to warehouse | Fail pipeline |
| **Incremental** | `@greater(newRecords, 0)` | Incremental load | Full load |
| **Error Recovery** | `@equals(retryCount, 0)` | First attempt | Retry logic |

---

**Real-World Example: Incremental vs. Full Load Decision**

**Scenario:**
- Daily pipeline needs to decide between incremental and full load
- Decision based on watermark table existence and last load date
- If incremental fails, fall back to full load

```json
{
  "activities": [
    {
      "name": "CheckWatermarkExists",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "query": "SELECT COUNT(*) AS WatermarkCount FROM Watermark WHERE TableName = '@{pipeline().parameters.TableName}'"
        },
        "firstRowOnly": true
      }
    },
    {
      "name": "DecideLoadType",
      "type": "IfCondition",
      "dependsOn": [{"activity": "CheckWatermarkExists", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "expression": {
          "value": "@and(
            greater(activity('CheckWatermarkExists').output.firstRow.WatermarkCount, 0),
            lessOrEquals(
              dateDiff(
                activity('CheckWatermarkExists').output.firstRow.LastLoadDate,
                utcNow(),
                'Day'
              ),
              7
            )
          )",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "IncrementalLoad",
            "type": "Copy",
            "typeProperties": {
              "source": {
                "query": "SELECT * FROM SourceTable WHERE ModifiedDate > '@{activity('CheckWatermarkExists').output.firstRow.LastLoadDate}'"
              },
              "sink": {
                "tableName": "@{pipeline().parameters.DestinationTable}",
                "writeBehavior": "Insert"
              }
            }
          },
          {
            "name": "UpdateWatermark",
            "type": "Script",
            "dependsOn": [{"activity": "IncrementalLoad", "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {
              "scripts": [{
                "text": "UPDATE Watermark SET LastLoadDate = '@{utcNow()}' WHERE TableName = '@{pipeline().parameters.TableName}'"
              }]
            }
          }
        ],
        "ifFalseActivities": [
          {
            "name": "FullLoad",
            "type": "Copy",
            "typeProperties": {
              "source": {
                "query": "SELECT * FROM SourceTable"
              },
              "sink": {
                "tableName": "@{pipeline().parameters.DestinationTable}",
                "writeBehavior": "Overwrite"
              }
            }
          },
          {
            "name": "InitializeWatermark",
            "type": "Script",
            "dependsOn": [{"activity": "FullLoad", "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {
              "scripts": [{
                "text": "INSERT INTO Watermark (TableName, LastLoadDate) VALUES ('@{pipeline().parameters.TableName}', '@{utcNow()}')"
              }]
            }
          }
        ]
      }
    },
    {
      "name": "LogLoadType",
      "type": "Web",
      "dependsOn": [{"activity": "DecideLoadType", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "url": "@pipeline().parameters.LoggingEndpoint",
        "method": "POST",
        "body": {
          "TableName": "@pipeline().parameters.TableName",
          "LoadType": "@if(greater(activity('CheckWatermarkExists').output.firstRow.WatermarkCount, 0), 'Incremental', 'Full')",
          "Timestamp": "@utcNow()",
          "Pipeline": "@pipeline().Pipeline",
          "RunId": "@pipeline().RunId"
        }
      }
    }
  ]
}
```

**Logic:**
1. Check if watermark exists for this table
2. **Condition:** Watermark exists AND last load was within 7 days
3. **If True:** Incremental load (load only changes) → Update watermark
4. **If False:** Full load (load everything) → Initialize watermark
5. Log which load type was performed

**Benefits:**
- Automatic decision-making (no manual intervention)
- Graceful degradation (full load if incremental not possible)
- Efficient (incremental when possible)
- Resilient (handles first-time loads)
- Auditable (logs load type)

---

**Debugging If Condition:**

**1. Inspect Expression Values:**

```json
{
  "name": "DebugExpression",
  "type": "SetVariable",
  "typeProperties": {
    "variableName": "ExpressionResult",
    "value": "@string(greater(activity('Lookup1').output.firstRow.Count, 100))"
  }
}

// View variable value in monitoring hub
// Result: "true" or "false"
```

**2. Test Both Paths:**

```
Debug run with parameter sets:
- Set 1: ForceTrue = 'yes' (test True path)
- Set 2: ForceTrue = 'no' (test False path)

Expression:
@equals(pipeline().parameters.ForceTrue, 'yes')
```

**3. Use Web Activity for Logging:**

```json
{
  "ifTrueActivities": [
    {
      "name": "LogTruePath",
      "type": "Web",
      "typeProperties": {
        "url": "https://webhook.site/your-unique-id",
        "method": "POST",
        "body": {
          "message": "If Condition evaluated to TRUE",
          "activityOutput": "@activity('CheckData').output"
        }
      }
    }
  ]
}
```

---

**When to Use If Condition:**

**✅ Use When:**
- Need to execute different logic based on runtime conditions
- Validating data before processing
- Environment-based routing (dev/prod)
- Implementing business rules
- Skipping optional steps conditionally
- Adaptive pipeline behavior needed

**❌ Don't Use When:**
- Simple filtering (use WHERE clause in source query)
- Always executing same activities (remove If Condition)
- Complex multi-way branching (use Switch activity instead)
- Too many nested conditions (refactor to separate pipelines)

---

**If Condition vs. Alternatives:**

| Scenario | Use If Condition | Alternative |
|----------|-----------------|-------------|
| **2 paths (true/false)** | ✅ Perfect use case | - |
| **3+ paths** | ❌ Use Switch instead | Switch Activity |
| **Simple filter** | ❌ Use query WHERE | Source query filtering |
| **Repeating until true** | ❌ Use Until | Until Activity |
| **Always true/false** | ❌ Remove condition | Direct activities |

---

**Key Takeaways:**

1. **Branching Logic:** If Condition enables true/false branching in pipelines
2. **Expression Power:** Supports complex expressions with AND/OR/NOT logic
3. **Both Paths:** Always consider both True and False paths
4. **Keep Simple:** Avoid overly complex nested conditions
5. **Test Thoroughly:** Debug both paths with different parameter values
6. **Common Uses:** Data validation, environment routing, conditional processing
7. **Alternatives:** Use Switch for multi-way branching, Until for loops

The If Condition Activity is **fundamental** for building adaptive, intelligent data pipelines!

---

### Set Variable Activity

**Definition:**
The Set Variable Activity is a control flow activity that assigns a value to a pipeline variable at runtime. Variables act as temporary storage within a pipeline execution, holding values that can be updated and referenced by subsequent activities. Unlike parameters (which are immutable), variables can be modified during pipeline execution, enabling dynamic behavior and stateful processing.

**Purpose:**
- Store intermediate values during pipeline execution
- Build dynamic values from expressions
- Track pipeline state and progress
- Pass computed values between activities
- Accumulate results across iterations
- Implement counters and flags
- Create reusable expression values

**Key Features:**

**1. Variable Types Supported:**
- **String:** Text values
- **Integer:** Whole numbers
- **Float:** Decimal numbers
- **Boolean:** true/false
- **Array:** List of values
- **Object:** Not directly supported (use String with JSON)

**2. Variable Scope:**
- Pipeline-level (accessible by all activities)
- Persists for entire pipeline run
- Reset on each pipeline execution
- Not shared across pipeline runs

**3. Expression Support:**
- Dynamic value assignment using expressions
- Access activity outputs: `@activity('name').output`
- Use pipeline functions: `@concat()`, `@string()`, `@int()`
- Reference other variables/parameters

**4. Sequential Execution:**
- Set Variable is synchronous (blocks until complete)
- Variables updated one at a time
- Order matters when multiple Set Variable activities exist

---

**Basic Configuration:**

```json
{
  "name": "SetProcessedDate",
  "type": "SetVariable",
  "typeProperties": {
    "variableName": "ProcessedDate",
    "value": "@formatDateTime(utcNow(), 'yyyy-MM-dd')"
  }
}
```

**Variable Declaration** (in pipeline definition):

```json
{
  "name": "MyPipeline",
  "properties": {
    "variables": {
      "ProcessedDate": {
        "type": "String",
        "defaultValue": ""
      },
      "RowCount": {
        "type": "Integer",
        "defaultValue": 0
      },
      "IsSuccess": {
        "type": "Boolean",
        "defaultValue": false
      },
      "FileList": {
        "type": "Array",
        "defaultValue": []
      }
    },
    "activities": [
      // Activities here
    ]
  }
}
```

---

**Common Patterns:**

**1. Capture Activity Output for Reuse:**

```json
{
  "activities": [
    {
      "name": "GetRowCount",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "query": "SELECT COUNT(*) AS TotalRows FROM SourceTable"
        },
        "firstRowOnly": true
      }
    },
    {
      "name": "StoreRowCount",
      "type": "SetVariable",
      "dependsOn": [{"activity": "GetRowCount", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "variableName": "TotalRowCount",
        "value": "@string(activity('GetRowCount').output.firstRow.TotalRows)"
      }
    },
    {
      "name": "ProcessData",
      "type": "Copy",
      "dependsOn": [{"activity": "StoreRowCount", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "source": {
          "query": "SELECT * FROM SourceTable"
        },
        "sink": {
          "tableName": "DestinationTable"
        }
      }
    },
    {
      "name": "LogCompletion",
      "type": "Web",
      "dependsOn": [{"activity": "ProcessData", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "url": "@pipeline().parameters.LoggingEndpoint",
        "method": "POST",
        "body": {
          "message": "Processed @{variables('TotalRowCount')} rows",
          "pipeline": "@{pipeline().Pipeline}",
          "timestamp": "@{utcNow()}"
        }
      }
    }
  ]
}
```

**Benefits:**
- Avoid recomputing activity output
- Reference same value in multiple places
- Cleaner expressions in dependent activities

---

**2. Build Dynamic File Path:**

```json
{
  "activities": [
    {
      "name": "BuildFilePath",
      "type": "SetVariable",
      "typeProperties": {
        "variableName": "OutputFilePath",
        "value": "@concat(
          pipeline().parameters.BasePath,
          '/',
          formatDateTime(utcNow(), 'yyyy/MM/dd'),
          '/',
          pipeline().parameters.FileName,
          '_',
          pipeline().RunId,
          '.csv'
        )"
      }
    },
    {
      "name": "SaveToFile",
      "type": "Copy",
      "dependsOn": [{"activity": "BuildFilePath", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "sink": {
          "type": "DelimitedTextSink",
          "fileName": "@variables('OutputFilePath')"
        }
      }
    }
  ]
}

// Example result:
// /data/lakehouse/2026/04/25/sales_abc-123-def.csv
```

---

**3. Conditional Value Assignment:**

```json
{
  "activities": [
    {
      "name": "CheckEnvironment",
      "type": "SetVariable",
      "typeProperties": {
        "variableName": "ConnectionString",
        "value": "@if(
          equals(pipeline().parameters.Environment, 'Production'),
          'Server=prod-server;Database=ProdDB',
          'Server=dev-server;Database=DevDB'
        )"
      }
    },
    {
      "name": "ConnectToDatabase",
      "type": "Lookup",
      "dependsOn": [{"activity": "CheckEnvironment", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "source": {
          "connectionString": "@variables('ConnectionString')",
          "query": "SELECT * FROM ConfigTable"
        }
      }
    }
  ]
}
```

---

**4. Track Processing State:**

```json
{
  "activities": [
    {
      "name": "InitializeStatus",
      "type": "SetVariable",
      "typeProperties": {
        "variableName": "ProcessStatus",
        "value": "Started"
      }
    },
    {
      "name": "ExtractData",
      "type": "Copy",
      "dependsOn": [{"activity": "InitializeStatus", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        // Copy configuration
      }
    },
    {
      "name": "UpdateStatusAfterExtract",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "ExtractData", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "ProcessStatus",
        "value": "Extracted"
      }
    },
    {
      "name": "TransformData",
      "type": "Notebook",
      "dependsOn": [{"activity": "UpdateStatusAfterExtract", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        // Notebook configuration
      }
    },
    {
      "name": "UpdateStatusAfterTransform",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "TransformData", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "ProcessStatus",
        "value": "Transformed"
      }
    },
    {
      "name": "LoadData",
      "type": "Copy",
      "dependsOn": [{"activity": "UpdateStatusAfterTransform", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        // Copy configuration
      }
    },
    {
      "name": "UpdateStatusComplete",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "LoadData", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "ProcessStatus",
        "value": "Completed"
      }
    },
    {
      "name": "LogFinalStatus",
      "type": "Web",
      "dependsOn": [{"activity": "UpdateStatusComplete", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "url": "@pipeline().parameters.LoggingEndpoint",
        "method": "POST",
        "body": {
          "status": "@variables('ProcessStatus')",
          "pipeline": "@pipeline().Pipeline"
        }
      }
    }
  ]
}
```

**State Tracking:**
```
Started → Extracted → Transformed → Completed
```

---

**5. Type Conversion:**

```json
{
  "activities": [
    {
      "name": "ConvertStringToInt",
      "type": "SetVariable",
      "typeProperties": {
        "variableName": "MaxRecords",
        "value": "@int(pipeline().parameters.MaxRecordsString)"
      }
    },
    {
      "name": "ConvertIntToString",
      "type": "SetVariable",
      "typeProperties": {
        "variableName": "RowCountText",
        "value": "@string(activity('Lookup1').output.firstRow.Count)"
      }
    },
    {
      "name": "ConvertToBool",
      "type": "SetVariable",
      "typeProperties": {
        "variableName": "IsActive",
        "value": "@bool(pipeline().parameters.ActiveFlag)"
      }
    }
  ]
}
```

---

**6. Build JSON String for Web Activity:**

```json
{
  "activities": [
    {
      "name": "BuildJSONPayload",
      "type": "SetVariable",
      "typeProperties": {
        "variableName": "WebhookPayload",
        "value": "@concat(
          '{',
            '\"tableName\":\"', pipeline().parameters.TableName, '\",',
            '\"rowCount\":', string(activity('GetCount').output.firstRow.Count), ',',
            '\"processedBy\":\"', pipeline().Pipeline, '\",',
            '\"timestamp\":\"', utcNow(), '\"',
          '}'
        )"
      }
    },
    {
      "name": "SendWebhook",
      "type": "Web",
      "dependsOn": [{"activity": "BuildJSONPayload", "dependencyConditions": ["Succeeded"]}],
      "typeProperties": {
        "url": "@pipeline().parameters.WebhookURL",
        "method": "POST",
        "headers": {
          "Content-Type": "application/json"
        },
        "body": "@json(variables('WebhookPayload'))"
      }
    }
  ]
}
```

---

**Set Variable vs. Append Variable:**

| Aspect | Set Variable | Append Variable |
|--------|-------------|----------------|
| **Purpose** | Replace entire value | Add to array |
| **Variable Types** | All types | Array only |
| **Operation** | Assignment (=) | Append (+=) |
| **Use Case** | Store new value | Build list over iterations |
| **Example** | `var = "new value"` | `arr.append("item")` |

**Set Variable Example:**
```json
{
  "name": "SetValue",
  "type": "SetVariable",
  "typeProperties": {
    "variableName": "CurrentFile",
    "value": "file1.csv"  // Replaces previous value
  }
}
```

**Append Variable Example:**
```json
{
  "name": "AppendValue",
  "type": "AppendVariable",
  "typeProperties": {
    "variableName": "ProcessedFiles",  // Must be Array type
    "value": "file1.csv"  // Adds to array
  }
}

// After 3 appends:
// ProcessedFiles = ["file1.csv", "file2.csv", "file3.csv"]
```

---

**Best Practices:**

**1. Variable Design:**

**✅ DO:**
- Use descriptive variable names (CurrentFileName, not x)
- Initialize variables with default values
- Choose appropriate data types
- Document variable purpose in pipeline description
- Limit number of variables (too many = complex)

**❌ DON'T:**
- Use variables for large datasets (use tables/files)
- Create unnecessary variables
- Reuse variables for different purposes
- Use ambiguous names (temp, var1, etc.)

**2. Performance:**

```json
// ✅ GOOD: Set variable once, reference many times
Set Variable (RowCount) → Use in 5 activities

// ❌ BAD: Recompute same value
Activity 1: @activity('Lookup1').output.firstRow.Count
Activity 2: @activity('Lookup1').output.firstRow.Count
Activity 3: @activity('Lookup1').output.firstRow.Count
// Cleaner to store in variable!
```

**3. Type Safety:**

```json
// ✅ GOOD: Explicit type conversion
{
  "variableName": "RowCount",  // Integer type
  "value": "@int(activity('Lookup1').output.firstRow.CountString)"
}

// ❌ BAD: Type mismatch
{
  "variableName": "RowCount",  // Integer type
  "value": "@activity('Lookup1').output.firstRow.CountString"  // String!
}
```

**4. Error Handling:**

```json
// ✅ GOOD: Handle potential NULL
{
  "variableName": "FileName",
  "value": "@if(
    equals(activity('GetFile').output.firstRow.FileName, null),
    'default.csv',
    activity('GetFile').output.firstRow.FileName
  )"
}

// ❌ BAD: No NULL handling
{
  "variableName": "FileName",
  "value": "@activity('GetFile').output.firstRow.FileName"  // May be NULL!
}
```

**5. Variable Reuse:**

```json
// ✅ GOOD: Separate variables for different purposes
SetVariable: CurrentDate (today's date)
SetVariable: ProcessedFiles (file count)

// ❌ BAD: Reusing same variable
SetVariable: TempValue = "2026-04-25"
... (use it)
SetVariable: TempValue = 12345
... (use it)
// Confusing! Use separate variables.
```

---

**Real-World Example: Dynamic Date Range Processing**

**Scenario:**
- Process data for date range
- Start date from parameter
- End date = today
- Build dynamic query with date range
- Log date range used

```json
{
  "properties": {
    "parameters": {
      "StartDate": {
        "type": "String",
        "defaultValue": "2026-01-01"
      }
    },
    "variables": {
      "StartDateFormatted": {
        "type": "String"
      },
      "EndDateFormatted": {
        "type": "String"
      },
      "DateRangeQuery": {
        "type": "String"
      },
      "RecordsProcessed": {
        "type": "String"
      }
    },
    "activities": [
      {
        "name": "SetStartDate",
        "type": "SetVariable",
        "typeProperties": {
          "variableName": "StartDateFormatted",
          "value": "@formatDateTime(pipeline().parameters.StartDate, 'yyyy-MM-dd')"
        }
      },
      {
        "name": "SetEndDate",
        "type": "SetVariable",
        "dependsOn": [{"activity": "SetStartDate", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
          "variableName": "EndDateFormatted",
          "value": "@formatDateTime(utcNow(), 'yyyy-MM-dd')"
        }
      },
      {
        "name": "BuildQuery",
        "type": "SetVariable",
        "dependsOn": [{"activity": "SetEndDate", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
          "variableName": "DateRangeQuery",
          "value": "@concat(
            'SELECT * FROM SalesTable WHERE OrderDate >= ''',
            variables('StartDateFormatted'),
            ''' AND OrderDate <= ''',
            variables('EndDateFormatted'),
            ''''
          )"
        }
      },
      {
        "name": "ExtractSalesData",
        "type": "Copy",
        "dependsOn": [{"activity": "BuildQuery", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
          "source": {
            "type": "SqlSource",
            "query": "@variables('DateRangeQuery')"
          },
          "sink": {
            "type": "LakehouseSink",
            "tableName": "Bronze.Sales"
          }
        }
      },
      {
        "name": "SetRecordCount",
        "type": "SetVariable",
        "dependsOn": [{"activity": "ExtractSalesData", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
          "variableName": "RecordsProcessed",
          "value": "@string(activity('ExtractSalesData').output.rowsCopied)"
        }
      },
      {
        "name": "LogProcessing",
        "type": "Web",
        "dependsOn": [{"activity": "SetRecordCount", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
          "url": "@pipeline().parameters.LoggingEndpoint",
          "method": "POST",
          "body": {
            "pipeline": "@{pipeline().Pipeline}",
            "startDate": "@{variables('StartDateFormatted')}",
            "endDate": "@{variables('EndDateFormatted')}",
            "recordsProcessed": "@{variables('RecordsProcessed')}",
            "query": "@{variables('DateRangeQuery')}",
            "timestamp": "@{utcNow()}"
          }
        }
      }
    ]
  }
}
```

**Execution Flow:**
```
1. SetStartDate: "2026-01-01"
2. SetEndDate: "2026-04-25"
3. BuildQuery: "SELECT * FROM SalesTable WHERE OrderDate >= '2026-01-01' AND OrderDate <= '2026-04-25'"
4. ExtractSalesData: Execute query, copy data
5. SetRecordCount: "12,345"
6. LogProcessing: Send log with all variables
```

**Benefits:**
- **Readable:** Variables make complex expressions clear
- **Reusable:** Date values computed once, used multiple times
- **Debuggable:** Can inspect variable values in monitoring
- **Auditable:** Log contains exact query and date range used
- **Maintainable:** Easy to modify date logic in one place

---

**Debugging Variables:**

**1. View Variable Values in Monitoring Hub:**

```
Monitoring Hub → Pipeline Run → Activity Runs → Set Variable activity
- Click on activity
- View "Output" tab
- See variable value assigned
```

**2. Log Variables with Web Activity:**

```json
{
  "name": "DebugVariable",
  "type": "Web",
  "typeProperties": {
    "url": "https://webhook.site/your-unique-id",
    "method": "POST",
    "body": {
      "variableName": "CurrentFile",
      "variableValue": "@variables('CurrentFile')",
      "timestamp": "@utcNow()"
    }
  }
}
```

**3. Use Multiple Set Variable for Debugging:**

```json
{
  "name": "DebugExpression",
  "type": "SetVariable",
  "typeProperties": {
    "variableName": "DebugValue",
    "value": "@concat(
      'Activity output: ',
      string(activity('Lookup1').output.firstRow.Count),
      ' | Expression result: ',
      string(greater(activity('Lookup1').output.firstRow.Count, 100))
    )"
  }
}

// Result in monitoring: "Activity output: 250 | Expression result: true"
```

---

**When to Use Set Variable:**

**✅ Use When:**
- Need to store intermediate computation results
- Building complex dynamic values
- Reusing same expression in multiple activities
- Tracking pipeline state/progress
- Type conversion needed
- Simplifying complex expressions

**❌ Don't Use When:**
- Value never referenced again (waste of activity)
- Could use parameter instead (parameters are free)
- Storing large datasets (use tables/files)
- Value only used once (use expression directly)

---

**Set Variable vs. Parameter:**

| Aspect | Variable | Parameter |
|--------|----------|-----------|
| **Mutability** | Can change during run | Read-only |
| **Scope** | Pipeline execution | Entire pipeline |
| **Assignment** | Set Variable activity | External/trigger |
| **Performance** | Activity overhead | No overhead |
| **Use Case** | Runtime computation | Configuration |

---

**Common Variable Naming Conventions:**

```json
// ✅ GOOD: Clear, descriptive names
"variables": {
  "CurrentFileName": {"type": "String"},
  "TotalRowsProcessed": {"type": "Integer"},
  "IsIncrementalLoad": {"type": "Boolean"},
  "ProcessedFileList": {"type": "Array"},
  "QueryStartDate": {"type": "String"}
}

// ❌ BAD: Ambiguous names
"variables": {
  "var1": {"type": "String"},
  "temp": {"type": "Integer"},
  "x": {"type": "Boolean"},
  "arr": {"type": "Array"}
}
```

---

**Key Takeaways:**

1. **Purpose:** Set Variable stores values that can change during pipeline execution
2. **Types:** Supports String, Integer, Float, Boolean, Array
3. **Scope:** Pipeline-level, accessible by all activities
4. **Performance:** Adds activity overhead, use judiciously
5. **vs. Parameter:** Parameters are immutable, variables can change
6. **vs. Append:** Set replaces value, Append adds to array
7. **Best Practice:** Use descriptive names, initialize with defaults, handle NULLs
8. **Common Uses:** Store activity outputs, build dynamic expressions, track state

Set Variable Activity is **essential** for creating dynamic, stateful pipelines!

---

### Web Activity

**Definition:**
The Web Activity is a versatile control flow activity that makes HTTP requests to REST endpoints, webhooks, or custom APIs from within a pipeline. It enables integration with external systems, services, and custom applications by sending HTTP requests (GET, POST, PUT, DELETE, PATCH) and processing responses. This makes it a powerful tool for extending pipeline capabilities beyond built-in connectors.

**Purpose:**
- Call REST APIs to trigger external processes
- Send notifications via webhooks (Teams, Slack, custom)
- Retrieve configuration from external services
- Post pipeline status/metrics to monitoring systems
- Trigger Azure Functions or Logic Apps
- Implement custom authentication flows
- Integrate with third-party services
- Call internal microservices

**Key Features:**

**1. HTTP Methods Supported:**
- **GET:** Retrieve data from API
- **POST:** Send data to API
- **PUT:** Update resources
- **DELETE:** Delete resources
- **PATCH:** Partial updates

**2. Authentication Types:**
- **None:** No authentication
- **Basic:** Username + password
- **Client Certificate:** Certificate-based auth
- **MSI (Managed Service Identity):** Azure AD identity
- **Service Principal:** Azure AD app registration
- **Web Activity Authentication:** Token-based

**3. Request Configuration:**
- Custom headers
- Request body (JSON, text, etc.)
- Query parameters
- Timeout settings
- Retry policy

**4. Response Handling:**
- Access response body: `@activity('WebActivity').output.Response`
- Access status code: `@activity('WebActivity').output.statusCode`
- Parse JSON responses
- Use response in subsequent activities

---

**Basic Configuration:**

```json
{
  "name": "CallRESTAPI",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://api.example.com/data",
    "method": "GET",
    "headers": {
      "Content-Type": "application/json"
    },
    "authentication": {
      "type": "None"
    }
  }
}
```

**POST with Body:**

```json
{
  "name": "SendDataToAPI",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://api.example.com/submit",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "Authorization": "Bearer @{pipeline().parameters.APIToken}"
    },
    "body": {
      "tableName": "@{pipeline().parameters.TableName}",
      "rowCount": "@{activity('GetCount').output.firstRow.Count}",
      "timestamp": "@{utcNow()}"
    }
  }
}
```

---

**Common Patterns:**

**1. Send Teams/Slack Notification:**

**Microsoft Teams:**
```json
{
  "name": "SendTeamsNotification",
  "type": "WebActivity",
  "dependsOn": [
    {"activity": "DataLoad", "dependencyConditions": ["Succeeded"]}
  ],
  "typeProperties": {
    "url": "@pipeline().parameters.TeamsWebhookURL",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json"
    },
    "body": {
      "@type": "MessageCard",
      "@context": "https://schema.org/extensions",
      "summary": "Pipeline Completed",
      "themeColor": "0078D7",
      "title": "✅ Data Pipeline Success",
      "sections": [
        {
          "activityTitle": "Pipeline: @{pipeline().Pipeline}",
          "facts": [
            {
              "name": "Run ID:",
              "value": "@{pipeline().RunId}"
            },
            {
              "name": "Table:",
              "value": "@{pipeline().parameters.TableName}"
            },
            {
              "name": "Rows Loaded:",
              "value": "@{activity('DataLoad').output.rowsCopied}"
            },
            {
              "name": "Completed:",
              "value": "@{utcNow()}"
            }
          ]
        }
      ]
    }
  }
}
```

**Slack:**
```json
{
  "name": "SendSlackNotification",
  "type": "WebActivity",
  "typeProperties": {
    "url": "@pipeline().parameters.SlackWebhookURL",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json"
    },
    "body": {
      "text": "Pipeline completed successfully!",
      "blocks": [
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": "*Pipeline:* @{pipeline().Pipeline}\n*Status:* ✅ Success\n*Rows:* @{activity('Copy1').output.rowsCopied}"
          }
        }
      ]
    }
  }
}
```

---

**2. Error Notification (On Failure):**

```json
{
  "activities": [
    {
      "name": "DataLoad",
      "type": "Copy",
      "typeProperties": {
        // Copy configuration
      }
    },
    {
      "name": "NotifySuccess",
      "type": "WebActivity",
      "dependsOn": [
        {"activity": "DataLoad", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "url": "@pipeline().parameters.WebhookURL",
        "method": "POST",
        "body": {
          "status": "SUCCESS",
          "pipeline": "@{pipeline().Pipeline}",
          "message": "Data load completed successfully",
          "rowsLoaded": "@{activity('DataLoad').output.rowsCopied}"
        }
      }
    },
    {
      "name": "NotifyFailure",
      "type": "WebActivity",
      "dependsOn": [
        {"activity": "DataLoad", "dependencyConditions": ["Failed"]}
      ],
      "typeProperties": {
        "url": "@pipeline().parameters.WebhookURL",
        "method": "POST",
        "body": {
          "status": "FAILURE",
          "pipeline": "@{pipeline().Pipeline}",
          "message": "Data load failed",
          "error": "@{activity('DataLoad').error.message}",
          "errorCode": "@{activity('DataLoad').error.errorCode}"
        }
      }
    }
  ]
}
```

---

**3. Call Azure Function:**

```json
{
  "name": "TriggerAzureFunction",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://my-function-app.azurewebsites.net/api/ProcessData",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "x-functions-key": "@{pipeline().parameters.FunctionKey}"
    },
    "body": {
      "tableName": "@{pipeline().parameters.TableName}",
      "processingDate": "@{formatDateTime(utcNow(), 'yyyy-MM-dd')}",
      "pipelineRunId": "@{pipeline().RunId}"
    }
  }
}

// Use function response
{
  "name": "CheckFunctionResponse",
  "type": "IfCondition",
  "dependsOn": [
    {"activity": "TriggerAzureFunction", "dependencyConditions": ["Succeeded"]}
  ],
  "typeProperties": {
    "expression": {
      "value": "@equals(activity('TriggerAzureFunction').output.status, 'success')",
      "type": "Expression"
    },
    "ifTrueActivities": [
      // Continue processing
    ],
    "ifFalseActivities": [
      // Handle failure
    ]
  }
}
```

---

**4. Get Configuration from External API:**

```json
{
  "activities": [
    {
      "name": "GetConfigFromAPI",
      "type": "WebActivity",
      "typeProperties": {
        "url": "https://config-service.com/api/pipeline-config",
        "method": "GET",
        "headers": {
          "Authorization": "Bearer @{pipeline().parameters.ConfigAPIToken}"
        }
      }
    },
    {
      "name": "SetConnectionString",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "GetConfigFromAPI", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "DBConnectionString",
        "value": "@activity('GetConfigFromAPI').output.Response.connectionString"
      }
    },
    {
      "name": "SetBatchSize",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "GetConfigFromAPI", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "BatchSize",
        "value": "@string(activity('GetConfigFromAPI').output.Response.batchSize)"
      }
    },
    {
      "name": "ProcessWithConfig",
      "type": "Copy",
      "dependsOn": [
        {"activity": "SetConnectionString", "dependencyConditions": ["Succeeded"]},
        {"activity": "SetBatchSize", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "source": {
          "connectionString": "@variables('DBConnectionString')",
          "query": "SELECT TOP @{variables('BatchSize')} * FROM SourceTable"
        }
      }
    }
  ]
}
```

---

**5. Custom Logging/Auditing:**

```json
{
  "name": "LogPipelineStart",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://audit-service.com/api/log",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "X-API-Key": "@{pipeline().parameters.AuditAPIKey}"
    },
    "body": {
      "eventType": "PipelineStart",
      "pipelineName": "@{pipeline().Pipeline}",
      "runId": "@{pipeline().RunId}",
      "triggeredBy": "@{pipeline().TriggeredByPipelineName}",
      "triggerType": "@{pipeline().TriggerType}",
      "triggerTime": "@{pipeline().TriggerTime}",
      "parameters": {
        "tableName": "@{pipeline().parameters.TableName}",
        "environment": "@{pipeline().parameters.Environment}"
      },
      "timestamp": "@{utcNow()}"
    }
  }
}

// Log completion
{
  "name": "LogPipelineEnd",
  "type": "WebActivity",
  "dependsOn": [
    {"activity": "ProcessData", "dependencyConditions": ["Succeeded"]}
  ],
  "typeProperties": {
    "url": "https://audit-service.com/api/log",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "X-API-Key": "@{pipeline().parameters.AuditAPIKey}"
    },
    "body": {
      "eventType": "PipelineEnd",
      "pipelineName": "@{pipeline().Pipeline}",
      "runId": "@{pipeline().RunId}",
      "status": "Success",
      "rowsProcessed": "@{activity('ProcessData').output.rowsCopied}",
      "duration": "@{activity('ProcessData').output.copyDuration}",
      "timestamp": "@{utcNow()}"
    }
  }
}
```

---

**6. Trigger External Workflow (Logic App):**

```json
{
  "name": "TriggerLogicApp",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://prod-123.eastus.logic.azure.com:443/workflows/abc123.../triggers/manual/paths/invoke",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json"
    },
    "body": {
      "dataSource": "@{pipeline().parameters.Source}",
      "destinationTable": "@{pipeline().parameters.Destination}",
      "recordCount": "@{activity('CopyData').output.rowsCopied}",
      "pipelineRunId": "@{pipeline().RunId}"
    }
  }
}
```

---

**7. Dynamic API Call Based on Metadata:**

```json
{
  "activities": [
    {
      "name": "GetTableMetadata",
      "type": "Lookup",
      "typeProperties": {
        "source": {
          "query": "SELECT TableName, APIEndpoint, APIMethod FROM MetadataTable WHERE TableID = @{pipeline().parameters.TableID}"
        },
        "firstRowOnly": true
      }
    },
    {
      "name": "CallDynamicAPI",
      "type": "WebActivity",
      "dependsOn": [
        {"activity": "GetTableMetadata", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "url": "@activity('GetTableMetadata').output.firstRow.APIEndpoint",
        "method": "@activity('GetTableMetadata').output.firstRow.APIMethod",
        "headers": {
          "Content-Type": "application/json"
        },
        "body": {
          "table": "@{activity('GetTableMetadata').output.firstRow.TableName}",
          "timestamp": "@{utcNow()}"
        }
      }
    }
  ]
}
```

---

**Authentication Examples:**

**1. Basic Authentication:**

```json
{
  "name": "WebWithBasicAuth",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://api.example.com/data",
    "method": "GET",
    "authentication": {
      "type": "Basic",
      "username": "@pipeline().parameters.APIUsername",
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "MyKeyVault",
          "type": "LinkedServiceReference"
        },
        "secretName": "APIPassword"
      }
    }
  }
}
```

**2. Bearer Token:**

```json
{
  "name": "WebWithBearerToken",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://api.example.com/data",
    "method": "GET",
    "headers": {
      "Authorization": "Bearer @{pipeline().parameters.BearerToken}"
    }
  }
}
```

**3. MSI (Managed Service Identity):**

```json
{
  "name": "WebWithMSI",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://management.azure.com/subscriptions/.../resourceGroups/...",
    "method": "GET",
    "authentication": {
      "type": "MSI",
      "resource": "https://management.azure.com/"
    }
  }
}
```

---

**Response Handling:**

**1. Access JSON Response:**

```json
{
  "activities": [
    {
      "name": "CallAPI",
      "type": "WebActivity",
      "typeProperties": {
        "url": "https://api.example.com/getConfig",
        "method": "GET"
      }
    },
    {
      "name": "UseResponseValue",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "CallAPI", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "MaxRecords",
        "value": "@string(activity('CallAPI').output.Response.maxRecords)"
      }
    }
  ]
}

// API Response:
// {
//   "maxRecords": 1000,
//   "batchSize": 100,
//   "enabled": true
// }
```

**2. Check Status Code:**

```json
{
  "name": "CheckAPIStatus",
  "type": "IfCondition",
  "dependsOn": [
    {"activity": "CallAPI", "dependencyConditions": ["Succeeded"]}
  ],
  "typeProperties": {
    "expression": {
      "value": "@equals(activity('CallAPI').output.statusCode, 200)",
      "type": "Expression"
    },
    "ifTrueActivities": [
      // Process response
    ],
    "ifFalseActivities": [
      // Handle error
    ]
  }
}
```

**3. Parse Array Response:**

```json
{
  "activities": [
    {
      "name": "GetFileList",
      "type": "WebActivity",
      "typeProperties": {
        "url": "https://api.example.com/files",
        "method": "GET"
      }
    },
    {
      "name": "ProcessFiles",
      "type": "ForEach",
      "dependsOn": [
        {"activity": "GetFileList", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "items": "@activity('GetFileList').output.Response.files",
        "activities": [
          {
            "name": "ProcessFile",
            "type": "Copy",
            "typeProperties": {
              "source": {
                "fileName": "@item().name"
              }
            }
          }
        ]
      }
    }
  ]
}

// API Response:
// {
//   "files": [
//     {"name": "file1.csv", "size": 1024},
//     {"name": "file2.csv", "size": 2048}
//   ]
// }
```

---

**Best Practices:**

**1. Security:**

**✅ DO:**
- Store API keys/tokens in Azure Key Vault
- Use Managed Service Identity when possible
- Use HTTPS endpoints only
- Rotate credentials regularly
- Implement least privilege access

**❌ DON'T:**
- Hardcode credentials in pipeline
- Use HTTP (unencrypted) for sensitive data
- Log sensitive data in request/response
- Share API keys across environments

**2. Error Handling:**

```json
{
  "activities": [
    {
      "name": "CallAPI",
      "type": "WebActivity",
      "typeProperties": {
        "url": "@pipeline().parameters.APIURL",
        "method": "POST",
        "body": {"data": "value"}
      }
    },
    {
      "name": "OnAPISuccess",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "CallAPI", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "APIStatus",
        "value": "Success"
      }
    },
    {
      "name": "OnAPIFailure",
      "type": "Web",
      "dependsOn": [
        {"activity": "CallAPI", "dependencyConditions": ["Failed"]}
      ],
      "typeProperties": {
        "url": "@pipeline().parameters.ErrorWebhook",
        "method": "POST",
        "body": {
          "error": "API call failed",
          "activity": "CallAPI",
          "pipeline": "@{pipeline().Pipeline}",
          "runId": "@{pipeline().RunId}"
        }
      }
    }
  ]
}
```

**3. Timeout & Retry:**

```json
{
  "name": "CallAPIWithRetry",
  "type": "WebActivity",
  "typeProperties": {
    "url": "https://api.example.com/data",
    "method": "GET",
    "timeout": "00:05:00"  // 5 minutes
  },
  "policy": {
    "timeout": "00:10:00",
    "retry": 3,
    "retryIntervalInSeconds": 30
  }
}
```

**4. Dynamic URLs:**

```json
{
  "name": "CallDynamicEndpoint",
  "type": "WebActivity",
  "typeProperties": {
    "url": "@concat(
      pipeline().parameters.BaseURL,
      '/api/v1/',
      pipeline().parameters.Endpoint,
      '?date=',
      formatDateTime(utcNow(), 'yyyy-MM-dd')
    )",
    "method": "GET"
  }
}

// Example result:
// https://api.example.com/api/v1/sales?date=2026-04-25
```

**5. Request Body Templates:**

```json
{
  "name": "SendStructuredData",
  "type": "WebActivity",
  "typeProperties": {
    "url": "@pipeline().parameters.WebhookURL",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json"
    },
    "body": {
      "metadata": {
        "pipeline": "@{pipeline().Pipeline}",
        "runId": "@{pipeline().RunId}",
        "triggerTime": "@{pipeline().TriggerTime}",
        "environment": "@{pipeline().parameters.Environment}"
      },
      "data": {
        "tableName": "@{pipeline().parameters.TableName}",
        "recordCount": "@{activity('GetCount').output.firstRow.Count}",
        "processedDate": "@{formatDateTime(utcNow(), 'yyyy-MM-dd HH:mm:ss')}"
      },
      "status": {
        "code": "200",
        "message": "Processing completed successfully"
      }
    }
  }
}
```

---

**Real-World Example: Complete Notification System**

**Scenario:**
- Send pipeline start notification
- Process data
- Send success/failure notification with details
- Log to external system

```json
{
  "properties": {
    "parameters": {
      "TeamsWebhookURL": {"type": "String"},
      "LoggingAPIEndpoint": {"type": "String"},
      "TableName": {"type": "String"}
    },
    "variables": {
      "StartTime": {"type": "String"},
      "EndTime": {"type": "String"},
      "RowsProcessed": {"type": "String"}
    },
    "activities": [
      {
        "name": "SetStartTime",
        "type": "SetVariable",
        "typeProperties": {
          "variableName": "StartTime",
          "value": "@utcNow()"
        }
      },
      {
        "name": "NotifyStart",
        "type": "WebActivity",
        "dependsOn": [
          {"activity": "SetStartTime", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "url": "@pipeline().parameters.TeamsWebhookURL",
          "method": "POST",
          "body": {
            "@type": "MessageCard",
            "themeColor": "0078D7",
            "title": "🔄 Pipeline Started",
            "sections": [{
              "facts": [
                {"name": "Pipeline", "value": "@{pipeline().Pipeline}"},
                {"name": "Table", "value": "@{pipeline().parameters.TableName}"},
                {"name": "Start Time", "value": "@{variables('StartTime')}"}
              ]
            }]
          }
        }
      },
      {
        "name": "ProcessData",
        "type": "Copy",
        "dependsOn": [
          {"activity": "NotifyStart", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "source": {
            "query": "SELECT * FROM @{pipeline().parameters.TableName}"
          },
          "sink": {
            "tableName": "Destination.@{pipeline().parameters.TableName}"
          }
        }
      },
      {
        "name": "SetEndTime",
        "type": "SetVariable",
        "dependsOn": [
          {"activity": "ProcessData", "dependencyConditions": ["Completed"]}
        ],
        "typeProperties": {
          "variableName": "EndTime",
          "value": "@utcNow()"
        }
      },
      {
        "name": "SetRowCount",
        "type": "SetVariable",
        "dependsOn": [
          {"activity": "ProcessData", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "variableName": "RowsProcessed",
          "value": "@string(activity('ProcessData').output.rowsCopied)"
        }
      },
      {
        "name": "NotifySuccess",
        "type": "WebActivity",
        "dependsOn": [
          {"activity": "SetRowCount", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "url": "@pipeline().parameters.TeamsWebhookURL",
          "method": "POST",
          "body": {
            "@type": "MessageCard",
            "themeColor": "28a745",
            "title": "✅ Pipeline Completed Successfully",
            "sections": [{
              "facts": [
                {"name": "Pipeline", "value": "@{pipeline().Pipeline}"},
                {"name": "Table", "value": "@{pipeline().parameters.TableName}"},
                {"name": "Rows Processed", "value": "@{variables('RowsProcessed')}"},
                {"name": "Start Time", "value": "@{variables('StartTime')}"},
                {"name": "End Time", "value": "@{variables('EndTime')}"},
                {"name": "Duration", "value": "@{activity('ProcessData').output.copyDuration} seconds"}
              ]
            }]
          }
        }
      },
      {
        "name": "NotifyFailure",
        "type": "WebActivity",
        "dependsOn": [
          {"activity": "ProcessData", "dependencyConditions": ["Failed"]}
        ],
        "typeProperties": {
          "url": "@pipeline().parameters.TeamsWebhookURL",
          "method": "POST",
          "body": {
            "@type": "MessageCard",
            "themeColor": "dc3545",
            "title": "❌ Pipeline Failed",
            "sections": [{
              "facts": [
                {"name": "Pipeline", "value": "@{pipeline().Pipeline}"},
                {"name": "Table", "value": "@{pipeline().parameters.TableName}"},
                {"name": "Error", "value": "@{activity('ProcessData').error.message}"},
                {"name": "Error Code", "value": "@{activity('ProcessData').error.errorCode}"},
                {"name": "Failed At", "value": "@{utcNow()}"}
              ]
            }]
          }
        }
      },
      {
        "name": "LogToExternalSystem",
        "type": "WebActivity",
        "dependsOn": [
          {"activity": "SetEndTime", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "url": "@pipeline().parameters.LoggingAPIEndpoint",
          "method": "POST",
          "headers": {
            "Content-Type": "application/json"
          },
          "body": {
            "pipelineName": "@{pipeline().Pipeline}",
            "runId": "@{pipeline().RunId}",
            "tableName": "@{pipeline().parameters.TableName}",
            "status": "@{if(equals(activity('ProcessData').status, 'Succeeded'), 'Success', 'Failed')}",
            "rowsProcessed": "@{if(equals(activity('ProcessData').status, 'Succeeded'), variables('RowsProcessed'), '0')}",
            "startTime": "@{variables('StartTime')}",
            "endTime": "@{variables('EndTime')}",
            "durationSeconds": "@{if(equals(activity('ProcessData').status, 'Succeeded'), activity('ProcessData').output.copyDuration, 0)}",
            "errorMessage": "@{if(equals(activity('ProcessData').status, 'Failed'), activity('ProcessData').error.message, '')}"
          }
        }
      }
    ]
  }
}
```

**Execution Flow:**

**Success Path:**
```
1. SetStartTime → "2026-04-25T10:00:00Z"
2. NotifyStart → Teams message: "🔄 Pipeline Started"
3. ProcessData → Copy 10,000 rows
4. SetEndTime → "2026-04-25T10:05:30Z"
5. SetRowCount → "10000"
6. NotifySuccess → Teams message: "✅ Pipeline Completed Successfully | 10,000 rows | 5.5 minutes"
7. LogToExternalSystem → POST log entry
```

**Failure Path:**
```
1. SetStartTime → "2026-04-25T10:00:00Z"
2. NotifyStart → Teams message: "🔄 Pipeline Started"
3. ProcessData → FAILED (connection timeout)
4. SetEndTime → "2026-04-25T10:02:15Z"
5. NotifyFailure → Teams message: "❌ Pipeline Failed | Error: Connection timeout"
6. LogToExternalSystem → POST failure log
```

---

**When to Use Web Activity:**

**✅ Use When:**
- Calling REST APIs or webhooks
- Sending notifications (Teams, Slack, email services)
- Triggering external processes (Azure Functions, Logic Apps)
- Getting configuration from external services
- Logging/auditing to external systems
- Integrating with third-party services
- Custom authentication workflows

**❌ Don't Use When:**
- Built-in connector exists (use Copy Activity instead)
- Need complex data transformation (use Notebook/Data Flow)
- Simple internal pipeline communication (use Invoke Pipeline)
- Large data transfer (use Copy Activity)

---

**Web Activity vs. Alternatives:**

| Use Case | Web Activity | Alternative |
|----------|--------------|-------------|
| **REST API call** | ✅ Perfect | - |
| **Webhook notification** | ✅ Perfect | Teams Activity (limited) |
| **Trigger Azure Function** | ✅ Good | Functions Activity |
| **Get external config** | ✅ Good | Lookup (for databases) |
| **Large data transfer** | ❌ Use Copy | Copy Activity |
| **Complex transformation** | ❌ Use Notebook | Notebook Activity |

---

**Common HTTP Status Codes:**

| Code | Meaning | Action |
|------|---------|--------|
| 200 | OK | Success, process response |
| 201 | Created | Resource created successfully |
| 204 | No Content | Success, no response body |
| 400 | Bad Request | Check request body/parameters |
| 401 | Unauthorized | Check authentication |
| 403 | Forbidden | Check permissions |
| 404 | Not Found | Check URL/endpoint |
| 429 | Too Many Requests | Implement retry with backoff |
| 500 | Server Error | Retry or contact API owner |
| 503 | Service Unavailable | Retry later |

---

**Key Takeaways:**

1. **Purpose:** Web Activity calls REST APIs and webhooks from pipelines
2. **Methods:** Supports GET, POST, PUT, DELETE, PATCH
3. **Authentication:** Multiple types (Basic, Bearer, MSI, Service Principal)
4. **Response:** Access via `@activity('name').output.Response`
5. **Common Uses:** Notifications, external triggers, configuration retrieval, logging
6. **Security:** Always use Key Vault for credentials
7. **Error Handling:** Implement retry logic and failure notifications
8. **Best Practice:** Use for integration, not data movement

Web Activity is **essential** for integrating pipelines with external systems and enabling rich notification capabilities!

---

### Wait Activity

**Definition:**
The Wait Activity is a simple control flow activity that pauses pipeline execution for a specified duration. It introduces a deliberate delay before proceeding to the next activity, similar to a `sleep()` function in programming. This is useful for timing control, rate limiting, waiting for external processes, or coordinating with scheduled operations.

**Purpose:**
- Add delays between activities
- Wait for external processes to complete
- Implement rate limiting for API calls
- Coordinate with scheduled operations
- Allow time for file availability
- Space out resource-intensive operations
- Implement polling intervals

**Key Features:**

**1. Duration Specification:**
- Specify wait time in seconds
- Minimum: 1 second
- Maximum: 24 hours (86,400 seconds)
- Exact wait time guaranteed

**2. Simple Configuration:**
- Only one parameter: `waitTimeInSeconds`
- No complex logic needed
- Synchronous execution (blocks pipeline)

**3. Use Cases:**
- Wait for file to be fully written
- Delay before retry
- Rate limiting between API calls
- Wait for external batch jobs
- Time-based coordination

---

**Basic Configuration:**

```json
{
  "name": "Wait60Seconds",
  "type": "Wait",
  "typeProperties": {
    "waitTimeInSeconds": 60
  }
}
```

**Dynamic Wait Time:**

```json
{
  "name": "DynamicWait",
  "type": "Wait",
  "typeProperties": {
    "waitTimeInSeconds": "@pipeline().parameters.WaitSeconds"
  }
}

// Parameter: WaitSeconds = 120
```

---

**Common Patterns:**

**1. Wait for File to Be Fully Written:**

```json
{
  "activities": [
    {
      "name": "DetectFileArrival",
      "type": "GetMetadata",
      "typeProperties": {
        "dataset": {
          "referenceName": "InputFiles",
          "type": "DatasetReference"
        },
        "fieldList": ["exists", "lastModified"]
      }
    },
    {
      "name": "WaitForFileToSettle",
      "type": "Wait",
      "dependsOn": [
        {"activity": "DetectFileArrival", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "waitTimeInSeconds": 300  // Wait 5 minutes for file to be fully written
      }
    },
    {
      "name": "ProcessFile",
      "type": "Copy",
      "dependsOn": [
        {"activity": "WaitForFileToSettle", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        // Process the file
      }
    }
  ]
}
```

**Scenario:**
- Large file being uploaded to storage
- Don't want to start processing while upload is still in progress
- Wait 5 minutes after file detection to ensure upload is complete

---

**2. Rate Limiting API Calls:**

```json
{
  "name": "CallAPIWithRateLimit",
  "type": "ForEach",
  "typeProperties": {
    "items": "@range(1, 20)",  // 20 API calls
    "isSequential": true,  // One at a time
    "activities": [
      {
        "name": "CallAPI",
        "type": "WebActivity",
        "typeProperties": {
          "url": "https://api.example.com/data?id=@{item()}",
          "method": "GET"
        }
      },
      {
        "name": "WaitBetweenCalls",
        "type": "Wait",
        "dependsOn": [
          {"activity": "CallAPI", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "waitTimeInSeconds": 2  // 2 seconds between calls
        }
      }
    ]
  }
}
```

**Result:**
- Make 20 API calls sequentially
- 2-second delay between each call
- Prevents hitting API rate limits (e.g., 30 calls per minute)

---

**3. Retry with Backoff:**

```json
{
  "activities": [
    {
      "name": "CallExternalService",
      "type": "WebActivity",
      "typeProperties": {
        "url": "@pipeline().parameters.ExternalServiceURL",
        "method": "GET"
      }
    },
    {
      "name": "OnSuccess",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "CallExternalService", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "Status",
        "value": "Success"
      }
    },
    {
      "name": "OnFailure_WaitBeforeRetry",
      "type": "Wait",
      "dependsOn": [
        {"activity": "CallExternalService", "dependencyConditions": ["Failed"]}
      ],
      "typeProperties": {
        "waitTimeInSeconds": 30  // Wait 30 seconds before retry
      }
    },
    {
      "name": "RetryCall",
      "type": "WebActivity",
      "dependsOn": [
        {"activity": "OnFailure_WaitBeforeRetry", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "url": "@pipeline().parameters.ExternalServiceURL",
        "method": "GET"
      }
    }
  ]
}
```

**Logic:**
- Try external service call
- If fails: Wait 30 seconds → Retry
- If succeeds: Continue

---

**4. Wait for Scheduled Process:**

```json
{
  "activities": [
    {
      "name": "TriggerExternalBatchJob",
      "type": "WebActivity",
      "typeProperties": {
        "url": "@pipeline().parameters.BatchJobTriggerURL",
        "method": "POST",
        "body": {
          "jobName": "DailyProcessing",
          "parameters": {"date": "@{utcNow()}"}
        }
      }
    },
    {
      "name": "WaitForBatchJobToComplete",
      "type": "Wait",
      "dependsOn": [
        {"activity": "TriggerExternalBatchJob", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "waitTimeInSeconds": 3600  // Wait 1 hour for batch job
      }
    },
    {
      "name": "CheckJobStatus",
      "type": "WebActivity",
      "dependsOn": [
        {"activity": "WaitForBatchJobToComplete", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "url": "@pipeline().parameters.BatchJobStatusURL",
        "method": "GET"
      }
    }
  ]
}
```

**Use Case:**
- Trigger external batch job
- Batch job typically takes 45-60 minutes
- Wait 1 hour then check status

---

**5. Stagger Parallel Loads:**

```json
{
  "activities": [
    {
      "name": "LoadTable1",
      "type": "Copy",
      "typeProperties": {
        "source": {"tableName": "Table1"},
        "sink": {"tableName": "DWH.Table1"}
      }
    },
    {
      "name": "Wait30Seconds",
      "type": "Wait",
      "dependsOn": [
        {"activity": "LoadTable1", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "waitTimeInSeconds": 30
      }
    },
    {
      "name": "LoadTable2",
      "type": "Copy",
      "dependsOn": [
        {"activity": "Wait30Seconds", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "source": {"tableName": "Table2"},
        "sink": {"tableName": "DWH.Table2"}
      }
    },
    {
      "name": "Wait30Seconds_2",
      "type": "Wait",
      "dependsOn": [
        {"activity": "LoadTable2", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "waitTimeInSeconds": 30
      }
    },
    {
      "name": "LoadTable3",
      "type": "Copy",
      "dependsOn": [
        {"activity": "Wait30Seconds_2", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "source": {"tableName": "Table3"},
        "sink": {"tableName": "DWH.Table3"}
      }
    }
  ]
}
```

**Strategy:**
- Load 3 large tables sequentially
- 30-second pause between each
- Reduces concurrent resource pressure
- Prevents overwhelming destination system

---

**6. Business Hours Delay:**

```json
{
  "activities": [
    {
      "name": "CheckIfBusinessHours",
      "type": "IfCondition",
      "typeProperties": {
        "expression": {
          "value": "@and(
            greaterOrEquals(int(formatDateTime(utcNow(), 'HH')), 9),
            lessOrEquals(int(formatDateTime(utcNow(), 'HH')), 17)
          )",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "WaitUntilAfterHours",
            "type": "Wait",
            "typeProperties": {
              "waitTimeInSeconds": "@mul(
                sub(18, int(formatDateTime(utcNow(), 'HH'))),
                3600
              )"  // Wait until 6 PM
            }
          }
        ],
        "ifFalseActivities": []
      }
    },
    {
      "name": "RunIntensiveProcess",
      "type": "Copy",
      "dependsOn": [
        {"activity": "CheckIfBusinessHours", "dependencyConditions": ["Completed"]}
      ],
      "typeProperties": {
        // Intensive data load
      }
    }
  ]
}
```

**Logic:**
- If currently business hours (9 AM - 5 PM): Wait until 6 PM
- If after hours: Run immediately
- Ensures intensive processes run off-peak

---

**Wait Time Calculations:**

```json
// Fixed duration
"waitTimeInSeconds": 60  // 1 minute
"waitTimeInSeconds": 300  // 5 minutes
"waitTimeInSeconds": 3600  // 1 hour
"waitTimeInSeconds": 86400  // 24 hours (maximum)

// Dynamic from parameter
"waitTimeInSeconds": "@pipeline().parameters.WaitSeconds"

// Calculated from variables
"waitTimeInSeconds": "@mul(variables('WaitMinutes'), 60)"

// Conditional wait time
"waitTimeInSeconds": "@if(
  equals(pipeline().parameters.Priority, 'High'),
  30,
  300
)"
// High priority: 30 seconds
// Normal priority: 300 seconds

// Based on activity output
"waitTimeInSeconds": "@if(
  greater(activity('CheckSize').output.firstRow.FileSize, 1000000000),
  600,
  60
)"
// Large files (>1GB): 10 minutes
// Small files: 1 minute
```

---

**Best Practices:**

**1. When to Use Wait:**

**✅ DO Use Wait:**
- Waiting for external processes with known duration
- Rate limiting API calls
- File settling time
- Staggering parallel operations
- Simple polling intervals (with Until activity)

**❌ DON'T Use Wait:**
- For long waits (>1 hour) → Use scheduled triggers instead
- When exact completion time is unknown → Use Until with polling
- As primary retry mechanism → Use built-in retry policy
- To wait for pipeline activities → Use dependsOn instead

**2. Performance Considerations:**

```json
// ✅ GOOD: Wait only when necessary
If Condition (check if file exists)
├─ If True: Process immediately
└─ If False: Wait → Retry

// ❌ BAD: Always waiting even if not needed
Wait 5 minutes → Check file → Process
// Wastes 5 minutes every time!
```

**3. Maximum Wait Time:**

```json
// ✅ GOOD: Long waits handled by scheduling
Pipeline triggered every hour
→ No need for long waits

// ❌ BAD: Very long wait in activity
"waitTimeInSeconds": 86400  // 24 hours!
// Use scheduled trigger instead
```

**4. Wait in Loops:**

```json
// ✅ GOOD: Controlled iteration
Until (file ready)
├─ Check file status
├─ Wait 30 seconds
└─ Repeat until file exists

// ❌ BAD: Excessive waits
ForEach (1000 items)
├─ Process item
└─ Wait 10 seconds
// Total: 2.7 hours of just waiting!
```

---

**Wait vs. Alternatives:**

| Scenario | Wait Activity | Better Alternative |
|----------|---------------|-------------------|
| **Fixed delay** | ✅ Perfect | - |
| **Wait for file** | ✅ With polling | Until + GetMetadata |
| **Long delay (>1 hour)** | ❌ | Scheduled trigger |
| **Wait for activity** | ❌ | dependsOn |
| **Conditional wait** | ✅ With If Condition | If Condition + Wait |
| **Retry with delay** | ✅ | Built-in retry policy |
| **Wait for webhook** | ❌ | Webhook activity |

---

**Real-World Example: Polling for File with Exponential Backoff**

```json
{
  "properties": {
    "parameters": {
      "FileName": {"type": "String"},
      "MaxRetries": {"type": "Integer", "defaultValue": 5}
    },
    "variables": {
      "RetryCount": {"type": "Integer", "defaultValue": 0},
      "FileExists": {"type": "Boolean", "defaultValue": false},
      "WaitSeconds": {"type": "Integer", "defaultValue": 10}
    },
    "activities": [
      {
        "name": "UntilFileExists",
        "type": "Until",
        "typeProperties": {
          "expression": {
            "value": "@or(
              equals(variables('FileExists'), true),
              greaterOrEquals(variables('RetryCount'), pipeline().parameters.MaxRetries)
            )",
            "type": "Expression"
          },
          "timeout": "00:30:00",  // 30 minutes max
          "activities": [
            {
              "name": "CheckFileExists",
              "type": "GetMetadata",
              "typeProperties": {
                "dataset": {
                  "referenceName": "InputFile",
                  "type": "DatasetReference",
                  "parameters": {
                    "FileName": "@pipeline().parameters.FileName"
                  }
                },
                "fieldList": ["exists"]
              }
            },
            {
              "name": "SetFileExistsFlag",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "CheckFileExists", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "FileExists",
                "value": "@activity('CheckFileExists').output.exists"
              }
            },
            {
              "name": "IfFileNotFound",
              "type": "IfCondition",
              "dependsOn": [
                {"activity": "SetFileExistsFlag", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "expression": {
                  "value": "@not(variables('FileExists'))",
                  "type": "Expression"
                },
                "ifTrueActivities": [
                  {
                    "name": "IncrementRetryCount",
                    "type": "SetVariable",
                    "typeProperties": {
                      "variableName": "RetryCount",
                      "value": "@add(variables('RetryCount'), 1)"
                    }
                  },
                  {
                    "name": "CalculateBackoff",
                    "type": "SetVariable",
                    "dependsOn": [
                      {"activity": "IncrementRetryCount", "dependencyConditions": ["Succeeded"]}
                    ],
                    "typeProperties": {
                      "variableName": "WaitSeconds",
                      "value": "@mul(10, variables('RetryCount'))"
                    }
                  },
                  {
                    "name": "WaitWithBackoff",
                    "type": "Wait",
                    "dependsOn": [
                      {"activity": "CalculateBackoff", "dependencyConditions": ["Succeeded"]}
                    ],
                    "typeProperties": {
                      "waitTimeInSeconds": "@variables('WaitSeconds')"
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      {
        "name": "CheckIfFileFound",
        "type": "IfCondition",
        "dependsOn": [
          {"activity": "UntilFileExists", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "expression": {
            "value": "@variables('FileExists')",
            "type": "Expression"
          },
          "ifTrueActivities": [
            {
              "name": "ProcessFile",
              "type": "Copy",
              "typeProperties": {
                "source": {
                  "type": "DelimitedTextSource",
                  "fileName": "@pipeline().parameters.FileName"
                },
                "sink": {
                  "type": "LakehouseSink"
                }
              }
            }
          ],
          "ifFalseActivities": [
            {
              "name": "FailPipeline",
              "type": "Fail",
              "typeProperties": {
                "message": "File @{pipeline().parameters.FileName} not found after @{pipeline().parameters.MaxRetries} retries",
                "errorCode": "FileNotFound"
              }
            }
          ]
        }
      }
    ]
  }
}
```

**Execution Flow (File Not Immediately Available):**

```
Retry 1: Check file → Not found → Wait 10 seconds
Retry 2: Check file → Not found → Wait 20 seconds
Retry 3: Check file → Not found → Wait 30 seconds
Retry 4: Check file → Found! → Process file

Total wait: 10 + 20 + 30 = 60 seconds
```

**Exponential Backoff:**
- Retry 1: 10 seconds
- Retry 2: 20 seconds
- Retry 3: 30 seconds
- Retry 4: 40 seconds
- Retry 5: 50 seconds

**Benefits:**
- Doesn't waste time with constant checking
- Increases wait time between retries
- Reduces load on storage system
- Gives file upload more time to complete

---

**Debugging Wait Activity:**

**1. Monitor Wait Duration:**

```
Fabric Monitoring Hub → Pipeline Run → Activity Runs
- Find Wait activity
- Check "Duration" column
- Verify it matches expected wait time
```

**2. Log Wait Events:**

```json
{
  "name": "LogWaitStart",
  "type": "Web",
  "typeProperties": {
    "url": "https://logging-service.com/log",
    "method": "POST",
    "body": {
      "event": "WaitStart",
      "waitSeconds": 60,
      "timestamp": "@utcNow()"
    }
  }
},
{
  "name": "Wait60Seconds",
  "type": "Wait",
  "dependsOn": [
    {"activity": "LogWaitStart", "dependencyConditions": ["Succeeded"]}
  ],
  "typeProperties": {
    "waitTimeInSeconds": 60
  }
},
{
  "name": "LogWaitEnd",
  "type": "Web",
  "dependsOn": [
    {"activity": "Wait60Seconds", "dependencyConditions": ["Succeeded"]}
  ],
  "typeProperties": {
    "url": "https://logging-service.com/log",
    "method": "POST",
    "body": {
      "event": "WaitEnd",
      "timestamp": "@utcNow()"
    }
  }
}
```

---

**Common Wait Durations:**

| Duration | Seconds | Use Case |
|----------|---------|----------|
| **30 seconds** | 30 | Quick file settling |
| **1 minute** | 60 | File upload completion |
| **2 minutes** | 120 | Short API retry |
| **5 minutes** | 300 | File availability check |
| **10 minutes** | 600 | External process completion |
| **30 minutes** | 1800 | Batch job wait |
| **1 hour** | 3600 | Long-running processes |
| **24 hours** | 86400 | Maximum (use trigger instead) |

---

**Key Takeaways:**

1. **Purpose:** Wait Activity pauses pipeline execution for specified duration
2. **Duration:** 1 second to 24 hours (86,400 seconds)
3. **Synchronous:** Blocks pipeline execution
4. **Common Uses:** File settling, rate limiting, retry delays, staggering loads
5. **Alternatives:** For long waits use scheduled triggers, for unknown duration use Until
6. **Best Practice:** Use sparingly, prefer scheduled triggers for long waits
7. **Debugging:** Check duration in monitoring hub
8. **Performance:** Waiting counts toward pipeline execution time

Wait Activity is **useful** for timing control but should be used judiciously to avoid unnecessary pipeline delays!

---

### Get Metadata Activity

**Definition:**
The Get Metadata Activity is a control flow activity that retrieves metadata information about datasets, files, or folders without actually reading or copying the data content. It can fetch properties like file existence, size, last modified date, child items (list of files in folder), column structure, and more. This metadata can then be used to make decisions in subsequent pipeline activities.

**Purpose:**
- Check if files/folders exist before processing
- Get list of files in a folder for dynamic processing
- Retrieve file properties (size, last modified, etc.)
- Validate data structures before loading
- Get row count estimates
- Check schema/column information
- Implement file-based triggers and patterns

**Key Features:**

**1. Metadata Fields Supported:**

**For Files:**
- `exists` - Check if file exists
- `itemName` - Get file name
- `itemType` - File or Folder
- `size` - File size in bytes
- `created` - Creation timestamp
- `lastModified` - Last modified timestamp
- `structure` - Column schema

**For Folders:**
- `exists` - Check if folder exists
- `itemName` - Folder name
- `itemType` - File or Folder
- `childItems` - List of files/subfolders
- `created` - Creation timestamp
- `lastModified` - Last modified timestamp

**For Tables:**
- `exists` - Check if table exists
- `tableName` - Table name
- `structure` - Column names and types
- `columnCount` - Number of columns

**2. Output Access:**
- `@activity('GetMetadata').output.exists` - True/false
- `@activity('GetMetadata').output.size` - File size
- `@activity('GetMetadata').output.childItems` - Array of files
- `@activity('GetMetadata').output.structure` - Schema information

**3. Use Cases:**
- File existence validation
- Dynamic file list generation
- File pattern matching
- Data validation gates
- Metadata-driven processing

---

**Basic Configuration:**

```json
{
  "name": "GetFileMetadata",
  "type": "GetMetadata",
  "typeProperties": {
    "dataset": {
      "referenceName": "InputFileDataset",
      "type": "DatasetReference"
    },
    "fieldList": [
      "exists",
      "size",
      "lastModified"
    ]
  }
}
```

**Output Example:**

```json
{
  "exists": true,
  "size": 1048576,  // 1 MB
  "lastModified": "2026-04-25T10:30:00Z"
}
```

---

**Common Patterns:**

**1. Check File Existence Before Processing:**

```json
{
  "activities": [
    {
      "name": "CheckInputFileExists",
      "type": "GetMetadata",
      "typeProperties": {
        "dataset": {
          "referenceName": "InputFile",
          "type": "DatasetReference",
          "parameters": {
            "FileName": "@pipeline().parameters.InputFileName"
          }
        },
        "fieldList": ["exists"]
      }
    },
    {
      "name": "IfFileExists",
      "type": "IfCondition",
      "dependsOn": [
        {"activity": "CheckInputFileExists", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "expression": {
          "value": "@activity('CheckInputFileExists').output.exists",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "ProcessFile",
            "type": "Copy",
            "typeProperties": {
              "source": {
                "type": "DelimitedTextSource",
                "fileName": "@pipeline().parameters.InputFileName"
              },
              "sink": {
                "type": "LakehouseSink",
                "tableName": "Bronze.Data"
              }
            }
          }
        ],
        "ifFalseActivities": [
          {
            "name": "FailPipeline",
            "type": "Fail",
            "typeProperties": {
              "message": "Input file @{pipeline().parameters.InputFileName} does not exist",
              "errorCode": "FileNotFound"
            }
          }
        ]
      }
    }
  ]
}
```

---

**2. Get List of Files for Dynamic Processing:**

```json
{
  "activities": [
    {
      "name": "GetFileList",
      "type": "GetMetadata",
      "typeProperties": {
        "dataset": {
          "referenceName": "InputFolderDataset",
          "type": "DatasetReference"
        },
        "fieldList": [
          "childItems"
        ]
      }
    },
    {
      "name": "FilterCSVFiles",
      "type": "Filter",
      "dependsOn": [
        {"activity": "GetFileList", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "items": "@activity('GetFileList').output.childItems",
        "condition": "@endsWith(item().name, '.csv')"
      }
    },
    {
      "name": "ProcessEachFile",
      "type": "ForEach",
      "dependsOn": [
        {"activity": "FilterCSVFiles", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "items": "@activity('FilterCSVFiles').output.value",
        "isSequential": false,
        "batchCount": 5,
        "activities": [
          {
            "name": "CopyFile",
            "type": "Copy",
            "typeProperties": {
              "source": {
                "type": "DelimitedTextSource",
                "fileName": "@item().name"
              },
              "sink": {
                "type": "LakehouseSink",
                "tableName": "Bronze.@{replace(item().name, '.csv', '')}"
              }
            }
          }
        ]
      }
    }
  ]
}
```

**Execution Flow:**
```
1. GetFileList → Returns: [file1.csv, file2.csv, data.xlsx, file3.csv]
2. FilterCSVFiles → Filters to: [file1.csv, file2.csv, file3.csv]
3. ProcessEachFile → Process 3 CSV files in parallel
```

---

**3. File Size Validation:**

```json
{
  "activities": [
    {
      "name": "GetFileSize",
      "type": "GetMetadata",
      "typeProperties": {
        "dataset": {
          "referenceName": "InputFile",
          "type": "DatasetReference"
        },
        "fieldList": ["size"]
      }
    },
    {
      "name": "CheckFileSize",
      "type": "IfCondition",
      "dependsOn": [
        {"activity": "GetFileSize", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "expression": {
          "value": "@greater(activity('GetFileSize').output.size, 1073741824)",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "ProcessLargeFile",
            "type": "Copy",
            "typeProperties": {
              "source": {
                "type": "DelimitedTextSource"
              },
              "sink": {
                "type": "LakehouseSink"
              },
              "enableStaging": true,
              "parallelCopies": 10
            }
          }
        ],
        "ifFalseActivities": [
          {
            "name": "ProcessSmallFile",
            "type": "Copy",
            "typeProperties": {
              "source": {
                "type": "DelimitedTextSource"
              },
              "sink": {
                "type": "LakehouseSink"
              },
              "parallelCopies": 2
            }
          }
        ]
      }
    }
  ]
}

// Logic:
// If file > 1 GB: Use staging + 10 parallel copies
// If file ≤ 1 GB: Standard copy with 2 parallel copies
```

---

**4. Last Modified Date Filtering:**

```json
{
  "activities": [
    {
      "name": "GetFileMetadata",
      "type": "GetMetadata",
      "typeProperties": {
        "dataset": {
          "referenceName": "InputFile",
          "type": "DatasetReference"
        },
        "fieldList": ["lastModified"]
      }
    },
    {
      "name": "CheckIfRecentlyModified",
      "type": "IfCondition",
      "dependsOn": [
        {"activity": "GetFileMetadata", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "expression": {
          "value": "@greaterOrEquals(
            ticks(activity('GetFileMetadata').output.lastModified),
            ticks(addDays(utcNow(), -1))
          )",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "ProcessRecentFile",
            "type": "Copy",
            "typeProperties": {
              // Process file modified in last 24 hours
            }
          }
        ],
        "ifFalseActivities": [
          {
            "name": "SkipOldFile",
            "type": "Web",
            "typeProperties": {
              "url": "@pipeline().parameters.LoggingEndpoint",
              "method": "POST",
              "body": {
                "message": "File skipped - not modified in last 24 hours",
                "lastModified": "@activity('GetFileMetadata').output.lastModified"
              }
            }
          }
        ]
      }
    }
  ]
}
```

---

**5. Get Schema Information:**

```json
{
  "activities": [
    {
      "name": "GetTableStructure",
      "type": "GetMetadata",
      "typeProperties": {
        "dataset": {
          "referenceName": "SourceTableDataset",
          "type": "DatasetReference"
        },
        "fieldList": [
          "structure",
          "columnCount"
        ]
      }
    },
    {
      "name": "ValidateSchema",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "GetTableStructure", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "ColumnCount",
        "value": "@string(activity('GetTableStructure').output.columnCount)"
      }
    },
    {
      "name": "CheckColumnCount",
      "type": "IfCondition",
      "dependsOn": [
        {"activity": "ValidateSchema", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "expression": {
          "value": "@equals(activity('GetTableStructure').output.columnCount, 10)",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "ProcessTable",
            "type": "Copy",
            "typeProperties": {
              // Schema matches expectation (10 columns)
            }
          }
        ],
        "ifFalseActivities": [
          {
            "name": "ReportSchemaMismatch",
            "type": "Fail",
            "typeProperties": {
              "message": "Schema validation failed. Expected 10 columns, found @{activity('GetTableStructure').output.columnCount}",
              "errorCode": "SchemaMismatch"
            }
          }
        ]
      }
    }
  ]
}

// Structure output example:
// {
//   "structure": [
//     {"name": "CustomerID", "type": "Int32"},
//     {"name": "CustomerName", "type": "String"},
//     {"name": "OrderDate", "type": "DateTime"}
//   ],
//   "columnCount": 3
// }
```

---

**6. Count Files Before Processing:**

```json
{
  "activities": [
    {
      "name": "GetFolderMetadata",
      "type": "GetMetadata",
      "typeProperties": {
        "dataset": {
          "referenceName": "InputFolder",
          "type": "DatasetReference"
        },
        "fieldList": ["childItems"]
      }
    },
    {
      "name": "SetFileCount",
      "type": "SetVariable",
      "dependsOn": [
        {"activity": "GetFolderMetadata", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "variableName": "FileCount",
        "value": "@string(length(activity('GetFolderMetadata').output.childItems))"
      }
    },
    {
      "name": "CheckFileCount",
      "type": "IfCondition",
      "dependsOn": [
        {"activity": "SetFileCount", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "expression": {
          "value": "@greater(length(activity('GetFolderMetadata').output.childItems), 0)",
          "type": "Expression"
        },
        "ifTrueActivities": [
          {
            "name": "ProcessFiles",
            "type": "ForEach",
            "typeProperties": {
              "items": "@activity('GetFolderMetadata').output.childItems",
              "activities": [
                {
                  "name": "ProcessFile",
                  "type": "Copy",
                  "typeProperties": {
                    "source": {
                      "fileName": "@item().name"
                    }
                  }
                }
              ]
            }
          },
          {
            "name": "LogFileCount",
            "type": "Web",
            "dependsOn": [
              {"activity": "ProcessFiles", "dependencyConditions": ["Succeeded"]}
            ],
            "typeProperties": {
              "url": "@pipeline().parameters.LoggingEndpoint",
              "method": "POST",
              "body": {
                "message": "Processed @{variables('FileCount')} files",
                "timestamp": "@utcNow()"
              }
            }
          }
        ],
        "ifFalseActivities": [
          {
            "name": "LogNoFiles",
            "type": "Web",
            "typeProperties": {
              "url": "@pipeline().parameters.LoggingEndpoint",
              "method": "POST",
              "body": {
                "message": "No files found to process",
                "timestamp": "@utcNow()"
              }
            }
          }
        ]
      }
    }
  ]
}
```

---

**7. Archive Old Files Pattern:**

```json
{
  "activities": [
    {
      "name": "GetAllFiles",
      "type": "GetMetadata",
      "typeProperties": {
        "dataset": {
          "referenceName": "SourceFolder",
          "type": "DatasetReference"
        },
        "fieldList": ["childItems"]
      }
    },
    {
      "name": "CheckEachFile",
      "type": "ForEach",
      "dependsOn": [
        {"activity": "GetAllFiles", "dependencyConditions": ["Succeeded"]}
      ],
      "typeProperties": {
        "items": "@activity('GetAllFiles').output.childItems",
        "activities": [
          {
            "name": "GetFileDetails",
            "type": "GetMetadata",
            "typeProperties": {
              "dataset": {
                "referenceName": "SourceFileDataset",
                "type": "DatasetReference",
                "parameters": {
                  "FileName": "@item().name"
                }
              },
              "fieldList": ["lastModified"]
            }
          },
          {
            "name": "CheckIfOld",
            "type": "IfCondition",
            "dependsOn": [
              {"activity": "GetFileDetails", "dependencyConditions": ["Succeeded"]}
            ],
            "typeProperties": {
              "expression": {
                "value": "@less(
                  ticks(activity('GetFileDetails').output.lastModified),
                  ticks(addDays(utcNow(), -30))
                )",
                "type": "Expression"
              },
              "ifTrueActivities": [
                {
                  "name": "MoveToArchive",
                  "type": "Copy",
                  "typeProperties": {
                    "source": {
                      "type": "BinarySource",
                      "fileName": "@item().name"
                    },
                    "sink": {
                      "type": "BinarySink",
                      "folderPath": "archive/@{formatDateTime(utcNow(), 'yyyy/MM')}"
                    }
                  }
                },
                {
                  "name": "DeleteOriginal",
                  "type": "Delete",
                  "dependsOn": [
                    {"activity": "MoveToArchive", "dependencyConditions": ["Succeeded"]}
                  ],
                  "typeProperties": {
                    "dataset": {
                      "referenceName": "SourceFileDataset",
                      "type": "DatasetReference",
                      "parameters": {
                        "FileName": "@item().name"
                      }
                    }
                  }
                }
              ]
            }
          }
        ]
      }
    }
  ]
}

// Logic:
// 1. Get all files in source folder
// 2. For each file:
//    - Get lastModified date
//    - If older than 30 days:
//      * Copy to archive folder
//      * Delete from source
```

---

**Best Practices:**

**1. Field Selection:**

**✅ DO:**
- Only request fields you need (performance)
- Use specific field lists

```json
// Good: Only get what you need
"fieldList": ["exists", "size"]

// Bad: Getting unnecessary data
"fieldList": ["exists", "size", "created", "lastModified", "structure", "childItems"]
```

**❌ DON'T:**
- Request all fields unnecessarily
- Get childItems for very large folders (performance hit)

**2. Combine with Other Activities:**

```json
// ✅ GOOD: GetMetadata → Validate → Process
GetMetadata → If Condition → Copy Activity

// ❌ BAD: Processing without validation
Copy Activity (may fail if file doesn't exist)
```

**3. Error Handling:**

```json
{
  "name": "GetFileMetadataSafe",
  "type": "GetMetadata",
  "typeProperties": {
    "dataset": {
      "referenceName": "InputFile",
      "type": "DatasetReference"
    },
    "fieldList": ["exists"]
  }
},
{
  "name": "OnMetadataSuccess",
  "type": "IfCondition",
  "dependsOn": [
    {"activity": "GetFileMetadataSafe", "dependencyConditions": ["Succeeded"]}
  ],
  "typeProperties": {
    "expression": {
      "value": "@activity('GetFileMetadataSafe').output.exists",
      "type": "Expression"
    },
    "ifTrueActivities": [/* Process file */],
    "ifFalseActivities": [/* Handle missing file */]
  }
},
{
  "name": "OnMetadataFailure",
  "type": "Web",
  "dependsOn": [
    {"activity": "GetFileMetadataSafe", "dependencyConditions": ["Failed"]}
  ],
  "typeProperties": {
    "url": "@pipeline().parameters.ErrorWebhook",
    "method": "POST",
    "body": {
      "error": "Could not retrieve metadata",
      "pipeline": "@{pipeline().Pipeline}"
    }
  }
}
```

**4. Large Folders:**

```json
// ✅ GOOD: Filter at source when possible
GetMetadata → Get childItems for specific pattern
Filter → Filter to needed files
Process → ForEach

// ❌ BAD: Getting all files from huge folder
GetMetadata → Get childItems (10,000 files!)
// Performance impact, large output
```

---

**Common Field List Examples:**

```json
// Check existence only
"fieldList": ["exists"]

// File details
"fieldList": ["exists", "size", "lastModified"]

// Folder contents
"fieldList": ["childItems"]

// Table schema
"fieldList": ["structure", "columnCount"]

// Complete file metadata
"fieldList": ["exists", "itemName", "itemType", "size", "created", "lastModified"]

// Folder with details
"fieldList": ["exists", "childItems", "itemName"]
```

---

**Accessing Output:**

```json
// Existence
@activity('GetMetadata1').output.exists  // true/false

// Size
@activity('GetMetadata1').output.size  // 1048576 (bytes)

// Last Modified
@activity('GetMetadata1').output.lastModified  // "2026-04-25T10:30:00Z"

// Child Items (array)
@activity('GetMetadata1').output.childItems  
// [{"name": "file1.csv", "type": "File"}, {"name": "file2.csv", "type": "File"}]

// Child Items count
@length(activity('GetMetadata1').output.childItems)  // 2

// Specific child item
@activity('GetMetadata1').output.childItems[0].name  // "file1.csv"

// Structure (schema)
@activity('GetMetadata1').output.structure
// [{"name": "ID", "type": "Int32"}, {"name": "Name", "type": "String"}]

// Column count
@activity('GetMetadata1').output.columnCount  // 10

// Item name
@activity('GetMetadata1').output.itemName  // "myfile.csv"

// Item type
@activity('GetMetadata1').output.itemType  // "File" or "Folder"
```

---

**Real-World Example: Smart File Processing with Validation**

**Scenario:**
- Daily folder receives multiple CSV files
- Only process files modified today
- Validate file size (must be > 1 KB)
- Get schema and validate column count
- Process valid files
- Move invalid files to error folder

```json
{
  "properties": {
    "parameters": {
      "SourceFolderPath": {"type": "String"},
      "ExpectedColumnCount": {"type": "Integer", "defaultValue": 10}
    },
    "variables": {
      "TotalFiles": {"type": "Integer", "defaultValue": 0},
      "ProcessedFiles": {"type": "Integer", "defaultValue": 0},
      "ErrorFiles": {"type": "Integer", "defaultValue": 0}
    },
    "activities": [
      {
        "name": "GetAllFiles",
        "type": "GetMetadata",
        "typeProperties": {
          "dataset": {
            "referenceName": "SourceFolderDataset",
            "type": "DatasetReference",
            "parameters": {
              "FolderPath": "@pipeline().parameters.SourceFolderPath"
            }
          },
          "fieldList": ["childItems"]
        }
      },
      {
        "name": "SetTotalFileCount",
        "type": "SetVariable",
        "dependsOn": [
          {"activity": "GetAllFiles", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "variableName": "TotalFiles",
          "value": "@length(activity('GetAllFiles').output.childItems)"
        }
      },
      {
        "name": "ProcessEachFile",
        "type": "ForEach",
        "dependsOn": [
          {"activity": "SetTotalFileCount", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "items": "@activity('GetAllFiles').output.childItems",
          "isSequential": false,
          "batchCount": 5,
          "activities": [
            {
              "name": "GetFileMetadata",
              "type": "GetMetadata",
              "typeProperties": {
                "dataset": {
                  "referenceName": "SourceFileDataset",
                  "type": "DatasetReference",
                  "parameters": {
                    "FileName": "@item().name"
                  }
                },
                "fieldList": ["size", "lastModified", "structure", "columnCount"]
              }
            },
            {
              "name": "ValidateFile",
              "type": "IfCondition",
              "dependsOn": [
                {"activity": "GetFileMetadata", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "expression": {
                  "value": "@and(
                    and(
                      greaterOrEquals(
                        ticks(activity('GetFileMetadata').output.lastModified),
                        ticks(startOfDay(utcNow()))
                      ),
                      greater(activity('GetFileMetadata').output.size, 1024)
                    ),
                    equals(
                      activity('GetFileMetadata').output.columnCount,
                      pipeline().parameters.ExpectedColumnCount
                    )
                  )",
                  "type": "Expression"
                },
                "ifTrueActivities": [
                  {
                    "name": "ProcessValidFile",
                    "type": "Copy",
                    "typeProperties": {
                      "source": {
                        "type": "DelimitedTextSource",
                        "fileName": "@item().name"
                      },
                      "sink": {
                        "type": "LakehouseSink",
                        "tableName": "Bronze.@{replace(item().name, '.csv', '')}"
                      }
                    }
                  },
                  {
                    "name": "LogSuccess",
                    "type": "SetVariable",
                    "dependsOn": [
                      {"activity": "ProcessValidFile", "dependencyConditions": ["Succeeded"]}
                    ],
                    "typeProperties": {
                      "variableName": "ProcessedFiles",
                      "value": "@add(variables('ProcessedFiles'), 1)"
                    }
                  }
                ],
                "ifFalseActivities": [
                  {
                    "name": "MoveToErrorFolder",
                    "type": "Copy",
                    "typeProperties": {
                      "source": {
                        "type": "BinarySource",
                        "fileName": "@item().name"
                      },
                      "sink": {
                        "type": "BinarySink",
                        "folderPath": "error/@{formatDateTime(utcNow(), 'yyyy-MM-dd')}"
                      }
                    }
                  },
                  {
                    "name": "LogError",
                    "type": "Web",
                    "dependsOn": [
                      {"activity": "MoveToErrorFolder", "dependencyConditions": ["Succeeded"]}
                    ],
                    "typeProperties": {
                      "url": "@pipeline().parameters.LoggingEndpoint",
                      "method": "POST",
                      "body": {
                        "fileName": "@item().name",
                        "size": "@activity('GetFileMetadata').output.size",
                        "lastModified": "@activity('GetFileMetadata').output.lastModified",
                        "columnCount": "@activity('GetFileMetadata').output.columnCount",
                        "expectedColumnCount": "@pipeline().parameters.ExpectedColumnCount",
                        "reason": "Validation failed",
                        "timestamp": "@utcNow()"
                      }
                    }
                  },
                  {
                    "name": "IncrementErrorCount",
                    "type": "SetVariable",
                    "dependsOn": [
                      {"activity": "LogError", "dependencyConditions": ["Succeeded"]}
                    ],
                    "typeProperties": {
                      "variableName": "ErrorFiles",
                      "value": "@add(variables('ErrorFiles'), 1)"
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      {
        "name": "SendSummaryNotification",
        "type": "Web",
        "dependsOn": [
          {"activity": "ProcessEachFile", "dependencyConditions": ["Completed"]}
        ],
        "typeProperties": {
          "url": "@pipeline().parameters.NotificationWebhook",
          "method": "POST",
          "body": {
            "pipeline": "@{pipeline().Pipeline}",
            "totalFiles": "@{variables('TotalFiles')}",
            "processedFiles": "@{variables('ProcessedFiles')}",
            "errorFiles": "@{variables('ErrorFiles')}",
            "timestamp": "@{utcNow()}"
          }
        }
      }
    ]
  }
}
```

**Validation Logic:**
```
File is valid if ALL conditions met:
1. Last modified today (>= start of day)
2. Size > 1 KB
3. Column count = expected (e.g., 10)
```

**Execution Flow:**
```
Folder contains: [file1.csv (modified yesterday), file2.csv (100 bytes), file3.csv (valid)]

1. GetAllFiles → 3 files
2. For each file:
   - file1.csv: 
     * GetMetadata → lastModified = yesterday
     * Validation FAILED (old file)
     * Move to error folder
   
   - file2.csv:
     * GetMetadata → size = 100 bytes
     * Validation FAILED (too small)
     * Move to error folder
   
   - file3.csv:
     * GetMetadata → All validations pass
     * Process file → Load to Lakehouse
     * Increment processed count

3. SendSummaryNotification:
   - Total: 3 files
   - Processed: 1
   - Errors: 2
```

---

**When to Use Get Metadata:**

**✅ Use When:**
- Checking file/folder existence
- Getting list of files for dynamic processing
- Validating file properties (size, date, etc.)
- Getting schema information
- Implementing file-based triggers
- Metadata-driven orchestration

**❌ Don't Use When:**
- Need to read actual data content (use Lookup instead)
- Copying data (use Copy Activity)
- Very large folders with thousands of files (performance impact)
- Simple file copy (Copy Activity handles existence check)

---

**Get Metadata vs. Alternatives:**

| Use Case | Get Metadata | Alternative |
|----------|--------------|-------------|
| **Check file exists** | ✅ Perfect | - |
| **Get file list** | ✅ Perfect | - |
| **Read data content** | ❌ | Lookup Activity |
| **Get row count** | ❌ | Lookup with COUNT(*) |
| **Copy data** | ❌ | Copy Activity |
| **Schema only** | ✅ Good | Lookup + INFORMATION_SCHEMA |

---

**Key Takeaways:**

1. **Purpose:** Get Metadata retrieves metadata without reading data content
2. **Fields:** exists, size, lastModified, childItems, structure, columnCount
3. **Output:** Access via `@activity('name').output.fieldName`
4. **Common Uses:** File validation, dynamic file lists, schema checks
5. **Performance:** Only request needed fields, avoid large folder childItems
6. **Combine With:** If Condition, ForEach, Filter for powerful patterns
7. **vs. Lookup:** Metadata for structure, Lookup for data content
8. **Best Practice:** Use for validation gates before processing

Get Metadata Activity is **essential** for building robust, validated data pipelines!

---

### Until Activity

**Definition:**
The Until Activity is a looping control flow activity that repeatedly executes a set of inner activities until a specified boolean condition evaluates to true. It implements a "Do-Until" loop pattern (similar to `do-while` loops in programming), where activities execute at least once and continue iterating until the exit condition is met or a timeout occurs. This is ideal for polling scenarios, waiting for external processes, or implementing retry logic with conditions.

**Purpose:**
- Poll external systems until a condition is met
- Wait for file availability
- Retry operations until success
- Check status repeatedly until complete
- Implement conditional loops
- Monitor external processes
- Wait for data readiness

**Key Features:**

**1. Loop Execution:**
- Activities execute at least once
- Loop continues until expression = true
- Maximum iterations: 100 by default
- Timeout protection

**2. Exit Conditions:**
- Boolean expression evaluation
- Timeout expiration (default: 1 hour)
- Explicit failure inside loop

**3. Inner Activities:**
- Can contain any activities
- Multiple activities supported
- Activities can have dependencies
- Variables can be updated in loop

**4. Timeout Configuration:**
- Format: `HH:MM:SS`
- Default: `01:00:00` (1 hour)
- Maximum: `7.00:00:00` (7 days)

---

**Basic Configuration:**

```json
{
  "name": "UntilFileReady",
  "type": "Until",
  "typeProperties": {
    "expression": {
      "value": "@equals(variables('FileExists'), true)",
      "type": "Expression"
    },
    "activities": [
      {
        "name": "CheckFileExists",
        "type": "GetMetadata",
        "typeProperties": {
          "dataset": {
            "referenceName": "InputFile",
            "type": "DatasetReference"
          },
          "fieldList": ["exists"]
        }
      },
      {
        "name": "UpdateFileExistsFlag",
        "type": "SetVariable",
        "dependsOn": [
          {"activity": "CheckFileExists", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "variableName": "FileExists",
          "value": "@activity('CheckFileExists').output.exists"
        }
      },
      {
        "name": "WaitBeforeRetry",
        "type": "Wait",
        "dependsOn": [
          {"activity": "UpdateFileExistsFlag", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "waitTimeInSeconds": 30
        }
      }
    ],
    "timeout": "00:30:00"  // 30 minutes max
  }
}
```

**Logic:**
```
1. Check if file exists
2. Update FileExists variable
3. Wait 30 seconds
4. Repeat until FileExists = true OR timeout (30 min)
```

---

**Common Patterns:**

**1. Poll for File Availability:**

```json
{
  "properties": {
    "parameters": {
      "ExpectedFileName": {"type": "String"}
    },
    "variables": {
      "FileReady": {"type": "Boolean", "defaultValue": false},
      "RetryCount": {"type": "Integer", "defaultValue": 0}
    },
    "activities": [
      {
        "name": "WaitForFile",
        "type": "Until",
        "typeProperties": {
          "expression": {
            "value": "@or(
              equals(variables('FileReady'), true),
              greater(variables('RetryCount'), 10)
            )",
            "type": "Expression"
          },
          "timeout": "01:00:00",  // 1 hour max
          "activities": [
            {
              "name": "CheckFile",
              "type": "GetMetadata",
              "typeProperties": {
                "dataset": {
                  "referenceName": "InputFileDataset",
                  "type": "DatasetReference",
                  "parameters": {
                    "FileName": "@pipeline().parameters.ExpectedFileName"
                  }
                },
                "fieldList": ["exists"]
              }
            },
            {
              "name": "UpdateFileReadyFlag",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "CheckFile", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "FileReady",
                "value": "@activity('CheckFile').output.exists"
              }
            },
            {
              "name": "IncrementRetryCount",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "UpdateFileReadyFlag", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "RetryCount",
                "value": "@add(variables('RetryCount'), 1)"
              }
            },
            {
              "name": "WaitIfNotReady",
              "type": "IfCondition",
              "dependsOn": [
                {"activity": "IncrementRetryCount", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "expression": {
                  "value": "@not(variables('FileReady'))",
                  "type": "Expression"
                },
                "ifTrueActivities": [
                  {
                    "name": "Wait60Seconds",
                    "type": "Wait",
                    "typeProperties": {
                      "waitTimeInSeconds": 60
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      {
        "name": "CheckResult",
        "type": "IfCondition",
        "dependsOn": [
          {"activity": "WaitForFile", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "expression": {
            "value": "@variables('FileReady')",
            "type": "Expression"
          },
          "ifTrueActivities": [
            {
              "name": "ProcessFile",
              "type": "Copy",
              "typeProperties": {
                "source": {
                  "type": "DelimitedTextSource",
                  "fileName": "@pipeline().parameters.ExpectedFileName"
                },
                "sink": {
                  "type": "LakehouseSink"
                }
              }
            }
          ],
          "ifFalseActivities": [
            {
              "name": "FailPipeline",
              "type": "Fail",
              "typeProperties": {
                "message": "File @{pipeline().parameters.ExpectedFileName} not available after 10 retries",
                "errorCode": "FileNotAvailable"
              }
            }
          ]
        }
      }
    ]
  }
}
```

**Execution Flow:**
```
Iteration 1: File not found → Wait 60s
Iteration 2: File not found → Wait 60s
Iteration 3: File not found → Wait 60s
Iteration 4: File found! → Exit loop → Process file
```

---

**2. Wait for External Process Completion:**

```json
{
  "properties": {
    "parameters": {
      "JobStatusURL": {"type": "String"},
      "JobID": {"type": "String"}
    },
    "variables": {
      "JobComplete": {"type": "Boolean", "defaultValue": false},
      "JobStatus": {"type": "String", "defaultValue": "Running"}
    },
    "activities": [
      {
        "name": "PollJobStatus",
        "type": "Until",
        "typeProperties": {
          "expression": {
            "value": "@equals(variables('JobComplete'), true)",
            "type": "Expression"
          },
          "timeout": "02:00:00",  // 2 hours max
          "activities": [
            {
              "name": "CheckJobStatus",
              "type": "WebActivity",
              "typeProperties": {
                "url": "@concat(pipeline().parameters.JobStatusURL, '/', pipeline().parameters.JobID)",
                "method": "GET",
                "headers": {
                  "Authorization": "Bearer @{pipeline().parameters.APIToken}"
                }
              }
            },
            {
              "name": "SetJobStatus",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "CheckJobStatus", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "JobStatus",
                "value": "@activity('CheckJobStatus').output.status"
              }
            },
            {
              "name": "CheckIfComplete",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "SetJobStatus", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "JobComplete",
                "value": "@or(
                  equals(variables('JobStatus'), 'Completed'),
                  equals(variables('JobStatus'), 'Failed')
                )"
              }
            },
            {
              "name": "WaitBetweenPolls",
              "type": "IfCondition",
              "dependsOn": [
                {"activity": "CheckIfComplete", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "expression": {
                  "value": "@not(variables('JobComplete'))",
                  "type": "Expression"
                },
                "ifTrueActivities": [
                  {
                    "name": "Wait120Seconds",
                    "type": "Wait",
                    "typeProperties": {
                      "waitTimeInSeconds": 120  // Poll every 2 minutes
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      {
        "name": "ProcessJobResult",
        "type": "IfCondition",
        "dependsOn": [
          {"activity": "PollJobStatus", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "expression": {
            "value": "@equals(variables('JobStatus'), 'Completed')",
            "type": "Expression"
          },
          "ifTrueActivities": [
            {
              "name": "ProcessSuccessfulJob",
              "type": "Copy",
              "typeProperties": {
                // Process job results
              }
            }
          ],
          "ifFalseActivities": [
            {
              "name": "HandleJobFailure",
              "type": "Fail",
              "typeProperties": {
                "message": "External job failed with status: @{variables('JobStatus')}",
                "errorCode": "ExternalJobFailed"
              }
            }
          ]
        }
      }
    ]
  }
}
```

**API Response Examples:**
```json
// Iteration 1:
{"jobId": "123", "status": "Running", "progress": 25}

// Iteration 2:
{"jobId": "123", "status": "Running", "progress": 60}

// Iteration 3:
{"jobId": "123", "status": "Completed", "progress": 100}
→ Exit loop
```

---

**3. Retry Until Success:**

```json
{
  "properties": {
    "variables": {
      "Success": {"type": "Boolean", "defaultValue": false},
      "AttemptCount": {"type": "Integer", "defaultValue": 0}
    },
    "activities": [
      {
        "name": "RetryUntilSuccess",
        "type": "Until",
        "typeProperties": {
          "expression": {
            "value": "@or(
              equals(variables('Success'), true),
              greaterOrEquals(variables('AttemptCount'), 3)
            )",
            "type": "Expression"
          },
          "timeout": "00:15:00",
          "activities": [
            {
              "name": "AttemptOperation",
              "type": "WebActivity",
              "typeProperties": {
                "url": "@pipeline().parameters.UnreliableAPIEndpoint",
                "method": "POST",
                "body": {"data": "value"}
              }
            },
            {
              "name": "MarkSuccess",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "AttemptOperation", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "Success",
                "value": true
              }
            },
            {
              "name": "IncrementAttempt",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "AttemptOperation", "dependencyConditions": ["Completed"]}
              ],
              "typeProperties": {
                "variableName": "AttemptCount",
                "value": "@add(variables('AttemptCount'), 1)"
              }
            },
            {
              "name": "WaitOnFailure",
              "type": "IfCondition",
              "dependsOn": [
                {"activity": "AttemptOperation", "dependencyConditions": ["Failed"]}
              ],
              "typeProperties": {
                "expression": {
                  "value": "@less(variables('AttemptCount'), 3)",
                  "type": "Expression"
                },
                "ifTrueActivities": [
                  {
                    "name": "WaitBeforeRetry",
                    "type": "Wait",
                    "typeProperties": {
                      "waitTimeInSeconds": "@mul(variables('AttemptCount'), 30)"  // Exponential backoff
                    }
                  }
                ]
              }
            }
          ]
        }
      }
    ]
  }
}
```

**Execution Flow:**
```
Attempt 1: API call fails → Wait 30s
Attempt 2: API call fails → Wait 60s
Attempt 3: API call succeeds → Exit loop
```

---

**4. Dynamic Data Availability Check:**

```json
{
  "properties": {
    "parameters": {
      "SourceTable": {"type": "String"},
      "ExpectedMinRows": {"type": "Integer", "defaultValue": 1000}
    },
    "variables": {
      "DataReady": {"type": "Boolean", "defaultValue": false},
      "CurrentRowCount": {"type": "Integer", "defaultValue": 0}
    },
    "activities": [
      {
        "name": "WaitForSufficientData",
        "type": "Until",
        "typeProperties": {
          "expression": {
            "value": "@variables('DataReady')",
            "type": "Expression"
          },
          "timeout": "01:00:00",
          "activities": [
            {
              "name": "GetRowCount",
              "type": "Lookup",
              "typeProperties": {
                "source": {
                  "query": "SELECT COUNT(*) AS RowCount FROM @{pipeline().parameters.SourceTable} WHERE LoadDate = CAST(GETDATE() AS DATE)"
                },
                "firstRowOnly": true
              }
            },
            {
              "name": "SetCurrentRowCount",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "GetRowCount", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "CurrentRowCount",
                "value": "@activity('GetRowCount').output.firstRow.RowCount"
              }
            },
            {
              "name": "CheckDataReady",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "SetCurrentRowCount", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "DataReady",
                "value": "@greaterOrEquals(variables('CurrentRowCount'), pipeline().parameters.ExpectedMinRows)"
              }
            },
            {
              "name": "WaitIfNotReady",
              "type": "IfCondition",
              "dependsOn": [
                {"activity": "CheckDataReady", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "expression": {
                  "value": "@not(variables('DataReady'))",
                  "type": "Expression"
                },
                "ifTrueActivities": [
                  {
                    "name": "Wait2Minutes",
                    "type": "Wait",
                    "typeProperties": {
                      "waitTimeInSeconds": 120
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      {
        "name": "ProcessData",
        "type": "Copy",
        "dependsOn": [
          {"activity": "WaitForSufficientData", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "source": {
            "query": "SELECT * FROM @{pipeline().parameters.SourceTable} WHERE LoadDate = CAST(GETDATE() AS DATE)"
          },
          "sink": {
            "tableName": "DWH.@{pipeline().parameters.SourceTable}"
          }
        }
      },
      {
        "name": "LogCompletion",
        "type": "Web",
        "dependsOn": [
          {"activity": "ProcessData", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "url": "@pipeline().parameters.LoggingEndpoint",
          "method": "POST",
          "body": {
            "table": "@{pipeline().parameters.SourceTable}",
            "rowsProcessed": "@{variables('CurrentRowCount')}",
            "expectedRows": "@{pipeline().parameters.ExpectedMinRows}",
            "timestamp": "@{utcNow()}"
          }
        }
      }
    ]
  }
}
```

**Use Case:**
- Source system loads data throughout the day
- Don't start processing until at least 1000 rows available
- Poll every 2 minutes
- Proceed when threshold met

---

**5. Multiple Condition Check:**

```json
{
  "properties": {
    "variables": {
      "AllFilesReady": {"type": "Boolean", "defaultValue": false},
      "File1Exists": {"type": "Boolean", "defaultValue": false},
      "File2Exists": {"type": "Boolean", "defaultValue": false},
      "File3Exists": {"type": "Boolean", "defaultValue": false}
    },
    "activities": [
      {
        "name": "WaitForAllFiles",
        "type": "Until",
        "typeProperties": {
          "expression": {
            "value": "@variables('AllFilesReady')",
            "type": "Expression"
          },
          "timeout": "00:30:00",
          "activities": [
            {
              "name": "CheckFile1",
              "type": "GetMetadata",
              "typeProperties": {
                "dataset": {
                  "referenceName": "File1Dataset",
                  "type": "DatasetReference"
                },
                "fieldList": ["exists"]
              }
            },
            {
              "name": "CheckFile2",
              "type": "GetMetadata",
              "typeProperties": {
                "dataset": {
                  "referenceName": "File2Dataset",
                  "type": "DatasetReference"
                },
                "fieldList": ["exists"]
              }
            },
            {
              "name": "CheckFile3",
              "type": "GetMetadata",
              "typeProperties": {
                "dataset": {
                  "referenceName": "File3Dataset",
                  "type": "DatasetReference"
                },
                "fieldList": ["exists"]
              }
            },
            {
              "name": "SetFile1Flag",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "CheckFile1", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "File1Exists",
                "value": "@activity('CheckFile1').output.exists"
              }
            },
            {
              "name": "SetFile2Flag",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "CheckFile2", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "File2Exists",
                "value": "@activity('CheckFile2').output.exists"
              }
            },
            {
              "name": "SetFile3Flag",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "CheckFile3", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "File3Exists",
                "value": "@activity('CheckFile3').output.exists"
              }
            },
            {
              "name": "CheckAllReady",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "SetFile1Flag", "dependencyConditions": ["Succeeded"]},
                {"activity": "SetFile2Flag", "dependencyConditions": ["Succeeded"]},
                {"activity": "SetFile3Flag", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "AllFilesReady",
                "value": "@and(
                  and(variables('File1Exists'), variables('File2Exists')),
                  variables('File3Exists')
                )"
              }
            },
            {
              "name": "WaitIfNotAllReady",
              "type": "IfCondition",
              "dependsOn": [
                {"activity": "CheckAllReady", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "expression": {
                  "value": "@not(variables('AllFilesReady'))",
                  "type": "Expression"
                },
                "ifTrueActivities": [
                  {
                    "name": "Wait1Minute",
                    "type": "Wait",
                    "typeProperties": {
                      "waitTimeInSeconds": 60
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      {
        "name": "ProcessAllFiles",
        "type": "Copy",
        "dependsOn": [
          {"activity": "WaitForAllFiles", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          // Process all 3 files together
        }
      }
    ]
  }
}
```

**Logic:**
- Wait until ALL 3 files are present
- Check every minute
- Only proceed when all files exist

---

**Best Practices:**

**1. Exit Condition:**

**✅ DO:**
- Always have a timeout
- Include maximum iteration check
- Make conditions clear and testable

```json
// Good: Multiple exit conditions
"expression": {
  "value": "@or(
    equals(variables('Success'), true),
    greater(variables('RetryCount'), 5)
  )"
}

// Also include timeout
"timeout": "00:30:00"
```

**❌ DON'T:**
- Create infinite loops (no exit condition)
- Use very long timeouts without reason
- Forget to update loop variables

**2. Wait Between Iterations:**

**✅ DO:**
- Always include Wait activity in loop
- Use appropriate intervals (don't poll too frequently)

```json
// Good: Wait between checks
Until loop:
├─ Check condition
├─ Update variables
└─ Wait 60 seconds (if not ready)

// Bad: No wait (hammers system!)
Until loop:
├─ Check condition
└─ Update variables
// Runs immediately again!
```

**3. Variable Updates:**

```json
// ✅ GOOD: Update variables that affect condition
Until (FileReady = true)
├─ Get Metadata
├─ Set Variable: FileReady = output.exists
└─ Wait

// ❌ BAD: Never updating loop condition variable
Until (FileReady = true)
├─ Get Metadata
└─ Wait
// FileReady never changes! Infinite loop until timeout!
```

**4. Timeout Configuration:**

```json
// ✅ GOOD: Reasonable timeouts
File check: "timeout": "00:30:00"  // 30 minutes
API poll: "timeout": "01:00:00"    // 1 hour
Batch job: "timeout": "04:00:00"   // 4 hours

// ❌ BAD: Extremely long timeout
"timeout": "7.00:00:00"  // 7 days! (maximum)
// Use scheduled trigger instead
```

**5. Error Handling:**

```json
{
  "name": "UntilWithErrorHandling",
  "type": "Until",
  "typeProperties": {
    "expression": {
      "value": "@or(
        equals(variables('Success'), true),
        greater(variables('ErrorCount'), 3)
      )",
      "type": "Expression"
    },
    "activities": [
      {
        "name": "AttemptOperation",
        "type": "WebActivity",
        "typeProperties": {
          "url": "@pipeline().parameters.APIURL",
          "method": "GET"
        }
      },
      {
        "name": "OnSuccess",
        "type": "SetVariable",
        "dependsOn": [
          {"activity": "AttemptOperation", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "variableName": "Success",
          "value": true
        }
      },
      {
        "name": "OnFailure",
        "type": "SetVariable",
        "dependsOn": [
          {"activity": "AttemptOperation", "dependencyConditions": ["Failed"]}
        ],
        "typeProperties": {
          "variableName": "ErrorCount",
          "value": "@add(variables('ErrorCount'), 1)"
        }
      },
      {
        "name": "WaitBeforeRetry",
        "type": "Wait",
        "dependsOn": [
          {"activity": "OnFailure", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "waitTimeInSeconds": 30
        }
      }
    ]
  }
}

// After loop:
{
  "name": "CheckFinalStatus",
  "type": "IfCondition",
  "dependsOn": [
    {"activity": "UntilWithErrorHandling", "dependencyConditions": ["Succeeded"]}
  ],
  "typeProperties": {
    "expression": {
      "value": "@variables('Success')",
      "type": "Expression"
    },
    "ifTrueActivities": [/* Continue */],
    "ifFalseActivities": [
      {
        "name": "FailPipeline",
        "type": "Fail",
        "typeProperties": {
          "message": "Operation failed after 3 retries",
          "errorCode": "MaxRetriesExceeded"
        }
      }
    ]
  }
}
```

---

**Until vs. Alternatives:**

| Scenario | Until Activity | Alternative |
|----------|----------------|-------------|
| **Poll for file** | ✅ Perfect | - |
| **Fixed iterations** | ❌ | ForEach |
| **Fixed delay** | ❌ | Wait Activity |
| **Conditional retry** | ✅ Good | Built-in retry policy |
| **Unknown duration** | ✅ Perfect | - |
| **Scheduled check** | ❌ | Scheduled trigger |
| **Single check** | ❌ | If Condition |

---

**Common Wait Intervals:**

| Polling Target | Recommended Interval |
|----------------|---------------------|
| **File arrival** | 30-60 seconds |
| **API status** | 60-120 seconds |
| **Database data** | 60-180 seconds |
| **Batch job** | 120-300 seconds |
| **External process** | 300-600 seconds |

---

**Real-World Example: Complete File Processing with Polling**

```json
{
  "properties": {
    "parameters": {
      "ExpectedFileName": {"type": "String"},
      "MaxWaitMinutes": {"type": "Integer", "defaultValue": 60},
      "PollingIntervalSeconds": {"type": "Integer", "defaultValue": 60}
    },
    "variables": {
      "FileFound": {"type": "Boolean", "defaultValue": false},
      "ElapsedMinutes": {"type": "Integer", "defaultValue": 0},
      "FileSize": {"type": "Integer", "defaultValue": 0},
      "LastModified": {"type": "String", "defaultValue": ""}
    },
    "activities": [
      {
        "name": "PollForFile",
        "type": "Until",
        "typeProperties": {
          "expression": {
            "value": "@or(
              equals(variables('FileFound'), true),
              greaterOrEquals(variables('ElapsedMinutes'), pipeline().parameters.MaxWaitMinutes)
            )",
            "type": "Expression"
          },
          "timeout": "@concat('00:', string(pipeline().parameters.MaxWaitMinutes), ':00')",
          "activities": [
            {
              "name": "CheckFileExists",
              "type": "GetMetadata",
              "typeProperties": {
                "dataset": {
                  "referenceName": "InputFileDataset",
                  "type": "DatasetReference",
                  "parameters": {
                    "FileName": "@pipeline().parameters.ExpectedFileName"
                  }
                },
                "fieldList": ["exists", "size", "lastModified"]
              }
            },
            {
              "name": "UpdateFileFound",
              "type": "SetVariable",
              "dependsOn": [
                {"activity": "CheckFileExists", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "variableName": "FileFound",
                "value": "@activity('CheckFileExists').output.exists"
              }
            },
            {
              "name": "StoreFileMetadata",
              "type": "IfCondition",
              "dependsOn": [
                {"activity": "UpdateFileFound", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "expression": {
                  "value": "@variables('FileFound')",
                  "type": "Expression"
                },
                "ifTrueActivities": [
                  {
                    "name": "SetFileSize",
                    "type": "SetVariable",
                    "typeProperties": {
                      "variableName": "FileSize",
                      "value": "@activity('CheckFileExists').output.size"
                    }
                  },
                  {
                    "name": "SetLastModified",
                    "type": "SetVariable",
                    "dependsOn": [
                      {"activity": "SetFileSize", "dependencyConditions": ["Succeeded"]}
                    ],
                    "typeProperties": {
                      "variableName": "LastModified",
                      "value": "@string(activity('CheckFileExists').output.lastModified)"
                    }
                  }
                ],
                "ifFalseActivities": [
                  {
                    "name": "IncrementElapsedTime",
                    "type": "SetVariable",
                    "typeProperties": {
                      "variableName": "ElapsedMinutes",
                      "value": "@add(variables('ElapsedMinutes'), div(pipeline().parameters.PollingIntervalSeconds, 60))"
                    }
                  },
                  {
                    "name": "WaitBeforeNextCheck",
                    "type": "Wait",
                    "dependsOn": [
                      {"activity": "IncrementElapsedTime", "dependencyConditions": ["Succeeded"]}
                    ],
                    "typeProperties": {
                      "waitTimeInSeconds": "@pipeline().parameters.PollingIntervalSeconds"
                    }
                  }
                ]
              }
            }
          ]
        }
      },
      {
        "name": "ValidateFileFound",
        "type": "IfCondition",
        "dependsOn": [
          {"activity": "PollForFile", "dependencyConditions": ["Succeeded"]}
        ],
        "typeProperties": {
          "expression": {
            "value": "@variables('FileFound')",
            "type": "Expression"
          },
          "ifTrueActivities": [
            {
              "name": "LogFileDetected",
              "type": "Web",
              "typeProperties": {
                "url": "@pipeline().parameters.LoggingEndpoint",
                "method": "POST",
                "body": {
                  "event": "FileDetected",
                  "fileName": "@{pipeline().parameters.ExpectedFileName}",
                  "fileSize": "@{variables('FileSize')}",
                  "lastModified": "@{variables('LastModified')}",
                  "waitTimeMinutes": "@{variables('ElapsedMinutes')}",
                  "timestamp": "@{utcNow()}"
                }
              }
            },
            {
              "name": "ProcessFile",
              "type": "Copy",
              "dependsOn": [
                {"activity": "LogFileDetected", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "source": {
                  "type": "DelimitedTextSource",
                  "fileName": "@pipeline().parameters.ExpectedFileName"
                },
                "sink": {
                  "type": "LakehouseSink",
                  "tableName": "Bronze.Data"
                }
              }
            },
            {
              "name": "LogProcessingComplete",
              "type": "Web",
              "dependsOn": [
                {"activity": "ProcessFile", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "url": "@pipeline().parameters.LoggingEndpoint",
                "method": "POST",
                "body": {
                  "event": "ProcessingComplete",
                  "fileName": "@{pipeline().parameters.ExpectedFileName}",
                  "rowsProcessed": "@{activity('ProcessFile').output.rowsCopied}",
                  "timestamp": "@{utcNow()}"
                }
              }
            }
          ],
          "ifFalseActivities": [
            {
              "name": "LogTimeout",
              "type": "Web",
              "typeProperties": {
                "url": "@pipeline().parameters.LoggingEndpoint",
                "method": "POST",
                "body": {
                  "event": "FileNotFound",
                  "fileName": "@{pipeline().parameters.ExpectedFileName}",
                  "maxWaitMinutes": "@{pipeline().parameters.MaxWaitMinutes}",
                  "timestamp": "@{utcNow()}"
                }
              }
            },
            {
              "name": "FailPipeline",
              "type": "Fail",
              "dependsOn": [
                {"activity": "LogTimeout", "dependencyConditions": ["Succeeded"]}
              ],
              "typeProperties": {
                "message": "File @{pipeline().parameters.ExpectedFileName} not found within @{pipeline().parameters.MaxWaitMinutes} minutes",
                "errorCode": "FileWaitTimeout"
              }
            }
          ]
        }
      }
    ]
  }
}
```

**Execution Flow (File arrives after 15 minutes):**

```
00:00 - Start polling
00:00 - Iteration 1: File not found → Wait 60s
00:01 - Iteration 2: File not found → Wait 60s
...
00:15 - Iteration 15: File found!
        → Exit loop
        → Log file detected (size: 5MB, modified: 2026-04-25 10:15:00)
        → Process file
        → Log processing complete (10,000 rows)
        → Success!
```

**Execution Flow (File never arrives):**

```
00:00 - Start polling
00:00 - Iteration 1: File not found → Wait 60s
00:01 - Iteration 2: File not found → Wait 60s
...
01:00 - Iteration 60: File not found → Timeout!
        → Log timeout
        → Fail pipeline with error
```

---

**Monitoring Until Activity:**

```
Fabric Monitoring Hub → Pipeline Run → Activity Runs → Until Activity
- Click on Until activity
- View "Iterations" count
- Click through to see individual iteration details
- Check which iteration succeeded/failed
```

---

**Key Takeaways:**

1. **Purpose:** Until Activity loops until condition = true or timeout
2. **Execution:** At least one iteration, continues until exit condition
3. **Timeout:** Default 1 hour, max 7 days
4. **Exit Conditions:** Boolean expression OR timeout OR max iterations
5. **Wait in Loop:** Always include Wait activity to avoid hammering systems
6. **Variables:** Must update variables that affect loop condition
7. **Common Uses:** File polling, status checking, retry logic
8. **Best Practice:** Set reasonable timeout, include max iteration check, wait between iterations
9. **vs. ForEach:** ForEach for known iterations, Until for unknown duration
10. **Error Handling:** Check final state after loop, handle timeout scenarios

Until Activity is **powerful** for implementing polling and retry patterns in data pipelines!

---

## 9. Fabric Data Optimization

**Definition:**
Fabric Data Optimization encompasses a set of techniques and features designed to improve the performance, efficiency, and cost-effectiveness of data stored and processed in Microsoft Fabric. It includes write-time optimizations (V-Order, Optimize Write), read-time optimizations (Z-Order), table maintenance operations (OPTIMIZE, VACUUM), and merge optimizations. These techniques work together to address common challenges like the small file problem, slow query performance, and storage bloat in Delta Lake tables.

**Purpose:**
- Improve query performance for analytical workloads
- Reduce storage costs by removing obsolete files
- Optimize file sizes for efficient data processing
- Enhance compression ratios
- Accelerate read operations across Fabric engines
- Maintain healthy Delta Lake tables over time
- Balance write and read performance based on workload patterns

---

**Key Optimization Techniques:**

### 1. V-Order (Write-Time Optimization)

**What It Is:**
V-Order is a write-time optimization for Parquet files that reorganizes the internal file layout to improve downstream query performance across all Fabric engines (Spark, Power BI, SQL Endpoint, Data Warehouse).

**How It Works:**
- Reorganizes row group distribution within Parquet files
- Applies optimized encoding schemes
- Improves compression ratios (up to 50% better compression)
- Maintains full Parquet/Delta compatibility (open-source compliant)

**Performance Impact:**
- **Writes:** ~15% slower on average (optimization overhead)
- **Reads:** Significantly faster (varies by workload)
- **Compression:** Up to 50% more compression
- **Best For:** Read-heavy patterns (dashboards, interactive analytics, reporting)

**Default Behavior:**
- **Disabled by default** in new Fabric workspaces (`spark.sql.parquet.vorder.default=false`)
- Optimizes for write-heavy data engineering workloads
- Can be enabled at session, table, or write-operation level

**Configuration Levels:**

```python
# 1. Session-level (applies to all writes in session)
spark.conf.set("spark.sql.parquet.vorder.default", "true")

# 2. Table-level (applies to specific table)
spark.sql("""
  ALTER TABLE my_table 
  SET TBLPROPERTIES("delta.parquet.vorder.enabled" = "true")
""")

# 3. Write-operation level (applies to single write)
df.write \
  .format("delta") \
  .mode("append") \
  .option("parquet.vorder.enabled", "true") \
  .saveAsTable("my_table")
```

**When to Use V-Order:**

| Workload Type | Use V-Order? | Reason |
|---------------|--------------|--------|
| **Dashboards** | ✅ Yes | Repeated reads, multiple users |
| **Interactive queries** | ✅ Yes | Ad-hoc analysis, fast response needed |
| **Power BI Direct Lake** | ✅ Yes | Optimized for BI engine reads |
| **Data science exploration** | ✅ Yes | Frequent data scans |
| **ETL pipelines (write-heavy)** | ❌ No | Prioritize write speed |
| **Streaming ingestion** | ❌ No | High-frequency writes |
| **One-time data loads** | ❌ No | Data won't be read frequently |

---

### 2. Optimize Write (Small File Prevention)

**What It Is:**
Optimize Write is a Delta Lake feature that reduces the number of small files created during data ingestion by automatically combining data into larger, more optimally sized files during write operations.

**The Small File Problem:**
```
BAD: Many small files (inefficient)
Table: sales_data
├─ part-00001.parquet (10 KB)
├─ part-00002.parquet (15 KB)
├─ part-00003.parquet (8 KB)
├─ part-00004.parquet (12 KB)
└─ ... (1000s of small files)

Result: 
- Slow query performance (too many files to open)
- Metadata overhead
- Inefficient I/O
- Poor compression

GOOD: Fewer, larger files (efficient)
Table: sales_data
├─ part-00001.parquet (128 MB)
├─ part-00002.parquet (128 MB)
└─ part-00003.parquet (128 MB)

Result:
- Fast query performance
- Better compression
- Efficient I/O
- Reduced metadata overhead
```

**How It Works:**
- Automatically enabled by default in Fabric Runtime
- Combines small files into target file size (configurable)
- Happens during write operations (no separate command needed)
- Works with all write modes (append, overwrite, merge)

**Configuration:**

```python
# Check if Optimize Write is enabled
spark.conf.get("spark.microsoft.delta.optimizeWrite.enabled")
# Default: true

# Set target file size (default: 128 MB)
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "134217728")  # 128 MB in bytes

# Disable if needed (not recommended)
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "false")
```

**Use Cases:**
- Streaming data ingestion
- Batch ETL pipelines
- Frequent append operations
- Micro-batch processing

---

### 3. Z-Order (Data Clustering)

**What It Is:**
Z-Order (Z-Ordering) is a data clustering technique that co-locates related data in the same files based on specified columns. It improves query performance by enabling better data skipping (reading only relevant files).

**How It Works:**
```
WITHOUT Z-Order:
Query: SELECT * FROM sales WHERE region = 'West' AND product_category = 'Electronics'

Files scanned: ALL files (no data skipping)
├─ file1.parquet: region=[East, West, North, South], category=[All mixed]
├─ file2.parquet: region=[East, West, North, South], category=[All mixed]
└─ file3.parquet: region=[East, West, North, South], category=[All mixed]

WITH Z-Order on (region, product_category):
Query: SELECT * FROM sales WHERE region = 'West' AND product_category = 'Electronics'

Files scanned: Only relevant files (data skipping works!)
├─ file1.parquet: region=[West], category=[Electronics]  ← Read this
├─ file2.parquet: region=[East], category=[Clothing]     ← Skip
└─ file3.parquet: region=[North], category=[Food]        ← Skip
```

**Syntax:**

```sql
-- Z-Order during OPTIMIZE
OPTIMIZE my_table
ZORDER BY (region, product_category);

-- Z-Order with WHERE clause (partition-specific)
OPTIMIZE my_table
WHERE date >= '2026-01-01'
ZORDER BY (region, product_category);
```

**Best Practices:**

```python
# ✅ GOOD: Z-Order on frequently filtered columns
OPTIMIZE sales_table
ZORDER BY (customer_id, order_date)

# Use when queries like:
# SELECT * FROM sales WHERE customer_id = 123
# SELECT * FROM sales WHERE order_date = '2026-04-25'

# ❌ BAD: Z-Order on too many columns
OPTIMIZE sales_table
ZORDER BY (col1, col2, col3, col4, col5, col6)
# Effectiveness decreases with more columns

# ❌ BAD: Z-Order on high-cardinality columns only
OPTIMIZE sales_table
ZORDER BY (transaction_id)  # Every value is unique!
# No clustering benefit
```

**Column Selection Guidelines:**

| Column Type | Z-Order? | Reason |
|-------------|----------|--------|
| **Date/Time** | ✅ Excellent | Common filter, good clustering |
| **Category** | ✅ Good | Discrete values, frequent filters |
| **Customer ID** | ✅ Good | Common in WHERE clauses |
| **Region/Country** | ✅ Good | Low cardinality, common filter |
| **Status** | ✅ Good | Few values, often filtered |
| **Transaction ID** | ❌ Poor | Unique values, no clustering benefit |
| **Description** | ❌ Poor | Text field, not used in filters |
| **Amount** | ❌ Poor | Continuous values, rarely exact match |

**Recommended:** 2-4 columns maximum for Z-Order

---

### 4. OPTIMIZE Command (Table Compaction)

**What It Is:**
OPTIMIZE is a Delta Lake command that performs bin-packing compaction—consolidating small files into larger, more efficient files. It can optionally apply Z-Order and V-Order during the rewrite process.

**Syntax Variations:**

```sql
-- Basic OPTIMIZE (compaction only)
OPTIMIZE my_table;

-- OPTIMIZE with V-Order
OPTIMIZE my_table VORDER;

-- OPTIMIZE with Z-Order
OPTIMIZE my_table
ZORDER BY (region, product_category);

-- OPTIMIZE with both Z-Order and V-Order
OPTIMIZE my_table
ZORDER BY (region, product_category)
VORDER;

-- OPTIMIZE specific partition
OPTIMIZE my_table
WHERE date >= '2026-04-01' AND date < '2026-05-01';

-- OPTIMIZE partition with Z-Order and V-Order
OPTIMIZE my_table
WHERE date = '2026-04-25'
ZORDER BY (customer_id)
VORDER;
```

**What OPTIMIZE Does:**

```
BEFORE OPTIMIZE:
Table: sales_data
├─ part-00001.parquet (5 MB)
├─ part-00002.parquet (3 MB)
├─ part-00003.parquet (7 MB)
├─ part-00004.parquet (2 MB)
├─ part-00005.parquet (4 MB)
└─ ... (100 small files)

Total: 100 files, 500 MB data

AFTER OPTIMIZE:
Table: sales_data
├─ part-00001.parquet (128 MB)
├─ part-00002.parquet (128 MB)
├─ part-00003.parquet (128 MB)
└─ part-00004.parquet (116 MB)

Total: 4 files, 500 MB data (same data, fewer files!)
```

**PySpark API:**

```python
from delta.tables import DeltaTable

# Basic optimize
DeltaTable.forName(spark, "my_table").optimize().executeCompaction()

# Optimize with Z-Order
DeltaTable.forName(spark, "my_table") \
  .optimize() \
  .executeZOrderBy("region", "product_category")

# Optimize specific partition
DeltaTable.forPath(spark, "/path/to/table") \
  .optimize() \
  .where("date = '2026-04-25'") \
  .executeCompaction()
```

**When to Run OPTIMIZE:**

```python
# ✅ GOOD: After major ingestion
# Example: Daily batch load
df.write.format("delta").mode("append").saveAsTable("sales")
spark.sql("OPTIMIZE sales WHERE date = current_date()")

# ✅ GOOD: After many small writes
# Example: Streaming or micro-batch
for batch in batches:
    batch.write.format("delta").mode("append").saveAsTable("events")

# Run OPTIMIZE weekly:
spark.sql("OPTIMIZE events")

# ✅ GOOD: Before large analytical queries
spark.sql("OPTIMIZE large_fact_table ZORDER BY (customer_id, product_id)")
# Then run your analytics

# ❌ BAD: After every single write
for record in records:
    record.write.format("delta").mode("append").saveAsTable("table")
    spark.sql("OPTIMIZE table")  # Too frequent! Wastes resources
```

**OPTIMIZE Output:**

```python
result = spark.sql("OPTIMIZE my_table").collect()

# Output example:
# path: dbfs:/lakehouse/tables/my_table
# metrics.numFilesAdded: 4
# metrics.numFilesRemoved: 100
# metrics.filesAdded.min: 128000000
# metrics.filesAdded.max: 128000000
# metrics.filesAdded.avg: 128000000
# metrics.filesRemoved.min: 2000000
# metrics.filesRemoved.max: 10000000
# metrics.filesRemoved.avg: 5000000
# metrics.totalFilesSkipped: 0
# metrics.totalTimeMs: 45000
```

---

### 5. VACUUM Command (Storage Cleanup)

**What It Is:**
VACUUM is a Delta Lake command that removes files that are no longer referenced by the Delta log and are older than the retention threshold. It reclaims storage space by deleting obsolete data files.

**How Delta Lake Creates "Obsolete" Files:**

```
Scenario: UPDATE operation

BEFORE UPDATE:
Delta Log: version 5
Referenced Files:
├─ part-001.parquet (contains rows 1-1000)
└─ part-002.parquet (contains rows 1001-2000)

UPDATE sales SET price = price * 1.1 WHERE region = 'West'

AFTER UPDATE:
Delta Log: version 6
Referenced Files:
├─ part-001-new.parquet (updated rows from part-001) ← NEW, REFERENCED
├─ part-002.parquet (unchanged)                       ← REFERENCED

Unreferenced Files (ready for VACUUM):
└─ part-001.parquet                                   ← OLD, UNREFERENCED

Note: Old file kept for:
- Time travel (SELECT * FROM table VERSION AS OF 5)
- Concurrent readers (may still be reading old version)
```

**Syntax:**

```sql
-- VACUUM with default retention (7 days)
VACUUM my_table;

-- VACUUM with custom retention (in hours)
VACUUM my_table RETAIN 168 HOURS;  -- 7 days

-- VACUUM with custom retention (in days)
VACUUM my_table RETAIN 30 DAYS;

-- VACUUM specific path
VACUUM '/lakehouse/Files/my_table' RETAIN 168 HOURS;

-- DRY RUN (see what would be deleted, don't actually delete)
VACUUM my_table DRY RUN;
```

**Retention Considerations:**

| Retention Period | Use Case | Risk |
|------------------|----------|------|
| **7 days (default)** | Standard operations | Balanced: time travel + cleanup |
| **14-30 days** | Critical tables, frequent rollbacks | More storage cost, longer history |
| **3-5 days** | High-volume tables, storage cost focus | Less time travel capability |
| **< 7 days** | Special cases only | ⚠️ Requires safety check override |

**Safety Check for Short Retention:**

```python
# For retention < 7 days, must disable safety check

# ❌ This will FAIL:
spark.sql("VACUUM my_table RETAIN 24 HOURS")
# Error: Retention must be at least 168 hours

# ✅ Override safety check (use with caution):
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM my_table RETAIN 24 HOURS")
```

**VACUUM Dry Run:**

```sql
-- See what would be deleted without actually deleting
VACUUM my_table RETAIN 168 HOURS DRY RUN;

-- Output example:
-- path
-- dbfs:/lakehouse/tables/my_table/part-001.parquet
-- dbfs:/lakehouse/tables/my_table/part-003.parquet
-- dbfs:/lakehouse/tables/my_table/part-007.parquet
-- 
-- 3 files would be deleted
```

**Impact on Time Travel:**

```sql
-- Before VACUUM:
SELECT * FROM my_table VERSION AS OF 10;  -- Works (version 10 files exist)

-- After VACUUM RETAIN 7 DAYS:
SELECT * FROM my_table VERSION AS OF 10;  -- May FAIL if version 10 > 7 days old
-- Error: Files not found for version 10
```

**Best Practices:**

```python
# ✅ GOOD: Regular VACUUM schedule
# Run weekly or monthly depending on table update frequency
spark.sql("VACUUM my_table RETAIN 168 HOURS")

# ✅ GOOD: VACUUM after major cleanup operations
spark.sql("DELETE FROM my_table WHERE date < '2024-01-01'")
spark.sql("OPTIMIZE my_table")
spark.sql("VACUUM my_table RETAIN 168 HOURS")  # Remove old files

# ✅ GOOD: Test with DRY RUN first
files_to_delete = spark.sql("VACUUM my_table DRY RUN").collect()
print(f"Will delete {len(files_to_delete)} files")
spark.sql("VACUUM my_table")

# ❌ BAD: VACUUM too frequently
for i in range(100):
    spark.sql("VACUUM my_table")  # Wastes compute, no benefit

# ❌ BAD: Very short retention without consideration
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM my_table RETAIN 1 HOUR")  # Breaks time travel, concurrent reads!
```

**VACUUM Output:**

```python
result = spark.sql("VACUUM my_table").collect()

# Output example:
# path: dbfs:/lakehouse/tables/my_table
# Total files deleted: 150
# Total size deleted (MB): 2048
# Time taken (seconds): 12
```

---

### 6. Merge Optimization (Low Shuffle Merge)

**What It Is:**
Merge Optimization (Low Shuffle Merge) is an automatic optimization for Delta Lake MERGE operations that reduces unnecessary data shuffling when merging source data into target tables.

**The Problem It Solves:**

```python
# Traditional MERGE behavior:
MERGE INTO target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

# Without optimization:
# - Shuffles ALL target rows (even unchanged ones)
# - Expensive for large target tables with small updates

# Example: Target has 1 billion rows, source has 1000 rows
# Without optimization: Shuffles 1 billion rows!
# With Low Shuffle Merge: Shuffles only affected rows
```

**How It Works:**
- Automatically enabled in Fabric Runtime (`spark.microsoft.delta.merge.lowShuffle.enabled = true`)
- Identifies rows that won't change
- Skips shuffling unchanged rows
- Significantly reduces shuffle overhead

**Performance Impact:**

```
Scenario: MERGE 1000 new records into 1 billion record table

Without Low Shuffle Merge:
- Shuffle: 1,000,000,000 rows
- Time: 30 minutes
- Compute cost: High

With Low Shuffle Merge:
- Shuffle: ~1,000 rows (only affected)
- Time: 2 minutes
- Compute cost: Low
```

**No Code Changes Required:**

```python
# This MERGE automatically benefits from Low Shuffle Merge:
spark.sql("""
  MERGE INTO target_table t
  USING source_table s
  ON t.id = s.id
  WHEN MATCHED THEN
    UPDATE SET t.value = s.value, t.updated_date = current_timestamp()
  WHEN NOT MATCHED THEN
    INSERT (id, value, updated_date)
    VALUES (s.id, s.value, current_timestamp())
""")

# Check if enabled:
spark.conf.get("spark.microsoft.delta.merge.lowShuffle.enabled")
# Returns: true (default in Fabric)
```

---

**Complete Optimization Workflow:**

```python
# Recommended optimization workflow for Delta tables:

# 1. INITIAL SETUP: Enable V-Order for read-heavy tables
spark.sql("""
  ALTER TABLE sales_analytics 
  SET TBLPROPERTIES("delta.parquet.vorder.enabled" = "true")
""")

# 2. DURING INGESTION: Optimize Write handles small files automatically
# (No action needed - enabled by default)
df_sales.write \
  .format("delta") \
  .mode("append") \
  .saveAsTable("sales_analytics")

# 3. POST-INGESTION: Run OPTIMIZE with Z-Order and V-Order
spark.sql("""
  OPTIMIZE sales_analytics
  WHERE load_date = current_date()
  ZORDER BY (region, product_category)
  VORDER
""")

# 4. REGULAR MAINTENANCE: VACUUM old files
# Run weekly or monthly
spark.sql("VACUUM sales_analytics RETAIN 168 HOURS")

# 5. MERGE OPERATIONS: Low Shuffle Merge works automatically
spark.sql("""
  MERGE INTO sales_analytics t
  USING daily_updates s
  ON t.order_id = s.order_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
```

---

**Lakehouse Portal-Based Maintenance:**

In Fabric Lakehouse, you can run maintenance through the UI:

**Steps:**
1. Navigate to Lakehouse
2. Right-click table → **Maintenance**
3. Configure options:
   - **Optimize:** ON (compacts small files)
   - **Apply V-Order:** ✓ (if read-heavy workload)
   - **Vacuum:** ON
   - **Retention:** 168 hours (7 days)
4. Click **Run now**

**Pipeline-Based Maintenance:**

Use the **Lakehouse Maintenance Activity** in Data Factory pipelines:

```json
{
  "name": "MaintainSalesTable",
  "type": "LakehouseMaintenance",
  "typeProperties": {
    "workspace": "MyWorkspace",
    "lakehouse": "SalesLakehouse",
    "tableName": "sales_data",
    "optimize": true,
    "applyVOrder": true,
    "vacuum": true,
    "retentionHours": 168
  }
}
```

---

**Optimization Decision Matrix:**

| Workload Pattern | V-Order | Optimize Write | Z-Order | OPTIMIZE Frequency | VACUUM Frequency |
|------------------|---------|----------------|---------|-------------------|------------------|
| **Streaming ingestion** | ❌ Off | ✅ On | ❌ Skip | Weekly | Monthly |
| **Batch ETL** | ❌ Off | ✅ On | ✅ Yes | After each load | Monthly |
| **Read-heavy analytics** | ✅ On | ✅ On | ✅ Yes | After updates | Monthly |
| **Interactive BI** | ✅ On | ✅ On | ✅ Yes | Daily | Weekly |
| **ML training** | ✅ On | ✅ On | ✅ Yes | Before training | Monthly |
| **Archive tables** | ✅ On | ✅ On | ❌ Skip | Once at creation | Never |

---

**Common Optimization Patterns:**

**Pattern 1: Daily Batch with BI Reporting**

```python
# Daily ETL Pipeline
def daily_etl():
    # 1. Extract and load data (Optimize Write handles small files)
    df_today = extract_daily_data()
    df_today.write.format("delta").mode("append").saveAsTable("sales")
    
    # 2. Run OPTIMIZE with V-Order for BI queries
    spark.sql("""
        OPTIMIZE sales 
        WHERE load_date = current_date()
        ZORDER BY (region, product_id)
        VORDER
    """)
    
    # 3. Weekly VACUUM (run on Sundays)
    if datetime.today().weekday() == 6:  # Sunday
        spark.sql("VACUUM sales RETAIN 168 HOURS")
    
    # 4. Refresh Power BI dataset
    refresh_powerbi_dataset("Sales Dashboard")
```

**Pattern 2: Streaming with Periodic Compaction**

```python
# Streaming ingestion (no V-Order for write speed)
spark.conf.set("spark.sql.parquet.vorder.default", "false")

stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/events") \
    .table("real_time_events")

# Scheduled compaction job (runs hourly)
def hourly_compaction():
    spark.sql("""
        OPTIMIZE real_time_events
        WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
    """)
```

**Pattern 3: Analytics with Heavy Updates**

```python
# Table with frequent MERGE operations
def update_customer_data():
    # 1. MERGE new data (Low Shuffle Merge optimizes automatically)
    spark.sql("""
        MERGE INTO customer_360 t
        USING daily_updates s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # 2. OPTIMIZE after MERGE (consolidate updated files)
    spark.sql("""
        OPTIMIZE customer_360
        ZORDER BY (customer_id, last_purchase_date)
        VORDER
    """)
    
    # 3. VACUUM monthly (1st of month)
    if datetime.today().day == 1:
        spark.sql("VACUUM customer_360 RETAIN 30 DAYS")
```

---

**Monitoring Optimization Impact:**

**1. Check File Count:**

```sql
-- Check number of files in table
DESCRIBE DETAIL my_table;

-- Output:
-- numFiles: 1250 (BEFORE OPTIMIZE)
-- numFiles: 15 (AFTER OPTIMIZE)
```

**2. Check File Sizes:**

```python
# List files and sizes
files = spark.sql("DESCRIBE DETAIL my_table").select("sizeInBytes", "numFiles").collect()

print(f"Total size: {files[0]['sizeInBytes'] / (1024**3):.2f} GB")
print(f"Number of files: {files[0]['numFiles']}")
print(f"Average file size: {files[0]['sizeInBytes'] / files[0]['numFiles'] / (1024**2):.2f} MB")
```

**3. Compare Query Performance:**

```python
import time

# Before OPTIMIZE
start = time.time()
result = spark.sql("SELECT COUNT(*) FROM sales WHERE region = 'West'").collect()
before_time = time.time() - start

# Run OPTIMIZE
spark.sql("OPTIMIZE sales ZORDER BY (region) VORDER")

# After OPTIMIZE
start = time.time()
result = spark.sql("SELECT COUNT(*) FROM sales WHERE region = 'West'").collect()
after_time = time.time() - start

print(f"Query time BEFORE: {before_time:.2f} seconds")
print(f"Query time AFTER: {after_time:.2f} seconds")
print(f"Improvement: {((before_time - after_time) / before_time * 100):.1f}%")
```

**4. Check V-Order Usage:**

```python
# Check if table has V-Order enabled
props = spark.sql("SHOW TBLPROPERTIES my_table").collect()
vorder_enabled = [p for p in props if 'vorder' in p['key'].lower()]

for prop in vorder_enabled:
    print(f"{prop['key']}: {prop['value']}")
```

---

**Cost vs. Performance Trade-offs:**

| Technique | Compute Cost | Storage Cost | Read Performance | Write Performance |
|-----------|--------------|--------------|------------------|-------------------|
| **V-Order** | +15% write | 0 (better compression) | +++ | -15% |
| **Optimize Write** | +10% write | 0 | ++ | -10% |
| **Z-Order** | High (one-time) | 0 | +++ (filtered queries) | 0 |
| **OPTIMIZE** | Medium (periodic) | 0 | +++ | 0 |
| **VACUUM** | Low | --- (reclaims space) | 0 | 0 |

---

**Interview Questions:**

1. **Q:** What is V-Order and when should you use it in Microsoft Fabric?
   **A:**
   V-Order is a write-time optimization for Parquet files that reorganizes the internal file layout (row group distribution, encoding, compression) to improve query performance across all Fabric engines.
   
   **Key Characteristics:**
   - **Write Impact:** ~15% slower writes on average
   - **Read Impact:** Significantly faster reads (varies by workload)
   - **Compression:** Up to 50% better compression
   - **Compatibility:** Open-source Parquet compliant
   
   **When to Use:**
   - ✅ **Read-heavy workloads:** Dashboards, interactive analytics, reporting
   - ✅ **Power BI Direct Lake:** Optimized for BI engine reads
   - ✅ **Data science exploration:** Frequent data scans
   - ❌ **Write-heavy ETL:** Prioritize write speed over read optimization
   - ❌ **Streaming ingestion:** High-frequency writes
   
   **Configuration Levels:**
   ```python
   # Session-level
   spark.conf.set("spark.sql.parquet.vorder.default", "true")
   
   # Table-level
   ALTER TABLE my_table SET TBLPROPERTIES("delta.parquet.vorder.enabled" = "true")
   
   # Write-level
   df.write.option("parquet.vorder.enabled", "true").saveAsTable("table")
   ```
   
   **Default Behavior:**
   - Disabled by default in new Fabric workspaces (`vorder.default=false`)
   - Optimizes for write-heavy data engineering workloads
   - Enable for analytics/BI-focused tables
   
   **Decision Rule:**
   - If table read:write ratio > 10:1 → Enable V-Order
   - If table read:write ratio < 5:1 → Keep V-Order disabled
   - If mixed workload → Test both, measure query performance

2. **Q:** Explain the difference between OPTIMIZE and VACUUM commands in Delta Lake.
   **A:**
   OPTIMIZE and VACUUM serve different purposes in Delta Lake table maintenance:
   
   **OPTIMIZE (Performance Optimization):**
   - **Purpose:** Improve query performance by compacting small files
   - **What it does:**
     - Bin-packing: Combines small files into larger files (default 128 MB target)
     - Z-Order: Co-locates data by specified columns for better data skipping
     - V-Order: Applies read optimization during file rewrite
   - **Impact:**
     - Reduces number of files (fewer files to open during reads)
     - Does NOT delete old files (old files remain for time travel)
     - Increases metadata size temporarily (both old and new files in log)
   - **When to run:** After major ingestion, before analytical queries
   - **Frequency:** Daily/weekly for active tables
   
   ```sql
   -- Examples:
   OPTIMIZE my_table;
   OPTIMIZE my_table ZORDER BY (date, customer_id);
   OPTIMIZE my_table WHERE date = '2026-04-25' VORDER;
   ```
   
   **VACUUM (Storage Cleanup):**
   - **Purpose:** Reclaim storage by removing obsolete files
   - **What it does:**
     - Deletes files no longer referenced by Delta log
     - Only removes files older than retention threshold
     - Permanently deletes files (cannot be undone)
   - **Impact:**
     - Reduces storage costs
     - Limits time travel capability (old versions gone)
     - Can break concurrent readers if retention too short
   - **When to run:** Weekly/monthly after OPTIMIZE
   - **Frequency:** Less frequent than OPTIMIZE (monthly typical)
   
   ```sql
   -- Examples:
   VACUUM my_table;  -- Default: 7 days retention
   VACUUM my_table RETAIN 168 HOURS;  -- 7 days
   VACUUM my_table RETAIN 30 DAYS;
   ```
   
   **Key Differences:**
   
   | Aspect | OPTIMIZE | VACUUM |
   |--------|----------|--------|
   | **Goal** | Query performance | Storage cleanup |
   | **Deletes files?** | No | Yes |
   | **Time travel impact** | None | Removes old versions |
   | **When to run** | Before reads | After OPTIMIZE |
   | **Frequency** | Daily/Weekly | Monthly |
   | **Reversible?** | Yes (old files still exist) | No (permanent deletion) |
   
   **Recommended Workflow:**
   ```python
   # 1. Run OPTIMIZE (improve performance)
   spark.sql("OPTIMIZE sales ZORDER BY (region)")
   
   # 2. Wait (let time travel window expire)
   # Wait 7+ days
   
   # 3. Run VACUUM (reclaim storage)
   spark.sql("VACUUM sales RETAIN 168 HOURS")
   ```
   
   **Monitoring:**
   ```python
   # Before operations:
   spark.sql("DESCRIBE DETAIL sales").show()
   # Note: numFiles, sizeInBytes
   
   # After OPTIMIZE:
   # numFiles: Reduced significantly
   # sizeInBytes: Same or slightly larger (both old + new files)
   
   # After VACUUM:
   # numFiles: Same as after OPTIMIZE
   # sizeInBytes: Reduced (old files deleted)
   ```

3. **Q:** What is Z-Order and how does it improve query performance?
   **A:**
   Z-Order (Z-Ordering) is a data clustering technique that co-locates related data in the same set of files based on specified columns, enabling better data skipping during query execution.
   
   **How It Works:**
   
   **Without Z-Order:**
   ```
   Table: sales (1000 files, randomly distributed)
   
   Query: SELECT * FROM sales WHERE region = 'West' AND product = 'Widget'
   
   File distribution:
   file1.parquet: region=[East, West, North], product=[Widget, Gadget, Tool]
   file2.parquet: region=[South, West, East], product=[Widget, Gadget, Tool]
   ...
   file1000.parquet: region=[All mixed], product=[All mixed]
   
   Files scanned: 1000 (ALL files - no effective skipping)
   Data read: 100 GB
   Query time: 45 seconds
   ```
   
   **With Z-Order BY (region, product):**
   ```
   Table: sales (1000 files, Z-Ordered)
   
   Query: SELECT * FROM sales WHERE region = 'West' AND product = 'Widget'
   
   File distribution (after Z-Order):
   file1.parquet: region=[West], product=[Widget]        ← Read
   file2.parquet: region=[West], product=[Widget]        ← Read
   file3.parquet: region=[West], product=[Gadget]        ← Skip
   file4.parquet: region=[East], product=[Widget]        ← Skip
   file5.parquet: region=[East], product=[Gadget]        ← Skip
   ...
   
   Files scanned: 50 (Only files with West + Widget)
   Data read: 5 GB
   Query time: 3 seconds (15x faster!)
   ```
   
   **Data Skipping Mechanism:**
   - Delta Lake maintains min/max statistics for each column in each file
   - Query engine checks statistics before reading files
   - Skips files where min/max range doesn't match filter predicate
   - Z-Order maximizes data skipping effectiveness
   
   **Syntax:**
   ```sql
   -- Basic Z-Order
   OPTIMIZE sales
   ZORDER BY (region, product_category);
   
   -- Z-Order specific partition
   OPTIMIZE sales
   WHERE date >= '2026-04-01'
   ZORDER BY (customer_id);
   
   -- Z-Order with V-Order
   OPTIMIZE sales
   ZORDER BY (region, product_category)
   VORDER;
   ```
   
   **Column Selection Strategy:**
   
   **✅ GOOD Columns for Z-Order:**
   - Frequently filtered in WHERE clauses
   - Low to medium cardinality (10-10000 unique values)
   - Commonly used together in queries
   - Examples: date, region, category, status, customer_tier
   
   **❌ BAD Columns for Z-Order:**
   - High cardinality (unique values: transaction_id, email)
   - Never used in filters (description, comments)
   - Continuous values rarely used in exact match (amount, price)
   
   **Examples:**
   ```sql
   -- ✅ GOOD: Low cardinality, frequently filtered
   OPTIMIZE sales ZORDER BY (region, product_category, status)
   -- region: 5 values (North, South, East, West, Central)
   -- product_category: 20 values
   -- status: 4 values (Pending, Shipped, Delivered, Cancelled)
   
   -- ❌ BAD: High cardinality, unique values
   OPTIMIZE sales ZORDER BY (order_id, customer_email)
   -- order_id: 1M unique values (every row different!)
   -- customer_email: 100K unique values
   -- No clustering benefit
   
   -- ❌ BAD: Too many columns
   OPTIMIZE sales ZORDER BY (col1, col2, col3, col4, col5, col6)
   -- Effectiveness decreases with each additional column
   ```
   
   **Best Practices:**
   - **2-4 columns maximum:** Effectiveness decreases with more columns
   - **Order matters:** Most selective filter first
   - **Reorder periodically:** As query patterns change, reorder
   - **Combine with V-Order:** Get both clustering + read optimization
   
   **Query Pattern Analysis:**
   ```sql
   -- Analyze common query patterns:
   
   -- Pattern 1: Region + Date filters (80% of queries)
   SELECT * FROM sales WHERE region = 'West' AND order_date = '2026-04-25'
   → Z-Order BY (region, order_date)
   
   -- Pattern 2: Customer lookups (15% of queries)
   SELECT * FROM sales WHERE customer_id = 12345
   → Consider: Z-Order BY (customer_id, region)
   
   -- Pattern 3: Product category analysis (5% of queries)
   SELECT * FROM sales WHERE product_category = 'Electronics'
   → Already covered by region, order_date ordering
   ```
   
   **Performance Impact:**
   - Query time: 5-50x faster (highly dependent on selectivity)
   - Files scanned: Reduced by 80-95% for selective queries
   - OPTIMIZE time: Longer than basic OPTIMIZE (full table rewrite)
   - Storage: No additional storage (same data, different organization)
   
   **Z-Order vs. Partitioning:**
   
   | Technique | When to Use | Drawbacks |
   |-----------|-------------|-----------|
   | **Partitioning** | Very high cardinality date/time columns | Creates many directories, metadata overhead |
   | **Z-Order** | Medium cardinality, multiple filter columns | Requires periodic OPTIMIZE, full table scan for OPTIMIZE |
   | **Both** | Partition by date, Z-Order within partitions | Best performance, more complexity |
   
   **Example: Combining Partitioning + Z-Order:**
   ```python
   # Create partitioned table
   df.write \
     .format("delta") \
     .partitionBy("year", "month") \
     .saveAsTable("sales")
   
   # Z-Order within each partition
   spark.sql("""
     OPTIMIZE sales
     WHERE year = 2026 AND month = 4
     ZORDER BY (region, product_category)
   """)
   ```

4. **Q:** Explain the "small file problem" in data lakes and how Fabric addresses it.
   **A:**
   The small file problem occurs when a Delta Lake table accumulates thousands or millions of small files, leading to severe performance degradation and operational inefficiencies.
   
   **The Problem:**
   
   **Root Causes:**
   - Streaming ingestion: Each micro-batch creates new files
   - Frequent appends: Many small batch writes
   - High partition count: Each partition has separate files
   - UPDATE/DELETE operations: Create new versions of small file portions
   
   **Example Scenario:**
   ```
   Streaming pipeline ingesting IoT data:
   - Frequency: Every 10 seconds
   - Data per batch: 100 KB
   - Duration: 24 hours
   
   Result:
   - Files created per day: 8,640 files (24 * 60 * 6)
   - After 30 days: 259,200 files
   - Average file size: 100 KB (extremely small!)
   
   Optimal file size: 128 MB
   Actual file size: 100 KB (1,280x smaller than optimal!)
   ```
   
   **Performance Impact:**
   
   ```
   Query: SELECT AVG(temperature) FROM iot_sensors WHERE device_id = 'sensor123'
   
   With Small Files (259,200 files @ 100 KB each):
   - Files to list: 259,200 files
   - Metadata operations: 259,200 stat calls
   - Files to open: 259,200 file handles
   - Read overhead: Massive (each file has Parquet header/footer overhead)
   - Query time: 5 minutes
   - Executor memory pressure: High (too many file handles)
   
   With Optimized Files (200 files @ 128 MB each):
   - Files to list: 200 files
   - Metadata operations: 200 stat calls
   - Files to open: 200 file handles (1,296x fewer!)
   - Read overhead: Minimal
   - Query time: 5 seconds (60x faster!)
   - Executor memory: Normal
   ```
   
   **Specific Problems:**
   1. **Slow query planning:** Listing thousands of files takes time
   2. **Memory pressure:** Each file handle consumes memory
   3. **Poor parallelism:** Small files don't distribute well across executors
   4. **Inefficient I/O:** File header/footer overhead dominates small files
   5. **Poor compression:** Small files have worse compression ratios
   6. **Metadata bloat:** Delta log grows large tracking many files
   
   **Fabric Solutions:**
   
   **1. Optimize Write (Automatic Prevention):**
   ```python
   # Enabled by default in Fabric Runtime
   spark.conf.get("spark.microsoft.delta.optimizeWrite.enabled")
   # Returns: true
   
   # How it works:
   # - Automatically combines small files during write
   # - Target file size: 128 MB (configurable)
   # - No code changes needed
   
   # Before Optimize Write:
   df.write.format("delta").mode("append").saveAsTable("iot_data")
   # Creates: 1 file @ 100 KB
   
   # With Optimize Write (default):
   df.write.format("delta").mode("append").saveAsTable("iot_data")
   # Buffers data until reaching 128 MB, then writes
   # Creates: 1 file @ 128 MB (after multiple appends)
   ```
   
   **2. OPTIMIZE Command (Reactive Cleanup):**
   ```sql
   -- Check file count before OPTIMIZE
   DESCRIBE DETAIL iot_data;
   -- numFiles: 259,200 (small files)
   
   -- Run OPTIMIZE
   OPTIMIZE iot_data;
   
   -- Check file count after OPTIMIZE
   DESCRIBE DETAIL iot_data;
   -- numFiles: 200 (consolidated)
   
   -- Example output:
   -- metrics.numFilesAdded: 200
   -- metrics.numFilesRemoved: 259,200
   -- metrics.filesAdded.avg: 134217728 (128 MB)
   -- metrics.filesRemoved.avg: 102400 (100 KB)
   ```
   
   **3. Auto Optimize (Table Property):**
   ```sql
   -- Enable Auto Optimize on table (preview feature)
   ALTER TABLE iot_data
   SET TBLPROPERTIES(
     'delta.autoOptimize.optimizeWrite' = 'true',
     'delta.autoOptimize.autoCompact' = 'true'
   );
   
   -- Now writes automatically:
   -- 1. Use Optimize Write during write
   -- 2. Run OPTIMIZE in background after write
   ```
   
   **Complete Mitigation Strategy:**
   
   ```python
   # Strategy for Streaming Workload:
   
   # 1. Enable Optimize Write (default in Fabric)
   spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
   
   # 2. Configure appropriate file size
   spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "134217728")  # 128 MB
   
   # 3. Write streaming data
   stream_df.writeStream \
     .format("delta") \
     .outputMode("append") \
     .option("checkpointLocation", "/checkpoints/iot") \
     .table("iot_data")
   
   # 4. Schedule periodic OPTIMIZE (run hourly)
   def hourly_compaction():
       spark.sql("""
           OPTIMIZE iot_data
           WHERE ingestion_time >= current_timestamp() - INTERVAL 1 HOUR
       """)
   
   # 5. Monthly VACUUM to reclaim storage
   def monthly_vacuum():
       spark.sql("VACUUM iot_data RETAIN 168 HOURS")
   ```
   
   **Monitoring File Health:**
   
   ```python
   # Check table health
   def check_table_health(table_name):
       details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
       
       num_files = details['numFiles']
       size_bytes = details['sizeInBytes']
       avg_file_size_mb = (size_bytes / num_files) / (1024**2)
       
       print(f"Table: {table_name}")
       print(f"Files: {num_files:,}")
       print(f"Total size: {size_bytes / (1024**3):.2f} GB")
       print(f"Average file size: {avg_file_size_mb:.2f} MB")
       
       # Health assessment
       if avg_file_size_mb < 10:
           print("⚠️ WARNING: Very small files detected! Run OPTIMIZE.")
       elif avg_file_size_mb < 50:
           print("⚠️ CAUTION: Small files detected. Consider OPTIMIZE.")
       elif avg_file_size_mb < 200:
           print("✅ HEALTHY: File sizes are optimal.")
       else:
           print("ℹ️ INFO: Large files (may want more parallelism).")
   
   check_table_health("iot_data")
   
   # Output example:
   # Table: iot_data
   # Files: 259,200
   # Total size: 24.73 GB
   # Average file size: 0.10 MB
   # ⚠️ WARNING: Very small files detected! Run OPTIMIZE.
   ```
   
   **File Size Guidelines:**
   
   | File Size | Assessment | Action |
   |-----------|------------|--------|
   | **< 1 MB** | ⚠️ Critical | Run OPTIMIZE immediately |
   | **1-10 MB** | ⚠️ Warning | Schedule OPTIMIZE |
   | **10-50 MB** | ⚠️ Suboptimal | Consider OPTIMIZE if query perf poor |
   | **50-200 MB** | ✅ Optimal | No action needed |
   | **200 MB-1 GB** | ℹ️ Large | Acceptable for very large tables |
   | **> 1 GB** | ⚠️ Too large | May reduce parallelism |
   
   **Performance Comparison:**
   
   ```
   Real-world example: 100 GB table
   
   Small File Scenario:
   - Files: 1,000,000 @ 100 KB each
   - Query planning time: 2 minutes
   - Query execution time: 8 minutes
   - Total query time: 10 minutes
   - Executor failures: Common (OOM)
   
   After OPTIMIZE:
   - Files: 800 @ 128 MB each
   - Query planning time: 2 seconds
   - Query execution time: 30 seconds
   - Total query time: 32 seconds
   - Executor failures: None
   
   Improvement: 18.75x faster!
   ```
   
   **Key Takeaways:**
   - Small files are a common problem in streaming and frequent append scenarios
   - Fabric's Optimize Write prevents small files automatically (enabled by default)
   - OPTIMIZE command reactively fixes existing small file problems
   - Target file size: 50-200 MB (128 MB default)
   - Monitor file count and average size regularly
   - Combine Optimize Write + periodic OPTIMIZE for best results

5. **Q:** How do you decide between using V-Order, Z-Order, or both for a Delta table?
   **A:**
   V-Order and Z-Order optimize different aspects of query performance and can be used together for maximum benefit. The decision depends on workload characteristics and query patterns.
   
   **V-Order vs. Z-Order Comparison:**
   
   | Aspect | V-Order | Z-Order |
   |--------|---------|---------|
   | **What it optimizes** | Parquet file internal layout | Data distribution across files |
   | **Performance benefit** | Read speed across ALL queries | Read speed for FILTERED queries |
   | **Optimization scope** | File-level (encoding, compression, row groups) | Table-level (data clustering) |
   | **Query type** | Benefits all reads equally | Benefits queries with WHERE clauses |
   | **Write overhead** | ~15% slower writes | High (one-time OPTIMIZE cost) |
   | **When applied** | During write OR during OPTIMIZE | During OPTIMIZE only |
   | **Column dependency** | Column-agnostic | Depends on specified columns |
   | **Maintenance** | Set once, applies to future writes | Rerun OPTIMIZE when data changes |
   | **Compatibility** | Works with all Fabric engines | Works with all Fabric engines |
   
   **Decision Matrix:**
   
   **Use V-Order Only:**
   ```
   Scenario: Dashboards with varied, unpredictable queries
   
   Query patterns:
   - Users run different queries daily
   - No consistent WHERE clause columns
   - Full table scans common
   - Power BI with dynamic slicers
   
   Example queries:
   SELECT AVG(revenue) FROM sales GROUP BY product
   SELECT COUNT(*) FROM sales WHERE amount > 1000
   SELECT * FROM sales ORDER BY date DESC LIMIT 100
   
   Decision: V-Order ✅ / Z-Order ❌
   Reason: No consistent filter columns, need broad read optimization
   
   Configuration:
   ALTER TABLE sales SET TBLPROPERTIES("delta.parquet.vorder.enabled" = "true")
   OPTIMIZE sales VORDER;
   ```
   
   **Use Z-Order Only:**
   ```
   Scenario: Operational queries with consistent filters
   
   Query patterns:
   - 90% of queries filter on customer_id and order_date
   - High selectivity (queries return 0.1% of data)
   - Write-heavy workload (streaming ingestion)
   
   Example queries:
   SELECT * FROM orders WHERE customer_id = 12345
   SELECT * FROM orders WHERE order_date = '2026-04-25' AND status = 'Pending'
   
   Decision: V-Order ❌ / Z-Order ✅
   Reason: Consistent filter columns, write speed important
   
   Configuration:
   spark.conf.set("spark.sql.parquet.vorder.default", "false")
   OPTIMIZE orders ZORDER BY (customer_id, order_date, status);
   ```
   
   **Use Both V-Order AND Z-Order:**
   ```
   Scenario: Enterprise analytics with Power BI reporting
   
   Query patterns:
   - Mixed workload: filtered queries + full scans
   - Power BI dashboard (needs fast reads)
   - Common filters on region, product, date
   - Data refreshed daily (acceptable write overhead)
   
   Example queries:
   SELECT * FROM sales WHERE region = 'West' AND date = '2026-04-25'  (70%)
   SELECT SUM(revenue) FROM sales GROUP BY product                     (20%)
   SELECT * FROM sales WHERE customer_tier = 'Premium'                 (10%)
   
   Decision: V-Order ✅ / Z-Order ✅
   Reason: Get data skipping (Z-Order) + fast reads (V-Order)
   
   Configuration:
   ALTER TABLE sales SET TBLPROPERTIES("delta.parquet.vorder.enabled" = "true")
   OPTIMIZE sales ZORDER BY (region, date, product) VORDER;
   ```
   
   **Decision Framework:**
   
   ```python
   def optimization_recommendation(table_stats):
       """
       Recommend V-Order and/or Z-Order based on workload characteristics
       """
       read_write_ratio = table_stats['reads_per_day'] / table_stats['writes_per_day']
       filter_query_pct = table_stats['queries_with_filters'] / table_stats['total_queries']
       top_filter_columns_pct = table_stats['top3_filter_columns_usage']
       
       recommendations = []
       
       # V-Order decision
       if read_write_ratio > 10:  # Read-heavy
           recommendations.append("✅ ENABLE V-Order")
           recommendations.append("   Reason: Read-heavy workload (10:1 ratio)")
       elif read_write_ratio > 5:  # Moderately read-heavy
           recommendations.append("⚠️ CONSIDER V-Order")
           recommendations.append("   Reason: Moderate read-heavy workload (5:1 ratio)")
       else:  # Write-heavy
           recommendations.append("❌ SKIP V-Order")
           recommendations.append("   Reason: Write-heavy workload (prioritize ingestion speed)")
       
       # Z-Order decision
       if filter_query_pct > 0.7 and top_filter_columns_pct > 0.6:
           recommendations.append("✅ ENABLE Z-Order")
           recommendations.append(f"   Reason: {filter_query_pct*100:.0f}% queries use filters")
           recommendations.append(f"   Columns: Top 3 filters used in {top_filter_columns_pct*100:.0f}% of queries")
       elif filter_query_pct > 0.5:
           recommendations.append("⚠️ CONSIDER Z-Order")
           recommendations.append(f"   Reason: {filter_query_pct*100:.0f}% queries use filters")
       else:
           recommendations.append("❌ SKIP Z-Order")
           recommendations.append("   Reason: Few queries use consistent filters")
       
       return "\n".join(recommendations)
   
   # Example usage:
   table_stats = {
       'reads_per_day': 10000,
       'writes_per_day': 100,
       'total_queries': 10000,
       'queries_with_filters': 8000,
       'top3_filter_columns_usage': 0.75  # Top 3 columns used in 75% of filtered queries
   }
   
   print(optimization_recommendation(table_stats))
   
   # Output:
   # ✅ ENABLE V-Order
   #    Reason: Read-heavy workload (100:1 ratio)
   # ✅ ENABLE Z-Order
   #    Reason: 80% queries use filters
   #    Columns: Top 3 filters used in 75% of queries
   ```
   
   **Implementation Examples:**
   
   **1. V-Order Only (Dashboard/BI):**
   ```python
   # Enable V-Order for all future writes
   spark.sql("""
       ALTER TABLE analytics_dashboard
       SET TBLPROPERTIES("delta.parquet.vorder.enabled" = "true")
   """)
   
   # Apply V-Order to existing data
   spark.sql("OPTIMIZE analytics_dashboard VORDER")
   ```
   
   **2. Z-Order Only (Operational Queries):**
   ```python
   # Disable V-Order to prioritize write speed
   spark.conf.set("spark.sql.parquet.vorder.default", "false")
   
   # Analyze query patterns to identify top filter columns
   # Assume analysis shows: customer_id (80%), order_date (75%), status (60%)
   
   # Apply Z-Order on top 3 filtered columns
   spark.sql("""
       OPTIMIZE operational_orders
       ZORDER BY (customer_id, order_date, status)
   """)
   
   # Rerun monthly as data grows
   ```
   
   **3. Both V-Order + Z-Order (Enterprise Analytics):**
   ```python
   # Phase 1: Enable V-Order for future writes
   spark.sql("""
       ALTER TABLE enterprise_sales
       SET TBLPROPERTIES("delta.parquet.vorder.enabled" = "true")
   """)
   
   # Phase 2: Run OPTIMIZE with both Z-Order and V-Order
   spark.sql("""
       OPTIMIZE enterprise_sales
       ZORDER BY (region, product_category, date)
       VORDER
   """)
   
   # Schedule weekly re-optimization
   def weekly_optimization():
       spark.sql("""
           OPTIMIZE enterprise_sales
           WHERE date >= current_date() - INTERVAL 7 DAYS
           ZORDER BY (region, product_category, date)
           VORDER
       """)
   ```
   
   **4. Partitioned Table with Z-Order + V-Order:**
   ```python
   # Create partitioned table (by year, month)
   df.write \
       .format("delta") \
       .partitionBy("year", "month") \
       .option("parquet.vorder.enabled", "true") \
       .saveAsTable("sales_partitioned")
   
   # Z-Order within each partition
   spark.sql("""
       OPTIMIZE sales_partitioned
       WHERE year = 2026 AND month = 4
       ZORDER BY (region, product_id)
       VORDER
   """)
   ```
   
   **Performance Measurement:**
   
   ```python
   # Benchmark to measure optimization impact
   import time
   
   def benchmark_query(query, label):
       start = time.time()
       result = spark.sql(query).collect()
       duration = time.time() - start
       print(f"{label}: {duration:.2f} seconds")
       return duration
   
   # Test query
   test_query = """
       SELECT AVG(revenue), COUNT(*)
       FROM sales
       WHERE region = 'West' AND date >= '2026-01-01'
   """
   
   # Before optimization
   time_before = benchmark_query(test_query, "Before optimization")
   
   # Run Z-Order only
   spark.sql("OPTIMIZE sales ZORDER BY (region, date)")
   time_zorder = benchmark_query(test_query, "After Z-Order")
   
   # Run Z-Order + V-Order
   spark.sql("OPTIMIZE sales ZORDER BY (region, date) VORDER")
   time_both = benchmark_query(test_query, "After Z-Order + V-Order")
   
   # Results example:
   # Before optimization: 45.23 seconds
   # After Z-Order: 8.15 seconds (5.5x faster)
   # After Z-Order + V-Order: 3.42 seconds (13.2x faster)
   
   print(f"\nZ-Order improvement: {(time_before/time_zorder):.1f}x")
   print(f"Z-Order + V-Order improvement: {(time_before/time_both):.1f}x")
   print(f"V-Order additional benefit: {(time_zorder/time_both):.1f}x")
   ```
   
   **Cost Considerations:**
   
   ```python
   # Cost analysis for OPTIMIZE operations
   
   # Example table: 500 GB, 100K files
   
   # Scenario 1: OPTIMIZE only (no V-Order, no Z-Order)
   # - Compute time: 10 minutes
   # - Compute cost: $2
   # - Storage: 500 GB (same)
   
   # Scenario 2: OPTIMIZE + Z-Order
   # - Compute time: 30 minutes (full table rewrite)
   # - Compute cost: $6
   # - Storage: 500 GB (same)
   
   # Scenario 3: OPTIMIZE + V-Order
   # - Compute time: 15 minutes (V-Order encoding overhead)
   # - Compute cost: $3
   # - Storage: 400 GB (20% compression improvement)
   
   # Scenario 4: OPTIMIZE + Z-Order + V-Order
   # - Compute time: 35 minutes (both optimizations)
   # - Compute cost: $7
   # - Storage: 400 GB (20% compression improvement)
   
   # ROI calculation:
   # If this table supports 1000 queries/day:
   # - Query time reduction: 10x (from 45s to 4.5s per query)
   # - Compute savings: 1000 * (45-4.5) * $0.10/min = $67.50/day
   # - Monthly savings: $2,025
   # - Optimization cost: $7
   # - ROI: 289x (pays back in < 3 hours!)
   ```
   
   **Quick Reference:**
   
   | Workload Characteristic | Recommendation |
   |-------------------------|----------------|
   | **BI dashboards, many users** | V-Order + Z-Order |
   | **Power BI Direct Lake** | V-Order + Z-Order |
   | **Operational queries, consistent filters** | Z-Order only |
   | **Streaming ingestion, read-heavy** | V-Order only (after ingestion) |
   | **Streaming ingestion, write-heavy** | Neither (prioritize writes) |
   | **Ad-hoc analytics, no pattern** | V-Order only |
   | **ML training, full table scans** | V-Order only |
   | **Archive tables, rare reads** | V-Order (one-time) |
   
   **Key Decision Factors:**
   1. **Read/Write Ratio:** > 10:1 → V-Order beneficial
   2. **Filter Consistency:** > 70% queries use same columns → Z-Order beneficial
   3. **Query Selectivity:** High selectivity → Z-Order very beneficial
   4. **Workload Mix:** Mixed → Use both
   5. **Write Latency Tolerance:** Can tolerate 15% slower writes → V-Order OK

6. **Q:** When and how often should you run VACUUM on Delta tables?
   **A:**
   VACUUM removes old, unreferenced files from Delta tables to reclaim storage space. The timing and frequency depend on several factors including storage costs, time travel requirements, table update frequency, and operational risk tolerance.
   
   **VACUUM Timing Considerations:**
   
   **1. Retention Period Selection:**
   ```
   Default: 7 days (168 hours)
   - Balances storage costs vs. time travel capability
   - Allows rolling back up to 1 week
   - Safe for most concurrent readers
   
   Short Retention (1-3 days):
   - Use for: High-volume, low-value temporary tables
   - Risk: Limited time travel, concurrent reader failures
   - Requires: Safety check override
   
   Long Retention (30-90 days):
   - Use for: Critical tables, compliance requirements
   - Risk: Higher storage costs
   - Benefit: Extended time travel, safer for long-running queries
   ```
   
   **2. Frequency Guidelines:**
   
   | Table Update Frequency | VACUUM Frequency | Retention | Rationale |
   |------------------------|------------------|-----------|-----------|
   | **Streaming (continuous)** | Weekly | 7 days | Frequent file creation, regular cleanup needed |
   | **Daily batch** | Weekly to Monthly | 7-14 days | Moderate file accumulation |
   | **Weekly batch** | Monthly | 14-30 days | Less frequent cleanup needed |
   | **Monthly batch** | Quarterly | 30-90 days | Minimal cleanup needed |
   | **One-time load** | After OPTIMIZE, then never | N/A | Static table |
   | **Heavy UPDATE/DELETE** | Weekly | 7 days | Generates many obsolete files |
   
   **3. Decision Framework:**
   
   ```python
   def calculate_vacuum_schedule(table_characteristics):
       """
       Determine optimal VACUUM frequency and retention
       """
       update_freq = table_characteristics['updates_per_day']
       storage_cost_sensitivity = table_characteristics['storage_cost_priority']  # 1-10
       time_travel_need = table_characteristics['time_travel_days_needed']
       table_size_gb = table_characteristics['size_gb']
       
       # Calculate expected obsolete file accumulation rate
       if update_freq > 100:  # High frequency
           files_per_day = update_freq * 5  # Rough estimate
           vacuum_frequency = "Weekly"
       elif update_freq > 10:  # Medium frequency
           files_per_day = update_freq * 2
           vacuum_frequency = "Bi-weekly"
       else:  # Low frequency
           files_per_day = update_freq
           vacuum_frequency = "Monthly"
       
       # Adjust retention based on time travel need
       if time_travel_need <= 1:
           retention_days = 1
           warning = "⚠️ Very short retention - consider business impact"
       elif time_travel_need <= 7:
           retention_days = 7
           warning = ""
       elif time_travel_need <= 30:
           retention_days = 30
           warning = ""
       else:
           retention_days = time_travel_need
           warning = "ℹ️ Long retention will increase storage costs"
       
       # Estimate storage savings
       estimated_obsolete_files = files_per_day * retention_days
       estimated_savings_gb = (estimated_obsolete_files * table_size_gb) / 1000
       
       return {
           'vacuum_frequency': vacuum_frequency,
           'retention_days': retention_days,
           'estimated_savings_gb': estimated_savings_gb,
           'warning': warning
       }
   
   # Example usage:
   table_chars = {
       'updates_per_day': 50,
       'storage_cost_priority': 7,
       'time_travel_days_needed': 7,
       'size_gb': 500
   }
   
   schedule = calculate_vacuum_schedule(table_chars)
   print(f"Recommended VACUUM frequency: {schedule['vacuum_frequency']}")
   print(f"Recommended retention: {schedule['retention_days']} days")
   print(f"Estimated storage savings: {schedule['estimated_savings_gb']:.2f} GB")
   if schedule['warning']:
       print(schedule['warning'])
   
   # Output:
   # Recommended VACUUM frequency: Bi-weekly
   # Recommended retention: 7 days
   # Estimated storage savings: 175.00 GB
   ```
   
   **4. Implementation Patterns:**
   
   **Pattern 1: Simple Scheduled VACUUM**
   ```python
   # Run weekly via scheduled notebook/pipeline
   
   def weekly_vacuum():
       tables = ['sales', 'customers', 'orders', 'products']
       
       for table in tables:
           print(f"Vacuuming {table}...")
           result = spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
           print(f"  Completed: {table}")
   
   weekly_vacuum()
   ```
   
   **Pattern 2: Conditional VACUUM (Check Before Running)**
   ```python
   # Only VACUUM if significant obsolete files accumulated
   
   def smart_vacuum(table_name, retention_hours=168, threshold_gb=10):
       """
       VACUUM only if estimated savings > threshold
       """
       # Get table details
       details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
       current_size_gb = details['sizeInBytes'] / (1024**3)
       
       # Dry run to see what would be deleted
       dry_run = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
       files_to_delete = dry_run.count()
       
       if files_to_delete == 0:
           print(f"{table_name}: No files to vacuum")
           return
       
       # Estimate savings (rough approximation)
       avg_file_size_gb = current_size_gb / details['numFiles']
       estimated_savings_gb = files_to_delete * avg_file_size_gb
       
       if estimated_savings_gb > threshold_gb:
           print(f"{table_name}: Vacuuming (estimated savings: {estimated_savings_gb:.2f} GB)")
           spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
       else:
           print(f"{table_name}: Skipping (savings {estimated_savings_gb:.2f} GB < threshold {threshold_gb} GB)")
   
   # Run for all tables
   for table in tables:
       smart_vacuum(table, retention_hours=168, threshold_gb=10)
   ```
   
   **Pattern 3: Tiered Retention Strategy**
   ```python
   # Different retention for different table tiers
   
   def tiered_vacuum():
       # Critical tables: Long retention
       critical_tables = ['financial_transactions', 'customer_pii']
       for table in critical_tables:
           spark.sql(f"VACUUM {table} RETAIN 720 HOURS")  # 30 days
       
       # Standard tables: Default retention
       standard_tables = ['sales', 'orders', 'products']
       for table in standard_tables:
           spark.sql(f"VACUUM {table} RETAIN 168 HOURS")  # 7 days
       
       # Temporary tables: Short retention
       temp_tables = ['staging_temp', 'scratch_data']
       spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
       for table in temp_tables:
           spark.sql(f"VACUUM {table} RETAIN 24 HOURS")  # 1 day
       spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
   
   tiered_vacuum()
   ```
   
   **Pattern 4: VACUUM After OPTIMIZE**
   ```python
   # Coordinated OPTIMIZE + VACUUM workflow
   
   def optimize_and_vacuum(table_name):
       # Step 1: Run OPTIMIZE (creates new optimized files)
       print(f"Optimizing {table_name}...")
       spark.sql(f"OPTIMIZE {table_name}")
       
       # Step 2: Wait for retention period (7 days in production)
       # In this example, we'll VACUUM immediately for demo purposes
       # In production, VACUUM runs 7+ days after OPTIMIZE
       
       print(f"Vacuuming {table_name}...")
       spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
       
       print(f"Maintenance complete for {table_name}")
   
   optimize_and_vacuum("sales")
   ```
   
   **5. Monitoring VACUUM Impact:**
   
   ```python
   def vacuum_with_monitoring(table_name, retention_hours=168):
       """
       Run VACUUM with before/after metrics
       """
       # Before VACUUM
       before = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
       before_size_gb = before['sizeInBytes'] / (1024**3)
       before_files = before['numFiles']
       
       # Dry run to estimate
       dry_run = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
       files_to_delete = dry_run.count()
       
       print(f"=== VACUUM: {table_name} ===")
       print(f"Before:")
       print(f"  Size: {before_size_gb:.2f} GB")
       print(f"  Files: {before_files:,}")
       print(f"  Files to delete: {files_to_delete:,}")
       
       # Run VACUUM
       import time
       start_time = time.time()
       spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
       duration = time.time() - start_time
       
       # After VACUUM
       after = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
       after_size_gb = after['sizeInBytes'] / (1024**3)
       after_files = after['numFiles']
       
       savings_gb = before_size_gb - after_size_gb
       savings_pct = (savings_gb / before_size_gb) * 100 if before_size_gb > 0 else 0
       
       print(f"After:")
       print(f"  Size: {after_size_gb:.2f} GB")
       print(f"  Files: {after_files:,}")
       print(f"  Savings: {savings_gb:.2f} GB ({savings_pct:.1f}%)")
       print(f"  Duration: {duration:.1f} seconds")
       
       return {
           'savings_gb': savings_gb,
           'savings_pct': savings_pct,
           'files_deleted': files_to_delete,
           'duration_seconds': duration
       }
   
   # Example output:
   # === VACUUM: sales ===
   # Before:
   #   Size: 500.00 GB
   #   Files: 50,000
   #   Files to delete: 35,000
   # After:
   #   Size: 350.00 GB
   #   Files: 15,000
   #   Savings: 150.00 GB (30.0%)
   #   Duration: 120.5 seconds
   ```
   
   **6. Common Mistakes to Avoid:**
   
   ```python
   # ❌ MISTAKE 1: VACUUM too frequently
   # Running VACUUM daily when table updates weekly
   spark.sql("VACUUM low_update_table RETAIN 168 HOURS")  # Daily (wasteful!)
   
   # ✅ CORRECT: Match frequency to update pattern
   # Run VACUUM monthly for tables updated weekly
   spark.sql("VACUUM low_update_table RETAIN 168 HOURS")  # Monthly
   
   # ❌ MISTAKE 2: Very short retention without consideration
   spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
   spark.sql("VACUUM critical_table RETAIN 1 HOUR")  # Dangerous!
   # Risk: Breaks concurrent readers, eliminates time travel
   
   # ✅ CORRECT: Use appropriate retention
   spark.sql("VACUUM critical_table RETAIN 168 HOURS")  # Safe: 7 days
   
   # ❌ MISTAKE 3: VACUUM immediately after OPTIMIZE
   spark.sql("OPTIMIZE sales")
   spark.sql("VACUUM sales RETAIN 0 HOURS")  # Deletes old files immediately!
   # Risk: Breaks time travel, concurrent readers may fail
   
   # ✅ CORRECT: Wait for retention period
   spark.sql("OPTIMIZE sales")
   # Wait 7+ days...
   spark.sql("VACUUM sales RETAIN 168 HOURS")  # Safe
   
   # ❌ MISTAKE 4: Never running VACUUM
   # Table grows indefinitely, wasting storage
   
   # ✅ CORRECT: Regular VACUUM schedule
   # Weekly or monthly depending on update frequency
   ```
   
   **7. VACUUM in Data Factory Pipeline:**
   
   ```json
   {
     "name": "WeeklyTableMaintenance",
     "properties": {
       "activities": [
         {
           "name": "OptimizeTables",
           "type": "ForEach",
           "typeProperties": {
             "items": ["sales", "customers", "orders"],
             "activities": [
               {
                 "name": "OptimizeTable",
                 "type": "Notebook",
                 "typeProperties": {
                   "notebookPath": "/Maintenance/Optimize",
                   "parameters": {
                     "tableName": "@item()"
                   }
                 }
               }
             ]
           }
         },
         {
           "name": "Wait7Days",
           "type": "Wait",
           "dependsOn": [
             {"activity": "OptimizeTables", "dependencyConditions": ["Succeeded"]}
           ],
           "typeProperties": {
             "waitTimeInSeconds": 604800
           }
         },
         {
           "name": "VacuumTables",
           "type": "ForEach",
           "dependsOn": [
             {"activity": "Wait7Days", "dependencyConditions": ["Succeeded"]}
           ],
           "typeProperties": {
             "items": ["sales", "customers", "orders"],
             "activities": [
               {
                 "name": "VacuumTable",
                 "type": "Notebook",
                 "typeProperties": {
                   "notebookPath": "/Maintenance/Vacuum",
                   "parameters": {
                     "tableName": "@item()",
                     "retentionHours": 168
                   }
                 }
               }
             ]
           }
         }
       ]
     }
   }
   ```
   
   **8. Cost-Benefit Analysis:**
   
   ```
   Example: 1 TB table with daily updates
   
   Without VACUUM:
   - Storage growth: 50 GB/day (updates create new files)
   - Monthly storage: 1000 GB + (50 * 30) = 2,500 GB
   - Monthly cost: 2,500 GB * $0.023/GB = $57.50
   
   With Weekly VACUUM (7-day retention):
   - Storage maintained: 1000 GB + (50 * 7) = 1,350 GB
   - Monthly cost: 1,350 GB * $0.023/GB = $31.05
   - Compute cost: 4 VACUUM runs * $2 = $8
   - Total cost: $39.05
   
   Monthly savings: $57.50 - $39.05 = $18.45
   Annual savings: $18.45 * 12 = $221.40
   
   ROI: (221.40 - 96) / 96 = 130% annual return
   ```
   
   **Quick Reference:**
   
   | Table Type | VACUUM Frequency | Retention | Notes |
   |------------|------------------|-----------|-------|
   | **Streaming (real-time)** | Weekly | 7 days | High file creation rate |
   | **Batch (daily)** | Bi-weekly | 7-14 days | Moderate cleanup needed |
   | **Reporting (read-heavy)** | Monthly | 14-30 days | Less frequent updates |
   | **Archive (rarely updated)** | Quarterly or never | 90+ days | Minimal cleanup needed |
   | **Critical/Compliance** | Monthly | 30-90 days | Regulatory requirements |
   | **Temporary/Staging** | Daily | 1-3 days | Short-term data only |
   
   **Key Takeaways:**
   - VACUUM frequency should match table update frequency
   - Default 7-day retention balances storage cost vs. capabilities
   - Always run VACUUM well after OPTIMIZE (not immediately)
   - Use DRY RUN to preview impact before running
   - Monitor storage savings to validate VACUUM schedule
   - Consider time travel requirements when setting retention
   - Coordinate VACUUM with OPTIMIZE in maintenance workflows
   - Different tables may need different retention policies

7. **Q:** What is the recommended maintenance workflow for Delta tables in Fabric?
   **A:**
   A comprehensive Delta table maintenance workflow ensures optimal performance, manageable storage costs, and healthy table state over time. The workflow should combine write-time optimization, periodic compaction, and storage cleanup.
   
   **Complete Maintenance Workflow:**
   
   ```
   Phase 1: SETUP (One-time configuration)
   ├─ 1. Determine workload pattern (read-heavy vs write-heavy)
   ├─ 2. Enable/disable V-Order based on pattern
   ├─ 3. Verify Optimize Write is enabled (default)
   └─ 4. Plan maintenance schedule
   
   Phase 2: ONGOING WRITES (Continuous)
   ├─ 1. Data ingestion (Optimize Write prevents small files)
   ├─ 2. MERGE operations (Low Shuffle Merge optimizes automatically)
   └─ 3. Monitor file count and sizes
   
   Phase 3: PERIODIC OPTIMIZATION (Daily/Weekly)
   ├─ 1. Run OPTIMIZE (with Z-Order if applicable)
   ├─ 2. Apply V-Order during OPTIMIZE (if enabled)
   ├─ 3. Monitor optimization metrics
   └─ 4. Track query performance improvements
   
   Phase 4: STORAGE CLEANUP (Weekly/Monthly)
   ├─ 1. Wait for retention period to expire
   ├─ 2. Run VACUUM to remove obsolete files
   ├─ 3. Monitor storage savings
   └─ 4. Verify time travel still works as expected
   
   Phase 5: MONITORING (Continuous)
   ├─ 1. Track table health metrics
   ├─ 2. Monitor query performance
   ├─ 3. Review storage trends
   └─ 4. Adjust maintenance schedule as needed
   ```
   
   **Detailed Implementation:**
   
   **1. Initial Setup:**
   
   ```python
   # Step 1: Analyze workload pattern
   def analyze_table_workload(table_name):
       """
       Determine if table is read-heavy or write-heavy
       """
       # Query Fabric monitoring APIs or logs to get:
       # - Read operations per day
       # - Write operations per day
       # - Common query patterns
       
       # Example metrics:
       reads_per_day = 5000
       writes_per_day = 50
       read_write_ratio = reads_per_day / writes_per_day
       
       # Common filter columns from query logs
       top_filter_columns = analyze_query_patterns(table_name)
       # Returns: ['region', 'date', 'product_id']
       
       return {
           'read_write_ratio': read_write_ratio,
           'recommendation': 'read-heavy' if read_write_ratio > 10 else 'write-heavy',
           'top_filter_columns': top_filter_columns
       }
   
   # Step 2: Configure table based on workload
   def configure_table_optimization(table_name, workload_analysis):
       """
       Apply optimal configuration based on workload
       """
       if workload_analysis['recommendation'] == 'read-heavy':
           # Enable V-Order for read-heavy tables
           spark.sql(f"""
               ALTER TABLE {table_name}
               SET TBLPROPERTIES("delta.parquet.vorder.enabled" = "true")
           """)
           print(f"{table_name}: V-Order ENABLED (read-heavy workload)")
       else:
           # Keep V-Order disabled for write-heavy tables
           print(f"{table_name}: V-Order DISABLED (write-heavy workload)")
       
       # Optimize Write is enabled by default
       print(f"{table_name}: Optimize Write already enabled by default")
       
       # Store filter columns for future Z-Order operations
       filter_cols = workload_analysis['top_filter_columns'][:3]  # Max 3 columns
       print(f"{table_name}: Z-Order columns identified: {filter_cols}")
       
       return filter_cols
   
   # Run initial setup
   workload = analyze_table_workload("sales")
   zorder_cols = configure_table_optimization("sales", workload)
   ```
   
   **2. Daily Ingestion with Optimization:**
   
   ```python
   # Daily ETL with built-in optimization
   
   def daily_etl_with_optimization():
       """
       Daily data load with automatic optimization
       """
       # Step 1: Extract and transform data
       df_today = extract_daily_data()  # Your ETL logic
       df_transformed = transform_data(df_today)
       
       # Step 2: Load data (Optimize Write handles small files automatically)
       df_transformed.write \
           .format("delta") \
           .mode("append") \
           .saveAsTable("sales")
       
       print(f"Loaded {df_transformed.count():,} rows")
       
       # Step 3: Run OPTIMIZE on today's partition only (efficient)
       # This is cheap since only today's data is optimized
       spark.sql("""
           OPTIMIZE sales
           WHERE load_date = current_date()
           ZORDER BY (region, product_id, customer_tier)
           VORDER
       """)
       
       print("Daily partition optimized")
       
       # Step 4: Log metrics
       log_table_metrics("sales")
   
   def log_table_metrics(table_name):
       """
       Track table health over time
       """
       details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
       
       metrics = {
           'timestamp': datetime.now(),
           'table': table_name,
           'num_files': details['numFiles'],
           'size_gb': details['sizeInBytes'] / (1024**3),
           'avg_file_size_mb': (details['sizeInBytes'] / details['numFiles']) / (1024**2)
       }
       
       # Write to monitoring table
       spark.createDataFrame([metrics]).write \
           .format("delta") \
           .mode("append") \
           .saveAsTable("table_health_metrics")
   
   # Schedule to run daily
   daily_etl_with_optimization()
   ```
   
   **3. Weekly Full Table OPTIMIZE:**
   
   ```python
   # Weekly comprehensive optimization
   
   def weekly_table_optimization():
       """
       Full table OPTIMIZE - run weekly on Sunday night
       """
       tables_to_optimize = [
           {'name': 'sales', 'zorder_cols': ['region', 'product_id', 'date']},
           {'name': 'customers', 'zorder_cols': ['customer_tier', 'region']},
           {'name': 'orders', 'zorder_cols': ['order_status', 'order_date']}
       ]
       
       for table in tables_to_optimize:
           print(f"\n=== Optimizing {table['name']} ===")
           
           # Get before metrics
           before = spark.sql(f"DESCRIBE DETAIL {table['name']}").collect()[0]
           print(f"Before: {before['numFiles']:,} files, {before['sizeInBytes']/(1024**3):.2f} GB")
           
           # Run OPTIMIZE with Z-Order and V-Order
           zorder_clause = f"ZORDER BY ({', '.join(table['zorder_cols'])})"
           spark.sql(f"""
               OPTIMIZE {table['name']}
               {zorder_clause}
               VORDER
           """)
           
           # Get after metrics
           after = spark.sql(f"DESCRIBE DETAIL {table['name']}").collect()[0]
           print(f"After: {after['numFiles']:,} files, {after['sizeInBytes']/(1024**3):.2f} GB")
           
           reduction_pct = ((before['numFiles'] - after['numFiles']) / before['numFiles']) * 100
           print(f"File reduction: {reduction_pct:.1f}%")
   
   # Schedule to run weekly (Sundays at 2 AM)
   weekly_table_optimization()
   ```
   
   **4. Monthly VACUUM:**
   
   ```python
   # Monthly storage cleanup
   
   def monthly_vacuum():
       """
       VACUUM all tables - run monthly (1st of month)
       """
       # Define tables with retention policies
       tables_config = [
           {'name': 'sales', 'retention_days': 7, 'tier': 'standard'},
           {'name': 'customers', 'retention_days': 30, 'tier': 'critical'},
           {'name': 'orders', 'retention_days': 14, 'tier': 'standard'},
           {'name': 'staging_temp', 'retention_days': 1, 'tier': 'temporary'}
       ]
       
       total_savings_gb = 0
       
       for table_config in tables_config:
           table_name = table_config['name']
           retention_hours = table_config['retention_days'] * 24
           
           print(f"\n=== Vacuuming {table_name} ({table_config['tier']}) ===")
           
           # Get before size
           before = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
           before_size_gb = before['sizeInBytes'] / (1024**3)
           
           # Dry run first
           dry_run = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
           files_to_delete = dry_run.count()
           print(f"Files to delete: {files_to_delete:,}")
           
           if files_to_delete == 0:
               print(f"No files to vacuum")
               continue
           
           # Override safety check for short retention
           if retention_hours < 168:
               spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
           
           # Run VACUUM
           spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
           
           # Re-enable safety check
           spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
           
           # Get after size
           after = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
           after_size_gb = after['sizeInBytes'] / (1024**3)
           
           savings_gb = before_size_gb - after_size_gb
           total_savings_gb += savings_gb
           
           print(f"Storage saved: {savings_gb:.2f} GB")
       
       print(f"\n=== Total storage saved: {total_savings_gb:.2f} GB ===")
   
   # Schedule to run monthly (1st of month)
   monthly_vacuum()
   ```
   
   **5. Continuous Monitoring:**
   
   ```python
   # Dashboard monitoring
   
   def generate_table_health_report():
       """
       Generate health report for all Delta tables
       """
       tables = ['sales', 'customers', 'orders', 'products']
       
       health_data = []
       
       for table in tables:
           details = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
           
           num_files = details['numFiles']
           size_gb = details['sizeInBytes'] / (1024**3)
           avg_file_size_mb = (details['sizeInBytes'] / num_files) / (1024**2)
           
           # Determine health status
           if avg_file_size_mb < 10:
               health_status = "🔴 Critical - Run OPTIMIZE immediately"
           elif avg_file_size_mb < 50:
               health_status = "🟡 Warning - Schedule OPTIMIZE"
           else:
               health_status = "🟢 Healthy"
           
           health_data.append({
               'table': table,
               'files': num_files,
               'size_gb': round(size_gb, 2),
               'avg_file_mb': round(avg_file_size_mb, 2),
               'status': health_status
           })
       
       # Create DataFrame and display
       df_health = spark.createDataFrame(health_data)
       df_health.show(truncate=False)
       
       return df_health
   
   # Run daily to monitor table health
   generate_table_health_report()
   ```
   
   **6. Complete Automated Workflow (Pipeline):**
   
   ```python
   # Orchestrated maintenance pipeline
   
   class TableMaintenancePipeline:
       def __init__(self):
           self.tables = [
               {
                   'name': 'sales',
                   'zorder_cols': ['region', 'product_id', 'date'],
                   'vorder_enabled': True,
                   'vacuum_retention_days': 7
               },
               {
                   'name': 'customers',
                   'zorder_cols': ['customer_tier', 'region'],
                   'vorder_enabled': True,
                   'vacuum_retention_days': 30
               }
           ]
       
       def daily_maintenance(self):
           """
           Run after daily data load
           """
           print("=== DAILY MAINTENANCE ===")
           for table in self.tables:
               # Optimize only today's partition (fast)
               spark.sql(f"""
                   OPTIMIZE {table['name']}
                   WHERE load_date = current_date()
                   ZORDER BY ({', '.join(table['zorder_cols'])})
                   {'VORDER' if table['vorder_enabled'] else ''}
               """)
               print(f"✓ {table['name']}: Today's partition optimized")
       
       def weekly_maintenance(self):
           """
           Run Sunday nights
           """
           print("\n=== WEEKLY MAINTENANCE ===")
           for table in self.tables:
               # Full table OPTIMIZE
               spark.sql(f"""
                   OPTIMIZE {table['name']}
                   ZORDER BY ({', '.join(table['zorder_cols'])})
                   {'VORDER' if table['vorder_enabled'] else ''}
               """)
               print(f"✓ {table['name']}: Full table optimized")
       
       def monthly_maintenance(self):
           """
           Run 1st of month
           """
           print("\n=== MONTHLY MAINTENANCE ===")
           for table in self.tables:
               retention_hours = table['vacuum_retention_days'] * 24
               
               # VACUUM
               spark.sql(f"VACUUM {table['name']} RETAIN {retention_hours} HOURS")
               print(f"✓ {table['name']}: Vacuumed (retention: {table['vacuum_retention_days']} days)")
       
       def health_check(self):
           """
           Run daily
           """
           print("\n=== HEALTH CHECK ===")
           for table in self.tables:
               details = spark.sql(f"DESCRIBE DETAIL {table['name']}").collect()[0]
               avg_file_mb = (details['sizeInBytes'] / details['numFiles']) / (1024**2)
               
               status = "🟢" if avg_file_mb > 50 else "🟡" if avg_file_mb > 10 else "🔴"
               print(f"{status} {table['name']}: {details['numFiles']:,} files, avg {avg_file_mb:.1f} MB")
   
   # Usage:
   pipeline = TableMaintenancePipeline()
   
   # Daily (after ETL):
   pipeline.daily_maintenance()
   pipeline.health_check()
   
   # Weekly (Sundays):
   # pipeline.weekly_maintenance()
   
   # Monthly (1st of month):
   # pipeline.monthly_maintenance()
   ```
   
   **7. Fabric Data Factory Integration:**
   
   ```json
   {
     "name": "ComprehensiveTableMaintenance",
     "properties": {
       "activities": [
         {
           "name": "DailyETL",
           "type": "Notebook",
           "typeProperties": {
             "notebookPath": "/ETL/DailyLoad"
           }
         },
         {
           "name": "DailyOptimize",
           "type": "Notebook",
           "dependsOn": [
             {"activity": "DailyETL", "dependencyConditions": ["Succeeded"]}
           ],
           "typeProperties": {
             "notebookPath": "/Maintenance/DailyOptimize"
           }
         },
         {
           "name": "CheckIfSunday",
           "type": "IfCondition",
           "dependsOn": [
             {"activity": "DailyOptimize", "dependencyConditions": ["Succeeded"]}
           ],
           "typeProperties": {
             "expression": {
               "value": "@equals(dayOfWeek(utcNow()), 0)",
               "type": "Expression"
             },
             "ifTrueActivities": [
               {
                 "name": "WeeklyFullOptimize",
                 "type": "Notebook",
                 "typeProperties": {
                   "notebookPath": "/Maintenance/WeeklyOptimize"
                 }
               }
             ]
           }
         },
         {
           "name": "CheckIfFirstOfMonth",
           "type": "IfCondition",
           "dependsOn": [
             {"activity": "CheckIfSunday", "dependencyConditions": ["Completed"]}
           ],
           "typeProperties": {
             "expression": {
               "value": "@equals(dayOfMonth(utcNow()), 1)",
               "type": "Expression"
             },
             "ifTrueActivities": [
               {
                 "name": "MonthlyVacuum",
                 "type": "Notebook",
                 "typeProperties": {
                   "notebookPath": "/Maintenance/MonthlyVacuum"
                 }
               }
             ]
           }
         },
         {
           "name": "HealthCheck",
           "type": "Notebook",
           "dependsOn": [
             {"activity": "CheckIfFirstOfMonth", "dependencyConditions": ["Completed"]}
           ],
           "typeProperties": {
             "notebookPath": "/Monitoring/HealthCheck"
           }
         }
       ],
       "triggers": [
         {
           "name": "DailySchedule",
           "properties": {
             "type": "ScheduleTrigger",
             "typeProperties": {
               "recurrence": {
                 "frequency": "Day",
                 "interval": 1,
                 "startTime": "2026-01-01T02:00:00Z",
                 "timeZone": "UTC"
               }
             }
           }
         }
       ]
     }
   }
   ```
   
   **Maintenance Schedule Summary:**
   
   | Frequency | Activity | Purpose | Duration | Impact |
   |-----------|----------|---------|----------|--------|
   | **Continuous** | Optimize Write | Prevent small files | N/A (automatic) | Minimal write overhead |
   | **Continuous** | Low Shuffle Merge | Optimize MERGE ops | N/A (automatic) | Reduced shuffle cost |
   | **Daily** | OPTIMIZE (partition) | Compact today's data | 5-10 min | Fast queries |
   | **Daily** | Health Check | Monitor table state | 1 min | Visibility |
   | **Weekly** | OPTIMIZE (full table) | Full compaction + Z-Order | 30-60 min | Maximum query speed |
   | **Monthly** | VACUUM | Reclaim storage | 10-30 min | Reduced costs |
   | **Quarterly** | Review & Adjust | Update maintenance plan | 1 hour | Continuous improvement |
   
   **Key Takeaways:**
   - Maintenance should be proactive (scheduled) not reactive (when problems occur)
   - Combine automatic optimizations (Optimize Write) with periodic maintenance (OPTIMIZE, VACUUM)
   - Daily partition-level OPTIMIZE is cheap and effective
   - Weekly full-table OPTIMIZE with Z-Order for maximum performance
   - Monthly VACUUM to control storage costs
   - Continuous monitoring to catch issues early
   - Different tables may need different maintenance frequencies
   - Coordinate OPTIMIZE and VACUUM with appropriate timing
   - Automate everything via pipelines/notebooks for consistency

---

## 10. Fabric Data Warehouse

**Definition:**
Fabric Data Warehouse is a fully managed, enterprise-scale relational data warehouse built on a data lake foundation in Microsoft Fabric. It combines the power of traditional SQL-based data warehousing with the flexibility and scale of modern data lakes, all within the unified Fabric platform. Data is stored in Delta Lake format (Parquet files with transaction logs) in OneLake, enabling seamless collaboration between data engineers using Spark and business analysts using T-SQL.

**Purpose:**
- Provide enterprise-scale analytical database capabilities using T-SQL
- Support star schema, snowflake schema, and dimensional modeling patterns
- Enable business intelligence and reporting with Power BI integration
- Deliver high-performance query execution with autonomous workload management
- Offer ACID transactions for data consistency and reliability
- Simplify data warehouse creation with SaaS experience (no infrastructure management)
- Enable cross-database querying for unified analytics

---

**Key Characteristics:**

### 1. Architecture Foundation

**Storage:**
- **Delta Lake Format:** All data stored as Parquet files with Delta transaction logs
- **OneLake Integration:** Automatic replication to OneLake Files for external access
- **Separation of Storage and Compute:** Scale independently for cost optimization
- **Multi-format Support:** Parquet, Delta, optimized for analytical queries

**Compute:**
- **Distributed Query Processing:** Industry-leading MPP (Massively Parallel Processing) engine
- **Autonomous Workload Management:** No knobs to turn, automatic optimization
- **Instant Scaling:** Scale near-instantaneously to meet business demands
- **Shared SQL Engine:** Same engine as Lakehouse SQL analytics endpoint

**Data Organization:**
```
Fabric Workspace
├─ Warehouse (Item)
│   ├─ Tables
│   │   ├─ Fact tables (quantitative metrics)
│   │   ├─ Dimension tables (descriptive attributes)
│   │   └─ Staging/Integration tables (ETL processing)
│   ├─ Views (logical data abstractions)
│   ├─ Stored Procedures (business logic encapsulation)
│   ├─ Functions (reusable calculations)
│   └─ Schemas (logical grouping)
└─ OneLake Storage
    └─ Delta Parquet Files (physical storage)
```

---

### 2. T-SQL Development Experience

**Supported T-SQL Operations:**

**DDL (Data Definition Language):**
```sql
-- Create schemas for organization
CREATE SCHEMA Sales;
CREATE SCHEMA Finance;

-- Create tables
CREATE TABLE Sales.FactSales (
    SaleID INT NOT NULL,
    DateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    Quantity INT,
    Amount DECIMAL(18,2),
    PRIMARY KEY NONCLUSTERED (SaleID) NOT ENFORCED
);

-- Create table from query (CTAS)
CREATE TABLE Sales.TopCustomers AS
SELECT 
    CustomerKey,
    SUM(Amount) AS TotalRevenue,
    COUNT(*) AS OrderCount
FROM Sales.FactSales
GROUP BY CustomerKey
HAVING SUM(Amount) > 10000;

-- Create views
CREATE VIEW Sales.vw_SalesSummary AS
SELECT 
    d.Year,
    d.Month,
    p.ProductName,
    SUM(f.Quantity) AS TotalQuantity,
    SUM(f.Amount) AS TotalRevenue
FROM Sales.FactSales f
INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY d.Year, d.Month, p.ProductName;

-- Create stored procedures
CREATE PROCEDURE Sales.usp_LoadDailySales
    @LoadDate DATE
AS
BEGIN
    INSERT INTO Sales.FactSales (SaleID, DateKey, CustomerKey, ProductKey, Quantity, Amount)
    SELECT 
        SaleID,
        CONVERT(INT, FORMAT(@LoadDate, 'yyyyMMdd')),
        CustomerKey,
        ProductKey,
        Quantity,
        Amount
    FROM Staging.SalesImport
    WHERE ImportDate = @LoadDate;
END;

-- Create functions
CREATE FUNCTION Sales.fn_GetCustomerTier (@CustomerKey INT)
RETURNS VARCHAR(20)
AS
BEGIN
    DECLARE @Tier VARCHAR(20);
    
    SELECT @Tier = CASE
        WHEN SUM(Amount) > 100000 THEN 'Platinum'
        WHEN SUM(Amount) > 50000 THEN 'Gold'
        WHEN SUM(Amount) > 10000 THEN 'Silver'
        ELSE 'Bronze'
    END
    FROM Sales.FactSales
    WHERE CustomerKey = @CustomerKey;
    
    RETURN @Tier;
END;
```

**DML (Data Manipulation Language):**
```sql
-- INSERT
INSERT INTO Sales.DimCustomer (CustomerKey, CustomerName, Email, City, Country)
VALUES (1001, 'Acme Corp', 'contact@acme.com', 'Seattle', 'USA');

-- INSERT from SELECT
INSERT INTO Sales.FactSales (SaleID, DateKey, CustomerKey, ProductKey, Quantity, Amount)
SELECT SaleID, DateKey, CustomerKey, ProductKey, Quantity, Amount
FROM Staging.SalesImport
WHERE ProcessedFlag = 0;

-- UPDATE
UPDATE Sales.DimCustomer
SET City = 'Portland', State = 'OR'
WHERE CustomerKey = 1001;

-- DELETE
DELETE FROM Staging.SalesImport
WHERE ProcessedFlag = 1 AND ImportDate < DATEADD(DAY, -30, GETDATE());

-- MERGE (Upsert)
MERGE INTO Sales.DimProduct AS target
USING Staging.ProductUpdates AS source
ON target.ProductKey = source.ProductKey
WHEN MATCHED THEN
    UPDATE SET 
        ProductName = source.ProductName,
        Category = source.Category,
        Price = source.Price,
        ModifiedDate = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (ProductKey, ProductName, Category, Price, CreatedDate)
    VALUES (source.ProductKey, source.ProductName, source.Category, source.Price, GETDATE());

-- TRUNCATE
TRUNCATE TABLE Staging.SalesImport;
```

**DQL (Data Query Language):**
```sql
-- Complex analytical queries
SELECT 
    d.Year,
    d.QuarterName,
    c.Country,
    p.Category,
    SUM(f.Quantity) AS TotalQuantity,
    SUM(f.Amount) AS TotalRevenue,
    AVG(f.Amount) AS AvgOrderValue,
    COUNT(DISTINCT f.CustomerKey) AS UniqueCustomers
FROM Sales.FactSales f
INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
WHERE d.Year = 2026
GROUP BY d.Year, d.QuarterName, c.Country, p.Category
HAVING SUM(f.Amount) > 50000
ORDER BY TotalRevenue DESC;

-- Window functions
SELECT 
    CustomerKey,
    SaleDate,
    Amount,
    SUM(Amount) OVER (PARTITION BY CustomerKey ORDER BY SaleDate) AS RunningTotal,
    ROW_NUMBER() OVER (PARTITION BY CustomerKey ORDER BY Amount DESC) AS RankInCustomer,
    LAG(Amount, 1, 0) OVER (PARTITION BY CustomerKey ORDER BY SaleDate) AS PreviousSale
FROM Sales.FactSales
WHERE SaleDate >= '2026-01-01';

-- Common Table Expressions (CTEs)
WITH CustomerMetrics AS (
    SELECT 
        CustomerKey,
        COUNT(*) AS OrderCount,
        SUM(Amount) AS TotalRevenue,
        AVG(Amount) AS AvgOrderValue,
        MAX(SaleDate) AS LastOrderDate
    FROM Sales.FactSales
    GROUP BY CustomerKey
),
CustomerSegments AS (
    SELECT 
        *,
        CASE 
            WHEN TotalRevenue > 100000 THEN 'High Value'
            WHEN TotalRevenue > 10000 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS Segment
    FROM CustomerMetrics
)
SELECT 
    c.CustomerName,
    c.Country,
    cs.Segment,
    cs.OrderCount,
    cs.TotalRevenue,
    cs.AvgOrderValue,
    cs.LastOrderDate
FROM CustomerSegments cs
INNER JOIN Sales.DimCustomer c ON cs.CustomerKey = c.CustomerKey
WHERE cs.Segment = 'High Value'
ORDER BY cs.TotalRevenue DESC;
```

---

### 3. Table Types and Design

**Fact Tables (Measures):**
```sql
-- Sales fact table (transactional metrics)
CREATE TABLE Sales.FactSales (
    SaleID INT NOT NULL,
    DateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    StoreKey INT NOT NULL,
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    Discount DECIMAL(18,2),
    Tax DECIMAL(18,2),
    TotalAmount DECIMAL(18,2),
    PRIMARY KEY NONCLUSTERED (SaleID) NOT ENFORCED,
    FOREIGN KEY (DateKey) REFERENCES Sales.DimDate(DateKey) NOT ENFORCED,
    FOREIGN KEY (CustomerKey) REFERENCES Sales.DimCustomer(CustomerKey) NOT ENFORCED,
    FOREIGN KEY (ProductKey) REFERENCES Sales.DimProduct(ProductKey) NOT ENFORCED
);

-- Inventory snapshot fact table (periodic snapshot)
CREATE TABLE Inventory.FactInventorySnapshot (
    SnapshotDateKey INT NOT NULL,
    ProductKey INT NOT NULL,
    WarehouseKey INT NOT NULL,
    QuantityOnHand INT,
    QuantityOnOrder INT,
    ReorderPoint INT,
    UnitCost DECIMAL(18,2),
    PRIMARY KEY NONCLUSTERED (SnapshotDateKey, ProductKey, WarehouseKey) NOT ENFORCED
);

-- Event fact table (accumulating snapshot)
CREATE TABLE Logistics.FactOrder (
    OrderID INT NOT NULL,
    OrderDateKey INT NOT NULL,
    RequestedDateKey INT,
    ShipDateKey INT,
    DeliveryDateKey INT,
    CustomerKey INT NOT NULL,
    DaysBetweenOrderAndShip INT,
    DaysBetweenShipAndDelivery INT,
    OrderAmount DECIMAL(18,2),
    PRIMARY KEY NONCLUSTERED (OrderID) NOT ENFORCED
);
```

**Dimension Tables (Attributes):**
```sql
-- Customer dimension (SCD Type 1 - overwrite)
CREATE TABLE Sales.DimCustomer (
    CustomerKey INT NOT NULL,
    CustomerID VARCHAR(50) NOT NULL,
    CustomerName VARCHAR(200),
    Email VARCHAR(100),
    Phone VARCHAR(50),
    Address VARCHAR(200),
    City VARCHAR(100),
    State VARCHAR(50),
    Country VARCHAR(50),
    PostalCode VARCHAR(20),
    CustomerTier VARCHAR(20),
    CreatedDate DATETIME,
    ModifiedDate DATETIME,
    PRIMARY KEY NONCLUSTERED (CustomerKey) NOT ENFORCED,
    UNIQUE NONCLUSTERED (CustomerID) NOT ENFORCED
);

-- Product dimension (SCD Type 2 - historized)
CREATE TABLE Sales.DimProduct (
    ProductKey INT NOT NULL,  -- Surrogate key
    ProductID VARCHAR(50) NOT NULL,  -- Business key
    ProductName VARCHAR(200),
    Category VARCHAR(100),
    Subcategory VARCHAR(100),
    Brand VARCHAR(100),
    Color VARCHAR(50),
    Size VARCHAR(50),
    UnitPrice DECIMAL(18,2),
    Cost DECIMAL(18,2),
    IsActive BIT,
    EffectiveDate DATE,  -- SCD Type 2 fields
    ExpirationDate DATE,
    IsCurrent BIT,
    PRIMARY KEY NONCLUSTERED (ProductKey) NOT ENFORCED
);

-- Date dimension (pre-populated)
CREATE TABLE Sales.DimDate (
    DateKey INT NOT NULL,
    Date DATE NOT NULL,
    Year INT,
    Quarter INT,
    QuarterName VARCHAR(10),
    Month INT,
    MonthName VARCHAR(20),
    Week INT,
    DayOfMonth INT,
    DayOfWeek INT,
    DayName VARCHAR(20),
    IsWeekend BIT,
    IsHoliday BIT,
    HolidayName VARCHAR(100),
    FiscalYear INT,
    FiscalQuarter INT,
    PRIMARY KEY NONCLUSTERED (DateKey) NOT ENFORCED,
    UNIQUE NONCLUSTERED (Date) NOT ENFORCED
);
```

**Staging/Integration Tables:**
```sql
-- Staging table for raw imports
CREATE TABLE Staging.SalesImport (
    ImportID INT IDENTITY(1,1),
    ImportDate DATETIME DEFAULT GETDATE(),
    SourceSystem VARCHAR(50),
    SaleID VARCHAR(50),
    SaleDate DATE,
    CustomerID VARCHAR(50),
    ProductID VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    TotalAmount DECIMAL(18,2),
    ProcessedFlag BIT DEFAULT 0,
    ErrorMessage VARCHAR(500)
);

-- Integration table for deduplication
CREATE TABLE Integration.CustomerStaging (
    RowNum INT,
    CustomerID VARCHAR(50),
    CustomerName VARCHAR(200),
    Email VARCHAR(100),
    City VARCHAR(100),
    Country VARCHAR(50),
    IsDuplicate BIT,
    ProcessedFlag BIT
);
```

---

### 4. Data Loading Methods

**Method 1: COPY INTO (Bulk Load from Files):**

```sql
-- Load CSV from OneLake
COPY INTO Sales.FactSales
FROM 'https://onelake.dfs.fabric.microsoft.com/MyWorkspace/MyLakehouse/Files/Sales/*.csv'
WITH (
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    ENCODING = 'UTF8',
    DATEFORMAT = 'yyyy-MM-dd'
);

-- Load Parquet from external storage
COPY INTO Sales.FactSales (SaleID, DateKey, CustomerKey, ProductKey, Quantity, Amount)
FROM 'abfss://container@storage.dfs.core.windows.net/sales/2026/04/*.parquet'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
);
```

**Method 2: CTAS (Create Table As Select):**

```sql
-- Create aggregated table from fact table
CREATE TABLE Sales.MonthlySalesSummary AS
SELECT 
    d.Year,
    d.Month,
    c.Country,
    p.Category,
    SUM(f.Quantity) AS TotalQuantity,
    SUM(f.Amount) AS TotalRevenue,
    COUNT(DISTINCT f.CustomerKey) AS UniqueCustomers
FROM Sales.FactSales f
INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY d.Year, d.Month, c.Country, p.Category;
```

**Method 3: INSERT...SELECT:**

```sql
-- Load from staging to fact
INSERT INTO Sales.FactSales (SaleID, DateKey, CustomerKey, ProductKey, Quantity, Amount)
SELECT 
    s.SaleID,
    d.DateKey,
    c.CustomerKey,
    p.ProductKey,
    s.Quantity,
    s.TotalAmount
FROM Staging.SalesImport s
INNER JOIN Sales.DimDate d ON s.SaleDate = d.Date
INNER JOIN Sales.DimCustomer c ON s.CustomerID = c.CustomerID
INNER JOIN Sales.DimProduct p ON s.ProductID = p.ProductID AND p.IsCurrent = 1
WHERE s.ProcessedFlag = 0;

-- Mark as processed
UPDATE Staging.SalesImport
SET ProcessedFlag = 1
WHERE ProcessedFlag = 0;
```

**Method 4: SELECT INTO:**

```sql
-- Quick table creation for analysis
SELECT 
    CustomerKey,
    YEAR(SaleDate) AS Year,
    MONTH(SaleDate) AS Month,
    COUNT(*) AS OrderCount,
    SUM(Amount) AS Revenue
INTO Sales.CustomerMonthlyMetrics
FROM Sales.FactSales
WHERE SaleDate >= '2025-01-01'
GROUP BY CustomerKey, YEAR(SaleDate), MONTH(SaleDate);
```

**Method 5: Data Pipelines (Fabric Data Factory):**

```json
{
  "name": "LoadWarehouseTables",
  "type": "Copy",
  "inputs": [{
    "type": "DatasetReference",
    "referenceName": "LakehouseSalesData"
  }],
  "outputs": [{
    "type": "DatasetReference",
    "referenceName": "WarehouseFactSales"
  }],
  "typeProperties": {
    "source": {
      "type": "LakehouseTableSource"
    },
    "sink": {
      "type": "WarehouseTableSink",
      "preCopyScript": "TRUNCATE TABLE Sales.FactSales;",
      "tableOption": "autoCreate"
    },
    "enableStaging": false
  }
}
```

---

### 5. Temporary Tables (#temp)

**Non-Distributed Temp Tables (Default):**

```sql
-- Create non-distributed temp table (mdf-backed, faster for small datasets)
CREATE TABLE #CustomerAnalysis (
    CustomerKey INT,
    TotalRevenue DECIMAL(18,2),
    OrderCount INT,
    AvgOrderValue DECIMAL(18,2)
);

-- Populate temp table
INSERT INTO #CustomerAnalysis
SELECT 
    CustomerKey,
    SUM(Amount) AS TotalRevenue,
    COUNT(*) AS OrderCount,
    AVG(Amount) AS AvgOrderValue
FROM Sales.FactSales
WHERE SaleDate >= DATEADD(MONTH, -6, GETDATE())
GROUP BY CustomerKey;

-- Use temp table in complex query
SELECT 
    c.CustomerName,
    c.Country,
    t.TotalRevenue,
    t.OrderCount,
    t.AvgOrderValue,
    CASE 
        WHEN t.TotalRevenue > 100000 THEN 'High Value'
        WHEN t.TotalRevenue > 10000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS Segment
FROM #CustomerAnalysis t
INNER JOIN Sales.DimCustomer c ON t.CustomerKey = c.CustomerKey
WHERE t.OrderCount >= 5
ORDER BY t.TotalRevenue DESC;

-- Temp table automatically dropped at session end
```

**Distributed Temp Tables (Recommended for Large Data):**

```sql
-- Create distributed temp table (Parquet-backed, better for large datasets)
CREATE TABLE #LargeSalesAnalysis (
    ProductKey INT,
    CustomerKey INT,
    Year INT,
    Quarter INT,
    TotalQuantity INT,
    TotalRevenue DECIMAL(18,2)
) WITH (DISTRIBUTION = ROUND_ROBIN);

-- Load large dataset
INSERT INTO #LargeSalesAnalysis
SELECT 
    ProductKey,
    CustomerKey,
    d.Year,
    d.Quarter,
    SUM(Quantity) AS TotalQuantity,
    SUM(Amount) AS TotalRevenue
FROM Sales.FactSales f
INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
WHERE d.Year >= 2024
GROUP BY ProductKey, CustomerKey, d.Year, d.Quarter;

-- Query distributed temp table (leverages parallel processing)
SELECT 
    p.ProductName,
    t.Year,
    t.Quarter,
    SUM(t.TotalQuantity) AS Quantity,
    SUM(t.TotalRevenue) AS Revenue
FROM #LargeSalesAnalysis t
INNER JOIN Sales.DimProduct p ON t.ProductKey = p.ProductKey
GROUP BY p.ProductName, t.Year, t.Quarter
ORDER BY Revenue DESC;
```

**Temp Table Use Cases:**

```sql
-- Use Case 1: Multi-step ETL processing
CREATE TABLE #ValidatedSales (
    SaleID VARCHAR(50),
    ValidationStatus VARCHAR(50),
    ErrorMessage VARCHAR(500)
) WITH (DISTRIBUTION = ROUND_ROBIN);

-- Step 1: Validate
INSERT INTO #ValidatedSales
SELECT 
    SaleID,
    CASE 
        WHEN CustomerID IS NULL THEN 'Invalid - No Customer'
        WHEN ProductID IS NULL THEN 'Invalid - No Product'
        WHEN Quantity <= 0 THEN 'Invalid - Negative Quantity'
        WHEN TotalAmount <= 0 THEN 'Invalid - Negative Amount'
        ELSE 'Valid'
    END AS ValidationStatus,
    CASE 
        WHEN CustomerID IS NULL THEN 'Missing customer ID'
        WHEN ProductID IS NULL THEN 'Missing product ID'
        WHEN Quantity <= 0 THEN 'Quantity must be positive'
        WHEN TotalAmount <= 0 THEN 'Amount must be positive'
        ELSE NULL
    END AS ErrorMessage
FROM Staging.SalesImport
WHERE ProcessedFlag = 0;

-- Step 2: Load only valid records
INSERT INTO Sales.FactSales (SaleID, DateKey, CustomerKey, ProductKey, Quantity, Amount)
SELECT 
    s.SaleID,
    d.DateKey,
    c.CustomerKey,
    p.ProductKey,
    s.Quantity,
    s.TotalAmount
FROM Staging.SalesImport s
INNER JOIN #ValidatedSales v ON s.SaleID = v.SaleID
INNER JOIN Sales.DimDate d ON s.SaleDate = d.Date
INNER JOIN Sales.DimCustomer c ON s.CustomerID = c.CustomerID
INNER JOIN Sales.DimProduct p ON s.ProductID = p.ProductID
WHERE v.ValidationStatus = 'Valid';

-- Step 3: Log errors
INSERT INTO ErrorLog.DataQualityErrors (SaleID, ErrorMessage, ErrorDate)
SELECT SaleID, ErrorMessage, GETDATE()
FROM #ValidatedSales
WHERE ValidationStatus <> 'Valid';
```

---

### 6. Cross-Database Queries

**Query Across Multiple Warehouses:**

```sql
-- Query data from multiple warehouses in same workspace
SELECT 
    s.SaleID,
    s.Amount,
    c.CampaignName,
    i.CostPerClick
FROM SalesWarehouse.Sales.FactSales s
LEFT JOIN MarketingWarehouse.Campaigns.FactCampaigns c 
    ON s.CampaignKey = c.CampaignKey
LEFT JOIN FinanceWarehouse.Costs.FactMarketingCosts i 
    ON c.CampaignKey = i.CampaignKey
WHERE s.SaleDate >= '2026-01-01';
```

**Query Warehouse + Lakehouse:**

```sql
-- Combine warehouse structured data with lakehouse semi-structured data
SELECT 
    w.CustomerKey,
    w.CustomerName,
    w.TotalRevenue,
    l.WebVisits,
    l.PageViews,
    l.AvgSessionDuration
FROM SalesWarehouse.Sales.DimCustomer w
INNER JOIN MyLakehouse.WebAnalytics l 
    ON w.CustomerID = l.CustomerID
WHERE w.CustomerTier = 'Platinum';
```

**Query Multiple Lakehouses from Warehouse:**

```sql
-- Access data from multiple lakehouses
SELECT 
    p.ProductID,
    p.ProductName,
    s.QuantitySold,
    i.InventoryLevel,
    r.AverageRating,
    r.ReviewCount
FROM Warehouse.Sales.DimProduct p
LEFT JOIN SalesLakehouse.ProductSales s ON p.ProductID = s.ProductID
LEFT JOIN InventoryLakehouse.CurrentInventory i ON p.ProductID = i.ProductID
LEFT JOIN ReviewsLakehouse.ProductReviews r ON p.ProductID = r.ProductID;
```

---

### 7. Statistics and Performance

**Statistics Management:**

```sql
-- Create statistics manually
CREATE STATISTICS stat_Sales_CustomerKey 
ON Sales.FactSales (CustomerKey);

CREATE STATISTICS stat_Sales_DateKey 
ON Sales.FactSales (DateKey);

-- Create multi-column statistics
CREATE STATISTICS stat_Sales_Customer_Date 
ON Sales.FactSales (CustomerKey, DateKey);

-- Update statistics after significant data changes
UPDATE STATISTICS Sales.FactSales;

-- Update specific statistics
UPDATE STATISTICS Sales.FactSales stat_Sales_CustomerKey;

-- View statistics information
DBCC SHOW_STATISTICS('Sales.FactSales', stat_Sales_CustomerKey);
```

**Note:** Warehouse supports automatic creation of statistics, but manual updates are needed after major data loads.

---

### 8. SQL Analytics Endpoint (Read-Only)

Every Warehouse automatically includes a **SQL analytics endpoint** for read-only operations:

**Characteristics:**
- **Read-Only:** No INSERT, UPDATE, DELETE, TRUNCATE
- **T-SQL Support:** SELECT, CREATE VIEW, CREATE PROCEDURE, CREATE FUNCTION
- **Security:** Separate permissions from main warehouse
- **Use Case:** Provide reporting access without risk of data modification

**Example Usage:**

```sql
-- In SQL Analytics Endpoint (read-only)

-- Create view for reporting
CREATE VIEW Reports.vw_SalesPerformance AS
SELECT 
    d.Year,
    d.Month,
    c.Country,
    SUM(f.Amount) AS Revenue,
    COUNT(DISTINCT f.CustomerKey) AS Customers,
    AVG(f.Amount) AS AvgOrderValue
FROM Sales.FactSales f
INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
GROUP BY d.Year, d.Month, c.Country;

-- Create function for calculations
CREATE FUNCTION Reports.fn_GetYTDRevenue (@Year INT)
RETURNS DECIMAL(18,2)
AS
BEGIN
    DECLARE @Revenue DECIMAL(18,2);
    
    SELECT @Revenue = SUM(Amount)
    FROM Sales.FactSales f
    INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
    WHERE d.Year = @Year;
    
    RETURN @Revenue;
END;

-- Query in SQL analytics endpoint
SELECT * FROM Reports.vw_SalesPerformance
WHERE Year = 2026
ORDER BY Revenue DESC;

-- This would FAIL (read-only):
-- INSERT INTO Sales.FactSales VALUES (...);  -- Error!
```

---

### 9. Table Constraints

**Supported Constraints (NOT ENFORCED):**

```sql
-- Primary Key (NOT ENFORCED - for query optimizer only)
CREATE TABLE Sales.DimProduct (
    ProductKey INT NOT NULL,
    ProductID VARCHAR(50) NOT NULL,
    ProductName VARCHAR(200),
    PRIMARY KEY NONCLUSTERED (ProductKey) NOT ENFORCED
);

-- Unique constraint (NOT ENFORCED)
CREATE TABLE Sales.DimCustomer (
    CustomerKey INT NOT NULL,
    CustomerID VARCHAR(50) NOT NULL,
    CustomerName VARCHAR(200),
    Email VARCHAR(100),
    PRIMARY KEY NONCLUSTERED (CustomerKey) NOT ENFORCED,
    UNIQUE NONCLUSTERED (CustomerID) NOT ENFORCED,
    UNIQUE NONCLUSTERED (Email) NOT ENFORCED
);

-- Foreign Key (NOT ENFORCED)
CREATE TABLE Sales.FactSales (
    SaleID INT NOT NULL,
    DateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    Amount DECIMAL(18,2),
    PRIMARY KEY NONCLUSTERED (SaleID) NOT ENFORCED,
    FOREIGN KEY (DateKey) REFERENCES Sales.DimDate(DateKey) NOT ENFORCED,
    FOREIGN KEY (CustomerKey) REFERENCES Sales.DimCustomer(CustomerKey) NOT ENFORCED,
    FOREIGN KEY (ProductKey) REFERENCES Sales.DimProduct(ProductKey) NOT ENFORCED
);
```

**Why NOT ENFORCED?**
- Constraints are metadata-only (for query optimizer intelligence)
- Not enforced at runtime (application must ensure data quality)
- Improves query plans without enforcement overhead
- Similar to constraints in Synapse Dedicated SQL Pool

---

### 10. Collation

**Default Collation:**
- `Latin1_General_100_BIN2_UTF8` (case-sensitive, binary sort)
- Set at warehouse creation based on workspace collation
- **Cannot be changed** after warehouse is created

**Alternative Collation:**
- `Latin1_General_100_CI_AS_KS_WS_SC_UTF8` (case-insensitive)

**Setting Collation (REST API only):**

```http
POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/warehouses
Content-Type: application/json

{
  "displayName": "MyCIWarehouse",
  "collation": "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"
}
```

**Collation Impact:**

```sql
-- With case-sensitive collation (default):
SELECT * FROM Sales.DimCustomer WHERE CustomerName = 'ACME Corp';
-- Does NOT match 'Acme Corp' or 'acme corp'

-- With case-insensitive collation:
SELECT * FROM Sales.DimCustomer WHERE CustomerName = 'ACME Corp';
-- Matches 'ACME Corp', 'Acme Corp', 'acme corp', etc.
```

---

**Warehouse vs. Lakehouse Comparison:**

| Feature | Fabric Warehouse | Lakehouse (with SQL Endpoint) |
|---------|------------------|-------------------------------|
| **Primary Interface** | T-SQL | Spark (with T-SQL endpoint) |
| **Development Tool** | SQL queries, stored procs | Notebooks, Spark code |
| **DDL Operations** | Full (CREATE, ALTER, DROP) | Limited (SQL endpoint read-only) |
| **DML Operations** | Full (INSERT, UPDATE, DELETE, MERGE) | Via Spark (not SQL endpoint) |
| **Data Modification** | T-SQL | Spark (PySpark, Scala, R) |
| **Schema Enforcement** | Strong (explicit schemas) | Flexible (schema-on-read) |
| **Best For** | Structured, star schema, BI | Semi-structured, raw data, data science |
| **Target Users** | SQL developers, BI analysts | Data engineers, data scientists |
| **Transactional Support** | Full ACID | ACID (via Delta Lake) |
| **Query Performance** | Optimized for SQL workloads | Optimized for Spark workloads |
| **Modeling Pattern** | Dimensional (star/snowflake) | Flexible (any structure) |
| **Data Format** | Delta Lake (managed) | Delta Lake (direct access) |
| **Ingestion Methods** | COPY INTO, Pipelines, T-SQL | Spark writes, Pipelines |
| **Power BI Integration** | Direct Lake mode | Direct Lake mode |
| **Use Case** | Enterprise DW, reporting | Data engineering, ML, raw data |

---

**When to Choose Warehouse:**

✅ **Use Warehouse When:**
- You need full T-SQL DDL/DML capabilities
- Building dimensional models (star/snowflake schema)
- Team is primarily SQL-focused
- Need stored procedures and complex T-SQL logic
- Enterprise data warehouse migration
- Strong schema enforcement required
- BI and reporting is primary use case
- Need enterprise-scale performance with minimal tuning

❌ **Avoid Warehouse When:**
- Data is highly unstructured (JSON, XML, logs)
- Primary development tool is Spark/Python
- Need direct file-level access to data
- Exploratory data science is main use case
- Prototyping or lightweight analytics

---

**When to Choose Lakehouse:**

✅ **Use Lakehouse When:**
- Working with raw, unstructured, or semi-structured data
- Spark/Python is primary development tool
- Data engineering and ML workloads
- Need direct access to underlying files
- Schema flexibility is important
- Combining structured and unstructured data
- Exploratory data analysis
- Building data products with code

❌ **Avoid Lakehouse When:**
- Team only knows SQL
- Need complex stored procedures
- Require strict schema enforcement
- Enterprise DW patterns (star schema) are mandatory

---

**Combined Approach (Best Practice):**

Many organizations use both:

```
Lakehouse (Bronze/Silver):
- Raw data ingestion
- Data cleansing and transformation
- Exploratory analysis
- ML feature engineering

    ↓ (Spark transformations)

Warehouse (Gold):
- Curated dimensional models
- Star schema for BI
- Complex business logic (stored procs)
- Power BI reporting
- Self-service analytics
```

**Example Workflow:**

```python
# 1. Lakehouse: Ingest and clean raw data (Spark)
df_raw = spark.read.parquet("/lakehouse/bronze/sales_raw/")
df_cleaned = df_raw.dropDuplicates().filter("amount > 0")
df_cleaned.write.format("delta").mode("overwrite").saveAsTable("silver.sales_cleaned")

# 2. Warehouse: Load into dimensional model (T-SQL)
```

```sql
-- In Warehouse:
INSERT INTO Sales.FactSales (SaleID, DateKey, CustomerKey, ProductKey, Quantity, Amount)
SELECT 
    s.SaleID,
    d.DateKey,
    c.CustomerKey,
    p.ProductKey,
    s.Quantity,
    s.Amount
FROM SilverLakehouse.sales_cleaned s
INNER JOIN Sales.DimDate d ON s.SaleDate = d.Date
INNER JOIN Sales.DimCustomer c ON s.CustomerID = c.CustomerID
INNER JOIN Sales.DimProduct p ON s.ProductID = p.ProductID;

-- 3. Create BI views in Warehouse
CREATE VIEW Reports.vw_SalesByRegion AS
SELECT 
    d.Year,
    d.Month,
    c.Country,
    c.Region,
    SUM(f.Amount) AS Revenue
FROM Sales.FactSales f
INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
GROUP BY d.Year, d.Month, c.Country, c.Region;
```

```
-- 4. Power BI connects to Warehouse views (Direct Lake mode)
```

---

**Warehouse Limitations:**

Current limitations (as of April 2026):

- **Max 1,024 columns per table**
- **No computed columns**
- **No indexed views**
- **No partitioned tables** (automatic partitioning by system)
- **No user-defined types**
- **No triggers**
- **No external tables** (use cross-database queries instead)
- **No global temp tables** (only session-scoped #temp)
- **Constraints are NOT ENFORCED** (metadata only)
- **No ALTER on non-distributed temp tables**

**Workarounds:**

```sql
-- Computed columns → Use views
-- Instead of computed column:
CREATE VIEW Sales.vw_SalesWithMargin AS
SELECT 
    *,
    Amount - (Quantity * Cost) AS Margin,
    CASE 
        WHEN Amount > 1000 THEN 'Large'
        WHEN Amount > 100 THEN 'Medium'
        ELSE 'Small'
    END AS OrderSize
FROM Sales.FactSales;

-- Partitioned tables → Use WHERE clauses (optimizer auto-partitions)
-- Warehouse automatically optimizes based on query patterns

-- External tables → Use cross-database queries
-- Instead of external table:
SELECT * FROM MyLakehouse.ExternalData;
```

---

**Interview Questions:**

1. **Q:** What is Fabric Data Warehouse and how does it differ from traditional data warehouses?
   **A:**
   Fabric Data Warehouse is a fully managed, enterprise-scale relational data warehouse built on a modern data lake foundation (Delta Lake) within Microsoft Fabric. It combines traditional SQL-based data warehousing with modern cloud-native architecture.
   
   **Key Differences from Traditional Warehouses:**
   
   | Aspect | Fabric Warehouse | Traditional DW (e.g., SQL Server DW) |
   |--------|------------------|--------------------------------------|
   | **Deployment** | SaaS (fully managed) | Self-managed or IaaS |
   | **Storage** | Delta Lake (Parquet + transaction log) | Proprietary row/column store |
   | **Infrastructure** | No infrastructure management | Provision VMs, storage, networking |
   | **Scaling** | Instant, automatic | Manual, time-consuming |
   | **Storage/Compute** | Separated (scale independently) | Coupled (scale together) |
   | **Data Format** | Open (Parquet/Delta) | Proprietary |
   | **Integration** | Native Fabric ecosystem | External integrations needed |
   | **Pricing** | Capacity-based (Fabric CU) | Per-resource (DTU/vCore) |
   | **OneLake** | Automatic | Manual setup |
   | **Cross-DB Queries** | Native (zero-copy) | Linked servers (data copy) |
   | **Updates** | Automatic | Manual patching |
   
   **Architecture:**
   
   ```
   Traditional DW:
   ┌─────────────────────────────┐
   │   Coupled Storage+Compute   │
   │  ┌─────────────────────┐   │
   │  │  Proprietary Format │   │
   │  └─────────────────────┘   │
   │  Must scale together        │
   └─────────────────────────────┘
   
   Fabric Warehouse:
   ┌─────────────────────────────┐
   │    Compute (SQL Engine)     │  ← Scale independently
   └──────────────┬──────────────┘
                  │
   ┌──────────────▼──────────────┐
   │  Storage (Delta Lake)       │  ← Scale independently
   │  ┌─────────────────────┐   │
   │  │ Parquet + TX Log    │   │
   │  │ (Open Format)       │   │
   │  └─────────────────────┘   │
   │    OneLake Foundation       │
   └─────────────────────────────┘
   ```
   
   **Advantages:**
   - **Zero Infrastructure:** No VM provisioning, patching, or maintenance
   - **Instant Scale:** Add capacity in seconds, not hours
   - **Open Format:** Data readable by Spark, Python, external tools
   - **Cost Efficiency:** Pay for capacity, not idle infrastructure
   - **Integrated:** Native integration with Lakehouse, Notebooks, Power BI
   - **Autonomous:** Self-optimizing, no index tuning needed
   - **OneLake Native:** Automatic data synchronization
   
   **Use Cases:**
   - Enterprise data warehouse modernization
   - Star schema dimensional models for BI
   - SQL-based analytics and reporting
   - Power BI Direct Lake mode
   - Replacing Azure Synapse Dedicated SQL Pool
   - Migrating from on-premises SQL Server DW
   
   **When to Use:**
   - Team is SQL-focused (not Spark/Python)
   - Need full T-SQL DDL/DML capabilities
   - Building dimensional models (star/snowflake)
   - Enterprise-scale analytics with minimal administration
   - Want SaaS experience without infrastructure management

2. **Q:** Explain the difference between Warehouse tables and Lakehouse tables. How do you decide which to use?
   **A:**
   Warehouse tables and Lakehouse tables both use Delta Lake format but differ in access patterns, management, and use cases.
   
   **Technical Differences:**
   
   | Aspect | Warehouse Tables | Lakehouse Tables |
   |--------|------------------|------------------|
   | **Creation** | `CREATE TABLE` (T-SQL) | `spark.write.saveAsTable()` (Spark) |
   | **Modification** | T-SQL (INSERT, UPDATE, DELETE, MERGE) | Spark APIs or SQL endpoint (read-only) |
   | **Schema** | Explicit, enforced at creation | Flexible, schema-on-read |
   | **Primary Interface** | T-SQL | Spark (PySpark, Scala, SQL, R) |
   | **Stored Procedures** | Supported | Not supported (use notebooks) |
   | **Functions** | Supported (UDF, TVF) | Supported via Spark UDFs |
   | **Constraints** | Supported (NOT ENFORCED) | Not supported |
   | **Statistics** | Managed (auto + manual) | Delta statistics (auto) |
   | **Optimization** | Warehouse-managed | Manual (OPTIMIZE, VACUUM) |
   | **Temp Tables** | #temp tables | Temporary views |
   | **Direct File Access** | Not recommended | Native (read Parquet directly) |
   | **Best For** | Structured, governed data | Raw, semi-structured data |
   
   **Access Pattern Comparison:**
   
   **Warehouse Table:**
   ```sql
   -- Create in Warehouse (T-SQL)
   CREATE TABLE Sales.FactSales (
       SaleID INT NOT NULL,
       DateKey INT NOT NULL,
       CustomerKey INT NOT NULL,
       Amount DECIMAL(18,2),
       PRIMARY KEY NONCLUSTERED (SaleID) NOT ENFORCED
   );
   
   -- Modify in Warehouse (T-SQL)
   INSERT INTO Sales.FactSales VALUES (1001, 20260425, 5001, 1250.00);
   UPDATE Sales.FactSales SET Amount = 1300.00 WHERE SaleID = 1001;
   DELETE FROM Sales.FactSales WHERE SaleID = 1001;
   
   -- Query in Warehouse (T-SQL)
   SELECT * FROM Sales.FactSales WHERE CustomerKey = 5001;
   ```
   
   **Lakehouse Table:**
   ```python
   # Create in Lakehouse (Spark)
   df.write \
       .format("delta") \
       .mode("overwrite") \
       .option("mergeSchema", "true") \
       .saveAsTable("sales.fact_sales")
   
   # Modify in Lakehouse (Spark)
   from delta.tables import DeltaTable
   
   deltaTable = DeltaTable.forName(spark, "sales.fact_sales")
   deltaTable.update(
       condition = "SaleID = 1001",
       set = {"Amount": "1300.00"}
   )
   
   # Query in Lakehouse (Spark SQL or SQL endpoint - read-only)
   spark.sql("SELECT * FROM sales.fact_sales WHERE CustomerKey = 5001")
   
   # Direct file access (unique to Lakehouse)
   df_parquet = spark.read.parquet("/lakehouse/Tables/sales/fact_sales")
   ```
   
   **Decision Framework:**
   
   **Choose Warehouse Tables When:**
   
   ✅ **Schema & Governance:**
   - Need explicit schema enforcement
   - Require referential integrity (foreign keys) for query optimization
   - Want constraints for metadata/documentation
   
   ✅ **SQL-First Development:**
   - Team primarily uses SQL
   - Need complex stored procedures
   - Require T-SQL-specific features (MERGE, window functions)
   
   ✅ **BI & Reporting:**
   - Building star/snowflake schemas
   - Power BI Direct Lake consumption
   - Self-service analytics for business users
   
   ✅ **Enterprise Patterns:**
   - Dimensional modeling (fact/dimension tables)
   - Slowly Changing Dimensions (SCD Type 2)
   - Complex business logic in T-SQL
   
   **Example:**
   ```sql
   -- Warehouse: Star schema for sales analytics
   CREATE TABLE Sales.FactSales (...);
   CREATE TABLE Sales.DimCustomer (...);
   CREATE TABLE Sales.DimProduct (...);
   CREATE TABLE Sales.DimDate (...);
   
   CREATE VIEW Reports.vw_SalesPerformance AS
   SELECT 
       d.Year, d.Month,
       c.Country, c.Region,
       p.Category,
       SUM(f.Amount) AS Revenue
   FROM Sales.FactSales f
   JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   GROUP BY d.Year, d.Month, c.Country, c.Region, p.Category;
   ```
   
   **Choose Lakehouse Tables When:**
   
   ✅ **Data Engineering:**
   - Raw data ingestion and cleansing
   - Schema evolution needed (adding columns frequently)
   - Unstructured or semi-structured data (JSON, logs)
   
   ✅ **Spark Development:**
   - Team uses Python/Scala/R
   - Complex transformations with Spark
   - ML feature engineering
   
   ✅ **Flexibility:**
   - Exploratory data analysis
   - Rapid prototyping
   - Schema-on-read requirements
   
   ✅ **Direct File Access:**
   - External tools need Parquet access
   - Data lake consumption patterns
   - Integration with non-Fabric tools
   
   **Example:**
   ```python
   # Lakehouse: Bronze/Silver/Gold medallion architecture
   
   # Bronze: Raw ingestion
   df_raw = spark.read.json("/sources/raw_logs/*.json")
   df_raw.write.format("delta").mode("append").saveAsTable("bronze.raw_events")
   
   # Silver: Cleaned and validated
   df_silver = spark.sql("""
       SELECT 
           event_id,
           CAST(event_time AS TIMESTAMP) AS event_time,
           user_id,
           event_type,
           properties
       FROM bronze.raw_events
       WHERE event_id IS NOT NULL
   """)
   df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.clean_events")
   
   # Gold: Aggregated (could also be in Warehouse)
   df_gold = spark.sql("""
       SELECT 
           DATE(event_time) AS event_date,
           event_type,
           COUNT(*) AS event_count,
           COUNT(DISTINCT user_id) AS unique_users
       FROM silver.clean_events
       GROUP BY DATE(event_time), event_type
   """)
   df_gold.write.format("delta").mode("overwrite").saveAsTable("gold.event_summary")
   ```
   
   **Hybrid Approach (Recommended):**
   
   Most organizations use both:
   
   ```
   ┌─────────────────────────────────────────────┐
   │           LAKEHOUSE                         │
   │  ┌────────────────────────────────────┐    │
   │  │ Bronze (Raw)                       │    │
   │  │ - Raw files, JSON, logs            │    │
   │  │ - Schema-on-read                   │    │
   │  └────────────────────────────────────┘    │
   │              ↓ Spark transformations        │
   │  ┌────────────────────────────────────┐    │
   │  │ Silver (Cleaned)                   │    │
   │  │ - Validated, deduplicated          │    │
   │  │ - Schema enforced                  │    │
   │  └────────────────────────────────────┘    │
   └─────────────────────────────────────────────┘
                      ↓ T-SQL COPY or cross-DB query
   ┌─────────────────────────────────────────────┐
   │           WAREHOUSE                         │
   │  ┌────────────────────────────────────┐    │
   │  │ Gold (Dimensional)                 │    │
   │  │ - Star schema                      │    │
   │  │ - Fact/Dimension tables            │    │
   │  │ - Stored procedures                │    │
   │  │ - BI-optimized views               │    │
   │  └────────────────────────────────────┘    │
   │              ↓ Power BI (Direct Lake)       │
   └─────────────────────────────────────────────┘
                      ↓
              Business Users & Dashboards
   ```
   
   **Real-World Example:**
   ```python
   # Lakehouse: Process IoT sensor data
   df_sensors = spark.read \
       .format("delta") \
       .load("/lakehouse/bronze/sensor_telemetry")
   
   df_cleaned = df_sensors \
       .dropDuplicates(["sensor_id", "timestamp"]) \
       .filter("temperature IS NOT NULL") \
       .withColumn("processing_time", current_timestamp())
   
   df_cleaned.write \
       .format("delta") \
       .mode("append") \
       .saveAsTable("silver.sensor_data")
   ```
   
   ```sql
   -- Warehouse: Load into dimensional model
   INSERT INTO IoT.FactSensorReadings (ReadingID, DateKey, SensorKey, Temperature, Humidity)
   SELECT 
       ROW_NUMBER() OVER (ORDER BY timestamp),
       CONVERT(INT, FORMAT(timestamp, 'yyyyMMdd')),
       s.SensorKey,
       l.temperature,
       l.humidity
   FROM SensorLakehouse.silver.sensor_data l
   INNER JOIN IoT.DimSensor s ON l.sensor_id = s.sensor_id
   WHERE l.processing_time >= DATEADD(HOUR, -1, GETDATE());
   
   -- Create aggregated view for Power BI
   CREATE VIEW IoT.vw_HourlySensorSummary AS
   SELECT 
       d.Date,
       d.Hour,
       s.Location,
       s.SensorType,
       AVG(f.Temperature) AS AvgTemperature,
       MIN(f.Temperature) AS MinTemperature,
       MAX(f.Temperature) AS MaxTemperature,
       AVG(f.Humidity) AS AvgHumidity
   FROM IoT.FactSensorReadings f
   INNER JOIN IoT.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN IoT.DimSensor s ON f.SensorKey = s.SensorKey
   GROUP BY d.Date, d.Hour, s.Location, s.SensorType;
   ```
   
   **Key Takeaways:**
   - **Lakehouse:** Data engineering, raw/bronze/silver layers, Spark development
   - **Warehouse:** BI/reporting, gold layer, dimensional models, SQL development
   - **Both:** Lakehouse feeds Warehouse for complete analytics pipeline
   - **Cross-DB Queries:** Seamlessly query both from either location
   - **No Data Duplication:** OneLake ensures single copy, multiple access patterns

3. **Q:** How do you implement Slowly Changing Dimensions (SCD) in Fabric Data Warehouse?
   **A:**
   Slowly Changing Dimensions (SCD) track historical changes to dimension attributes over time. Fabric Warehouse supports SCD Type 1 (overwrite), Type 2 (historization), and Type 3 (limited history).
   
   **SCD Type 1 (Overwrite - No History):**
   
   Simply UPDATE the existing record, losing historical values.
   
   ```sql
   -- Type 1: Customer address changes (don't track history)
   CREATE TABLE Sales.DimCustomer_Type1 (
       CustomerKey INT NOT NULL,
       CustomerID VARCHAR(50) NOT NULL,
       CustomerName VARCHAR(200),
       Email VARCHAR(100),
       Address VARCHAR(200),
       City VARCHAR(100),
       State VARCHAR(50),
       ModifiedDate DATETIME,
       PRIMARY KEY NONCLUSTERED (CustomerKey) NOT ENFORCED
   );
   
   -- Update when address changes (overwrites old value)
   UPDATE Sales.DimCustomer_Type1
   SET 
       Address = '456 New St',
       City = 'Portland',
       State = 'OR',
       ModifiedDate = GETDATE()
   WHERE CustomerID = 'C1001';
   
   -- Result: Old address lost, only current address visible
   ```
   
   **When to use Type 1:**
   - Attribute corrections (typos, data quality fixes)
   - Non-critical attributes (phone number, address)
   - No need for historical analysis
   
   ---
   
   **SCD Type 2 (Full Historization):**
   
   Create new record for each change, maintaining complete history.
   
   ```sql
   -- Type 2: Product pricing changes (track full history)
   CREATE TABLE Sales.DimProduct_Type2 (
       ProductKey INT NOT NULL,              -- Surrogate key (auto-increment)
       ProductID VARCHAR(50) NOT NULL,       -- Business key (natural key)
       ProductName VARCHAR(200),
       Category VARCHAR(100),
       UnitPrice DECIMAL(18,2),
       Cost DECIMAL(18,2),
       -- SCD Type 2 tracking columns
       EffectiveDate DATE NOT NULL,          -- When this version became effective
       ExpirationDate DATE,                  -- When this version expired (NULL = current)
       IsCurrent BIT NOT NULL DEFAULT 1,     -- Flag for current record
       PRIMARY KEY NONCLUSTERED (ProductKey) NOT ENFORCED
   );
   
   -- Initial load
   INSERT INTO Sales.DimProduct_Type2 (ProductKey, ProductID, ProductName, Category, UnitPrice, Cost, EffectiveDate, ExpirationDate, IsCurrent)
   VALUES 
       (1, 'P1001', 'Widget A', 'Widgets', 19.99, 10.00, '2025-01-01', NULL, 1);
   
   -- When price changes (e.g., from $19.99 to $24.99 on 2026-04-01):
   
   -- Step 1: Expire the current record
   UPDATE Sales.DimProduct_Type2
   SET 
       ExpirationDate = '2026-03-31',  -- Day before new price effective
       IsCurrent = 0                    -- No longer current
   WHERE ProductID = 'P1001' AND IsCurrent = 1;
   
   -- Step 2: Insert new record with new price
   INSERT INTO Sales.DimProduct_Type2 (ProductKey, ProductID, ProductName, Category, UnitPrice, Cost, EffectiveDate, ExpirationDate, IsCurrent)
   VALUES 
       (2, 'P1001', 'Widget A', 'Widgets', 24.99, 10.00, '2026-04-01', NULL, 1);
   
   -- Result: Two records for same product
   -- ProductKey | ProductID | UnitPrice | EffectiveDate | ExpirationDate | IsCurrent
   -- -----------|-----------|-----------|---------------|----------------|----------
   --     1      |  P1001    |   19.99   |  2025-01-01   |  2026-03-31    |    0
   --     2      |  P1001    |   24.99   |  2026-04-01   |     NULL       |    1
   ```
   
   **SCD Type 2 Stored Procedure (Automated):**
   
   ```sql
   CREATE PROCEDURE Sales.usp_UpdateProduct_Type2
       @ProductID VARCHAR(50),
       @ProductName VARCHAR(200),
       @Category VARCHAR(100),
       @UnitPrice DECIMAL(18,2),
       @Cost DECIMAL(18,2),
       @EffectiveDate DATE
   AS
   BEGIN
       -- Check if product exists and values changed
       IF EXISTS (
           SELECT 1 
           FROM Sales.DimProduct_Type2
           WHERE ProductID = @ProductID 
             AND IsCurrent = 1
             AND (ProductName <> @ProductName 
                  OR Category <> @Category 
                  OR UnitPrice <> @UnitPrice 
                  OR Cost <> @Cost)
       )
       BEGIN
           -- Step 1: Expire current record
           UPDATE Sales.DimProduct_Type2
           SET 
               ExpirationDate = DATEADD(DAY, -1, @EffectiveDate),
               IsCurrent = 0
           WHERE ProductID = @ProductID AND IsCurrent = 1;
           
           -- Step 2: Insert new record
           DECLARE @NewProductKey INT;
           SELECT @NewProductKey = ISNULL(MAX(ProductKey), 0) + 1 
           FROM Sales.DimProduct_Type2;
           
           INSERT INTO Sales.DimProduct_Type2 
           (ProductKey, ProductID, ProductName, Category, UnitPrice, Cost, EffectiveDate, ExpirationDate, IsCurrent)
           VALUES 
           (@NewProductKey, @ProductID, @ProductName, @Category, @UnitPrice, @Cost, @EffectiveDate, NULL, 1);
       END
       ELSE IF NOT EXISTS (SELECT 1 FROM Sales.DimProduct_Type2 WHERE ProductID = @ProductID)
       BEGIN
           -- New product, insert first record
           DECLARE @NewProductKey2 INT;
           SELECT @NewProductKey2 = ISNULL(MAX(ProductKey), 0) + 1 
           FROM Sales.DimProduct_Type2;
           
           INSERT INTO Sales.DimProduct_Type2 
           (ProductKey, ProductID, ProductName, Category, UnitPrice, Cost, EffectiveDate, ExpirationDate, IsCurrent)
           VALUES 
           (@NewProductKey2, @ProductID, @ProductName, @Category, @UnitPrice, @Cost, @EffectiveDate, NULL, 1);
       END
       -- ELSE: No change, do nothing
   END;
   
   -- Usage:
   EXEC Sales.usp_UpdateProduct_Type2 
       @ProductID = 'P1001',
       @ProductName = 'Widget A',
       @Category = 'Widgets',
       @UnitPrice = 24.99,
       @Cost = 10.00,
       @EffectiveDate = '2026-04-01';
   ```
   
   **Querying Type 2 Dimensions:**
   
   ```sql
   -- Get current records only
   SELECT ProductID, ProductName, UnitPrice
   FROM Sales.DimProduct_Type2
   WHERE IsCurrent = 1;
   
   -- Get historical price for specific date
   SELECT ProductID, ProductName, UnitPrice
   FROM Sales.DimProduct_Type2
   WHERE ProductID = 'P1001'
     AND '2026-02-15' BETWEEN EffectiveDate AND ISNULL(ExpirationDate, '9999-12-31');
   
   -- Join fact table with dimension (point-in-time join)
   SELECT 
       f.SaleDate,
       p.ProductName,
       p.UnitPrice AS PriceAtSaleTime,
       f.Quantity,
       f.Amount
   FROM Sales.FactSales f
   INNER JOIN Sales.DimProduct_Type2 p 
       ON f.ProductKey = p.ProductKey  -- Join on surrogate key maintains historical accuracy
   WHERE f.SaleDate >= '2026-01-01';
   
   -- Analyze price changes over time
   SELECT 
       ProductID,
       ProductName,
       EffectiveDate,
       ExpirationDate,
       UnitPrice,
       LEAD(UnitPrice) OVER (PARTITION BY ProductID ORDER BY EffectiveDate) AS NextPrice,
       DATEDIFF(DAY, EffectiveDate, ISNULL(ExpirationDate, GETDATE())) AS DaysActive
   FROM Sales.DimProduct_Type2
   WHERE ProductID = 'P1001'
   ORDER BY EffectiveDate;
   ```
   
   **When to use Type 2:**
   - Price changes (need historical pricing)
   - Product attributes (category, specification changes)
   - Customer tier/segment changes
   - Any attribute where historical values affect analysis
   
   ---
   
   **SCD Type 3 (Limited History - Previous Value):**
   
   Store current value + one previous value in separate columns.
   
   ```sql
   -- Type 3: Customer tier (track current + previous)
   CREATE TABLE Sales.DimCustomer_Type3 (
       CustomerKey INT NOT NULL,
       CustomerID VARCHAR(50) NOT NULL,
       CustomerName VARCHAR(200),
       -- Current tier
       CurrentTier VARCHAR(20),
       CurrentTierEffectiveDate DATE,
       -- Previous tier
       PreviousTier VARCHAR(20),
       PreviousTierEffectiveDate DATE,
       PRIMARY KEY NONCLUSTERED (CustomerKey) NOT ENFORCED
   );
   
   -- Initial record
   INSERT INTO Sales.DimCustomer_Type3 (CustomerKey, CustomerID, CustomerName, CurrentTier, CurrentTierEffectiveDate)
   VALUES (1, 'C1001', 'Acme Corp', 'Silver', '2025-01-01');
   
   -- When tier changes from Silver to Gold:
   UPDATE Sales.DimCustomer_Type3
   SET 
       PreviousTier = CurrentTier,
       PreviousTierEffectiveDate = CurrentTierEffectiveDate,
       CurrentTier = 'Gold',
       CurrentTierEffectiveDate = '2026-04-01'
   WHERE CustomerID = 'C1001';
   
   -- Result: One record with current + previous
   -- CustomerID | CurrentTier | CurrentTierDate | PreviousTier | PreviousTierDate
   -- -----------|-------------|-----------------|--------------|------------------
   --   C1001    |    Gold     |   2026-04-01    |    Silver    |   2025-01-01
   
   -- Query: Find customers who upgraded from Silver to Gold
   SELECT CustomerID, CustomerName, CurrentTier, PreviousTier
   FROM Sales.DimCustomer_Type3
   WHERE PreviousTier = 'Silver' AND CurrentTier = 'Gold';
   ```
   
   **When to use Type 3:**
   - Only need one previous value
   - Limited history required (e.g., tier upgrades/downgrades)
   - Simpler than Type 2, less storage
   - Fast access to "before and after" comparisons
   
   ---
   
   **Hybrid Approach (Type 1 + Type 2):**
   
   ```sql
   -- Different attributes may need different SCD types
   CREATE TABLE Sales.DimCustomer_Hybrid (
       CustomerKey INT NOT NULL,               -- Surrogate key
       CustomerID VARCHAR(50) NOT NULL,        -- Business key
       -- Type 1 attributes (overwrite, no history)
       CustomerName VARCHAR(200),
       Email VARCHAR(100),
       Phone VARCHAR(50),
       Address VARCHAR(200),                   -- Current address only
       -- Type 2 attributes (track history)
       CustomerTier VARCHAR(20),               -- Track tier changes
       DiscountRate DECIMAL(5,2),              -- Track discount changes
       -- SCD Type 2 tracking
       EffectiveDate DATE NOT NULL,
       ExpirationDate DATE,
       IsCurrent BIT NOT NULL DEFAULT 1,
       -- Audit
       CreatedDate DATETIME DEFAULT GETDATE(),
       ModifiedDate DATETIME,
       PRIMARY KEY NONCLUSTERED (CustomerKey) NOT ENFORCED
   );
   
   -- Update procedure
   CREATE PROCEDURE Sales.usp_UpdateCustomer_Hybrid
       @CustomerID VARCHAR(50),
       @CustomerName VARCHAR(200),
       @Email VARCHAR(100),
       @Phone VARCHAR(50),
       @Address VARCHAR(200),
       @CustomerTier VARCHAR(20),
       @DiscountRate DECIMAL(5,2),
       @EffectiveDate DATE
   AS
   BEGIN
       -- Check if Type 2 attributes changed
       DECLARE @Type2Changed BIT = 0;
       
       IF EXISTS (
           SELECT 1 
           FROM Sales.DimCustomer_Hybrid
           WHERE CustomerID = @CustomerID 
             AND IsCurrent = 1
             AND (CustomerTier <> @CustomerTier OR DiscountRate <> @DiscountRate)
       )
           SET @Type2Changed = 1;
       
       IF @Type2Changed = 1
       BEGIN
           -- Expire current record
           UPDATE Sales.DimCustomer_Hybrid
           SET ExpirationDate = DATEADD(DAY, -1, @EffectiveDate), IsCurrent = 0
           WHERE CustomerID = @CustomerID AND IsCurrent = 1;
           
           -- Insert new record (Type 2 attributes changed)
           INSERT INTO Sales.DimCustomer_Hybrid 
           (CustomerKey, CustomerID, CustomerName, Email, Phone, Address, CustomerTier, DiscountRate, EffectiveDate, IsCurrent)
           SELECT 
               (SELECT MAX(CustomerKey) + 1 FROM Sales.DimCustomer_Hybrid),
               @CustomerID, @CustomerName, @Email, @Phone, @Address, @CustomerTier, @DiscountRate, @EffectiveDate, 1;
       END
       ELSE
       BEGIN
           -- Update Type 1 attributes only (no new record)
           UPDATE Sales.DimCustomer_Hybrid
           SET 
               CustomerName = @CustomerName,
               Email = @Email,
               Phone = @Phone,
               Address = @Address,
               ModifiedDate = GETDATE()
           WHERE CustomerID = @CustomerID AND IsCurrent = 1;
       END
   END;
   ```
   
   **Best Practices:**
   - **Use surrogate keys** (ProductKey) not natural keys (ProductID) in fact tables
   - **Index IsCurrent** column for fast current record lookups
   - **Index EffectiveDate and ExpirationDate** for point-in-time queries
   - **Automate with stored procedures** to ensure consistency
   - **Choose SCD type per attribute**, not per table
   - **Document SCD strategy** for each dimension
   
   **Common Mistakes:**
   
   ```sql
   -- ❌ WRONG: Joining fact to dimension on business key
   SELECT *
   FROM FactSales f
   INNER JOIN DimProduct p ON f.ProductID = p.ProductID  -- WRONG! Multiple rows returned
   WHERE f.SaleDate = '2026-04-15';
   
   -- ✅ CORRECT: Join on surrogate key
   SELECT *
   FROM FactSales f
   INNER JOIN DimProduct p ON f.ProductKey = p.ProductKey  -- CORRECT! One row guaranteed
   WHERE f.SaleDate = '2026-04-15';
   
   -- ❌ WRONG: Not checking IsCurrent when querying dimensions
   SELECT * FROM DimProduct WHERE ProductID = 'P1001';  -- Returns all historical versions!
   
   -- ✅ CORRECT: Filter for current record
   SELECT * FROM DimProduct WHERE ProductID = 'P1001' AND IsCurrent = 1;
   ```

4. **Q:** How do you optimize query performance in Fabric Data Warehouse?
   **A:**
   Fabric Data Warehouse uses autonomous workload management and automatic optimization, but there are still techniques to improve query performance.
   
   **1. Statistics Management:**
   
   ```sql
   -- Warehouse automatically creates statistics, but manual updates help after large loads
   
   -- Update all statistics on a table
   UPDATE STATISTICS Sales.FactSales;
   
   -- Create statistics on frequently filtered columns
   CREATE STATISTICS stat_Sales_CustomerKey ON Sales.FactSales (CustomerKey);
   CREATE STATISTICS stat_Sales_DateKey ON Sales.FactSales (DateKey);
   
   -- Multi-column statistics for composite filters
   CREATE STATISTICS stat_Sales_Customer_Date 
   ON Sales.FactSales (CustomerKey, DateKey);
   
   -- Update statistics after major data load
   INSERT INTO Sales.FactSales SELECT * FROM Staging.SalesImport;
   UPDATE STATISTICS Sales.FactSales;
   
   -- View statistics info
   DBCC SHOW_STATISTICS('Sales.FactSales', stat_Sales_CustomerKey);
   ```
   
   **When to update statistics:**
   - After loading > 20% of table rows
   - After major DELETE operations
   - Before running complex analytical queries
   - Monthly for large, frequently updated tables
   
   ---
   
   **2. Query Design Optimization:**
   
   ```sql
   -- ❌ BAD: SELECT *
   SELECT * FROM Sales.FactSales WHERE DateKey = 20260425;
   -- Reads all columns even if you only need a few
   
   -- ✅ GOOD: Select only needed columns
   SELECT SaleID, CustomerKey, Amount 
   FROM Sales.FactSales 
   WHERE DateKey = 20260425;
   
   -- ❌ BAD: Functions on filtered columns (prevents optimization)
   SELECT * FROM Sales.FactSales 
   WHERE YEAR(SaleDate) = 2026 AND MONTH(SaleDate) = 4;
   -- Cannot use indexes/statistics effectively
   
   -- ✅ GOOD: Direct column comparison
   SELECT * FROM Sales.FactSales 
   WHERE SaleDate >= '2026-04-01' AND SaleDate < '2026-05-01';
   
   -- ❌ BAD: Implicit data type conversion
   SELECT * FROM Sales.FactSales 
   WHERE CustomerKey = '1001';  -- CustomerKey is INT, '1001' is VARCHAR
   
   -- ✅ GOOD: Explicit type matching
   SELECT * FROM Sales.FactSales 
   WHERE CustomerKey = 1001;
   
   -- ❌ BAD: OR conditions (harder to optimize)
   SELECT * FROM Sales.FactSales 
   WHERE CustomerKey = 1001 OR CustomerKey = 1002 OR CustomerKey = 1003;
   
   -- ✅ GOOD: IN clause
   SELECT * FROM Sales.FactSales 
   WHERE CustomerKey IN (1001, 1002, 1003);
   
   -- ❌ BAD: NOT IN with NULLs (slow and incorrect results)
   SELECT * FROM Sales.DimCustomer 
   WHERE CustomerKey NOT IN (SELECT CustomerKey FROM Sales.FactSales);
   
   -- ✅ GOOD: NOT EXISTS or LEFT JOIN
   SELECT c.*
   FROM Sales.DimCustomer c
   WHERE NOT EXISTS (
       SELECT 1 FROM Sales.FactSales f WHERE f.CustomerKey = c.CustomerKey
   );
   ```
   
   ---
   
   **3. Join Optimization:**
   
   ```sql
   -- ✅ GOOD: Join on surrogate keys (integers)
   SELECT 
       f.SaleID,
       d.Date,
       c.CustomerName,
       f.Amount
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey;
   -- Fast: Integer joins
   
   -- ❌ BAD: Join on strings
   SELECT 
       f.SaleID,
       d.Date,
       c.CustomerName,
       f.Amount
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON CONVERT(VARCHAR, f.DateKey) = CONVERT(VARCHAR, d.DateKey)
   INNER JOIN Sales.DimCustomer c ON f.CustomerID = c.CustomerID;
   -- Slow: String comparisons
   
   -- ✅ GOOD: Join order (smallest table first in query plan)
   SELECT 
       f.SaleID,
       p.ProductName,
       SUM(f.Amount) AS Revenue
   FROM Sales.FactSales f
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   WHERE f.DateKey = 20260425
   GROUP BY f.SaleID, p.ProductName;
   -- Filter first (reduces rows), then join
   
   -- ❌ BAD: Cartesian product
   SELECT f.*, c.*
   FROM Sales.FactSales f, Sales.DimCustomer c;  -- Missing join condition!
   
   -- ✅ GOOD: Always specify join condition
   SELECT f.*, c.*
   FROM Sales.FactSales f
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey;
   ```
   
   ---
   
   **4. Aggregation Optimization:**
   
   ```sql
   -- ❌ BAD: Aggregating in application layer
   -- Fetch all rows, aggregate in Python/Power BI
   SELECT SaleID, Amount FROM Sales.FactSales WHERE Year = 2026;
   -- Transfers millions of rows over network
   
   -- ✅ GOOD: Aggregate in warehouse (push down)
   SELECT 
       YEAR(SaleDate) AS Year,
       MONTH(SaleDate) AS Month,
       SUM(Amount) AS Revenue
   FROM Sales.FactSales
   WHERE YEAR(SaleDate) = 2026
   GROUP BY YEAR(SaleDate), MONTH(SaleDate);
   -- Transfers only 12 rows
   
   -- ✅ BETTER: Create materialized aggregate table
   CREATE TABLE Sales.MonthlySummary AS
   SELECT 
       d.Year,
       d.Month,
       c.Country,
       SUM(f.Amount) AS Revenue,
       COUNT(*) AS OrderCount,
       COUNT(DISTINCT f.CustomerKey) AS UniqueCustomers
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   GROUP BY d.Year, d.Month, c.Country;
   
   -- Query aggregate table (much faster)
   SELECT * FROM Sales.MonthlySummary WHERE Year = 2026;
   ```
   
   ---
   
   **5. Temp Tables for Complex Queries:**
   
   ```sql
   -- ❌ BAD: Nested subqueries
   SELECT 
       c.CustomerName,
       (SELECT SUM(Amount) FROM Sales.FactSales WHERE CustomerKey = c.CustomerKey) AS Revenue,
       (SELECT COUNT(*) FROM Sales.FactSales WHERE CustomerKey = c.CustomerKey) AS OrderCount,
       (SELECT MAX(SaleDate) FROM Sales.FactSales WHERE CustomerKey = c.CustomerKey) AS LastOrder
   FROM Sales.DimCustomer c;
   -- Scans FactSales 3 times!
   
   -- ✅ GOOD: Use temp table
   CREATE TABLE #CustomerMetrics (
       CustomerKey INT,
       Revenue DECIMAL(18,2),
       OrderCount INT,
       LastOrder DATE
   ) WITH (DISTRIBUTION = ROUND_ROBIN);
   
   INSERT INTO #CustomerMetrics
   SELECT 
       CustomerKey,
       SUM(Amount) AS Revenue,
       COUNT(*) AS OrderCount,
       MAX(SaleDate) AS LastOrder
   FROM Sales.FactSales
   GROUP BY CustomerKey;
   
   SELECT 
       c.CustomerName,
       m.Revenue,
       m.OrderCount,
       m.LastOrder
   FROM Sales.DimCustomer c
   INNER JOIN #CustomerMetrics m ON c.CustomerKey = m.CustomerKey;
   -- Scans FactSales once
   ```
   
   ---
   
   **6. Avoid Row-by-Row Processing:**
   
   ```sql
   -- ❌ BAD: Cursor (row-by-row)
   DECLARE @CustomerKey INT;
   DECLARE cursor_customers CURSOR FOR
   SELECT CustomerKey FROM Sales.DimCustomer;
   
   OPEN cursor_customers;
   FETCH NEXT FROM cursor_customers INTO @CustomerKey;
   
   WHILE @@FETCH_STATUS = 0
   BEGIN
       UPDATE Sales.DimCustomer
       SET TotalRevenue = (
           SELECT SUM(Amount) FROM Sales.FactSales WHERE CustomerKey = @CustomerKey
       )
       WHERE CustomerKey = @CustomerKey;
       
       FETCH NEXT FROM cursor_customers INTO @CustomerKey;
   END;
   
   CLOSE cursor_customers;
   DEALLOCATE cursor_customers;
   -- Extremely slow!
   
   -- ✅ GOOD: Set-based operation
   UPDATE c
   SET TotalRevenue = f.Revenue
   FROM Sales.DimCustomer c
   INNER JOIN (
       SELECT CustomerKey, SUM(Amount) AS Revenue
       FROM Sales.FactSales
       GROUP BY CustomerKey
   ) f ON c.CustomerKey = f.CustomerKey;
   -- 100-1000x faster!
   ```
   
   ---
   
   **7. Leverage Materialized Views (when available):**
   
   ```sql
   -- Create materialized view for frequently accessed aggregations
   -- Note: Check Fabric roadmap for materialized view support
   
   -- Alternative: Use regular tables with scheduled refresh
   CREATE TABLE Sales.vw_DailySalesSummary AS
   SELECT 
       d.Date,
       c.Country,
       p.Category,
       SUM(f.Amount) AS Revenue,
       COUNT(*) AS OrderCount
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   GROUP BY d.Date, c.Country, p.Category;
   
   -- Refresh daily via pipeline
   TRUNCATE TABLE Sales.vw_DailySalesSummary;
   INSERT INTO Sales.vw_DailySalesSummary
   SELECT 
       d.Date,
       c.Country,
       p.Category,
       SUM(f.Amount) AS Revenue,
       COUNT(*) AS OrderCount
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   GROUP BY d.Date, c.Country, p.Category;
   ```
   
   ---
   
   **8. Monitor Query Performance:**
   
   ```sql
   -- Use query execution plan (in Fabric UI or SSMS)
   -- Look for:
   -- - Table scans (bad) vs. seeks (good)
   -- - Expensive operations (sorts, hash joins)
   -- - Estimated vs. actual row counts (statistics issue)
   
   -- Enable execution plan in SSMS:
   SET SHOWPLAN_TEXT ON;
   GO
   SELECT * FROM Sales.FactSales WHERE CustomerKey = 1001;
   GO
   SET SHOWPLAN_TEXT OFF;
   GO
   
   -- Check query execution time
   SET STATISTICS TIME ON;
   SELECT COUNT(*) FROM Sales.FactSales;
   SET STATISTICS TIME OFF;
   ```
   
   ---
   
   **Performance Checklist:**
   
   ✅ **Do:**
   - Update statistics after major data loads
   - Select only needed columns
   - Use WHERE clauses to filter early
   - Join on integer keys (not strings)
   - Use set-based operations (not cursors)
   - Create aggregate tables for common queries
   - Use temp tables for complex multi-step queries
   - Filter before joining when possible
   
   ❌ **Don't:**
   - Use SELECT *
   - Use functions on filtered columns
   - Join on strings or use implicit conversions
   - Use cursors for bulk operations
   - Fetch large result sets to application layer
   - Create Cartesian products
   - Use NOT IN with nullable columns
   
   **Autonomous Features:**
   - Warehouse automatically manages distribution, partitioning, and indexing
   - No need to manually tune these (unlike Synapse Dedicated SQL Pool)
   - Focus on query design and statistics

5. **Q:** How do you migrate data from an on-premises SQL Server data warehouse to Fabric Warehouse?
   **A:**
   Migrating from on-premises SQL Server DW to Fabric Warehouse involves schema migration, data migration, and code migration. Microsoft provides tools and recommended approaches.
   
   **Migration Phases:**
   
   ```
   Phase 1: Assessment & Planning
   ├─ Inventory: Schema, tables, views, procedures, functions
   ├─ Size: Data volume, row counts
   ├─ Dependencies: Identify cross-database references, linked servers
   └─ Compatibility: Check for unsupported features
   
   Phase 2: Schema Migration
   ├─ DDL Scripts: Export CREATE TABLE, CREATE VIEW, etc.
   ├─ Adapt: Modify for Fabric (constraints, data types)
   └─ Deploy: Execute in Fabric Warehouse
   
   Phase 3: Data Migration
   ├─ Initial Load: Bulk copy historical data
   ├─ Incremental: Load changes during cutover window
   └─ Validation: Row counts, checksums
   
   Phase 4: Code Migration
   ├─ Stored Procedures: Adapt T-SQL code
   ├─ Functions: Migrate UDFs, TVFs
   ├─ Jobs: Convert SQL Agent jobs to Fabric Pipelines
   └─ Reports: Reconnect Power BI to Fabric
   
   Phase 5: Cutover
   ├─ Final sync
   ├─ Switch applications to Fabric
   └─ Decommission on-prem
   ```
   
   ---
   
   **Phase 1: Assessment**
   
   ```sql
   -- On-premises SQL Server: Inventory all objects
   
   -- Get table list with row counts
   SELECT 
       SCHEMA_NAME(schema_id) AS SchemaName,
       name AS TableName,
       (SELECT SUM(rows) FROM sys.partitions p WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) AS RowCount
   FROM sys.tables t
   ORDER BY SchemaName, TableName;
   
   -- Get total database size
   EXEC sp_spaceused;
   
   -- List stored procedures
   SELECT SCHEMA_NAME(schema_id) AS SchemaName, name AS ProcedureName
   FROM sys.procedures
   ORDER BY SchemaName, name;
   
   -- List views
   SELECT SCHEMA_NAME(schema_id) AS SchemaName, name AS ViewName
   FROM sys.views
   ORDER BY SchemaName, name;
   
   -- List functions
   SELECT SCHEMA_NAME(schema_id) AS SchemaName, name AS FunctionName, type_desc AS FunctionType
   FROM sys.objects
   WHERE type IN ('FN', 'IF', 'TF')  -- Scalar, Inline TVF, Multi-statement TVF
   ORDER BY SchemaName, name;
   
   -- Check for unsupported features
   SELECT 
       t.name AS TableName,
       c.name AS ColumnName,
       ty.name AS DataType
   FROM sys.tables t
   INNER JOIN sys.columns c ON t.object_id = c.object_id
   INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
   WHERE ty.name IN ('geography', 'geometry', 'hierarchyid', 'xml', 'sql_variant')  -- Check compatibility
   ORDER BY t.name, c.name;
   ```
   
   ---
   
   **Phase 2: Schema Migration**
   
   **Option 1: Manual Script Generation (SSMS):**
   
   ```
   1. Right-click database → Tasks → Generate Scripts
   2. Select objects (tables, views, procedures)
   3. Set scripting options:
      - Script for: SQL Server 2022
      - Include: DROP and CREATE
      - Schema: Yes
   4. Save to file (schema.sql)
   5. Modify script for Fabric compatibility
   ```
   
   **Schema Adaptation for Fabric:**
   
   ```sql
   -- Original SQL Server table:
   CREATE TABLE Sales.FactSales (
       SaleID INT NOT NULL PRIMARY KEY CLUSTERED,
       DateKey INT NOT NULL,
       CustomerKey INT NOT NULL,
       Amount DECIMAL(18,2),
       FOREIGN KEY (DateKey) REFERENCES Sales.DimDate(DateKey),
       FOREIGN KEY (CustomerKey) REFERENCES Sales.DimCustomer(CustomerKey)
   );
   
   -- Adapted for Fabric Warehouse:
   CREATE TABLE Sales.FactSales (
       SaleID INT NOT NULL,
       DateKey INT NOT NULL,
       CustomerKey INT NOT NULL,
       Amount DECIMAL(18,2),
       PRIMARY KEY NONCLUSTERED (SaleID) NOT ENFORCED,  -- Changed: NONCLUSTERED, NOT ENFORCED
       FOREIGN KEY (DateKey) REFERENCES Sales.DimDate(DateKey) NOT ENFORCED,
       FOREIGN KEY (CustomerKey) REFERENCES Sales.DimCustomer(CustomerKey) NOT ENFORCED
   );
   -- Removed: CLUSTERED (not supported)
   -- Added: NOT ENFORCED (required in Fabric)
   ```
   
   **Common Schema Changes:**
   
   ```sql
   -- ❌ Not supported in Fabric:
   -- - CLUSTERED indexes (use NONCLUSTERED)
   -- - ENFORCED constraints (use NOT ENFORCED)
   -- - Computed columns (use views)
   -- - Partitioned tables (automatic in Fabric)
   -- - Indexed views (use regular views)
   
   -- ✅ Adaptation patterns:
   
   -- Computed columns → Views
   -- Original:
   CREATE TABLE Sales.FactSales (
       SaleID INT,
       Quantity INT,
       UnitPrice DECIMAL(18,2),
       TotalAmount AS (Quantity * UnitPrice) PERSISTED  -- Not supported
   );
   
   -- Fabric:
   CREATE TABLE Sales.FactSales (
       SaleID INT,
       Quantity INT,
       UnitPrice DECIMAL(18,2),
       TotalAmount DECIMAL(18,2)  -- Regular column
   );
   
   CREATE VIEW Sales.vw_FactSales AS
   SELECT 
       SaleID,
       Quantity,
       UnitPrice,
       Quantity * UnitPrice AS TotalAmount  -- Computed in view
   FROM Sales.FactSales;
   
   -- Partitioned tables → Regular tables
   -- Original:
   CREATE TABLE Sales.FactSales (
       SaleID INT,
       SaleDate DATE,
       Amount DECIMAL(18,2)
   )
   ON PartitionScheme_Date(SaleDate);  -- Not supported
   
   -- Fabric:
   CREATE TABLE Sales.FactSales (
       SaleID INT,
       SaleDate DATE,
       Amount DECIMAL(18,2)
   );
   -- Fabric automatically manages partitioning
   ```
   
   ---
   
   **Phase 3: Data Migration**
   
   **Option 1: Fabric Migration Assistant (Recommended):**
   
   ```
   1. Install Fabric Migration Assistant
   2. Connect to source (on-prem SQL Server)
   3. Connect to target (Fabric Warehouse)
   4. Select tables to migrate
   5. Run migration (handles schema + data)
   6. Validate results
   ```
   
   **Option 2: Azure Data Factory / Fabric Data Pipeline:**
   
   ```json
   {
     "name": "MigrateSQLServerToFabric",
     "type": "Copy",
     "inputs": [{
       "type": "DatasetReference",
       "referenceName": "OnPremSQLServer",
       "parameters": {
         "tableName": "Sales.FactSales"
       }
     }],
     "outputs": [{
       "type": "DatasetReference",
       "referenceName": "FabricWarehouse",
       "parameters": {
         "tableName": "Sales.FactSales"
       }
     }],
     "typeProperties": {
       "source": {
         "type": "SqlSource",
         "sqlReaderQuery": "SELECT * FROM Sales.FactSales"
       },
       "sink": {
         "type": "WarehouseTableSink",
         "preCopyScript": "TRUNCATE TABLE Sales.FactSales;",
         "writeBatchSize": 100000
       },
       "enableStaging": false,
       "parallelCopies": 32
     }
   }
   ```
   
   **Option 3: Export/Import via Parquet (Large Tables):**
   
   ```sql
   -- Step 1: Export from SQL Server to Parquet (via Azure Data Factory)
   -- Or use BCP to export to CSV, then convert to Parquet
   
   -- Step 2: Upload Parquet files to OneLake
   -- Use Azure Storage Explorer or AzCopy
   
   -- Step 3: Load into Fabric Warehouse via COPY INTO
   COPY INTO Sales.FactSales
   FROM 'https://onelake.dfs.fabric.microsoft.com/MyWorkspace/MyLakehouse/Files/Migration/FactSales/*.parquet'
   WITH (
       FILE_TYPE = 'PARQUET',
       CREDENTIAL = (IDENTITY = 'Managed Identity')
   );
   ```
   
   **Incremental Migration (Minimize Downtime):**
   
   ```sql
   -- Initial bulk load (historical data)
   COPY INTO Sales.FactSales
   FROM 'https://onelake.../InitialLoad/*.parquet'
   WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));
   
   -- Track watermark
   CREATE TABLE Migration.Watermark (
       TableName VARCHAR(200) PRIMARY KEY,
       LastSyncDate DATETIME
   );
   
   INSERT INTO Migration.Watermark VALUES ('Sales.FactSales', '2026-04-01 00:00:00');
   
   -- Incremental sync (daily during migration period)
   DECLARE @LastSync DATETIME;
   SELECT @LastSync = LastSyncDate FROM Migration.Watermark WHERE TableName = 'Sales.FactSales';
   
   -- Pull changes from on-prem since last sync
   INSERT INTO Sales.FactSales
   SELECT *
   FROM OPENQUERY(OnPremLinkedServer, 
       'SELECT * FROM Sales.FactSales WHERE ModifiedDate > ''' + CONVERT(VARCHAR, @LastSync, 120) + '''');
   
   -- Update watermark
   UPDATE Migration.Watermark 
   SET LastSyncDate = GETDATE() 
   WHERE TableName = 'Sales.FactSales';
   ```
   
   ---
   
   **Phase 4: Code Migration**
   
   **Stored Procedures:**
   
   ```sql
   -- Most T-SQL stored procedures work as-is
   -- Check for:
   -- - Linked servers (replace with cross-database queries)
   -- - Temp tables (distributed vs. non-distributed)
   -- - Dynamic SQL (test carefully)
   
   -- Original (SQL Server):
   CREATE PROCEDURE Sales.usp_LoadDailySales
       @LoadDate DATE
   AS
   BEGIN
       INSERT INTO Sales.FactSales
       SELECT * FROM LinkedServer.RemoteDB.dbo.Sales
       WHERE SaleDate = @LoadDate;
   END;
   
   -- Adapted (Fabric):
   CREATE PROCEDURE Sales.usp_LoadDailySales
       @LoadDate DATE
   AS
   BEGIN
       INSERT INTO Sales.FactSales
       SELECT * FROM StagingLakehouse.SalesImport  -- Cross-database query
       WHERE SaleDate = @LoadDate;
   END;
   ```
   
   **SQL Agent Jobs → Fabric Pipelines:**
   
   ```
   SQL Agent Job:
   ├─ Step 1: Run stored procedure (usp_LoadDailySales)
   ├─ Step 2: Update statistics
   └─ Step 3: Send email notification
   
   Fabric Pipeline:
   ├─ Activity 1: Script Activity (EXEC usp_LoadDailySales)
   ├─ Activity 2: Script Activity (UPDATE STATISTICS)
   └─ Activity 3: Web Activity (send notification via Logic App)
   ```
   
   ---
   
   **Phase 5: Validation**
   
   ```sql
   -- Row count validation
   -- On-prem:
   SELECT COUNT(*) FROM Sales.FactSales;  -- 10,000,000
   
   -- Fabric:
   SELECT COUNT(*) FROM Sales.FactSales;  -- 10,000,000 (must match)
   
   -- Checksum validation
   -- On-prem:
   SELECT SUM(CAST(CHECKSUM(*) AS BIGINT)) FROM Sales.FactSales;
   
   -- Fabric:
   SELECT SUM(CAST(CHECKSUM(*) AS BIGINT)) FROM Sales.FactSales;
   
   -- Sample data comparison
   -- On-prem:
   SELECT TOP 100 * FROM Sales.FactSales ORDER BY SaleID;
   
   -- Fabric:
   SELECT TOP 100 * FROM Sales.FactSales ORDER BY SaleID;
   ```
   
   ---
   
   **Best Practices:**
   
   1. **Start Small:** Migrate non-critical tables first
   2. **Parallel Migration:** Migrate schema, then data (don't wait for all schema)
   3. **Incremental Approach:** Bulk load historical, then incremental sync
   4. **Validation:** Row counts, checksums, spot checks
   5. **Cutover Plan:** Define clear go/no-go criteria
   6. **Rollback Plan:** Keep on-prem live during validation
   7. **Performance Testing:** Run representative queries in Fabric before cutover
   8. **Documentation:** Track schema changes, unsupported features
   
   **Timeline Example:**
   
   ```
   Week 1-2: Assessment
   - Inventory objects
   - Identify incompatibilities
   - Size data volumes
   
   Week 3-4: Schema Migration
   - Adapt DDL scripts
   - Create schemas in Fabric
   - Test with sample data
   
   Week 5-6: Initial Data Load
   - Bulk load historical data
   - Validate row counts
   
   Week 7: Incremental Sync
   - Daily incremental loads
   - Test code migrations
   
   Week 8: Validation & Cutover
   - Final sync
   - Performance testing
   - Switch production traffic
   - Monitor closely
   ```

6. **Q:** What are Cross-Database Queries in Fabric and how do they work?
   **A:**
   Cross-database queries allow you to query data across multiple Fabric items (Warehouses, Lakehouses, SQL databases) in a single SQL query without moving or copying data. This is a powerful feature unique to Fabric that enables zero-copy data integration.
   
   **How It Works:**
   
   ```
   Traditional Approach (Data Copy):
   ┌────────────────┐     Copy      ┌────────────────┐
   │ Sales Warehouse│────────────▶  │ Marketing DW   │
   └────────────────┘    ETL Job    │ (Duplicate)    │
                                     └────────────────┘
   Problem: Data duplication, sync lag, storage cost
   
   Fabric Cross-Database Queries (Zero-Copy):
   ┌────────────────┐               ┌────────────────┐
   │ Sales Warehouse│◄──────────────│ Your Query     │
   └────────────────┘    Query      │                │
   ┌────────────────┐    Multiple   │  SELECT *      │
   │Marketing Lakeh.│◄──────────────│  FROM Sales... │
   └────────────────┘    Sources    │  JOIN Market...│
   ┌────────────────┐               └────────────────┘
   │ Finance DB     │◄──────────────┘
   └────────────────┘
   Benefits: No data copy, always current, no sync delay
   ```
   
   **Syntax:**
   
   The three-part naming convention: `ItemName.SchemaName.ObjectName`
   
   ```sql
   -- Format:
   SELECT * FROM [ItemName].[SchemaName].[TableName]
   
   -- Examples:
   SELECT * FROM SalesWarehouse.Sales.FactSales
   SELECT * FROM MarketingLakehouse.Campaigns.WebEvents
   SELECT * FROM FinanceDB.Accounting.Ledger
   ```
   
   **Use Case 1: Query Multiple Warehouses**
   
   ```sql
   -- Combine sales data from Sales Warehouse with marketing data from Marketing Warehouse
   SELECT 
       s.OrderID,
       s.OrderDate,
       s.CustomerID,
       s.OrderAmount,
       c.CampaignName,
       c.CampaignType,
       c.CampaignBudget,
       -- Calculate ROI
       s.OrderAmount / NULLIF(c.CostPerAcquisition, 0) AS ROI
   FROM SalesWarehouse.Sales.FactOrders s
   LEFT JOIN MarketingWarehouse.Campaigns.FactCampaigns c 
       ON s.CampaignID = c.CampaignID
   WHERE s.OrderDate >= '2026-01-01'
     AND c.CampaignType = 'Digital';
   
   -- Result: Combined sales + marketing insights without data duplication
   ```
   
   **Use Case 2: Query Warehouse + Lakehouse**
   
   ```sql
   -- Join structured warehouse data with semi-structured lakehouse data
   SELECT 
       w.CustomerKey,
       w.CustomerName,
       w.CustomerTier,
       w.TotalPurchases,
       -- From Lakehouse: Web analytics
       l.TotalWebVisits,
       l.PageViewsPerVisit,
       l.AvgSessionDuration,
       l.BounceRate,
       -- From Lakehouse: Social media sentiment
       sm.SentimentScore,
       sm.MentionCount,
       -- Calculate engagement score
       (l.TotalWebVisits * 0.4 + sm.MentionCount * 0.3 + w.TotalPurchases * 0.3) AS EngagementScore
   FROM SalesWarehouse.Customer.DimCustomer w
   LEFT JOIN AnalyticsLakehouse.WebData.CustomerWebMetrics l 
       ON w.CustomerID = l.CustomerID
   LEFT JOIN SocialLakehouse.Sentiment.CustomerSentiment sm 
       ON w.CustomerID = sm.CustomerID
   WHERE w.CustomerTier = 'Platinum'
   ORDER BY EngagementScore DESC;
   ```
   
   **Use Case 3: Query Multiple Lakehouses**
   
   ```sql
   -- Federate data from multiple lakehouses
   SELECT 
       p.ProductID,
       p.ProductName,
       p.Category,
       -- From Sales Lakehouse
       s.UnitsSold,
       s.Revenue,
       -- From Inventory Lakehouse
       i.StockLevel,
       i.WarehouseLocation,
       -- From Reviews Lakehouse
       r.AverageRating,
       r.ReviewCount,
       r.TopReviewKeywords,
       -- Calculate inventory health
       CASE 
           WHEN i.StockLevel < s.UnitsSold * 0.1 THEN 'Critical - Restock'
           WHEN i.StockLevel < s.UnitsSold * 0.5 THEN 'Low - Monitor'
           ELSE 'Healthy'
       END AS InventoryStatus
   FROM ProductCatalog.Products.DimProduct p
   INNER JOIN SalesLakehouse.Aggregates.ProductSales s 
       ON p.ProductID = s.ProductID
   INNER JOIN InventoryLakehouse.Current.ProductInventory i 
       ON p.ProductID = i.ProductID
   LEFT JOIN ReviewsLakehouse.Summary.ProductReviews r 
       ON p.ProductID = r.ProductID
   WHERE s.Revenue > 10000
   ORDER BY s.Revenue DESC;
   ```
   
   **Use Case 4: Cross-Database ETL**
   
   ```sql
   -- Load fact table from multiple sources without staging
   INSERT INTO TargetWarehouse.Sales.FactSales (
       OrderID,
       OrderDate,
       CustomerKey,
       ProductKey,
       Quantity,
       Amount,
       CampaignKey
   )
   SELECT 
       o.OrderID,
       o.OrderDate,
       c.CustomerKey,
       p.ProductKey,
       o.Quantity,
       o.Amount,
       cm.CampaignKey
   FROM SourceLakehouse.RawOrders.Orders o
   INNER JOIN TargetWarehouse.Customer.DimCustomer c 
       ON o.CustomerID = c.CustomerID
   INNER JOIN TargetWarehouse.Product.DimProduct p 
       ON o.ProductID = p.ProductID
   LEFT JOIN MarketingWarehouse.Campaign.DimCampaign cm 
       ON o.CampaignCode = cm.CampaignCode
   WHERE o.OrderDate = CAST(GETDATE() AS DATE)
     AND NOT EXISTS (
         SELECT 1 
         FROM TargetWarehouse.Sales.FactSales f 
         WHERE f.OrderID = o.OrderID
     );
   
   -- Result: Direct load from multiple sources, no intermediate staging
   ```
   
   **Use Case 5: Real-Time Analytics Across Sources**
   
   ```sql
   -- Real-time dashboard query combining multiple data sources
   CREATE VIEW Analytics.vw_RealTimeSalesDashboard AS
   SELECT 
       -- From Warehouse: Historical sales
       COALESCE(h.Date, CAST(GETDATE() AS DATE)) AS Date,
       COALESCE(h.TotalRevenue, 0) AS HistoricalRevenue,
       COALESCE(h.OrderCount, 0) AS HistoricalOrders,
       -- From Lakehouse: Real-time streaming data
       COALESCE(rt.CurrentRevenue, 0) AS RealTimeRevenue,
       COALESCE(rt.CurrentOrders, 0) AS RealTimeOrders,
       -- Combined totals
       COALESCE(h.TotalRevenue, 0) + COALESCE(rt.CurrentRevenue, 0) AS TotalRevenue,
       COALESCE(h.OrderCount, 0) + COALESCE(rt.CurrentOrders, 0) AS TotalOrders,
       -- From Marketing: Campaign performance
       c.ActiveCampaigns,
       c.CampaignSpend
   FROM SalesWarehouse.Reports.DailySalesSummary h
   FULL OUTER JOIN RealTimeLakehouse.Streaming.CurrentSales rt 
       ON h.Date = rt.Date
   LEFT JOIN MarketingWarehouse.Daily.CampaignSummary c 
       ON h.Date = c.Date;
   
   -- Query the view
   SELECT * FROM Analytics.vw_RealTimeSalesDashboard
   WHERE Date >= DATEADD(DAY, -7, GETDATE());
   ```
   
   **Performance Considerations:**
   
   ```sql
   -- ❌ BAD: Pulling large datasets across items
   SELECT *  -- Don't use SELECT *
   FROM LargeWarehouse.Sales.FactSales  -- 100M rows
   CROSS JOIN AnotherWarehouse.Product.DimProduct;  -- Cartesian product!
   -- Result: Network transfer of billions of rows
   
   -- ✅ GOOD: Filter early, aggregate, then transfer
   SELECT 
       p.Category,
       SUM(s.Amount) AS TotalRevenue,
       COUNT(*) AS OrderCount
   FROM LargeWarehouse.Sales.FactSales s
   INNER JOIN AnotherWarehouse.Product.DimProduct p 
       ON s.ProductKey = p.ProductKey
   WHERE s.OrderDate >= '2026-04-01'  -- Filter early
   GROUP BY p.Category;  -- Aggregate before transfer
   -- Result: Only aggregated summary transferred
   
   -- ✅ BEST: Use temp tables for complex cross-database queries
   -- Step 1: Materialize filtered data locally
   CREATE TABLE #FilteredSales (
       OrderID INT,
       ProductKey INT,
       Amount DECIMAL(18,2)
   ) WITH (DISTRIBUTION = ROUND_ROBIN);
   
   INSERT INTO #FilteredSales
   SELECT OrderID, ProductKey, Amount
   FROM LargeWarehouse.Sales.FactSales
   WHERE OrderDate >= '2026-04-01';
   
   -- Step 2: Join with local temp table (faster)
   SELECT 
       p.Category,
       SUM(s.Amount) AS TotalRevenue
   FROM #FilteredSales s
   INNER JOIN AnotherWarehouse.Product.DimProduct p 
       ON s.ProductKey = p.ProductKey
   GROUP BY p.Category;
   ```
   
   **Limitations:**
   
   | What Works | What Doesn't |
   |------------|--------------|
   | ✅ SELECT queries | ❌ INSERT into remote items |
   | ✅ JOIN across items | ❌ UPDATE remote items |
   | ✅ WHERE filters | ❌ DELETE from remote items |
   | ✅ GROUP BY aggregations | ❌ CREATE TABLE in remote items |
   | ✅ Views referencing remote items | ❌ Transactions spanning items |
   | ✅ Stored procedures with remote queries | ❌ Temp tables in remote items |
   
   ```sql
   -- ✅ ALLOWED: Read from remote
   SELECT * FROM RemoteWarehouse.Sales.FactSales;
   
   -- ✅ ALLOWED: Write to local from remote
   INSERT INTO LocalWarehouse.Sales.FactSales
   SELECT * FROM RemoteWarehouse.Sales.FactSales;
   
   -- ❌ NOT ALLOWED: Write to remote
   INSERT INTO RemoteWarehouse.Sales.FactSales VALUES (...);
   -- Error: Cannot modify remote objects
   
   -- ❌ NOT ALLOWED: Update remote
   UPDATE RemoteWarehouse.Sales.FactSales SET Amount = 100;
   -- Error: Cannot modify remote objects
   ```
   
   **Security:**
   
   ```sql
   -- Users need READ permission on remote items
   -- Permissions are checked at query time
   
   -- Example: User queries cross-database
   SELECT * FROM SalesWarehouse.Sales.FactSales;  -- User needs READ on SalesWarehouse
   
   -- If user lacks permission:
   -- Error: User does not have permission to read from 'SalesWarehouse'
   ```
   
   **Best Practices:**
   
   1. **Filter Early:** Apply WHERE clauses to reduce data transfer
   2. **Aggregate When Possible:** Use GROUP BY to summarize before transfer
   3. **Limit Columns:** Select only needed columns, avoid SELECT *
   4. **Use Temp Tables:** For complex queries, materialize remote data locally first
   5. **Monitor Performance:** Check query plans for cross-item data movement
   6. **Document Dependencies:** Track which queries depend on which items
   7. **Test Permissions:** Ensure users have access to all referenced items
   
   **Common Patterns:**
   
   ```sql
   -- Pattern 1: Dimension enrichment from another warehouse
   SELECT 
       f.*,
       c.CustomerName,
       c.CustomerTier,
       p.ProductName,
       p.Category
   FROM LocalWarehouse.Sales.FactSales f
   LEFT JOIN SharedWarehouse.Customer.DimCustomer c ON f.CustomerKey = c.CustomerKey
   LEFT JOIN SharedWarehouse.Product.DimProduct p ON f.ProductKey = p.ProductKey;
   
   -- Pattern 2: Federated reporting across business units
   SELECT 
       'North America' AS Region,
       SUM(Amount) AS Revenue
   FROM NAWarehouse.Sales.FactSales
   WHERE OrderDate >= '2026-01-01'
   UNION ALL
   SELECT 
       'Europe' AS Region,
       SUM(Amount) AS Revenue
   FROM EUWarehouse.Sales.FactSales
   WHERE OrderDate >= '2026-01-01'
   UNION ALL
   SELECT 
       'Asia' AS Region,
       SUM(Amount) AS Revenue
   FROM AsiaWarehouse.Sales.FactSales
   WHERE OrderDate >= '2026-01-01';
   
   -- Pattern 3: Lakehouse + Warehouse hybrid analytics
   SELECT 
       l.EventDate,
       l.EventType,
       l.UserCount,
       w.Revenue,
       w.OrderCount,
       -- Calculate conversion rate
       CASE 
           WHEN l.UserCount > 0 
           THEN CAST(w.OrderCount AS FLOAT) / l.UserCount * 100
           ELSE 0
       END AS ConversionRate
   FROM WebEventsLakehouse.Analytics.DailyEvents l
   LEFT JOIN SalesWarehouse.Summary.DailySales w 
       ON l.EventDate = w.SaleDate;
   ```
   
   **Key Advantages:**
   
   - **Zero Data Movement:** No ETL jobs to maintain
   - **Always Current:** Query live data, no sync lag
   - **Reduced Storage:** No data duplication
   - **Simplified Architecture:** Fewer data copies to manage
   - **Flexible:** Mix structured (Warehouse) and semi-structured (Lakehouse) data
   - **Cost Effective:** No extra storage or compute for staging

7. **Q:** What is CTAS (Create Table As Select) and when should you use it?
   **A:**
   CTAS (Create Table As Select) is a T-SQL statement that creates a new table and populates it with the results of a SELECT query in a single operation. It's one of the fastest ways to create and load tables in Fabric Data Warehouse.
   
   **Syntax:**
   
   ```sql
   CREATE TABLE new_table_name AS
   SELECT column1, column2, ...
   FROM source_table
   WHERE conditions;
   ```
   
   **How It Works:**
   
   ```
   Traditional Approach (2 steps):
   Step 1: CREATE TABLE with explicit schema
   ┌─────────────────────────────────────┐
   │ CREATE TABLE Sales.Summary (       │
   │   Year INT,                         │
   │   Revenue DECIMAL(18,2),            │
   │   OrderCount INT                    │
   │ );                                  │
   └─────────────────────────────────────┘
   
   Step 2: INSERT data
   ┌─────────────────────────────────────┐
   │ INSERT INTO Sales.Summary           │
   │ SELECT Year, SUM(Amount), COUNT(*)  │
   │ FROM Sales.FactSales                │
   │ GROUP BY Year;                      │
   └─────────────────────────────────────┘
   
   CTAS Approach (1 step):
   ┌─────────────────────────────────────┐
   │ CREATE TABLE Sales.Summary AS       │
   │ SELECT                              │
   │   Year,                             │
   │   SUM(Amount) AS Revenue,           │
   │   COUNT(*) AS OrderCount            │
   │ FROM Sales.FactSales                │
   │ GROUP BY Year;                      │
   └─────────────────────────────────────┘
   
   Benefits:
   - Schema inferred from SELECT
   - Single operation (faster)
   - Less code to write
   - Atomic operation
   ```
   
   **Basic Examples:**
   
   ```sql
   -- Example 1: Copy entire table
   CREATE TABLE Sales.FactSales_Backup AS
   SELECT * FROM Sales.FactSales;
   
   -- Example 2: Create filtered subset
   CREATE TABLE Sales.Sales_2026 AS
   SELECT * 
   FROM Sales.FactSales
   WHERE YEAR(OrderDate) = 2026;
   
   -- Example 3: Create aggregated summary
   CREATE TABLE Sales.MonthlySummary AS
   SELECT 
       YEAR(OrderDate) AS Year,
       MONTH(OrderDate) AS Month,
       SUM(Amount) AS TotalRevenue,
       COUNT(*) AS OrderCount,
       AVG(Amount) AS AvgOrderValue,
       COUNT(DISTINCT CustomerKey) AS UniqueCustomers
   FROM Sales.FactSales
   GROUP BY YEAR(OrderDate), MONTH(OrderDate);
   
   -- Example 4: Create denormalized table (star schema → flat)
   CREATE TABLE Sales.FlatSalesData AS
   SELECT 
       f.OrderID,
       f.OrderDate,
       f.Quantity,
       f.Amount,
       c.CustomerName,
       c.CustomerTier,
       c.Country,
       c.City,
       p.ProductName,
       p.Category,
       p.Brand,
       d.Year,
       d.Quarter,
       d.Month,
       d.DayName
   FROM Sales.FactSales f
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey;
   ```
   
   **Advanced Use Cases:**
   
   **Use Case 1: Data Mart Creation**
   
   ```sql
   -- Create departmental data mart from enterprise warehouse
   CREATE TABLE MarketingDataMart.CustomerCampaignAnalysis AS
   SELECT 
       c.CustomerKey,
       c.CustomerName,
       c.Email,
       c.CustomerTier,
       c.Country,
       cm.CampaignName,
       cm.CampaignType,
       cm.StartDate,
       cm.EndDate,
       COUNT(DISTINCT o.OrderID) AS OrdersInCampaign,
       SUM(o.Amount) AS RevenueInCampaign,
       AVG(o.Amount) AS AvgOrderValue,
       MIN(o.OrderDate) AS FirstPurchase,
       MAX(o.OrderDate) AS LastPurchase,
       DATEDIFF(DAY, MIN(o.OrderDate), MAX(o.OrderDate)) AS CustomerLifespanDays
   FROM EnterpriseWarehouse.Customer.DimCustomer c
   INNER JOIN EnterpriseWarehouse.Sales.FactSales o ON c.CustomerKey = o.CustomerKey
   INNER JOIN EnterpriseWarehouse.Marketing.DimCampaign cm ON o.CampaignKey = cm.CampaignKey
   GROUP BY 
       c.CustomerKey, c.CustomerName, c.Email, c.CustomerTier, c.Country,
       cm.CampaignName, cm.CampaignType, cm.StartDate, cm.EndDate;
   ```
   
   **Use Case 2: Snapshot Tables**
   
   ```sql
   -- Daily snapshot of current state
   CREATE TABLE Inventory.ProductInventorySnapshot_20260425 AS
   SELECT 
       CAST(GETDATE() AS DATE) AS SnapshotDate,
       p.ProductID,
       p.ProductName,
       p.Category,
       i.WarehouseID,
       w.WarehouseName,
       w.Location,
       i.QuantityOnHand,
       i.QuantityReserved,
       i.QuantityAvailable,
       i.ReorderPoint,
       p.UnitCost,
       i.QuantityOnHand * p.UnitCost AS TotalInventoryValue,
       CASE 
           WHEN i.QuantityAvailable <= i.ReorderPoint THEN 'Reorder'
           WHEN i.QuantityAvailable <= i.ReorderPoint * 2 THEN 'Low'
           ELSE 'Adequate'
       END AS StockStatus
   FROM Product.DimProduct p
   INNER JOIN Inventory.FactCurrentInventory i ON p.ProductKey = i.ProductKey
   INNER JOIN Warehouse.DimWarehouse w ON i.WarehouseKey = w.WarehouseKey;
   ```
   
   **Use Case 3: Pre-Aggregated Performance Tables**
   
   ```sql
   -- Create pre-aggregated table for faster dashboard queries
   CREATE TABLE Reports.DailySalesPerformance AS
   SELECT 
       d.Date,
       d.Year,
       d.Quarter,
       d.Month,
       d.DayOfWeek,
       d.DayName,
       c.Country,
       c.Region,
       p.Category,
       p.Brand,
       -- Sales metrics
       COUNT(DISTINCT f.OrderID) AS OrderCount,
       COUNT(DISTINCT f.CustomerKey) AS UniqueCustomers,
       SUM(f.Quantity) AS TotalQuantity,
       SUM(f.Amount) AS TotalRevenue,
       AVG(f.Amount) AS AvgOrderValue,
       MIN(f.Amount) AS MinOrderValue,
       MAX(f.Amount) AS MaxOrderValue,
       -- Time comparisons
       LAG(SUM(f.Amount)) OVER (
           PARTITION BY c.Country, p.Category 
           ORDER BY d.Date
       ) AS PreviousDayRevenue,
       SUM(f.Amount) - LAG(SUM(f.Amount)) OVER (
           PARTITION BY c.Country, p.Category 
           ORDER BY d.Date
       ) AS RevenueDayOverDayChange
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   GROUP BY 
       d.Date, d.Year, d.Quarter, d.Month, d.DayOfWeek, d.DayName,
       c.Country, c.Region,
       p.Category, p.Brand;
   ```
   
   **Use Case 4: Cross-Database ETL with CTAS**
   
   ```sql
   -- Create table from multiple sources (using cross-database queries)
   CREATE TABLE IntegratedWarehouse.Sales.ConsolidatedOrders AS
   SELECT 
       'Online' AS Channel,
       o.OrderID,
       o.OrderDate,
       o.CustomerID,
       o.ProductID,
       o.Quantity,
       o.Amount
   FROM OnlineWarehouse.Sales.FactOrders o
   WHERE o.OrderDate >= '2026-01-01'
   UNION ALL
   SELECT 
       'Retail' AS Channel,
       r.OrderID,
       r.OrderDate,
       r.CustomerID,
       r.ProductID,
       r.Quantity,
       r.Amount
   FROM RetailWarehouse.Sales.FactOrders r
   WHERE r.OrderDate >= '2026-01-01'
   UNION ALL
   SELECT 
       'Partner' AS Channel,
       p.OrderID,
       p.OrderDate,
       p.CustomerID,
       p.ProductID,
       p.Quantity,
       p.Amount
   FROM PartnerLakehouse.Orders.SalesData p
   WHERE p.OrderDate >= '2026-01-01';
   ```
   
   **Use Case 5: Temporary Analysis Tables**
   
   ```sql
   -- Create temporary analysis table for ad-hoc investigation
   CREATE TABLE Analysis.HighValueCustomerSegment AS
   SELECT 
       c.CustomerKey,
       c.CustomerID,
       c.CustomerName,
       c.Email,
       c.Country,
       -- Purchase metrics
       COUNT(DISTINCT o.OrderID) AS TotalOrders,
       SUM(o.Amount) AS LifetimeValue,
       AVG(o.Amount) AS AvgOrderValue,
       MIN(o.OrderDate) AS FirstPurchaseDate,
       MAX(o.OrderDate) AS LastPurchaseDate,
       DATEDIFF(DAY, MIN(o.OrderDate), MAX(o.OrderDate)) AS CustomerTenureDays,
       -- Recency
       DATEDIFF(DAY, MAX(o.OrderDate), GETDATE()) AS DaysSinceLastPurchase,
       -- Frequency
       COUNT(DISTINCT o.OrderID) * 1.0 / 
           NULLIF(DATEDIFF(DAY, MIN(o.OrderDate), MAX(o.OrderDate)), 0) * 365 AS OrdersPerYear,
       -- Segmentation
       CASE 
           WHEN SUM(o.Amount) > 100000 THEN 'VIP'
           WHEN SUM(o.Amount) > 50000 THEN 'High Value'
           WHEN SUM(o.Amount) > 10000 THEN 'Medium Value'
           ELSE 'Low Value'
       END AS ValueSegment,
       CASE 
           WHEN DATEDIFF(DAY, MAX(o.OrderDate), GETDATE()) <= 30 THEN 'Active'
           WHEN DATEDIFF(DAY, MAX(o.OrderDate), GETDATE()) <= 90 THEN 'At Risk'
           ELSE 'Churned'
       END AS ActivityStatus
   FROM Sales.DimCustomer c
   INNER JOIN Sales.FactSales o ON c.CustomerKey = o.CustomerKey
   GROUP BY c.CustomerKey, c.CustomerID, c.CustomerName, c.Email, c.Country
   HAVING SUM(o.Amount) > 10000;
   
   -- Query the analysis table
   SELECT 
       ValueSegment,
       ActivityStatus,
       COUNT(*) AS CustomerCount,
       AVG(LifetimeValue) AS AvgLifetimeValue,
       AVG(OrdersPerYear) AS AvgOrdersPerYear
   FROM Analysis.HighValueCustomerSegment
   GROUP BY ValueSegment, ActivityStatus
   ORDER BY ValueSegment, ActivityStatus;
   ```
   
   **Schema Inference:**
   
   ```sql
   -- CTAS infers data types from SELECT
   CREATE TABLE Test.InferredTypes AS
   SELECT 
       1 AS IntegerColumn,              -- Inferred as INT
       'Text' AS VarcharColumn,          -- Inferred as VARCHAR
       CAST(123.45 AS DECIMAL(10,2)) AS DecimalColumn,  -- DECIMAL(10,2)
       GETDATE() AS DateTimeColumn,      -- DATETIME
       CAST('2026-04-25' AS DATE) AS DateColumn,  -- DATE
       CAST(1 AS BIT) AS BitColumn,      -- BIT
       NULL AS NullColumn;               -- Inferred from context
   
   -- Check inferred schema
   SELECT 
       COLUMN_NAME,
       DATA_TYPE,
       CHARACTER_MAXIMUM_LENGTH,
       NUMERIC_PRECISION,
       NUMERIC_SCALE
   FROM INFORMATION_SCHEMA.COLUMNS
   WHERE TABLE_SCHEMA = 'Test' AND TABLE_NAME = 'InferredTypes';
   ```
   
   **Performance Benefits:**
   
   ```sql
   -- ❌ SLOW: Two-step approach
   -- Step 1: Create empty table
   CREATE TABLE Sales.Summary (
       Year INT,
       Revenue DECIMAL(18,2),
       OrderCount INT
   );
   
   -- Step 2: Insert 10 million rows
   INSERT INTO Sales.Summary
   SELECT 
       YEAR(OrderDate),
       SUM(Amount),
       COUNT(*)
   FROM Sales.FactSales  -- 100 million rows
   GROUP BY YEAR(OrderDate);
   -- Time: ~5 minutes
   
   -- ✅ FAST: CTAS (single operation)
   CREATE TABLE Sales.Summary AS
   SELECT 
       YEAR(OrderDate) AS Year,
       SUM(Amount) AS Revenue,
       COUNT(*) AS OrderCount
   FROM Sales.FactSales  -- 100 million rows
   GROUP BY YEAR(OrderDate);
   -- Time: ~2 minutes (2-3x faster)
   
   -- Why faster?
   -- - Single operation (no transaction overhead)
   -- - Optimized execution plan
   -- - Parallel processing
   -- - No logging overhead for empty table
   ```
   
   **Refresh Pattern:**
   
   ```sql
   -- Daily refresh of summary table
   
   -- Option 1: DROP and recreate (simplest)
   DROP TABLE IF EXISTS Sales.DailySummary;
   
   CREATE TABLE Sales.DailySummary AS
   SELECT 
       CAST(OrderDate AS DATE) AS Date,
       COUNT(*) AS OrderCount,
       SUM(Amount) AS Revenue
   FROM Sales.FactSales
   WHERE OrderDate >= DATEADD(DAY, -90, GETDATE())  -- Rolling 90 days
   GROUP BY CAST(OrderDate AS DATE);
   
   -- Option 2: TRUNCATE and INSERT (preserves metadata, constraints)
   TRUNCATE TABLE Sales.DailySummary;
   
   INSERT INTO Sales.DailySummary
   SELECT 
       CAST(OrderDate AS DATE) AS Date,
       COUNT(*) AS OrderCount,
       SUM(Amount) AS Revenue
   FROM Sales.FactSales
   WHERE OrderDate >= DATEADD(DAY, -90, GETDATE())
   GROUP BY CAST(OrderDate AS DATE);
   
   -- Option 3: Create with suffix, then swap (zero downtime)
   CREATE TABLE Sales.DailySummary_NEW AS
   SELECT 
       CAST(OrderDate AS DATE) AS Date,
       COUNT(*) AS OrderCount,
       SUM(Amount) AS Revenue
   FROM Sales.FactSales
   WHERE OrderDate >= DATEADD(DAY, -90, GETDATE())
   GROUP BY CAST(OrderDate AS DATE);
   
   -- Atomic swap
   BEGIN TRANSACTION;
       DROP TABLE IF EXISTS Sales.DailySummary_OLD;
       EXEC sp_rename 'Sales.DailySummary', 'DailySummary_OLD';
       EXEC sp_rename 'Sales.DailySummary_NEW', 'DailySummary';
   COMMIT TRANSACTION;
   
   -- Clean up
   DROP TABLE Sales.DailySummary_OLD;
   ```
   
   **Limitations:**
   
   ```sql
   -- ❌ Cannot specify constraints in CTAS
   CREATE TABLE Sales.Summary AS
   SELECT 
       Year,
       SUM(Amount) AS Revenue
   FROM Sales.FactSales
   GROUP BY Year
   PRIMARY KEY (Year);  -- Error: Syntax not supported
   
   -- ✅ Add constraints after creation
   CREATE TABLE Sales.Summary AS
   SELECT 
       Year,
       SUM(Amount) AS Revenue
   FROM Sales.FactSales
   GROUP BY Year;
   
   ALTER TABLE Sales.Summary
   ADD PRIMARY KEY NONCLUSTERED (Year) NOT ENFORCED;
   
   -- ❌ Cannot specify identity columns in CTAS
   -- ✅ Use ROW_NUMBER() instead
   CREATE TABLE Sales.SummaryWithID AS
   SELECT 
       ROW_NUMBER() OVER (ORDER BY Year) AS ID,
       Year,
       SUM(Amount) AS Revenue
   FROM Sales.FactSales
   GROUP BY Year;
   ```
   
   **When to Use CTAS:**
   
   ✅ **Use CTAS When:**
   - Creating aggregate/summary tables
   - Building data marts
   - Creating table backups
   - Materializing complex query results
   - Creating snapshots
   - Prototyping table structures
   - ETL transformations (source → target)
   - Creating temporary analysis tables
   
   ❌ **Avoid CTAS When:**
   - Need specific constraints (primary key, foreign key)
   - Need identity columns
   - Need computed columns
   - Schema must be explicitly defined
   - Need to preserve existing table metadata
   
   **Best Practices:**
   
   1. **Explicit Aliases:** Always alias computed columns
      ```sql
      -- ❌ BAD: No alias for computed column
      CREATE TABLE Test AS
      SELECT Year, SUM(Amount) FROM Sales;  -- Column name unclear
      
      -- ✅ GOOD: Explicit alias
      CREATE TABLE Test AS
      SELECT Year, SUM(Amount) AS TotalRevenue FROM Sales;
      ```
   
   2. **Explicit Type Casting:** For precise control
      ```sql
      -- ❌ Implicit types (may surprise you)
      CREATE TABLE Test AS
      SELECT 
          Year,
          SUM(Amount) AS Revenue  -- Inferred type (may be wrong precision)
      FROM Sales GROUP BY Year;
      
      -- ✅ Explicit types
      CREATE TABLE Test AS
      SELECT 
          CAST(Year AS INT) AS Year,
          CAST(SUM(Amount) AS DECIMAL(18,2)) AS Revenue
      FROM Sales GROUP BY Year;
      ```
   
   3. **Filter for Relevance:** Don't copy unnecessary data
      ```sql
      -- ❌ Copy entire history
      CREATE TABLE Sales.RecentOrders AS
      SELECT * FROM Sales.FactSales;  -- 10 years of data
      
      -- ✅ Filter to relevant period
      CREATE TABLE Sales.RecentOrders AS
      SELECT * FROM Sales.FactSales
      WHERE OrderDate >= DATEADD(YEAR, -1, GETDATE());  -- Last year only
      ```
   
   4. **Document Purpose:** Add comments
      ```sql
      -- Create daily summary for Power BI dashboard
      -- Refreshed nightly at 2 AM via pipeline
      CREATE TABLE Reports.DailySalesSummary AS
      SELECT 
          CAST(OrderDate AS DATE) AS Date,
          COUNT(*) AS OrderCount,
          SUM(Amount) AS Revenue
      FROM Sales.FactSales
      WHERE OrderDate >= DATEADD(DAY, -90, GETDATE())
      GROUP BY CAST(OrderDate AS DATE);
      ```
   
   **Key Advantages:**
   
   - **Simplicity:** Less code than CREATE + INSERT
   - **Performance:** Faster than two-step approach
   - **Atomic:** Single operation (all or nothing)
   - **Schema Inference:** Automatic type detection
   - **Flexibility:** Combine with complex queries, JOINs, aggregations
   - **Cross-Database:** Works with cross-database queries

8. **Q:** What is Dimensional Data Modeling and how do you implement star and snowflake schemas in Fabric Warehouse?
   **A:**
   Dimensional data modeling is a data warehouse design technique that organizes data into **Fact tables** (measurements/metrics) and **Dimension tables** (descriptive context). It's optimized for analytical queries and business intelligence workloads.
   
   **Core Concepts:**
   
   ```
   Dimensional Model Structure:
   ┌────────────────────────────────────────────┐
   │         FACT TABLE (Center)                │
   │  - Numeric measurements (Revenue, Qty)     │
   │  - Foreign keys to dimensions              │
   │  - High volume, frequently updated         │
   └────────────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
   ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
   │ DIM     │   │ DIM     │   │ DIM     │
   │ Date    │   │Customer │   │ Product │
   │         │   │         │   │         │
   │ (Desc)  │   │ (Desc)  │   │ (Desc)  │
   └─────────┘   └─────────┘   └─────────┘
   ```
   
   **Fact Tables:**
   - Store quantitative **measurements** or **metrics**
   - Contain **foreign keys** to dimension tables
   - Usually **large** in volume (millions/billions of rows)
   - **Granularity** defines the detail level (e.g., one row per sale)
   - Types: Transaction, Snapshot, Accumulating Snapshot
   
   **Dimension Tables:**
   - Store **descriptive** attributes or **context**
   - Contain **natural keys** (business keys) + **surrogate keys**
   - Typically **smaller** in volume (thousands to millions of rows)
   - Support **filtering**, **grouping**, and **labeling** in reports
   - May be **slowly changing** (SCD Type 1, 2, 3)
   
   ---
   
   **Star Schema (Simple & Fast):**
   
   A star schema has a central fact table directly connected to dimension tables (one level deep).
   
   ```
   Structure:
           DimDate              DimCustomer
               │                     │
               └─────┐         ┌─────┘
                     │         │
                  ┌──▼─────────▼──┐
                  │   FactSales    │ ← Central fact
                  └──┬─────────┬──┘
                     │         │
               ┌─────┘         └─────┐
               │                     │
          DimProduct            DimStore
   ```
   
   **Implementation:**
   
   ```sql
   -- Star Schema Example: Sales Analytics
   
   -- 1. Date Dimension (Time intelligence)
   CREATE TABLE Sales.DimDate (
       DateKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       Date DATE NOT NULL UNIQUE NONCLUSTERED NOT ENFORCED,
       Year INT,
       Quarter INT,
       QuarterName VARCHAR(10),  -- 'Q1 2026'
       Month INT,
       MonthName VARCHAR(20),    -- 'April'
       MonthYear VARCHAR(20),    -- 'Apr 2026'
       Week INT,
       WeekOfYear INT,
       DayOfMonth INT,
       DayOfWeek INT,            -- 1 = Sunday, 7 = Saturday
       DayName VARCHAR(20),      -- 'Thursday'
       IsWeekend BIT,
       IsHoliday BIT,
       HolidayName VARCHAR(100),
       FiscalYear INT,
       FiscalQuarter INT,
       FiscalMonth INT
   );
   
   -- 2. Customer Dimension (Who)
   CREATE TABLE Sales.DimCustomer (
       CustomerKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       CustomerID VARCHAR(50) NOT NULL UNIQUE NONCLUSTERED NOT ENFORCED,
       CustomerName VARCHAR(200),
       CustomerType VARCHAR(50),      -- 'Individual', 'Business'
       CustomerTier VARCHAR(20),      -- 'Bronze', 'Silver', 'Gold', 'Platinum'
       Email VARCHAR(100),
       Phone VARCHAR(50),
       -- Address (denormalized in star schema)
       AddressLine1 VARCHAR(200),
       AddressLine2 VARCHAR(200),
       City VARCHAR(100),
       State VARCHAR(50),
       Country VARCHAR(50),
       PostalCode VARCHAR(20),
       Region VARCHAR(50),            -- 'North America', 'EMEA', 'APAC'
       -- Demographics
       DateOfBirth DATE,
       Gender VARCHAR(10),
       IncomeLevel VARCHAR(50),
       -- Metadata
       FirstPurchaseDate DATE,
       IsActive BIT,
       CreatedDate DATETIME,
       ModifiedDate DATETIME
   );
   
   -- 3. Product Dimension (What)
   CREATE TABLE Sales.DimProduct (
       ProductKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       ProductID VARCHAR(50) NOT NULL UNIQUE NONCLUSTERED NOT ENFORCED,
       ProductName VARCHAR(200),
       ProductDescription VARCHAR(500),
       -- Hierarchy (denormalized in star schema)
       Category VARCHAR(100),
       Subcategory VARCHAR(100),
       Brand VARCHAR(100),
       Manufacturer VARCHAR(100),
       -- Attributes
       Color VARCHAR(50),
       Size VARCHAR(50),
       Weight DECIMAL(10,2),
       UnitOfMeasure VARCHAR(20),
       -- Pricing
       ListPrice DECIMAL(18,2),
       StandardCost DECIMAL(18,2),
       -- Inventory
       SafetyStockLevel INT,
       ReorderPoint INT,
       -- Status
       IsActive BIT,
       DiscontinuedDate DATE,
       -- Metadata
       IntroducedDate DATE,
       CreatedDate DATETIME,
       ModifiedDate DATETIME
   );
   
   -- 4. Store Dimension (Where)
   CREATE TABLE Sales.DimStore (
       StoreKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       StoreID VARCHAR(50) NOT NULL UNIQUE NONCLUSTERED NOT ENFORCED,
       StoreName VARCHAR(200),
       StoreType VARCHAR(50),         -- 'Flagship', 'Outlet', 'Pop-up'
       -- Location
       AddressLine1 VARCHAR(200),
       City VARCHAR(100),
       State VARCHAR(50),
       Country VARCHAR(50),
       PostalCode VARCHAR(20),
       Region VARCHAR(50),
       Latitude DECIMAL(10,6),
       Longitude DECIMAL(10,6),
       -- Details
       SquareFootage INT,
       OpenDate DATE,
       ManagerName VARCHAR(200),
       PhoneNumber VARCHAR(50),
       IsActive BIT,
       CreatedDate DATETIME
   );
   
   -- 5. Fact Table: Sales Transactions
   CREATE TABLE Sales.FactSales (
       -- Surrogate key
       SalesKey BIGINT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       
       -- Foreign keys to dimensions (star schema: direct links)
       DateKey INT NOT NULL,
       CustomerKey INT NOT NULL,
       ProductKey INT NOT NULL,
       StoreKey INT NOT NULL,
       
       -- Degenerate dimensions (attributes with no dimension table)
       OrderNumber VARCHAR(50),
       LineNumber INT,
       
       -- Measurements (facts)
       Quantity INT,
       UnitPrice DECIMAL(18,2),
       Discount DECIMAL(18,2),
       Tax DECIMAL(18,2),
       ShippingCost DECIMAL(18,2),
       TotalAmount DECIMAL(18,2),
       
       -- Calculated measures (can also be calculated in queries/views)
       NetAmount AS (TotalAmount - Discount - Tax),
       
       -- Foreign key constraints (NOT ENFORCED in Fabric)
       FOREIGN KEY (DateKey) REFERENCES Sales.DimDate(DateKey) NOT ENFORCED,
       FOREIGN KEY (CustomerKey) REFERENCES Sales.DimCustomer(CustomerKey) NOT ENFORCED,
       FOREIGN KEY (ProductKey) REFERENCES Sales.DimProduct(ProductKey) NOT ENFORCED,
       FOREIGN KEY (StoreKey) REFERENCES Sales.DimStore(StoreKey) NOT ENFORCED
   );
   ```
   
   **Star Schema Query Example:**
   
   ```sql
   -- Business Question: What are monthly sales by region and product category?
   SELECT 
       d.Year,
       d.MonthName,
       c.Region AS CustomerRegion,
       p.Category AS ProductCategory,
       -- Aggregations
       COUNT(DISTINCT f.CustomerKey) AS UniqueCustomers,
       COUNT(DISTINCT f.OrderNumber) AS Orders,
       SUM(f.Quantity) AS TotalQuantity,
       SUM(f.TotalAmount) AS TotalRevenue,
       AVG(f.TotalAmount) AS AvgOrderValue,
       SUM(f.TotalAmount) / NULLIF(SUM(f.Quantity), 0) AS AvgPricePerUnit
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   INNER JOIN Sales.DimStore s ON f.StoreKey = s.StoreKey
   WHERE d.Year = 2026
     AND d.Quarter = 2
   GROUP BY d.Year, d.MonthName, c.Region, p.Category
   ORDER BY TotalRevenue DESC;
   
   -- Result: Simple query with 4 joins, fast execution (star schema optimized)
   ```
   
   **Star Schema Advantages:**
   - ✅ **Simple queries:** Fewer joins (1 level deep)
   - ✅ **Fast performance:** Query optimizer friendly
   - ✅ **Easy to understand:** Business users grasp the model
   - ✅ **Denormalized dimensions:** All attributes in one table
   
   **Star Schema Disadvantages:**
   - ❌ **Data redundancy:** Repeated values in denormalized dimensions
   - ❌ **Larger dimension tables:** All attributes in single table
   - ❌ **Update complexity:** Changing hierarchy requires many updates
   
   ---
   
   **Snowflake Schema (Normalized & Complex):**
   
   A snowflake schema normalizes dimension tables into multiple related tables (multiple levels deep).
   
   ```
   Structure:
                DimDate
                   │
               ┌───▼───────────┐
               │   FactSales   │
               └───┬───────────┘
                   │
         ┌─────────┼─────────┐
         │         │         │
     DimCustomer   │    DimProduct
         │         │         │
         │         │         ├─ DimCategory
         │         │         ├─ DimSubcategory
         │         │         └─ DimBrand
         │         │
         ├─ DimCity         DimStore
         │   │               │
         │   └─ DimState    ├─ DimStoreType
         │       │           └─ DimRegion
         └─ DimCountry
   ```
   
   **Implementation:**
   
   ```sql
   -- Snowflake Schema Example: Normalized Product Hierarchy
   
   -- 1. Product Category (Top level)
   CREATE TABLE Sales.DimCategory (
       CategoryKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       CategoryID VARCHAR(50) NOT NULL UNIQUE NONCLUSTERED NOT ENFORCED,
       CategoryName VARCHAR(100),
       CategoryDescription VARCHAR(500)
   );
   
   -- 2. Product Subcategory (Mid level)
   CREATE TABLE Sales.DimSubcategory (
       SubcategoryKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       SubcategoryID VARCHAR(50) NOT NULL UNIQUE NONCLUSTERED NOT ENFORCED,
       SubcategoryName VARCHAR(100),
       CategoryKey INT NOT NULL,  -- Link to parent
       FOREIGN KEY (CategoryKey) REFERENCES Sales.DimCategory(CategoryKey) NOT ENFORCED
   );
   
   -- 3. Product Brand (Parallel dimension)
   CREATE TABLE Sales.DimBrand (
       BrandKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       BrandID VARCHAR(50) NOT NULL UNIQUE NONCLUSTERED NOT ENFORCED,
       BrandName VARCHAR(100),
       Manufacturer VARCHAR(100),
       Country VARCHAR(50)
   );
   
   -- 4. Product (Leaf level - links to fact)
   CREATE TABLE Sales.DimProduct_Snowflake (
       ProductKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       ProductID VARCHAR(50) NOT NULL UNIQUE NONCLUSTERED NOT ENFORCED,
       ProductName VARCHAR(200),
       ProductDescription VARCHAR(500),
       -- Foreign keys to normalized dimensions
       SubcategoryKey INT NOT NULL,
       BrandKey INT NOT NULL,
       -- Product-specific attributes
       Color VARCHAR(50),
       Size VARCHAR(50),
       ListPrice DECIMAL(18,2),
       StandardCost DECIMAL(18,2),
       IsActive BIT,
       -- Foreign keys
       FOREIGN KEY (SubcategoryKey) REFERENCES Sales.DimSubcategory(SubcategoryKey) NOT ENFORCED,
       FOREIGN KEY (BrandKey) REFERENCES Sales.DimBrand(BrandKey) NOT ENFORCED
   );
   
   -- 5. Customer Geography (Normalized)
   CREATE TABLE Sales.DimCountry (
       CountryKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       CountryCode VARCHAR(3),
       CountryName VARCHAR(100),
       Region VARCHAR(50)  -- 'North America', 'EMEA', 'APAC'
   );
   
   CREATE TABLE Sales.DimState (
       StateKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       StateCode VARCHAR(10),
       StateName VARCHAR(100),
       CountryKey INT NOT NULL,
       FOREIGN KEY (CountryKey) REFERENCES Sales.DimCountry(CountryKey) NOT ENFORCED
   );
   
   CREATE TABLE Sales.DimCity (
       CityKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       CityName VARCHAR(100),
       StateKey INT NOT NULL,
       PostalCode VARCHAR(20),
       Latitude DECIMAL(10,6),
       Longitude DECIMAL(10,6),
       FOREIGN KEY (StateKey) REFERENCES Sales.DimState(StateKey) NOT ENFORCED
   );
   
   -- 6. Customer (Leaf level - links to fact)
   CREATE TABLE Sales.DimCustomer_Snowflake (
       CustomerKey INT NOT NULL PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       CustomerID VARCHAR(50) NOT NULL UNIQUE NONCLUSTERED NOT ENFORCED,
       CustomerName VARCHAR(200),
       Email VARCHAR(100),
       Phone VARCHAR(50),
       -- Foreign key to normalized geography
       CityKey INT NOT NULL,
       AddressLine1 VARCHAR(200),
       -- Customer-specific attributes
       CustomerTier VARCHAR(20),
       DateOfBirth DATE,
       IsActive BIT,
       FOREIGN KEY (CityKey) REFERENCES Sales.DimCity(CityKey) NOT ENFORCED
   );
   ```
   
   **Snowflake Schema Query Example:**
   
   ```sql
   -- Business Question: Sales by country, state, category, subcategory
   SELECT 
       co.CountryName,
       st.StateName,
       cat.CategoryName,
       sub.SubcategoryName,
       br.BrandName,
       SUM(f.TotalAmount) AS TotalRevenue,
       COUNT(DISTINCT f.CustomerKey) AS UniqueCustomers
   FROM Sales.FactSales f
   INNER JOIN Sales.DimCustomer_Snowflake c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimCity ci ON c.CityKey = ci.CityKey
   INNER JOIN Sales.DimState st ON ci.StateKey = st.StateKey
   INNER JOIN Sales.DimCountry co ON st.CountryKey = co.CountryKey
   INNER JOIN Sales.DimProduct_Snowflake p ON f.ProductKey = p.ProductKey
   INNER JOIN Sales.DimSubcategory sub ON p.SubcategoryKey = sub.SubcategoryKey
   INNER JOIN Sales.DimCategory cat ON sub.CategoryKey = cat.CategoryKey
   INNER JOIN Sales.DimBrand br ON p.BrandKey = br.BrandKey
   GROUP BY co.CountryName, st.StateName, cat.CategoryName, sub.SubcategoryName, br.BrandName
   ORDER BY TotalRevenue DESC;
   
   -- Result: More complex query with 9 joins (snowflake schema)
   ```
   
   **Snowflake Schema Advantages:**
   - ✅ **Less redundancy:** Normalized, no repeated values
   - ✅ **Smaller tables:** Attributes split across multiple tables
   - ✅ **Easier updates:** Change hierarchy in one place
   - ✅ **Enforces consistency:** Referential integrity (conceptually)
   
   **Snowflake Schema Disadvantages:**
   - ❌ **Complex queries:** More joins (multiple levels)
   - ❌ **Slower performance:** More joins = more processing
   - ❌ **Harder to understand:** Business users need training
   - ❌ **More tables:** More objects to manage
   
   ---
   
   **Star vs Snowflake Comparison:**
   
   | Aspect | Star Schema | Snowflake Schema |
   |--------|-------------|------------------|
   | **Dimension Structure** | Denormalized (1 table) | Normalized (multiple tables) |
   | **Query Complexity** | Simple (fewer joins) | Complex (more joins) |
   | **Query Performance** | Faster | Slower |
   | **Data Redundancy** | Higher | Lower |
   | **Storage Size** | Larger dimensions | Smaller dimensions |
   | **Update Complexity** | Higher | Lower |
   | **Business User Friendly** | Very | Moderate |
   | **ETL Complexity** | Lower | Higher |
   | **When to Use** | BI, reporting, dashboards | Highly normalized legacy data |
   
   **Recommendation:** ⭐ **Star schema is preferred** for most Fabric Warehouse implementations.
   
   ---
   
   **Fact Table Types:**
   
   **1. Transaction Fact Table (Most Common):**
   ```sql
   -- One row per business event
   CREATE TABLE Sales.FactOrders (
       OrderKey BIGINT PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       DateKey INT NOT NULL,
       CustomerKey INT NOT NULL,
       ProductKey INT NOT NULL,
       OrderNumber VARCHAR(50),
       Quantity INT,
       Amount DECIMAL(18,2),
       FOREIGN KEY (DateKey) REFERENCES Sales.DimDate(DateKey) NOT ENFORCED
   );
   -- Grain: One row per order line
   ```
   
   **2. Periodic Snapshot Fact Table:**
   ```sql
   -- Regular snapshots of state
   CREATE TABLE Inventory.FactInventorySnapshot (
       SnapshotDateKey INT NOT NULL,
       ProductKey INT NOT NULL,
       WarehouseKey INT NOT NULL,
       QuantityOnHand INT,
       QuantityReserved INT,
       QuantityAvailable INT,
       UnitCost DECIMAL(18,2),
       PRIMARY KEY NONCLUSTERED (SnapshotDateKey, ProductKey, WarehouseKey) NOT ENFORCED
   );
   -- Grain: One row per product per warehouse per day
   ```
   
   **3. Accumulating Snapshot Fact Table:**
   ```sql
   -- Tracks process from start to finish
   CREATE TABLE Logistics.FactOrder (
       OrderKey BIGINT PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       OrderDateKey INT NOT NULL,
       RequestedDateKey INT,
       ShipDateKey INT,
       DeliveryDateKey INT,
       CustomerKey INT NOT NULL,
       -- Milestone metrics
       DaysBetweenOrderAndShip INT,
       DaysBetweenShipAndDelivery INT,
       OrderAmount DECIMAL(18,2),
       ShippingCost DECIMAL(18,2)
   );
   -- Grain: One row per order (updated as order progresses)
   -- Columns updated: NULL → DateKey as milestones complete
   ```
   
   ---
   
   **Dimension Table Patterns:**
   
   **1. Conformed Dimensions (Shared):**
   ```sql
   -- DimDate used by ALL fact tables
   CREATE TABLE Shared.DimDate (...);
   
   -- FactSales uses DimDate
   CREATE TABLE Sales.FactSales (
       DateKey INT NOT NULL,
       FOREIGN KEY (DateKey) REFERENCES Shared.DimDate(DateKey) NOT ENFORCED
   );
   
   -- FactOrders uses same DimDate
   CREATE TABLE Orders.FactOrders (
       OrderDateKey INT NOT NULL,
       FOREIGN KEY (OrderDateKey) REFERENCES Shared.DimDate(DateKey) NOT ENFORCED
   );
   
   -- Benefit: Consistent time dimensions across all facts
   ```
   
   **2. Role-Playing Dimensions:**
   ```sql
   -- Same dimension used multiple times in one fact
   CREATE TABLE Sales.FactOrders (
       OrderKey BIGINT PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       OrderDateKey INT NOT NULL,      -- DimDate playing "Order Date" role
       ShipDateKey INT,                -- DimDate playing "Ship Date" role
       DeliveryDateKey INT,            -- DimDate playing "Delivery Date" role
       FOREIGN KEY (OrderDateKey) REFERENCES Shared.DimDate(DateKey) NOT ENFORCED,
       FOREIGN KEY (ShipDateKey) REFERENCES Shared.DimDate(DateKey) NOT ENFORCED,
       FOREIGN KEY (DeliveryDateKey) REFERENCES Shared.DimDate(DateKey) NOT ENFORCED
   );
   
   -- Query with role-playing:
   SELECT 
       o.OrderDate AS OrderDate,
       s.Date AS ShipDate,
       d.Date AS DeliveryDate
   FROM FactOrders f
   LEFT JOIN DimDate o ON f.OrderDateKey = o.DateKey      -- Order date
   LEFT JOIN DimDate s ON f.ShipDateKey = s.DateKey       -- Ship date
   LEFT JOIN DimDate d ON f.DeliveryDateKey = d.DateKey;  -- Delivery date
   ```
   
   **3. Junk Dimensions (Low-cardinality flags):**
   ```sql
   -- Combine low-cardinality flags into one dimension
   CREATE TABLE Sales.DimOrderFlags (
       OrderFlagKey INT PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       IsExpress BIT,
       IsGiftWrapped BIT,
       IsInternational BIT,
       RequiresSignature BIT,
       PaymentMethod VARCHAR(50)
   );
   
   -- Pre-populate all combinations (2^4 * payment methods)
   INSERT INTO Sales.DimOrderFlags VALUES
       (1, 0, 0, 0, 0, 'Credit Card'),
       (2, 1, 0, 0, 0, 'Credit Card'),
       (3, 0, 1, 0, 0, 'Credit Card'),
       -- ... all combinations
   ;
   
   -- Fact table references junk dimension
   CREATE TABLE Sales.FactOrders (
       OrderKey BIGINT PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       OrderFlagKey INT NOT NULL,
       FOREIGN KEY (OrderFlagKey) REFERENCES Sales.DimOrderFlags(OrderFlagKey) NOT ENFORCED
   );
   
   -- Benefit: Avoid many tiny dimension tables
   ```
   
   **4. Degenerate Dimensions (No dimension table):**
   ```sql
   -- Dimension data stored directly in fact table
   CREATE TABLE Sales.FactSales (
       SalesKey BIGINT PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       OrderNumber VARCHAR(50),  -- Degenerate dimension (no DimOrder table)
       InvoiceNumber VARCHAR(50), -- Degenerate dimension
       LineNumber INT,            -- Degenerate dimension
       DateKey INT,
       CustomerKey INT,
       ProductKey INT,
       Quantity INT,
       Amount DECIMAL(18,2)
   );
   
   -- Use case: High-cardinality, low-value attributes
   -- Not worth creating separate dimension table
   ```
   
   ---
   
   **Best Practices:**
   
   **1. Surrogate Keys:**
   ```sql
   -- ✅ GOOD: Use surrogate keys (auto-increment integers)
   CREATE TABLE Sales.DimCustomer (
       CustomerKey INT PRIMARY KEY,  -- Surrogate key (warehouse-generated)
       CustomerID VARCHAR(50),        -- Natural key (source system)
       ...
   );
   
   -- Why?
   -- - Small (4 bytes vs variable)
   -- - Join performance (integer comparison fast)
   -- - Handles source system key changes
   -- - Supports SCD Type 2 (multiple rows same natural key)
   ```
   
   **2. Grain Declaration:**
   ```sql
   -- Document grain explicitly
   
   -- FactSales grain: One row per order line item
   CREATE TABLE Sales.FactSales (
       SalesKey BIGINT,           -- Unique per row
       OrderNumber VARCHAR(50),   -- Can repeat (multiple lines per order)
       LineNumber INT,            -- Line within order
       DateKey INT,
       ProductKey INT,
       Quantity INT,
       Amount DECIMAL(18,2)
   );
   
   -- FactDailySales grain: One row per product per day
   CREATE TABLE Sales.FactDailySales (
       DateKey INT,
       ProductKey INT,
       TotalQuantity INT,
       TotalAmount DECIMAL(18,2),
       PRIMARY KEY NONCLUSTERED (DateKey, ProductKey) NOT ENFORCED
   );
   ```
   
   **3. Additive vs Non-Additive Measures:**
   ```sql
   -- Additive: Can sum across all dimensions
   SELECT SUM(Quantity), SUM(Amount) FROM FactSales;  ✅
   
   -- Semi-additive: Can sum across some dimensions (not time)
   SELECT SUM(AccountBalance) FROM FactAccountSnapshot  
   WHERE SnapshotDateKey = 20260425;  -- ✅ Sum across accounts on same day
   
   SELECT SUM(AccountBalance) FROM FactAccountSnapshot;  
   -- ❌ WRONG: Summing across dates double-counts
   
   -- Non-additive: Cannot sum
   SELECT AVG(UnitPrice) FROM FactSales;  ✅ Use AVG, not SUM
   SELECT AVG(Temperature) FROM FactSensorReadings;  ✅
   ```
   
   **4. Naming Conventions:**
   ```sql
   -- Consistent naming helps understanding
   
   -- Dimensions: Dim prefix
   DimDate, DimCustomer, DimProduct
   
   -- Facts: Fact prefix
   FactSales, FactOrders, FactInventory
   
   -- Surrogate keys: TableName + "Key"
   CustomerKey, ProductKey, DateKey
   
   -- Natural keys: TableName + "ID"
   CustomerID, ProductID
   
   -- Foreign keys: Same name as referenced dimension key
   CREATE TABLE FactSales (
       CustomerKey INT,  -- Matches DimCustomer.CustomerKey
       ProductKey INT,   -- Matches DimProduct.ProductKey
       DateKey INT       -- Matches DimDate.DateKey
   );
   ```
   
   **Key Takeaways:**
   - **Star schema:** Preferred for most BI/analytics use cases (simple, fast)
   - **Snowflake schema:** Use when normalization is critical (rare in DW)
   - **Fact tables:** Store measurements, high volume, many-to-one with dimensions
   - **Dimension tables:** Store context, lower volume, descriptive attributes
   - **Surrogate keys:** Always use for joins (performance + SCD support)
   - **Grain:** Document explicitly, maintain consistency
   - **Conformed dimensions:** Share across fact tables for consistency

9. **Q:** What are Views in Fabric Data Warehouse and how do you use them effectively?
   **A:**
   Views in Fabric Data Warehouse are virtual tables defined by a SELECT query. They don't store data themselves but provide a logical layer over physical tables, simplifying queries, enforcing security, and enabling data abstraction.
   
   **View Characteristics:**
   
   ```
   Physical Tables (Storage):
   ┌─────────────┐  ┌──────────────┐  ┌─────────────┐
   │ FactSales   │  │ DimCustomer  │  │ DimProduct  │
   │ (10M rows)  │  │ (100K rows)  │  │ (50K rows)  │
   └─────────────┘  └──────────────┘  └─────────────┘
          │                │                  │
          └────────────────┼──────────────────┘
                           │
                    ┌──────▼──────┐
                    │    VIEW     │ ← Virtual table (no data stored)
                    │ vw_SalesByRegion
                    └─────────────┘
                           │
                    Query executes
                    against base tables
   ```
   
   - **Virtual:** No data storage (views store query definition only)
   - **Dynamic:** Always reflects current data in base tables
   - **Security:** Can hide columns/rows from users
   - **Abstraction:** Hide complexity from end users
   - **Read-only:** Cannot directly modify data (use base tables or INSTEAD OF triggers)
   
   ---
   
   **Basic View Syntax:**
   
   ```sql
   CREATE VIEW schema_name.view_name AS
   SELECT column1, column2, ...
   FROM table_name
   WHERE condition;
   ```
   
   **Use Case 1: Simplify Complex Queries**
   
   ```sql
   -- Complex query users need to run repeatedly:
   SELECT 
       d.Year,
       d.MonthName,
       c.Country,
       c.Region,
       p.Category,
       COUNT(DISTINCT f.CustomerKey) AS UniqueCustomers,
       COUNT(DISTINCT f.OrderNumber) AS Orders,
       SUM(f.Quantity) AS TotalQuantity,
       SUM(f.Amount) AS TotalRevenue,
       AVG(f.Amount) AS AvgOrderValue
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   GROUP BY d.Year, d.MonthName, c.Country, c.Region, p.Category;
   
   -- Create view to encapsulate complexity:
   CREATE VIEW Sales.vw_SalesSummary AS
   SELECT 
       d.Year,
       d.MonthName,
       c.Country,
       c.Region,
       p.Category,
       COUNT(DISTINCT f.CustomerKey) AS UniqueCustomers,
       COUNT(DISTINCT f.OrderNumber) AS Orders,
       SUM(f.Quantity) AS TotalQuantity,
       SUM(f.Amount) AS TotalRevenue,
       AVG(f.Amount) AS AvgOrderValue
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   GROUP BY d.Year, d.MonthName, c.Country, c.Region, p.Category;
   
   -- Users query the view (simple!):
   SELECT * FROM Sales.vw_SalesSummary
   WHERE Year = 2026 AND Country = 'USA'
   ORDER BY TotalRevenue DESC;
   ```
   
   **Use Case 2: Create Business-Friendly Layer**
   
   ```sql
   -- Technical table names and columns:
   CREATE TABLE Sales.FactSales (
       SalesKey BIGINT,
       DateKey INT,
       CustomerKey INT,
       ProductKey INT,
       Qty INT,
       Amt DECIMAL(18,2)
   );
   
   -- Business-friendly view with readable names:
   CREATE VIEW Reports.vw_SalesTransactions AS
   SELECT 
       f.SalesKey AS TransactionID,
       d.Date AS TransactionDate,
       c.CustomerName,
       c.Email AS CustomerEmail,
       c.Country,
       p.ProductName,
       p.Category AS ProductCategory,
       f.Qty AS Quantity,
       f.Amt AS SalesAmount,
       -- Calculated columns
       f.Amt / NULLIF(f.Qty, 0) AS PricePerUnit,
       CASE 
           WHEN f.Amt > 1000 THEN 'Large'
           WHEN f.Amt > 100 THEN 'Medium'
           ELSE 'Small'
       END AS OrderSize
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey;
   
   -- Business users query friendly names:
   SELECT 
       TransactionDate,
       CustomerName,
       ProductName,
       SalesAmount,
       OrderSize
   FROM Reports.vw_SalesTransactions
   WHERE Country = 'Canada'
     AND TransactionDate >= '2026-04-01';
   ```
   
   **Use Case 3: Security and Column-Level Access Control**
   
   ```sql
   -- Base table with sensitive columns:
   CREATE TABLE HR.Employees (
       EmployeeID INT PRIMARY KEY NONCLUSTERED NOT ENFORCED,
       FirstName VARCHAR(100),
       LastName VARCHAR(100),
       Email VARCHAR(100),
       PhoneNumber VARCHAR(50),
       DepartmentID INT,
       JobTitle VARCHAR(100),
       HireDate DATE,
       Salary DECIMAL(18,2),          -- Sensitive
       SSN VARCHAR(11),                -- Sensitive
       BankAccountNumber VARCHAR(50),  -- Sensitive
       ManagerID INT,
       IsActive BIT
   );
   
   -- View for general users (hide sensitive columns):
   CREATE VIEW HR.vw_EmployeeDirectory AS
   SELECT 
       EmployeeID,
       FirstName,
       LastName,
       Email,
       PhoneNumber,
       DepartmentID,
       JobTitle,
       HireDate,
       ManagerID,
       IsActive
       -- Salary, SSN, BankAccountNumber EXCLUDED
   FROM HR.Employees
   WHERE IsActive = 1;
   
   -- Grant access to view (not base table):
   GRANT SELECT ON HR.vw_EmployeeDirectory TO [DomainUsers];
   -- Users can see directory but not salaries
   
   -- View for HR managers (include sensitive data):
   CREATE VIEW HR.vw_EmployeeCompensation AS
   SELECT 
       EmployeeID,
       FirstName,
       LastName,
       DepartmentID,
       JobTitle,
       HireDate,
       Salary,
       ManagerID
       -- SSN, BankAccountNumber still excluded
   FROM HR.Employees;
   
   -- Grant to HR managers only:
   GRANT SELECT ON HR.vw_EmployeeCompensation TO [HRManagers];
   ```
   
   **Use Case 4: Row-Level Security via Views**
   
   ```sql
   -- Base table with all customer data:
   CREATE TABLE Sales.Customers (
       CustomerID INT,
       CustomerName VARCHAR(200),
       Region VARCHAR(50),
       SalesRepID INT,
       Revenue DECIMAL(18,2)
   );
   
   -- View for sales reps (only see their customers):
   CREATE VIEW Sales.vw_MyCustomers AS
   SELECT 
       CustomerID,
       CustomerName,
       Region,
       Revenue
   FROM Sales.Customers
   WHERE SalesRepID = CAST(SESSION_CONTEXT(N'UserID') AS INT);
   -- Filters to current user's customers only
   
   -- Application sets context before query:
   EXEC sp_set_session_context @key = N'UserID', @value = 123;
   
   -- Sales rep queries view (sees only their data):
   SELECT * FROM Sales.vw_MyCustomers;
   -- Returns only customers where SalesRepID = 123
   
   -- Alternative: Use SUSER_NAME() for authentication-based filtering
   CREATE VIEW Sales.vw_MyCustomersAlt AS
   SELECT 
       CustomerID,
       CustomerName,
       Region,
       Revenue
   FROM Sales.Customers c
   INNER JOIN HR.SalesReps sr 
       ON c.SalesRepID = sr.SalesRepID
   WHERE sr.EmailAddress = SUSER_NAME();
   ```
   
   **Use Case 5: Aggregate/Summary Views**
   
   ```sql
   -- Pre-aggregated summary for performance:
   CREATE VIEW Reports.vw_MonthlySalesByRegion AS
   SELECT 
       d.Year,
       d.Month,
       d.MonthName,
       c.Region,
       c.Country,
       COUNT(DISTINCT f.CustomerKey) AS UniqueCustomers,
       COUNT(DISTINCT f.OrderNumber) AS TotalOrders,
       SUM(f.Quantity) AS TotalQuantity,
       SUM(f.Amount) AS TotalRevenue,
       AVG(f.Amount) AS AvgOrderValue,
       MIN(d.Date) AS FirstSaleDate,
       MAX(d.Date) AS LastSaleDate
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   GROUP BY d.Year, d.Month, d.MonthName, c.Region, c.Country;
   
   -- Power BI connects to this view (faster than querying fact table):
   SELECT * FROM Reports.vw_MonthlySalesByRegion
   WHERE Year = 2026 AND Region = 'North America';
   ```
   
   **Use Case 6: Denormalized Views (Star Schema → Flat)**
   
   ```sql
   -- Flatten star schema for external tools or exports:
   CREATE VIEW Exports.vw_SalesFlat AS
   SELECT 
       -- Date attributes
       d.Date,
       d.Year,
       d.Quarter,
       d.Month,
       d.MonthName,
       d.DayName,
       -- Customer attributes
       c.CustomerID,
       c.CustomerName,
       c.Email,
       c.Country,
       c.Region,
       c.City,
       c.CustomerTier,
       -- Product attributes
       p.ProductID,
       p.ProductName,
       p.Category,
       p.Subcategory,
       p.Brand,
       -- Store attributes
       s.StoreID,
       s.StoreName,
       s.StoreType,
       s.City AS StoreCity,
       s.Country AS StoreCountry,
       -- Facts
       f.OrderNumber,
       f.Quantity,
       f.UnitPrice,
       f.Discount,
       f.Tax,
       f.TotalAmount
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey
   INNER JOIN Sales.DimStore s ON f.StoreKey = s.StoreKey;
   
   -- Export to CSV for external system:
   SELECT * FROM Exports.vw_SalesFlat
   WHERE Date >= '2026-04-01';
   ```
   
   **Use Case 7: Calculated Columns and Business Logic**
   
   ```sql
   -- Add business logic in view layer:
   CREATE VIEW Analytics.vw_CustomerSegmentation AS
   SELECT 
       c.CustomerKey,
       c.CustomerID,
       c.CustomerName,
       c.Country,
       -- Calculate metrics
       COUNT(DISTINCT f.OrderNumber) AS LifetimeOrders,
       SUM(f.Amount) AS LifetimeValue,
       AVG(f.Amount) AS AvgOrderValue,
       MIN(d.Date) AS FirstPurchaseDate,
       MAX(d.Date) AS LastPurchaseDate,
       DATEDIFF(DAY, MIN(d.Date), MAX(d.Date)) AS CustomerTenureDays,
       DATEDIFF(DAY, MAX(d.Date), GETDATE()) AS DaysSinceLastPurchase,
       -- Segmentation logic
       CASE 
           WHEN SUM(f.Amount) > 100000 THEN 'VIP'
           WHEN SUM(f.Amount) > 50000 THEN 'High Value'
           WHEN SUM(f.Amount) > 10000 THEN 'Medium Value'
           ELSE 'Low Value'
       END AS ValueSegment,
       CASE 
           WHEN DATEDIFF(DAY, MAX(d.Date), GETDATE()) <= 30 THEN 'Active'
           WHEN DATEDIFF(DAY, MAX(d.Date), GETDATE()) <= 90 THEN 'At Risk'
           ELSE 'Churned'
       END AS ActivityStatus,
       CASE 
           WHEN COUNT(DISTINCT f.OrderNumber) >= 10 THEN 'Frequent'
           WHEN COUNT(DISTINCT f.OrderNumber) >= 3 THEN 'Occasional'
           ELSE 'Rare'
       END AS FrequencySegment
   FROM Sales.DimCustomer c
   LEFT JOIN Sales.FactSales f ON c.CustomerKey = f.CustomerKey
   LEFT JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   GROUP BY c.CustomerKey, c.CustomerID, c.CustomerName, c.Country;
   
   -- Query segmentation:
   SELECT 
       ValueSegment,
       ActivityStatus,
       COUNT(*) AS CustomerCount,
       AVG(LifetimeValue) AS AvgLifetimeValue
   FROM Analytics.vw_CustomerSegmentation
   GROUP BY ValueSegment, ActivityStatus
   ORDER BY ValueSegment, ActivityStatus;
   ```
   
   **Use Case 8: Layered Views (View on View)**
   
   ```sql
   -- Base view: Sales with dimensions
   CREATE VIEW Sales.vw_SalesWithDimensions AS
   SELECT 
       f.SalesKey,
       d.Date,
       d.Year,
       d.Month,
       c.CustomerName,
       c.Country,
       p.ProductName,
       p.Category,
       f.Quantity,
       f.Amount
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   INNER JOIN Sales.DimProduct p ON f.ProductKey = p.ProductKey;
   
   -- Layer 2: Current year sales only
   CREATE VIEW Sales.vw_CurrentYearSales AS
   SELECT *
   FROM Sales.vw_SalesWithDimensions
   WHERE Year = YEAR(GETDATE());
   
   -- Layer 3: High-value current year sales
   CREATE VIEW Sales.vw_HighValueCurrentYearSales AS
   SELECT *
   FROM Sales.vw_CurrentYearSales
   WHERE Amount > 1000;
   
   -- Query top layer:
   SELECT Country, SUM(Amount) AS Revenue
   FROM Sales.vw_HighValueCurrentYearSales
   GROUP BY Country
   ORDER BY Revenue DESC;
   ```
   
   ---
   
   **View Management:**
   
   **Alter View:**
   ```sql
   -- Modify existing view definition:
   ALTER VIEW Sales.vw_SalesSummary AS
   SELECT 
       d.Year,
       d.MonthName,
       c.Country,
       SUM(f.Amount) AS TotalRevenue,
       COUNT(*) AS OrderCount  -- Added new column
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   GROUP BY d.Year, d.MonthName, c.Country;
   ```
   
   **Drop View:**
   ```sql
   -- Delete view:
   DROP VIEW IF EXISTS Sales.vw_SalesSummary;
   ```
   
   **View Metadata:**
   ```sql
   -- List all views in database:
   SELECT 
       SCHEMA_NAME(schema_id) AS SchemaName,
       name AS ViewName,
       create_date AS CreatedDate,
       modify_date AS ModifiedDate
   FROM sys.views
   ORDER BY SchemaName, ViewName;
   
   -- Get view definition:
   SELECT OBJECT_DEFINITION(OBJECT_ID('Sales.vw_SalesSummary'));
   
   -- Or use system view:
   SELECT definition
   FROM sys.sql_modules
   WHERE object_id = OBJECT_ID('Sales.vw_SalesSummary');
   ```
   
   **View Dependencies:**
   ```sql
   -- Find objects that depend on a view:
   SELECT 
       OBJECT_NAME(referencing_id) AS DependentObject,
       o.type_desc AS ObjectType
   FROM sys.sql_expression_dependencies d
   INNER JOIN sys.objects o ON d.referencing_id = o.object_id
   WHERE referenced_id = OBJECT_ID('Sales.vw_SalesSummary');
   
   -- Find objects a view depends on:
   SELECT 
       OBJECT_SCHEMA_NAME(referenced_id) + '.' + OBJECT_NAME(referenced_id) AS ReferencedObject,
       o.type_desc AS ObjectType
   FROM sys.sql_expression_dependencies
   WHERE referencing_id = OBJECT_ID('Sales.vw_SalesSummary');
   ```
   
   ---
   
   **Best Practices:**
   
   **1. Naming Conventions:**
   ```sql
   -- ✅ GOOD: Prefix with vw_
   CREATE VIEW Sales.vw_SalesSummary AS ...
   CREATE VIEW Reports.vw_CustomerAnalytics AS ...
   
   -- ❌ BAD: No prefix (confusing with tables)
   CREATE VIEW Sales.SalesSummary AS ...
   ```
   
   **2. Avoid SELECT *:**
   ```sql
   -- ❌ BAD: SELECT * (breaks when table schema changes)
   CREATE VIEW Sales.vw_Sales AS
   SELECT * FROM Sales.FactSales;
   
   -- ✅ GOOD: Explicit columns
   CREATE VIEW Sales.vw_Sales AS
   SELECT 
       SalesKey,
       DateKey,
       CustomerKey,
       ProductKey,
       Quantity,
       Amount
   FROM Sales.FactSales;
   ```
   
   **3. Document Views:**
   ```sql
   -- Add comments explaining view purpose
   /*
   View: Sales.vw_SalesSummary
   Purpose: Monthly sales summary by country and product category
   Used By: Power BI Monthly Sales Report
   Refresh: Real-time (no caching)
   Owner: Analytics Team
   Created: 2026-04-25
   Modified: 2026-04-25
   */
   CREATE VIEW Sales.vw_SalesSummary AS
   SELECT ...
   ```
   
   **4. Performance Considerations:**
   ```sql
   -- ❌ BAD: Complex view with subqueries
   CREATE VIEW Sales.vw_ComplexSales AS
   SELECT 
       c.CustomerName,
       (SELECT COUNT(*) FROM FactSales WHERE CustomerKey = c.CustomerKey) AS OrderCount,
       (SELECT SUM(Amount) FROM FactSales WHERE CustomerKey = c.CustomerKey) AS Revenue,
       (SELECT MAX(Date) FROM FactSales f JOIN DimDate d ON f.DateKey = d.DateKey WHERE f.CustomerKey = c.CustomerKey) AS LastOrderDate
   FROM DimCustomer c;
   -- Each subquery scans FactSales separately!
   
   -- ✅ GOOD: Single scan with joins
   CREATE VIEW Sales.vw_CustomerSales AS
   SELECT 
       c.CustomerName,
       COUNT(DISTINCT f.SalesKey) AS OrderCount,
       SUM(f.Amount) AS Revenue,
       MAX(d.Date) AS LastOrderDate
   FROM DimCustomer c
   LEFT JOIN FactSales f ON c.CustomerKey = f.CustomerKey
   LEFT JOIN DimDate d ON f.DateKey = d.DateKey
   GROUP BY c.CustomerName;
   -- Single scan with efficient aggregation
   ```
   
   **5. Security:**
   ```sql
   -- Grant SELECT on views, not base tables
   GRANT SELECT ON Sales.vw_SalesSummary TO [BusinessUsers];
   -- Don't grant access to Sales.FactSales
   
   -- Revoke if needed:
   REVOKE SELECT ON Sales.vw_SalesSummary FROM [BusinessUsers];
   ```
   
   ---
   
   **Views vs Materialized Views:**
   
   | Aspect | Regular View | Table (Alternative to Materialized View) |
   |--------|--------------|------------------------------------------|
   | **Storage** | No data stored | Data physically stored |
   | **Performance** | Query executes on base tables | Query reads pre-computed data |
   | **Freshness** | Always current | Stale (requires refresh) |
   | **Maintenance** | None | Requires periodic refresh |
   | **Use Case** | Simple queries, always fresh | Complex aggregations, refresh ok |
   
   **Workaround: Pre-Aggregated Tables (Alternative to Materialized Views):**
   ```sql
   -- Create aggregate table (like materialized view)
   CREATE TABLE Reports.MonthlySalesSummary AS
   SELECT 
       d.Year,
       d.Month,
       c.Country,
       SUM(f.Amount) AS TotalRevenue,
       COUNT(*) AS OrderCount
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   GROUP BY d.Year, d.Month, c.Country;
   
   -- Refresh nightly via pipeline:
   TRUNCATE TABLE Reports.MonthlySalesSummary;
   INSERT INTO Reports.MonthlySalesSummary
   SELECT 
       d.Year,
       d.Month,
       c.Country,
       SUM(f.Amount) AS TotalRevenue,
       COUNT(*) AS OrderCount
   FROM Sales.FactSales f
   INNER JOIN Sales.DimDate d ON f.DateKey = d.DateKey
   INNER JOIN Sales.DimCustomer c ON f.CustomerKey = c.CustomerKey
   GROUP BY d.Year, d.Month, c.Country;
   ```
   
   **Key Takeaways:**
   - **Views:** Virtual tables, no data storage, always current
   - **Use cases:** Simplify queries, security, business logic, denormalization
   - **Security:** Column-level and row-level filtering
   - **Performance:** Views don't improve performance (query runs on base tables)
   - **Naming:** Prefix with `vw_` for clarity
   - **Avoid SELECT *:** Explicit columns prevent breaking changes
   - **Layering:** Views can reference other views (use judiciously)
   - **Alternative:** Pre-aggregated tables for complex aggregations

10. **Q:** How is data governance maintained in Microsoft Fabric?
   **A:**
   Data governance in Microsoft Fabric is maintained through a comprehensive set of built-in and integrated tools that cover security, compliance, discovery, monitoring, and lifecycle management across the entire data estate.
   
   **Fabric Governance Framework:**
   
   ```
   Governance Pillars:
   ┌──────────────────────────────────────────────────────────┐
   │ 1. MANAGE                                                 │
   │    Admin Portal, Domains, Workspaces, Capacities         │
   └───────────────────────┬──────────────────────────────────┘
                           │
   ┌──────────────────────▼───────────────────────────────────┐
   │ 2. SECURE & COMPLY                                        │
   │    Information Protection, DLP, Encryption, Audit        │
   └───────────────────────┬──────────────────────────────────┘
                           │
   ┌──────────────────────▼───────────────────────────────────┐
   │ 3. DISCOVER & TRUST                                       │
   │    OneLake Data Hub, Endorsement, Lineage, Catalog       │
   └───────────────────────┬──────────────────────────────────┘
                           │
   ┌──────────────────────▼───────────────────────────────────┐
   │ 4. MONITOR & ACT                                          │
   │    Monitoring Hub, Capacity Metrics, Purview Hub         │
   └──────────────────────────────────────────────────────────┘
   ```
   
   ---
   
   **1. Organizational Structure & Access Management**
   
   **Admin Portal (Centralized Control):**
   ```
   Fabric Admin Portal manages:
   ├─ Tenant Settings (organization-wide controls)
   ├─ Capacity Management (compute resources)
   ├─ Domain Management (logical grouping)
   ├─ Workspace Management (team collaboration spaces)
   ├─ User Management (permissions and roles)
   └─ Feature Gates (enable/disable capabilities)
   
   Access: Fabric Administrator role required
   ```
   
   **Domains (Logical Grouping):**
   ```
   Domains organize data by business unit or department:
   
   Example:
   ┌─────────────────────────────────┐
   │ Finance Domain                   │
   │ ├─ Workspaces:                  │
   │ │   ├─ Finance Reporting         │
   │ │   ├─ Budget Planning           │
   │ │   └─ Audit Data                │
   │ ├─ Domain Admin: CFO             │
   │ └─ Settings: Delegated from tenant│
   └─────────────────────────────────┘
   
   ┌─────────────────────────────────┐
   │ Sales Domain                     │
   │ ├─ Workspaces:                  │
   │ │   ├─ Sales Analytics           │
   │ │   ├─ Customer Insights         │
   │ │   └─ Forecasting               │
   │ ├─ Domain Admin: VP Sales        │
   │ └─ Settings: Custom governance   │
   └─────────────────────────────────┘
   
   Benefits:
   - Better discoverability (filter by domain)
   - Delegated governance (domain-specific rules)
   - Clear ownership and accountability
   ```
   
   **Workspaces (Team Collaboration):**
   ```
   Workspace Roles:
   ┌─────────────┬─────────────────────────────────────────┐
   │ Admin       │ Full control (create, delete, share)    │
   │ Member      │ Create/edit items, manage permissions   │
   │ Contributor │ Create/edit items, no permission mgmt   │
   │ Viewer      │ Read-only access                        │
   └─────────────┴─────────────────────────────────────────┘
   
   Best Practice: Assign roles to AD groups, not individuals
   ```
   
   **Example Governance Setup:**
   ```
   Tenant: Contoso Corporation
   └─ Domain: Sales
       ├─ Workspace: Sales Analytics (Production)
       │   ├─ Admin: Sales-Admins@contoso.com (AD Group)
       │   ├─ Member: Sales-Engineers@contoso.com
       │   ├─ Viewer: Sales-Managers@contoso.com
       │   └─ Items:
       │       ├─ Warehouse: SalesWarehouse (certified)
       │       ├─ Lakehouse: RawSalesData
       │       ├─ Power BI Report: Monthly Sales Dashboard
       │       └─ Pipeline: Daily ETL
       └─ Workspace: Sales Analytics (Development)
           ├─ Admin: Sales-Admins@contoso.com
           ├─ Member: Sales-Engineers@contoso.com
           └─ Items: (development versions)
   
   Governance Controls:
   - Only Sales domain admins can certify items
   - Production workspace: only admins can edit
   - Development workspace: engineers have member access
   - Sensitivity labels required for all items
   ```
   
   ---
   
   **2. Security & Compliance**
   
   **Microsoft Purview Information Protection (Sensitivity Labels):**
   
   ```
   Sensitivity Label Hierarchy:
   ┌──────────────────────────────────────┐
   │ Public                                │
   │ - No protection                       │
   │ - Anyone can view                     │
   └──────────────────────────────────────┘
   ┌──────────────────────────────────────┐
   │ Internal                              │
   │ - Employees only                      │
   │ - Watermark applied                   │
   └──────────────────────────────────────┘
   ┌──────────────────────────────────────┐
   │ Confidential                          │
   │ - Restricted distribution             │
   │ - Encryption applied                  │
   │ - Cannot export                       │
   └──────────────────────────────────────┘
   ┌──────────────────────────────────────┐
   │ Highly Confidential                   │
   │ - Executive/Legal only                │
   │ - Encryption + auditing               │
   │ - Cannot print/copy/share             │
   └──────────────────────────────────────┘
   
   Application:
   - Manual: User selects label when creating item
   - Default: Workspace has default label (inherited)
   - Automatic: Purview DLP detects sensitive data
   ```
   
   **Example: Sensitivity Labels in Action:**
   ```
   Scenario: Finance team creates warehouse with salary data
   
   1. User creates warehouse "EmployeeCompensation"
   2. Default label "Confidential" applied (workspace setting)
   3. User loads salary data via pipeline
   4. Purview DLP scans data, detects SSN column
   5. Label auto-upgraded to "Highly Confidential"
   6. Encryption applied, export blocked
   7. Audit log captures: User X created item with HCI label
   8. Only users in "HR-Executives" group can access
   9. Attempts to share externally = blocked
   10. Item visible in OneLake hub with HCI badge
   ```
   
   **Data Loss Prevention (DLP):**
   
   ```
   DLP Policies protect sensitive data:
   
   Policy: Prevent PII Export
   ├─ Trigger: Detect SSN, Credit Card, Email patterns
   ├─ Scope: All warehouses, lakehouses, notebooks
   ├─ Action:
   │   ├─ Block: Export to external destinations
   │   ├─ Alert: Compliance team via email
   │   ├─ Educate: Show user policy tip
   │   └─ Audit: Log to Purview
   └─ Exceptions: Approved HR users
   
   Example Detection:
   CREATE TABLE HR.Employees (
       EmployeeID INT,
       Name VARCHAR(200),
       SSN VARCHAR(11),          ← DLP detects SSN pattern
       Salary DECIMAL(18,2)
   );
   
   DLP Action:
   - Label upgraded to "Confidential"
   - Export to CSV blocked
   - Alert sent to compliance team
   - User sees: "This data contains PII. Export restricted."
   ```
   
   **Row-Level Security (RLS) in Warehouse:**
   
   ```sql
   -- Scenario: Sales reps see only their region's data
   
   -- 1. Create security predicate function
   CREATE FUNCTION Security.fn_SalesRegionPredicate(@Region VARCHAR(50))
   RETURNS TABLE
   WITH SCHEMABINDING
   AS
   RETURN 
       SELECT 1 AS Result
       WHERE @Region = (
           SELECT Region 
           FROM Security.UserRegions 
           WHERE UserEmail = SUSER_NAME()
       );
   GO
   
   -- 2. Create security policy
   CREATE SECURITY POLICY Security.SalesRegionPolicy
   ADD FILTER PREDICATE Security.fn_SalesRegionPredicate(Region)
   ON Sales.FactSales
   WITH (STATE = ON);
   
   -- 3. Map users to regions
   CREATE TABLE Security.UserRegions (
       UserEmail VARCHAR(200) PRIMARY KEY,
       Region VARCHAR(50)
   );
   
   INSERT INTO Security.UserRegions VALUES
       ('john@contoso.com', 'North America'),
       ('jane@contoso.com', 'EMEA'),
       ('bob@contoso.com', 'APAC');
   
   -- Result: Users see only their region's data
   -- john@contoso.com queries: SELECT * FROM Sales.FactSales
   -- Returns: Only North America rows (RLS filters automatically)
   ```
   
   **Column-Level Security (CLS):**
   
   ```sql
   -- Hide salary column from non-HR users
   
   -- Deny SELECT on salary column
   DENY SELECT ON HR.Employees (Salary, SSN) TO [GeneralUsers];
   
   -- Grant SELECT to HR users
   GRANT SELECT ON HR.Employees (Salary, SSN) TO [HRManagers];
   
   -- Result:
   -- General users: SELECT * FROM HR.Employees → Error on Salary column
   -- HR managers: SELECT * FROM HR.Employees → Full access
   ```
   
   **Dynamic Data Masking:**
   
   ```sql
   -- Mask sensitive data for non-privileged users
   
   CREATE TABLE HR.Employees (
       EmployeeID INT,
       Name VARCHAR(200),
       Email VARCHAR(100) MASKED WITH (FUNCTION = 'email()'),
       SSN VARCHAR(11) MASKED WITH (FUNCTION = 'partial(0,"XXX-XX-",4)'),
       Salary DECIMAL(18,2) MASKED WITH (FUNCTION = 'default()')
   );
   
   -- Non-privileged user queries:
   SELECT * FROM HR.Employees;
   -- Returns:
   -- EmployeeID | Name        | Email           | SSN           | Salary
   -- 1          | John Smith  | jXXX@XXXX.com   | XXX-XX-1234   | 0.00
   
   -- Grant UNMASK permission to HR:
   GRANT UNMASK TO [HRManagers];
   
   -- HR manager queries:
   SELECT * FROM HR.Employees;
   -- Returns: Full unmasked data
   ```
   
   ---
   
   **3. Data Discovery & Trust**
   
   **OneLake Data Hub (Centralized Discovery):**
   
   ```
   OneLake Data Hub provides:
   ├─ Search: Find items across all workspaces
   ├─ Filter: By domain, item type, endorsement, tags
   ├─ Preview: Sample data without opening
   ├─ Metadata: Description, owner, last refresh
   └─ Access: Direct link to item
   
   User Experience:
   1. Open OneLake Data Hub
   2. Search: "customer sales"
   3. Filter: Domain = Sales, Certified only
   4. Result: Certified "CustomerSalesWarehouse"
   5. Click to open or add to report
   ```
   
   **Endorsement (Certification & Promotion):**
   
   ```
   Endorsement Levels:
   ┌──────────────────────────────────────┐
   │ None (Default)                        │
   │ - No endorsement badge                │
   │ - Use with caution                    │
   └──────────────────────────────────────┘
   ┌──────────────────────────────────────┐
   │ Promoted                              │
   │ - Creator believes it's high quality  │
   │ - ⬆️ badge in OneLake hub            │
   │ - Anyone can promote their items      │
   └──────────────────────────────────────┘
   ┌──────────────────────────────────────┐
   │ Certified                             │
   │ - Organization approved               │
   │ - ✓ badge in OneLake hub             │
   │ - Meets quality standards             │
   │ - Only authorized users can certify   │
   └──────────────────────────────────────┘
   
   Certification Process:
   1. Data engineer creates warehouse
   2. Run data quality tests
   3. Document in description
   4. Request certification from domain admin
   5. Domain admin reviews:
      - Data quality
      - Documentation
      - Security labels
      - Lineage
   6. If approved: Certify
   7. Item shows ✓ badge in hub
   8. Users prioritize certified items
   ```
   
   **Tags (Categorization):**
   
   ```
   Admin defines organizational tags:
   ├─ Business Unit: Finance, Sales, HR, Marketing
   ├─ Data Type: Transactional, Analytical, Reference
   ├─ Refresh Frequency: Real-time, Daily, Weekly, Monthly
   ├─ Audience: Executives, Managers, Analysts
   └─ Project: Q2-Initiative, CustomerChurn, Forecasting
   
   User applies tags to items:
   Warehouse: SalesWarehouse
   ├─ Tags: Sales, Analytical, Daily, Managers
   └─ Benefit: Searchable in OneLake hub
   
   Search Example:
   - Filter by "Sales" tag → All Sales domain items
   - Filter by "Real-time" → All streaming sources
   - Filter by "Q2-Initiative" → All project items
   ```
   
   **Lineage (Data Flow Visualization):**
   
   ```
   Lineage shows data flow across items:
   
   Example:
   Source → Transform → Destination
   
   [Azure SQL] 
       │
       ▼
   [Pipeline: DailyImport] ← Reads from Azure SQL
       │
       ▼
   [Lakehouse: RawSalesData] ← Populated by pipeline
       │
       ▼
   [Notebook: CleanSales] ← Reads RawSalesData
       │
       ▼
   [Lakehouse: CleanedSalesData] ← Written by notebook
       │
       ▼
   [Warehouse: SalesWarehouse] ← COPY INTO from CleanedSalesData
       │
       ▼
   [Power BI: Sales Dashboard] ← Connected to warehouse
   
   Impact Analysis:
   Question: What if I change Notebook CleanSales logic?
   Answer: Lineage shows:
   - Affects: CleanedSalesData lakehouse
   - Affects: SalesWarehouse
   - Affects: Sales Dashboard (Power BI)
   Action: Notify Power BI team before deployment
   ```
   
   ---
   
   **4. Monitoring & Auditing**
   
   **Monitoring Hub:**
   
   ```
   View all Fabric activities:
   ├─ Pipeline runs (success/failure)
   ├─ Notebook executions
   ├─ Data refresh status
   ├─ Query performance
   └─ Spark jobs
   
   Filtering:
   - By workspace
   - By item type
   - By status (running, succeeded, failed)
   - By time range
   
   Example:
   - View: All failed pipelines in last 24 hours
   - Filter: Workspace = Production
   - Result: "DailyETL" failed at 2 AM
   - Action: Click to see error details
   - Fix: Retry pipeline
   ```
   
   **Microsoft Purview Audit:**
   
   ```
   Audit captures all user activities:
   
   Event Types:
   ├─ Item created/deleted/modified
   ├─ Permissions granted/revoked
   ├─ Data accessed/exported
   ├─ Sensitivity label applied/changed
   ├─ DLP policy violation
   └─ Login/logout
   
   Audit Log Entry Example:
   {
     "Timestamp": "2026-04-25T10:30:15Z",
     "User": "john@contoso.com",
     "Action": "ViewReport",
     "Item": "Sales Dashboard",
     "Workspace": "Sales Analytics",
     "SensitivityLabel": "Confidential",
     "Result": "Success",
     "IPAddress": "192.168.1.100"
   }
   
   Compliance Use:
   - Who accessed sensitive salary data?
   - Who exported customer PII?
   - Who granted permissions to external user?
   - When was item deleted and by whom?
   
   Retention: 90 days default (configurable)
   Access: Purview Compliance Portal
   ```
   
   **Capacity Metrics (Resource Monitoring):**
   
   ```
   Capacity Metrics App shows:
   ├─ CU (Capacity Unit) consumption
   ├─ Utilization by workload (Warehouse, Lakehouse, Power BI)
   ├─ Throttling events
   ├─ Top consumers (workspaces, items)
   └─ Recommendations for optimization
   
   Example Alert:
   "Capacity 'Prod-F64' at 95% utilization
    Top consumer: Sales Analytics workspace (Warehouse queries)
    Recommendation: Optimize queries or upgrade capacity"
   ```
   
   **Purview Hub (Governance Insights):**
   
   ```
   Purview Hub provides dashboards:
   
   For Admins:
   ├─ Sensitivity label coverage (% items labeled)
   ├─ Endorsement summary (% certified vs promoted)
   ├─ DLP policy violations
   ├─ Unlabeled items requiring attention
   └─ Orphaned items (no owner)
   
   For Data Owners:
   ├─ My items' label status
   ├─ Endorsement recommendations
   ├─ Items nearing certification
   └─ Data quality metrics
   
   Example Insight:
   "75% of your Sales domain items are labeled
    Recommendation: Label remaining 25% by end of quarter
    Action: Click to see unlabeled items"
   ```
   
   ---
   
   **5. Microsoft Purview Integration (Enterprise Data Catalog)**
   
   **Data Map:**
   ```
   Purview scans Fabric items:
   ├─ Warehouses → Tables, columns, schemas
   ├─ Lakehouses → Files, folders, Delta tables
   ├─ Pipelines → Data flows
   └─ Power BI → Reports, datasets
   
   Metadata captured:
   - Item names and descriptions
   - Schemas and data types
   - Lineage relationships
   - Sensitivity classifications
   - Business glossary terms
   ```
   
   **Data Catalog:**
   ```
   Users search Purview Data Catalog:
   
   Search: "customer email"
   Results:
   ├─ SalesWarehouse.Customers.Email (Confidential)
   ├─ MarketingLakehouse.ContactsTable.EmailAddress (Internal)
   └─ CRMLakehouse.Leads.Email (Confidential)
   
   Details shown:
   - Data type: VARCHAR(100)
   - Sensitivity: Confidential
   - Owner: Sales team
   - Last updated: 2026-04-25
   - Lineage: Sourced from CRM
   - Related items: Power BI report "Email Campaign ROI"
   ```
   
   ---
   
   **Complete Governance Workflow Example:**
   
   **Scenario: Deploying Certified Sales Analytics Warehouse**
   
   ```
   Step 1: Workspace Setup
   - Create workspace: "Sales Analytics Production"
   - Assign to domain: "Sales"
   - Set default sensitivity label: "Confidential"
   - Assign roles:
     * Admin: Sales-Admins@contoso.com
     * Member: Sales-Engineers@contoso.com
     * Viewer: Sales-Managers@contoso.com
   
   Step 2: Create Warehouse
   - User (Sales Engineer) creates warehouse: "SalesWarehouse"
   - Inherits "Confidential" label
   - Loads data via pipeline from CRM
   - Applies RLS: Sales reps see only their region
   - Applies CLS: Hide salary data from non-HR
   
   Step 3: Data Quality & Documentation
   - Run data quality tests (no nulls, valid emails)
   - Add description: "Certified sales data warehouse. Contains customer, product, and transaction data. Refreshed daily at 2 AM."
   - Apply tags: ["Sales", "Analytical", "Daily", "Certified"]
   - Document lineage: CRM → Pipeline → Warehouse
   
   Step 4: Request Certification
   - Sales Engineer requests certification from domain admin
   - Domain admin reviews:
     ✓ Data quality passed
     ✓ Documentation complete
     ✓ Sensitivity label applied
     ✓ RLS/CLS configured
     ✓ Lineage documented
   - Domain admin certifies warehouse
   - ✓ Certified badge appears in OneLake hub
   
   Step 5: Consumption
   - Power BI team discovers certified warehouse in hub
   - Creates report connected to warehouse
   - Users search "certified sales" → Find SalesWarehouse
   - Lineage shows: CRM → Pipeline → Warehouse → Power BI
   
   Step 6: Monitoring & Auditing
   - Monitoring hub: Daily pipeline runs tracked
   - Purview audit: All data access logged
   - Capacity metrics: Warehouse query performance monitored
   - Purview hub: 100% of Sales domain certified
   
   Step 7: Ongoing Governance
   - Monthly: Domain admin reviews audit logs
   - Quarterly: Re-certify warehouse (quality check)
   - Continuous: DLP monitors for PII exports
   - Alerts: Notify if sensitivity label downgraded
   ```
   
   ---
   
   **Governance Best Practices:**
   
   **1. Establish Clear Ownership:**
   - Domain admins: Own governance for business unit
   - Workspace admins: Manage team collaboration
   - Data owners: Responsible for data quality
   - Security team: Monitor compliance
   
   **2. Implement Least Privilege:**
   - Grant minimum necessary permissions
   - Use AD groups, not individual users
   - Review permissions quarterly
   - Revoke access for offboarded users
   
   **3. Automate Where Possible:**
   - Default sensitivity labels on workspaces
   - Auto-detection of PII via DLP
   - Scheduled audits and reports
   - Alerts for policy violations
   
   **4. Educate Users:**
   - Train on sensitivity label selection
   - Explain certification process
   - Promote certified items in hub
   - Publish governance policies
   
   **5. Monitor & Iterate:**
   - Review Purview hub insights monthly
   - Track % items labeled and certified
   - Analyze audit logs for anomalies
   - Update policies based on findings
   
   **Key Takeaways:**
   - **Governance is multi-layered:** Admin portal, domains, workspaces, item permissions
   - **Security is comprehensive:** Sensitivity labels, DLP, RLS, CLS, encryption
   - **Discovery is centralized:** OneLake hub, endorsement, tags, lineage
   - **Monitoring is continuous:** Audit logs, monitoring hub, capacity metrics
   - **Purview integration:** Enterprise-wide data catalog and compliance

---

**Interview Questions:**

1. **Q:** What is Fabric Data Factory and how does it differ from Azure Data Factory?
   **A:**

1. **Q:** What is Fabric Data Factory and how does it differ from Azure Data Factory?
   **A:** 
   Fabric Data Factory is the native data integration service in Microsoft Fabric, evolved from Azure Data Factory with deep Fabric ecosystem integration.
   
   **Core Similarities:**
   - Same visual pipeline designer
   - Same activity types and connectors
   - Same authoring experience
   - Can import existing ADF pipelines
   
   **Key Differences:**
   
   | Aspect | Fabric Data Factory | Azure Data Factory |
   |--------|-------------------|-------------------|
   | **Deployment** | SaaS (fully managed) | PaaS (some config needed) |
   | **Licensing** | Fabric capacity-based | Pay-per-use (ADF pricing) |
   | **Storage** | OneLake (automatic) | Must provision storage account |
   | **Integration** | Native Fabric items | Azure services primarily |
   | **Compute** | Fabric capacity pools | Integration Runtime (IR) |
   | **Security** | Workspace-based RBAC | Azure AD + resource-level |
   | **Monitoring** | Fabric Monitoring Center | ADF Monitor + Azure Monitor |
   | **Git Integration** | Fabric Git integration | ADF Git integration |
   | **Destinations** | Optimized for OneLake | Azure services focus |
   
   **Fabric Data Factory Advantages:**
   - **Unified Experience:** Same workspace for pipelines, notebooks, warehouses
   - **OneLake Native:** Direct write to Lakehouse/Warehouse
   - **Simplified Management:** No IR provisioning for cloud sources
   - **Integrated Monitoring:** Single monitoring hub for all Fabric items
   - **Capacity Model:** Predictable costs with Fabric capacity
   
   **When to Use Fabric Data Factory:**
   - Building new Fabric-native solutions
   - Data integration within Fabric ecosystem
   - OneLake as primary destination
   - Want unified Fabric experience
   
   **When to Use Azure Data Factory:**
   - Existing ADF investments
   - Azure-only scenarios (no Fabric)
   - Need advanced IR configurations
   - Integration with Azure-specific services
   
   **Migration Path:**
   - ADF pipelines can be exported (JSON)
   - Imported into Fabric Data Factory
   - Minor adjustments for Fabric destinations
   - Test thoroughly before cutover

2. **Q:** Explain the different types of activities in Fabric Data Factory pipelines?
   **A:** 
   Activities are the building blocks of pipelines, categorized by function:
   
   **1. Data Movement Activities:**
   
   **Copy Activity:**
   - Copies data from source to sink
   - 100+ connectors
   - Schema mapping and transformations
   - Fault tolerance and retry logic
   
   ```json
   {
     "name": "CopySQLToLakehouse",
     "type": "Copy",
     "inputs": [{
       "referenceName": "SQLServerDataset",
       "type": "DatasetReference"
     }],
     "outputs": [{
       "referenceName": "LakehouseDataset",
       "type": "DatasetReference"
     }],
     "typeProperties": {
       "source": {
         "type": "SqlSource",
         "sqlReaderQuery": "SELECT * FROM Sales WHERE OrderDate >= '@{pipeline().parameters.StartDate}'"
       },
       "sink": {
         "type": "LakehouseSink",
         "tableOption": "autoCreate"
       }
     }
   }
   ```
   
   **2. Data Transformation Activities:**
   
   **Data Flow:**
   - Visual transformation designer
   - Spark-based processing
   - Complex transformations (join, aggregate, pivot)
   - Schema drift handling
   
   **Notebook Activity:**
   - Execute Spark notebooks
   - PySpark/Scala code
   - ML model training
   - Complex business logic
   
   ```json
   {
     "name": "TransformData",
     "type": "SparkNotebook",
     "typeProperties": {
       "notebook": {
         "referenceName": "Transform_Sales_Data",
         "type": "NotebookReference"
       },
       "parameters": {
         "input_path": "Files/bronze/sales",
         "output_path": "Tables/silver/sales"
       }
     }
   }
   ```
   
   **Script Activity:**
   - Execute SQL scripts
   - T-SQL in Warehouse
   - Stored procedures
   - DDL/DML operations
   
   **Stored Procedure Activity:**
   - Call database stored procedures
   - Parameter passing
   - Return values
   
   **3. Control Flow Activities:**
   
   **If Condition:**
   - Conditional branching
   - Execute different paths based on conditions
   
   ```json
   {
     "name": "CheckFileExists",
     "type": "IfCondition",
     "typeProperties": {
       "expression": {
         "value": "@greater(activity('GetMetadata').output.size, 0)",
         "type": "Expression"
       },
       "ifTrueActivities": [
         {"name": "ProcessFile", "type": "Copy"}
       ],
       "ifFalseActivities": [
         {"name": "SendAlert", "type": "WebActivity"}
       ]
     }
   }
   ```
   
   **ForEach:**
   - Loop through array/collection
   - Parallel or sequential execution
   - Process multiple files/tables
   
   ```json
   {
     "name": "ProcessAllTables",
     "type": "ForEach",
     "typeProperties": {
       "items": {
         "value": "@pipeline().parameters.TableList",
         "type": "Expression"
       },
       "isSequential": false,
       "batchCount": 4,
       "activities": [
         {"name": "CopyTable", "type": "Copy"}
       ]
     }
   }
   ```
   
   **Until:**
   - Loop until condition is met
   - Polling scenarios
   - Wait for file/status
   
   **Wait:**
   - Delay execution
   - Time-based coordination
   
   **4. External Activities:**
   
   **Web Activity:**
   - Call REST APIs
   - HTTP GET/POST/PUT/DELETE
   - Integration with external systems
   
   **Azure Function:**
   - Execute custom serverless code
   - Complex logic not in activities
   
   **5. Fabric-Specific Activities:**
   
   **Lakehouse/Warehouse Operations:**
   - Read/write tables
   - Execute SQL
   - Manage metadata
   
   **Refresh Semantic Model:**
   - Trigger Power BI dataset refresh
   - End-to-end orchestration
   
   **Activity Dependencies:**
   ```
   Activity A (Copy Data)
       ↓ (Success)
   Activity B (Transform)
       ↓ (Success)
   Activity C (Load to Warehouse)
       ↓ (Failure)
   Activity D (Send Error Notification)
   ```

3. **Q:** What is a Data Flow in Fabric Data Factory and when should you use it?
   **A:** 
   Data Flow is a visual, Spark-based transformation designer for complex data transformations without writing code.
   
   **What is Data Flow:**
   - Visual drag-and-drop interface
   - Spark execution engine
   - No code required
   - Reusable transformation logic
   - Schema drift handling
   
   **Data Flow Components:**
   
   **1. Sources:**
   - Read from datasets
   - Multiple sources in one flow
   - Schema projection
   
   **2. Transformations:**
   - **Row Modifiers:** Filter, Select, Derive Column
   - **Schema Modifiers:** Pivot, Unpivot, Flatten
   - **Aggregations:** Aggregate, Window
   - **Multiple Inputs:** Join, Union, Exists, Lookup
   - **Row Operations:** Sort, Rank
   - **Data Quality:** Assert, Distinct
   
   **3. Sinks:**
   - Write to destinations
   - Multiple sinks (branching)
   - Update methods (insert, upsert, delete)
   
   **Example Data Flow - Customer 360:**
   ```
   Source 1: SQL (Customer Master)
        ↓
   Source 2: CRM (Customer Interactions)
        ↓
   Join (Customer ID)
        ↓
   Filter (Active Customers only)
        ↓
   Derive Column (FullName = FirstName + LastName)
        ↓
   Aggregate (Total Purchases by Customer)
        ↓
   Pivot (Purchases by Product Category)
        ↓
   Sink: Lakehouse (Customer_360 table)
   ```
   
   **Data Flow vs Other Options:**
   
   **Use Data Flow When:**
   - ✅ Complex transformations (joins, aggregations)
   - ✅ Visual designer preferred
   - ✅ No coding skills
   - ✅ Reusable transformation logic
   - ✅ Large datasets (GBs to TBs)
   - ✅ Schema drift handling needed
   
   **Use Copy Activity When:**
   - ✅ Simple data movement
   - ✅ Minimal or no transformation
   - ✅ Column mapping only
   - ✅ Performance critical (faster than Data Flow)
   
   **Use Notebook When:**
   - ✅ Very complex custom logic
   - ✅ ML/AI transformations
   - ✅ Need full Spark capabilities
   - ✅ Python/Scala required
   - ✅ Iterative development
   
   **Use Dataflow Gen2 When:**
   - ✅ Self-service by business users
   - ✅ Power Query familiarity
   - ✅ Smaller datasets (<1GB)
   - ✅ Quick data prep
   
   **Data Flow Example - SCD Type 2:**
   ```
   Source: Incoming Customer Changes
        ↓
   Lookup: Existing Customer Dimension
        ↓
   Conditional Split:
   ├─→ New Records → Insert with StartDate
   ├─→ Changed Records → 
   │   ├─→ Expire old (set EndDate)
   │   └─→ Insert new version
   └─→ Unchanged → Skip
        ↓
   Sink: Customer Dimension (Lakehouse)
   ```
   
   **Performance Tips:**
   - Enable partition optimization
   - Use broadcast joins for small lookups
   - Minimize aggregations
   - Filter early in the flow
   - Use appropriate data integration units

4. **Q:** How do you implement error handling and retry logic in Fabric Data Factory pipelines?
   **A:** 
   
   **1. Activity-Level Retry:**
   
   Every activity has built-in retry settings:
   
   ```json
   {
     "name": "CopyDataWithRetry",
     "type": "Copy",
     "policy": {
       "retry": 3,
       "retryIntervalInSeconds": 30,
       "secureOutput": false,
       "secureInput": false,
       "timeout": "01:00:00"
     },
     "typeProperties": {
       "source": {...},
       "sink": {...}
     }
   }
   ```
   
   **Configuration:**
   - **retry:** Number of retry attempts (0-3)
   - **retryIntervalInSeconds:** Wait time between retries
   - **timeout:** Maximum execution time
   
   **2. Dependency Conditions:**
   
   Control execution based on previous activity outcome:
   
   ```json
   {
     "name": "ProcessOnSuccess",
     "dependsOn": [{
       "activity": "CopyData",
       "dependencyConditions": ["Succeeded"]
     }]
   }
   
   {
     "name": "SendErrorAlert",
     "dependsOn": [{
       "activity": "CopyData",
       "dependencyConditions": ["Failed"]
     }]
   }
   
   {
     "name": "AlwaysExecute",
     "dependsOn": [{
       "activity": "CopyData",
       "dependencyConditions": ["Completed"]  // Success or Failure
     }]
   }
   ```
   
   **3. Try-Catch Pattern (Until Loop):**
   
   ```
   Until Activity (Retry up to 3 times)
   ├── Copy Data Activity
   ├── If Condition (Check if succeeded)
   │   ├── True: Set Variable (ExitLoop = true)
   │   └── False: Wait 60 seconds
   └── Loop Exit Condition: @variables('ExitLoop') OR @greater(variables('RetryCount'), 3)
   ```
   
   **4. Error Handling Pipeline:**
   
   **Main Pipeline:**
   ```
   Try:
       Execute Pipeline: DataProcessing
   Catch (on failure):
       Execute Pipeline: ErrorHandling
           ├── Log Error to Table
           ├── Send Email Notification
           └── Create Service Ticket
   ```
   
   **5. Logging and Monitoring:**
   
   **Custom Logging:**
   ```python
   # In Notebook Activity
   import logging
   from datetime import datetime
   
   try:
       # Data processing
       df = spark.read.parquet("Files/input/")
       df.write.format("delta").save("Tables/output")
       
       # Log success
       log_df = spark.createDataFrame([{
           "PipelineRunId": dbutils.widgets.get("PipelineRunId"),
           "Status": "Success",
           "RowsProcessed": df.count(),
           "Timestamp": datetime.now()
       }])
       log_df.write.format("delta").mode("append").save("Tables/pipeline_logs")
       
   except Exception as e:
       # Log error
       error_log = spark.createDataFrame([{
           "PipelineRunId": dbutils.widgets.get("PipelineRunId"),
           "Status": "Failed",
           "ErrorMessage": str(e),
           "Timestamp": datetime.now()
       }])
       error_log.write.format("delta").mode("append").save("Tables/pipeline_errors")
       raise
   ```
   
   **6. Web Activity for Alerts:**
   
   ```json
   {
     "name": "SendTeamsAlert",
     "type": "WebActivity",
     "dependsOn": [{
       "activity": "DataProcessing",
       "dependencyConditions": ["Failed"]
     }],
     "typeProperties": {
       "url": "https://outlook.office.com/webhook/...",
       "method": "POST",
       "body": {
         "text": "Pipeline @{pipeline().Pipeline} failed at @{utcnow()}"
       }
     }
   }
   ```
   
   **7. Validation Activity:**
   
   ```json
   {
     "name": "ValidateFileExists",
     "type": "Validation",
     "typeProperties": {
       "dataset": {
         "referenceName": "InputFile"
       },
       "timeout": "00:10:00",
       "sleep": 60,
       "minimumSize": 100
     }
   }
   ```
   
   **Best Practices:**
   - Set appropriate timeouts for long-running activities
   - Use exponential backoff for retries
   - Log all errors with context (pipeline name, run ID, timestamp)
   - Implement alerts for critical failures
   - Use validation activities before processing
   - Handle transient vs. permanent errors differently
   - Clean up partial data on failure
   - Document error codes and remediation steps

5. **Q:** How do you parameterize pipelines in Fabric Data Factory?
   **A:** 
   Parameterization makes pipelines reusable and dynamic by passing values at runtime.
   
   **Types of Parameters:**
   
   **1. Pipeline Parameters:**
   - Defined at pipeline level
   - Passed when triggering pipeline
   - Used in activities and expressions
   
   **Definition:**
   ```json
   {
     "name": "GenericCopyPipeline",
     "parameters": {
       "SourceTable": {
         "type": "String",
         "defaultValue": "Sales"
       },
       "StartDate": {
         "type": "String"
       },
       "LoadType": {
         "type": "String",
         "defaultValue": "Incremental"
       }
     }
   }
   ```
   
   **Usage in Activities:**
   ```json
   {
     "name": "CopyData",
     "type": "Copy",
     "typeProperties": {
       "source": {
         "query": "SELECT * FROM @{pipeline().parameters.SourceTable} 
                   WHERE ModifiedDate >= '@{pipeline().parameters.StartDate}'"
       }
     }
   }
   ```
   
   **2. Dataset Parameters:**
   - Defined at dataset level
   - Make datasets dynamic
   - Reuse single dataset for multiple tables/files
   
   **Dataset Definition:**
   ```json
   {
     "name": "GenericSQLDataset",
     "type": "SqlServerTable",
     "parameters": {
       "TableName": {
         "type": "String"
       },
       "SchemaName": {
         "type": "String",
         "defaultValue": "dbo"
       }
     },
     "typeProperties": {
       "tableName": {
         "value": "@dataset().TableName",
         "type": "Expression"
       },
       "schema": {
         "value": "@dataset().SchemaName",
         "type": "Expression"
       }
     }
   }
   ```
   
   **Usage in Pipeline:**
   ```json
   {
     "name": "CopyActivity",
     "inputs": [{
       "referenceName": "GenericSQLDataset",
       "parameters": {
         "TableName": "@pipeline().parameters.SourceTable",
         "SchemaName": "sales"
       }
     }]
   }
   ```
   
   **3. System Variables:**
   - Built-in values
   - Pipeline metadata
   - Execution context
   
   **Available Variables:**
   ```
   @pipeline().Pipeline - Pipeline name
   @pipeline().RunId - Unique run ID
   @pipeline().TriggerType - Manual, Schedule, Event
   @pipeline().TriggerName - Trigger name
   @pipeline().TriggerTime - Trigger timestamp
   @pipeline().DataFactory - Data Factory name
   
   @trigger().startTime - Trigger start time
   @trigger().scheduledTime - Scheduled time
   
   @activity('ActivityName').output - Activity output
   @item() - Current item in ForEach loop
   ```
   
   **4. Variables:**
   - Mutable values within pipeline
   - Set and modify during execution
   
   **Definition:**
   ```json
   {
     "name": "ProcessingPipeline",
     "variables": {
       "ErrorCount": {
         "type": "Integer",
         "defaultValue": 0
       },
       "ProcessedTables": {
         "type": "Array"
       },
       "LastWatermark": {
         "type": "String"
       }
     }
   }
   ```
   
   **Set Variable Activity:**
   ```json
   {
     "name": "IncrementErrorCount",
     "type": "SetVariable",
     "typeProperties": {
       "variableName": "ErrorCount",
       "value": {
         "value": "@add(variables('ErrorCount'), 1)",
         "type": "Expression"
       }
     }
   }
   ```
   
   **5. Dynamic Content Expressions:**
   
   **String Functions:**
   ```
   @concat('Table_', pipeline().parameters.Date)
   @substring(pipeline().parameters.FilePath, 0, 10)
   @replace(pipeline().parameters.TableName, '_', '-')
   @toLower(pipeline().parameters.Environment)
   ```
   
   **Date Functions:**
   ```
   @utcnow()
   @formatDateTime(utcnow(), 'yyyy-MM-dd')
   @addDays(utcnow(), -1)
   @startOfDay(utcnow())
   ```
   
   **Logical Functions:**
   ```
   @if(equals(pipeline().parameters.LoadType, 'Full'), 'FullLoad', 'IncrementalLoad')
   @and(greater(activity('CopyData').output.rowsCopied, 0), equals(pipeline().parameters.Validate, true))
   ```
   
   **Real-World Example - Multi-Table Copy:**
   
   **Pipeline Parameters:**
   ```json
   {
     "TableList": ["Sales", "Customers", "Products"],
     "Environment": "Production",
     "LoadDate": "2026-04-24"
   }
   ```
   
   **ForEach Activity:**
   ```json
   {
     "name": "ForEachTable",
     "type": "ForEach",
     "typeProperties": {
       "items": {
         "value": "@pipeline().parameters.TableList"
       },
       "activities": [{
         "name": "CopyTable",
         "type": "Copy",
         "inputs": [{
           "referenceName": "GenericSQLDataset",
           "parameters": {
             "TableName": "@item()"
           }
         }],
         "outputs": [{
           "referenceName": "LakehouseDataset",
           "parameters": {
             "TableName": "@concat(toLower(item()), '_', pipeline().parameters.LoadDate)",
             "FolderPath": "@concat(pipeline().parameters.Environment, '/bronze/', item())"
           }
         }]
       }]
     }
   }
   ```
   
   **Best Practices:**
   - Use parameters for environment-specific values
   - Default values for optional parameters
   - Validate parameter values early
   - Use meaningful parameter names
   - Document parameter purpose and format
   - Use variables for mutable state
   - Minimize variable updates (performance)

6. **Q:** What are triggers in Fabric Data Factory and how do you schedule pipelines?
   **A:** 
   Triggers define when and how pipelines execute, supporting scheduled, event-based, and manual execution.
   
   **Types of Triggers:**
   
   **1. Schedule Trigger:**
   - Time-based execution
   - Cron-like scheduling
   - Recurring patterns
   
   **Simple Schedule:**
   ```json
   {
     "name": "DailyRefresh",
     "type": "ScheduleTrigger",
     "typeProperties": {
       "recurrence": {
         "frequency": "Day",
         "interval": 1,
         "startTime": "2026-01-01T02:00:00Z",
         "timeZone": "UTC",
         "schedule": {
           "hours": [2],
           "minutes": [0]
         }
       }
     }
   }
   ```
   
   **Complex Schedule (Business Days Only):**
   ```json
   {
     "name": "WeekdayRefresh",
     "type": "ScheduleTrigger",
     "typeProperties": {
       "recurrence": {
         "frequency": "Week",
         "interval": 1,
         "startTime": "2026-01-01T06:00:00Z",
         "schedule": {
           "weekDays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
           "hours": [6, 14],
           "minutes": [0]
         }
       }
     }
   }
   ```
   
   **2. Tumbling Window Trigger:**
   - Processes data in consecutive time windows
   - Handles backfill automatically
   - Dependency on previous runs
   - Better for incremental loads
   
   ```json
   {
     "name": "HourlyProcessing",
     "type": "TumblingWindowTrigger",
     "typeProperties": {
       "frequency": "Hour",
       "interval": 1,
       "startTime": "2026-01-01T00:00:00Z",
       "delay": "00:15:00",
       "maxConcurrency": 3,
       "retryPolicy": {
         "count": 3,
         "intervalInSeconds": 300
       }
     }
   }
   ```
   
   **Window Functions in Pipeline:**
   ```
   @trigger().outputs.windowStartTime  // 2026-04-24T10:00:00Z
   @trigger().outputs.windowEndTime    // 2026-04-24T11:00:00Z
   ```
   
   **Use in Query:**
   ```sql
   SELECT * FROM Sales
   WHERE OrderTime >= '@{trigger().outputs.windowStartTime}'
     AND OrderTime < '@{trigger().outputs.windowEndTime}'
   ```
   
   **3. Event-Based Trigger (Storage Events):**
   - File arrival/deletion triggers
   - Blob storage events
   - OneLake file events
   
   ```json
   {
     "name": "FileArrivalTrigger",
     "type": "BlobEventsTrigger",
     "typeProperties": {
       "blobPathBeginsWith": "/container/input/",
       "blobPathEndsWith": ".csv",
       "ignoreEmptyBlobs": true,
       "events": ["Microsoft.Storage.BlobCreated"]
     }
   }
   ```
   
   **4. Manual Trigger:**
   - On-demand execution
   - Debug runs
   - Testing
   
   **Trigger Comparison:**
   
   | Trigger Type | Use Case | Benefits | Limitations |
   |-------------|----------|----------|-------------|
   | **Schedule** | Regular batch jobs | Simple, flexible timing | No dependency handling |
   | **Tumbling Window** | Incremental processing | Backfill, dependencies | More complex setup |
   | **Event-Based** | File arrival | Real-time response | Limited to storage events |
   | **Manual** | Testing, ad-hoc | Full control | Manual intervention |
   
   **5. Trigger Dependencies (Tumbling Window):**
   
   **Self-Dependency:**
   ```json
   {
     "name": "IncrementalLoad",
     "type": "TumblingWindowTrigger",
     "typeProperties": {
       "frequency": "Hour",
       "interval": 1,
       "dependsOn": [{
         "type": "SelfDependencyTumblingWindowTriggerReference",
         "offset": "-01:00:00",
         "size": "01:00:00"
       }]
     }
   }
   ```
   - Wait for previous window to complete before starting next
   - Ensures sequential processing
   
   **Cross-Pipeline Dependency:**
   ```json
   {
     "name": "GoldLayerProcessing",
     "dependsOn": [{
       "type": "TumblingWindowTriggerDependencyReference",
       "referenceTrigger": {
         "referenceName": "SilverLayerProcessing",
         "type": "TriggerReference"
       },
       "offset": "00:00:00"
     }]
   }
   ```
   - Silver layer completes → Gold layer starts
   - Orchestrate multi-stage pipelines
   
   **6. Best Practices:**
   
   **Scheduling:**
   - Schedule during low-usage hours
   - Stagger dependent pipelines
   - Use tumbling window for incremental loads
   - Set appropriate timeouts
   - Consider time zones carefully
   
   **Error Handling:**
   - Configure retry policies
   - Monitor failed runs
   - Set up alerts for failures
   - Implement circuit breakers
   
   **Performance:**
   - Limit concurrent runs
   - Avoid overlapping executions
   - Use appropriate trigger granularity
   - Monitor trigger lag
   
   **Real-World Example - Medallion Architecture:**
   
   ```
   Bronze Layer (every 15 minutes):
   └── Tumbling Window: 15 min interval
       ├── Ingest from sources
       └── Write raw data
   
   Silver Layer (every hour, after Bronze):
   └── Tumbling Window: 1 hour interval
       ├── Depends on: Bronze layer
       ├── Clean and validate
       └── Write curated data
   
   Gold Layer (daily at 6 AM, after Silver):
   └── Schedule Trigger: Daily 6 AM
       ├── Check: Silver layer complete
       ├── Aggregate and model
       └── Write to Warehouse
   
   Power BI Refresh (daily at 7 AM):
   └── Schedule Trigger: Daily 7 AM
       └── Refresh semantic models
   ```

7. **Q:** How do you monitor and troubleshoot Fabric Data Factory pipelines?
   **A:** 
   
   **1. Fabric Monitoring Hub:**
   
   **Access:**
   - Workspace → Monitoring Hub
   - View all pipeline runs
   - Filter by status, time, pipeline
   
   **Key Metrics:**
   - Pipeline run status (Succeeded, Failed, InProgress)
   - Start time and duration
   - Trigger type
   - Activity-level details
   
   **2. Pipeline Run Details:**
   
   **Run Overview:**
   ```
   Pipeline: DailySalesRefresh
   Run ID: 12345-abcde-67890
   Status: Failed
   Start Time: 2026-04-24 02:00:00
   End Time: 2026-04-24 02:15:23
   Duration: 00:15:23
   Trigger: Schedule (DailyRefresh)
   ```
   
   **Activity View:**
   - Visual pipeline with activity status
   - Green = Success
   - Red = Failed
   - Blue = In Progress
   - Gray = Skipped
   
   **Activity Details:**
   ```
   Activity: CopyData
   Status: Failed
   Duration: 00:10:15
   Rows Read: 1,234,567
   Rows Written: 0
   Error Code: UserErrorFailedToConnectToSqlServer
   Error Message: Cannot open server requested by login
   ```
   
   **3. Activity Output:**
   
   ```json
   {
     "executionDetails": {
       "rowsRead": 1234567,
       "rowsCopied": 1234567,
       "rowsSkipped": 0,
       "copyDuration": 615,
       "throughput": 2008.28,
       "errors": [],
       "effectiveIntegrationRuntime": "FabricRuntime",
       "usedDataIntegrationUnits": 8,
       "billedDuration": 615
     }
   }
   ```
   
   **4. Diagnostic Logging:**
   
   **Enable Logging:**
   - Pipeline settings → Diagnostic logs
   - Send to Log Analytics workspace
   - Configure retention
   
   **Query Logs (KQL):**
   ```kql
   ADFPipelineRun
   | where TimeGenerated > ago(24h)
   | where Status == "Failed"
   | summarize FailureCount = count() by PipelineName, ErrorMessage
   | order by FailureCount desc
   ```
   
   **5. Common Errors and Solutions:**
   
   **Connection Errors:**
   ```
   Error: Cannot connect to source
   Solutions:
   - Check credentials/connection string
   - Verify network connectivity
   - Check firewall rules
   - Validate self-hosted IR is running
   ```
   
   **Timeout Errors:**
   ```
   Error: Activity execution timeout
   Solutions:
   - Increase timeout setting
   - Optimize query performance
   - Reduce data volume
   - Check source system performance
   ```
   
   **Permission Errors:**
   ```
   Error: Access denied to destination
   Solutions:
   - Grant workspace permissions
   - Check Lakehouse/Warehouse access
   - Verify service principal permissions
   ```
   
   **6. Performance Monitoring:**
   
   **Copy Activity Metrics:**
   - Data Integration Units (DIU) used
   - Throughput (MB/s)
   - Rows read vs. written
   - Parallel copy count
   
   **Optimization Tips:**
   ```
   Slow copy performance:
   - Increase DIU (4, 8, 16, 32)
   - Enable parallel copies
   - Use binary copy for same formats
   - Partition large datasets
   - Optimize source queries
   ```
   
   **7. Alerting:**
   
   **Set Up Alerts:**
   ```
   Monitoring Hub → Create Alert
   Conditions:
   - Pipeline run failed
   - Pipeline duration > threshold
   - Activity error rate > X%
   
   Actions:
   - Email notification
   - Teams message
   - Create service ticket
   ```
   
   **Web Activity Alert:**
   ```json
   {
     "name": "SendFailureAlert",
     "type": "WebActivity",
     "dependsOn": [{
       "activity": "DataProcessing",
       "dependencyConditions": ["Failed"]
     }],
     "typeProperties": {
       "url": "https://prod-XX.eastus.logic.azure.com:443/workflows/.../triggers/manual/paths/invoke",
       "method": "POST",
       "body": {
         "pipelineName": "@{pipeline().Pipeline}",
         "runId": "@{pipeline().RunId}",
         "errorMessage": "@{activity('DataProcessing').error.message}",
         "timestamp": "@{utcnow()}"
       }
     }
   }
   ```
   
   **8. Best Practices:**
   
   **Monitoring:**
   - ✅ Review failed runs daily
   - ✅ Set up alerts for critical pipelines
   - ✅ Monitor duration trends
   - ✅ Track data volumes
   - ✅ Use diagnostic logging
   
   **Troubleshooting:**
   - ✅ Check activity output for details
   - ✅ Test connections before running
   - ✅ Use debug mode for testing
   - ✅ Implement comprehensive logging
   - ✅ Document common errors and fixes
   
   **Performance:**
   - ✅ Monitor DIU usage
   - ✅ Optimize slow activities
   - ✅ Review execution patterns
   - ✅ Identify bottlenecks
   - ✅ Benchmark after changes

---

### Static vs Dynamic Pipelines

**Definition:**
Static and Dynamic pipelines represent two fundamental design approaches in Fabric Data Factory. **Static pipelines** have hardcoded values and fixed logic for specific tasks, while **Dynamic pipelines** use parameters, variables, and expressions to create reusable, configurable workflows that adapt to different inputs at runtime.

**Static Pipelines:**

**Characteristics:**
- Fixed table names, file paths, connection strings
- Hardcoded queries and filters
- Single-purpose design
- No runtime configuration
- Simple and straightforward
- Easy to understand and debug

**Example - Static Pipeline:**
```json
{
  "name": "CopySalesTable2026",
  "activities": [{
    "name": "CopySales",
    "type": "Copy",
    "inputs": [{
      "referenceName": "SQLServerSales",
      "type": "DatasetReference"
    }],
    "outputs": [{
      "referenceName": "LakehouseSales",
      "type": "DatasetReference"
    }],
    "typeProperties": {
      "source": {
        "type": "SqlSource",
        "sqlReaderQuery": "SELECT * FROM dbo.Sales WHERE Year = 2026"
      },
      "sink": {
        "type": "LakehouseSink",
        "tableName": "Sales_2026"
      }
    }
  }]
}
```

**Problem with Static Approach:**
- Need separate pipeline for each table
- 10 tables = 10 pipelines
- Changes require updating multiple pipelines
- Difficult to maintain at scale
- Code duplication

**Dynamic Pipelines:**

**Characteristics:**
- Uses parameters and variables
- Runtime configuration
- Reusable across multiple scenarios
- Metadata-driven execution
- Expression-based logic
- Enterprise-ready

**Example - Dynamic Pipeline:**
```json
{
  "name": "GenericCopyPipeline",
  "parameters": {
    "SourceSchema": {"type": "String"},
    "TableName": {"type": "String"},
    "FilterYear": {"type": "String"},
    "DestinationPath": {"type": "String"}
  },
  "activities": [{
    "name": "CopyData",
    "type": "Copy",
    "inputs": [{
      "referenceName": "GenericSQLDataset",
      "type": "DatasetReference",
      "parameters": {
        "SchemaName": "@pipeline().parameters.SourceSchema",
        "TableName": "@pipeline().parameters.TableName"
      }
    }],
    "outputs": [{
      "referenceName": "GenericLakehouseDataset",
      "type": "DatasetReference",
      "parameters": {
        "TablePath": "@pipeline().parameters.DestinationPath"
      }
    }],
    "typeProperties": {
      "source": {
        "type": "SqlSource",
        "sqlReaderQuery": {
          "value": "SELECT * FROM @{pipeline().parameters.SourceSchema}.@{pipeline().parameters.TableName} WHERE Year = @{pipeline().parameters.FilterYear}",
          "type": "Expression"
        }
      }
    }
  }]
}
```

**Invoke with different parameters:**
```json
// Copy Sales
{"SourceSchema": "dbo", "TableName": "Sales", "FilterYear": "2026", "DestinationPath": "Sales_2026"}

// Copy Customers
{"SourceSchema": "dbo", "TableName": "Customers", "FilterYear": "2026", "DestinationPath": "Customers_2026"}

// Copy Products
{"SourceSchema": "catalog", "TableName": "Products", "FilterYear": "2026", "DestinationPath": "Products_2026"}
```

**Result:** 1 pipeline handles all 3 tables (or 100 tables!)

**Metadata-Driven Pattern:**

**Most Advanced Approach:**
Store configuration in a control table and loop through it.

**Control Table:**
```sql
CREATE TABLE PipelineConfig (
    TableName VARCHAR(100),
    SourceSchema VARCHAR(50),
    SourceTable VARCHAR(100),
    DestinationPath VARCHAR(200),
    FilterColumn VARCHAR(50),
    FilterValue VARCHAR(50),
    IsActive BIT
);

INSERT INTO PipelineConfig VALUES
('Sales', 'dbo', 'Sales', 'Tables/Sales', 'Year', '2026', 1),
('Customers', 'dbo', 'Customers', 'Tables/Customers', 'Year', '2026', 1),
('Products', 'catalog', 'Products', 'Tables/Products', 'Year', '2026', 1),
('Orders', 'dbo', 'Orders', 'Tables/Orders', 'Year', '2026', 0);  -- Inactive
```

**Metadata-Driven Pipeline:**
```
Pipeline: MetadataDrivenCopy
├── Lookup Activity: Get active tables from PipelineConfig
│   └── Query: SELECT * FROM PipelineConfig WHERE IsActive = 1
├── ForEach Activity: Loop through table list
│   └── Items: @activity('Lookup').output.value
│       └── Copy Activity
│           ├── SourceSchema: @item().SourceSchema
│           ├── SourceTable: @item().SourceTable
│           ├── DestinationPath: @item().DestinationPath
│           └── Filter: @{item().FilterColumn} = @{item().FilterValue}
```

**Benefits:**
- Add new tables by inserting rows (no pipeline changes!)
- Enable/disable tables with IsActive flag
- Centralized configuration
- Non-technical users can manage
- Audit trail in database

**Interview Questions:**

1. **Q:** What are Static and Dynamic pipelines, and when would you use each?
   **A:** 
   Static and Dynamic pipelines represent different design philosophies for data integration workflows.
   
   **Static Pipelines:**
   - Hardcoded values (table names, paths, queries)
   - Single-purpose design
   - Fixed at development time
   - Simple to understand
   
   **Example Use Cases:**
   - One-off data migration
   - Unique business logic
   - Proof of concept
   - Quick prototypes
   - Simple, non-repetitive tasks
   
   **Dynamic Pipelines:**
   - Parameterized values
   - Reusable across scenarios
   - Configured at runtime
   - Metadata-driven
   
   **Example Use Cases:**
   - Multi-table loading (10+ tables)
   - Environment promotion (Dev → Test → Prod)
   - Incremental loads with varying dates
   - Standardized ETL patterns
   - Enterprise data warehouses
   
   **Comparison:**
   
   | Scenario | Static | Dynamic |
   |----------|--------|---------|
   | **10 Tables to Copy** | 10 pipelines | 1 pipeline with ForEach |
   | **Add New Table** | Create new pipeline | Add parameter value |
   | **Change Logic** | Update all pipelines | Update one pipeline |
   | **Maintenance** | High effort | Low effort |
   | **Initial Complexity** | Low | Medium |
   | **Long-term Value** | Low | High |
   | **Best For** | Simple/unique tasks | Repetitive patterns |
   
   **Decision Tree:**
   ```
   Is the task repetitive across multiple objects (tables/files)?
   ├── Yes → Use Dynamic Pipeline
   │   └── Store config in metadata table for easy management
   └── No → Is this a one-time task?
       ├── Yes → Static is fine
       └── No → Still prefer Dynamic for future flexibility
   ```
   
   **Real-World Recommendation:**
   - **Prototype/POC:** Start with Static for speed
   - **Production:** Always use Dynamic for maintainability
   - **Enterprise:** Use Metadata-Driven (Dynamic + Control Table)

2. **Q:** How do you implement a metadata-driven pipeline architecture?
   **A:** 
   Metadata-driven architecture stores pipeline configuration in database tables, making pipelines fully data-driven without code changes.
   
   **Architecture Components:**
   
   **1. Metadata Tables:**
   
   **PipelineConfig (What to process):**
   ```sql
   CREATE TABLE PipelineConfig (
       ConfigID INT PRIMARY KEY,
       PipelineName VARCHAR(100),
       SourceType VARCHAR(50),      -- 'SQL', 'Blob', 'API'
       SourceConnectionName VARCHAR(100),
       SourceSchema VARCHAR(50),
       SourceTable VARCHAR(100),
       DestinationType VARCHAR(50),  -- 'Lakehouse', 'Warehouse'
       DestinationPath VARCHAR(200),
       LoadType VARCHAR(20),         -- 'Full', 'Incremental'
       WatermarkColumn VARCHAR(50),
       IsActive BIT,
       Priority INT,                 -- Execution order
       CreatedDate DATETIME,
       ModifiedDate DATETIME
   );
   ```
   
   **PipelineExecution (Track runs):**
   ```sql
   CREATE TABLE PipelineExecution (
       ExecutionID INT PRIMARY KEY,
       ConfigID INT,
       PipelineRunID VARCHAR(100),
       StartTime DATETIME,
       EndTime DATETIME,
       Status VARCHAR(20),           -- 'Success', 'Failed', 'Running'
       RowsRead INT,
       RowsWritten INT,
       ErrorMessage VARCHAR(MAX),
       Duration INT
   );
   ```
   
   **Watermark Table (Incremental loads):**
   ```sql
   CREATE TABLE Watermark (
       TableName VARCHAR(100) PRIMARY KEY,
       WatermarkValue DATETIME,
       LastUpdateTime DATETIME
   );
   ```
   
   **2. Pipeline Implementation:**
   
   **Main Pipeline Structure:**
   ```
   Pipeline: MetadataDrivenETL
   ├── Lookup Activity: Get Active Configurations
   │   └── Query: SELECT * FROM PipelineConfig 
   │              WHERE IsActive = 1 
   │              ORDER BY Priority
   │
   ├── ForEach Activity: Process Each Config
   │   ├── Items: @activity('GetConfigs').output.value
   │   ├── IsSequential: False (parallel execution)
   │   ├── BatchCount: 4
   │   │
   │   └── Activities:
   │       ├── If Condition: Check Load Type
   │       │   ├── Condition: @equals(item().LoadType, 'Incremental')
   │       │   │
   │       │   ├── True Branch (Incremental):
   │       │   │   ├── Lookup: Get Last Watermark
   │       │   │   ├── Copy Activity: Load Incremental Data
   │       │   │   └── Stored Procedure: Update Watermark
   │       │   │
   │       │   └── False Branch (Full):
   │       │       └── Copy Activity: Load Full Data
   │       │
   │       ├── Stored Procedure: Log Execution
   │       │   └── Parameters: ConfigID, Status, Rows, Duration
   │       │
   │       └── Web Activity: Send Alert (on failure only)
   ```
   
   **3. Copy Activity (Dynamic):**
   ```json
   {
     "name": "DynamicCopy",
     "type": "Copy",
     "inputs": [{
       "referenceName": "GenericSourceDataset",
       "parameters": {
         "ConnectionName": "@item().SourceConnectionName",
         "SchemaName": "@item().SourceSchema",
         "TableName": "@item().SourceTable"
       }
     }],
     "outputs": [{
       "referenceName": "GenericLakehouseDataset",
       "parameters": {
         "Path": "@item().DestinationPath"
       }
     }],
     "typeProperties": {
       "source": {
         "type": "SqlSource",
         "sqlReaderQuery": {
           "value": "@if(equals(item().LoadType, 'Incremental'),
                        concat('SELECT * FROM ', item().SourceSchema, '.', item().SourceTable, 
                               ' WHERE ', item().WatermarkColumn, ' > ''', activity('GetWatermark').output.firstRow.WatermarkValue, ''''),
                        concat('SELECT * FROM ', item().SourceSchema, '.', item().SourceTable))",
           "type": "Expression"
         }
       }
     }
   }
   ```
   
   **4. Logging Stored Procedure:**
   ```sql
   CREATE PROCEDURE LogPipelineExecution
       @ConfigID INT,
       @PipelineRunID VARCHAR(100),
       @Status VARCHAR(20),
       @RowsRead INT,
       @RowsWritten INT,
       @ErrorMessage VARCHAR(MAX) = NULL
   AS
   BEGIN
       INSERT INTO PipelineExecution (
           ConfigID, PipelineRunID, StartTime, EndTime, 
           Status, RowsRead, RowsWritten, ErrorMessage, Duration
       )
       VALUES (
           @ConfigID,
           @PipelineRunID,
           DATEADD(SECOND, -@Duration, GETDATE()),
           GETDATE(),
           @Status,
           @RowsRead,
           @RowsWritten,
           @ErrorMessage,
           @Duration
       );
   END;
   ```
   
   **5. Benefits:**
   
   **Operational:**
   - ✅ Add tables without deploying code
   - ✅ Enable/disable loads with SQL UPDATE
   - ✅ Change priorities on the fly
   - ✅ Business users can manage config
   - ✅ Single pipeline for 100+ tables
   
   **Monitoring:**
   - ✅ Centralized execution history
   - ✅ Performance metrics per table
   - ✅ Easy troubleshooting
   - ✅ Audit trail
   
   **Governance:**
   - ✅ Configuration versioning
   - ✅ Approval workflows (via database)
   - ✅ Access control at table level
   
   **6. Advanced Patterns:**
   
   **Dependency Management:**
   ```sql
   CREATE TABLE PipelineDependencies (
       ConfigID INT,
       DependsOnConfigID INT,
       PRIMARY KEY (ConfigID, DependsOnConfigID)
   );
   
   -- Sales depends on Customers and Products
   INSERT INTO PipelineDependencies VALUES (3, 1), (3, 2);
   ```
   
   **Pipeline checks dependencies before execution:**
   ```sql
   SELECT pc.*
   FROM PipelineConfig pc
   LEFT JOIN PipelineDependencies pd ON pc.ConfigID = pd.ConfigID
   LEFT JOIN PipelineExecution pe ON pd.DependsOnConfigID = pe.ConfigID
   WHERE pc.IsActive = 1
     AND (pd.DependsOnConfigID IS NULL 
          OR pe.Status = 'Success')
   ORDER BY pc.Priority;
   ```
   
   **Error Handling Config:**
   ```sql
   ALTER TABLE PipelineConfig ADD
       MaxRetries INT DEFAULT 3,
       RetryInterval INT DEFAULT 60,  -- seconds
       OnErrorAction VARCHAR(20) DEFAULT 'Alert',  -- 'Alert', 'Skip', 'Stop'
       NotificationEmail VARCHAR(200);
   ```
   
   **Best Practices:**
   - Version control metadata tables (track schema changes)
   - Implement approval workflow for config changes
   - Archive old execution logs (partition by month)
   - Use separate config tables per environment
   - Document metadata schema thoroughly
   - Implement validation (prevent invalid configs)

3. **Q:** How do you convert a static pipeline to a dynamic pipeline?
   **A:** 
   
   **Step-by-Step Conversion Process:**
   
   **Step 1: Analyze Static Pipeline**
   
   **Original Static Pipeline:**
   ```json
   {
     "name": "CopySalesTable",
     "activities": [{
       "name": "CopySales",
       "type": "Copy",
       "inputs": [{
         "referenceName": "SQLSales2026",
         "type": "DatasetReference"
       }],
       "outputs": [{
         "referenceName": "LakehouseSales",
         "type": "DatasetReference"
       }],
       "typeProperties": {
         "source": {
           "sqlReaderQuery": "SELECT * FROM dbo.Sales WHERE Year = 2026 AND Region = 'East'"
         },
         "sink": {
           "tableName": "Sales_East_2026"
         }
       }
     }]
   }
   ```
   
   **Identify Hardcoded Values:**
   - Schema: `dbo`
   - Table: `Sales`
   - Year filter: `2026`
   - Region filter: `East`
   - Destination: `Sales_East_2026`
   
   **Step 2: Add Pipeline Parameters**
   
   ```json
   {
     "name": "DynamicCopyPipeline",
     "parameters": {
       "SourceSchema": {
         "type": "String",
         "defaultValue": "dbo"
       },
       "SourceTable": {
         "type": "String"
       },
       "FilterYear": {
         "type": "String"
       },
       "FilterRegion": {
         "type": "String"
       },
       "DestinationTable": {
         "type": "String"
       }
     }
   }
   ```
   
   **Step 3: Parameterize Datasets**
   
   **Before (Static Dataset):**
   ```json
   {
     "name": "SQLSales2026",
     "type": "SqlServerTable",
     "typeProperties": {
       "schema": "dbo",
       "table": "Sales"
     }
   }
   ```
   
   **After (Dynamic Dataset):**
   ```json
   {
     "name": "GenericSQLDataset",
     "parameters": {
       "SchemaName": {"type": "String"},
       "TableName": {"type": "String"}
     },
     "typeProperties": {
       "schema": {
         "value": "@dataset().SchemaName",
         "type": "Expression"
       },
       "table": {
         "value": "@dataset().TableName",
         "type": "Expression"
       }
     }
   }
   ```
   
   **Step 4: Update Copy Activity**
   
   ```json
   {
     "name": "DynamicCopy",
     "type": "Copy",
     "inputs": [{
       "referenceName": "GenericSQLDataset",
       "parameters": {
         "SchemaName": "@pipeline().parameters.SourceSchema",
         "TableName": "@pipeline().parameters.SourceTable"
       }
     }],
     "outputs": [{
       "referenceName": "GenericLakehouseDataset",
       "parameters": {
         "TableName": "@pipeline().parameters.DestinationTable"
       }
     }],
     "typeProperties": {
       "source": {
         "type": "SqlSource",
         "sqlReaderQuery": {
           "value": "@concat('SELECT * FROM ', 
                            pipeline().parameters.SourceSchema, '.', 
                            pipeline().parameters.SourceTable,
                            ' WHERE Year = ', pipeline().parameters.FilterYear,
                            ' AND Region = ''', pipeline().parameters.FilterRegion, '''')",
           "type": "Expression"
         }
       }
     }
   }
   ```
   
   **Step 5: Test with Different Parameters**
   
   ```json
   // Test Run 1: Original scenario
   {
     "SourceSchema": "dbo",
     "SourceTable": "Sales",
     "FilterYear": "2026",
     "FilterRegion": "East",
     "DestinationTable": "Sales_East_2026"
   }
   
   // Test Run 2: Different table
   {
     "SourceSchema": "dbo",
     "SourceTable": "Customers",
     "FilterYear": "2026",
     "FilterRegion": "West",
     "DestinationTable": "Customers_West_2026"
   }
   ```
   
   **Step 6: Add Advanced Features**
   
   **Optional Parameters:**
   ```json
   {
     "LoadType": {
       "type": "String",
       "defaultValue": "Full"
     },
     "PartitionKey": {
       "type": "String",
       "defaultValue": ""
     }
   }
   ```
   
   **Conditional Logic:**
   ```
   If Condition: Check Load Type
   ├── If LoadType = 'Incremental'
   │   └── Copy with watermark filter
   └── Else
       └── Copy full table
   ```
   
   **Conversion Checklist:**
   - ✅ Identify all hardcoded values
   - ✅ Create pipeline parameters
   - ✅ Parameterize datasets
   - ✅ Update activity expressions
   - ✅ Add default values
   - ✅ Test with multiple scenarios
   - ✅ Add error handling
   - ✅ Document parameters
   - ✅ Update monitoring/alerts
   
   **Common Pitfalls:**
   - ❌ Forgetting to escape quotes in SQL expressions
   - ❌ Not testing with special characters in table names
   - ❌ Missing default values for optional parameters
   - ❌ Overly complex expressions (hard to debug)
   - ❌ Not validating parameter values

4. **Q:** What are the performance implications of dynamic pipelines compared to static pipelines?
   **A:** 
   
   **Performance Characteristics:**
   
   **Runtime Performance:**
   
   | Aspect | Static Pipeline | Dynamic Pipeline | Impact |
   |--------|----------------|------------------|--------|
   | **Expression Evaluation** | None | Minimal (milliseconds) | Negligible |
   | **Copy Activity** | Same | Same | No difference |
   | **Data Movement** | Same | Same | No difference |
   | **Lookup Activities** | N/A | Adds overhead | Small (1-5 sec) |
   | **ForEach Loop** | N/A | Adds overhead | Depends on items |
   | **Total Overhead** | 0 sec | 1-10 sec | Usually acceptable |
   
   **Key Finding:** The actual data movement (Copy Activity) takes 99% of execution time, so dynamic overhead is typically <1% of total runtime.
   
   **Example:**
   ```
   Static Pipeline (Copy 1M rows):
   - Data Copy: 300 seconds
   - Total: 300 seconds
   
   Dynamic Pipeline (Copy 1M rows):
   - Lookup Config: 2 seconds
   - Expression Evaluation: 0.1 seconds
   - Data Copy: 300 seconds
   - Log Execution: 1 second
   - Total: 303.1 seconds
   
   Overhead: 1% (acceptable!)
   ```
   
   **Performance Optimizations:**
   
   **1. Minimize Lookups:**
   
   **❌ Bad Practice (Lookup per table):**
   ```
   ForEach Table:
       └── Lookup: Get config for this table (N lookups!)
   ```
   
   **✅ Good Practice (Single lookup):**
   ```
   Lookup: Get all configs (1 lookup)
   ForEach: Process configs
   ```
   
   **2. Parallel Execution:**
   
   **Static (Manual Parallelization):**
   - Must create concurrent pipelines
   - Complex orchestration
   - Hard to manage
   
   **Dynamic (Built-in Parallelization):**
   ```json
   {
     "type": "ForEach",
     "typeProperties": {
       "isSequential": false,
       "batchCount": 10  // Process 10 items in parallel
     }
   }
   ```
   
   **Result:** Dynamic pipelines often FASTER due to easy parallelization!
   
   **3. Expression Complexity:**
   
   **❌ Too Complex (slow evaluation):**
   ```json
   {
     "value": "@if(and(equals(pipeline().parameters.LoadType, 'Incremental'), 
                      greater(int(pipeline().parameters.BatchSize), 1000),
                      or(equals(pipeline().parameters.Region, 'US'),
                         equals(pipeline().parameters.Region, 'EU'))),
                  concat('SELECT * FROM ', 
                         pipeline().parameters.Schema, '.', 
                         pipeline().parameters.Table,
                         ' WHERE UpdatedDate > ''', 
                         formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd'),
                         ''' AND Region IN (''', 
                         pipeline().parameters.Region, 
                         ''') ORDER BY UpdatedDate LIMIT ', 
                         pipeline().parameters.BatchSize),
                  concat('SELECT * FROM ', 
                         pipeline().parameters.Schema, '.', 
                         pipeline().parameters.Table))"
   }
   ```
   
   **✅ Simplified (faster evaluation):**
   ```json
   // Store query in config table
   {
     "value": "@item().PrebuiltQuery",
     "type": "Expression"
   }
   ```
   
   **4. Caching:**
   
   **Use Variables to Cache Values:**
   ```
   Lookup: Get current date
   Set Variable: CurrentDate = @activity('Lookup').output.firstRow.Date
   
   ForEach (1000 items):
       └── Use @variables('CurrentDate')  // No re-evaluation!
   ```
   
   **5. Database vs Pipeline Logic:**
   
   **❌ Complex in Pipeline:**
   ```
   ForEach Table:
       ├── Lookup: Check if table exists
       ├── If Exists:
       │   ├── Lookup: Get row count
       │   ├── If Count > 0:
       │   │   └── Copy
   ```
   
   **✅ Push to Database:**
   ```sql
   -- Stored Procedure returns ready-to-process tables
   CREATE PROCEDURE GetTablesToProcess
   AS
   BEGIN
       SELECT TableName, RowCount, LastProcessed
       FROM PipelineConfig pc
       INNER JOIN sys.tables t ON pc.TableName = t.name
       WHERE pc.IsActive = 1
         AND EXISTS (SELECT 1 FROM sys.partitions p 
                     WHERE p.object_id = t.object_id AND p.rows > 0)
   END;
   ```
   
   **Performance Best Practices:**
   
   **✅ DO:**
   - Use parallel ForEach when possible
   - Cache frequently used values in variables
   - Perform complex logic in databases/notebooks
   - Use simple expressions
   - Minimize lookup activities
   - Partition large datasets
   
   **❌ DON'T:**
   - Nest multiple levels of conditions/loops
   - Evaluate complex expressions in tight loops
   - Perform row-by-row operations
   - Use sequential ForEach for large datasets
   - Chain many dependent activities
   
   **Real-World Performance:**
   ```
   Scenario: Load 50 tables (100K rows each)
   
   Static Approach (50 separate pipelines):
   - Must run sequentially (resource limits)
   - Total Time: 50 × 60 sec = 3000 sec (50 min)
   
   Dynamic Approach (1 pipeline, ForEach with batch=10):
   - Lookup config: 3 sec
   - Process 50 tables in 5 batches: 
     5 batches × 60 sec = 300 sec
   - Log results: 2 sec
   - Total Time: 305 sec (5 min)
   
   Result: Dynamic is 10× FASTER!
   ```
   
   **Bottom Line:**
   Dynamic pipelines have minimal overhead (typically <1% of total runtime) and often perform BETTER due to easier parallelization and optimized orchestration. The maintainability benefits far outweigh any tiny performance cost.

**When to Use Each Approach:**

| Scenario | Recommendation | Reason |
|----------|---------------|--------|
| **Loading 1 table** | Static OK | Simplicity wins |
| **Loading 5+ tables** | Dynamic | Avoid duplication |
| **Production system** | Dynamic | Maintainability |
| **Quick POC** | Static | Faster to build |
| **Enterprise DW** | Metadata-Driven | Scalability |
| **Changing requirements** | Dynamic | Flexibility |
| **One-time migration** | Static OK | Won't change |
| **Scheduled batch jobs** | Dynamic | Standard pattern |

**Best Practices:**

**For Dynamic Pipelines:**
- ✅ Use descriptive parameter names
- ✅ Provide default values where sensible
- ✅ Document parameter formats and examples
- ✅ Validate parameter values early
- ✅ Use metadata tables for complex configs
- ✅ Implement comprehensive logging
- ✅ Test with various parameter combinations

**For Static Pipelines:**
- ✅ Use clear, specific pipeline names
- ✅ Document the purpose and assumptions
- ✅ Consider future needs before committing
- ✅ Convert to dynamic if you create >3 similar pipelines

**Migration Path:**
```
Phase 1: Build Static (Prototype)
    ↓
Phase 2: Identify Patterns (Notice duplication)
    ↓
Phase 3: Refactor to Dynamic (Consolidate)
    ↓
Phase 4: Add Metadata (Scale to 50+ objects)
    ↓
Phase 5: Mature (Full enterprise framework)
```

**Key Takeaway:**
Dynamic pipelines require slightly more upfront effort but deliver massive long-term benefits in maintainability, scalability, and operational efficiency. In production environments, always prefer dynamic/metadata-driven approaches.

---

### Dataflow Gen1 vs Dataflow Gen2

**Definition:**
Dataflows are self-service, low-code/no-code data transformation tools in Microsoft Fabric that use Power Query (M language) to extract, transform, and load data. **Dataflow Gen1** is the original Power BI dataflow technology, while **Dataflow Gen2** is the modernized, Fabric-native version with enhanced capabilities, better integration, and improved performance. Both enable citizen data engineers and business analysts to prepare data without writing code.

**Purpose:**
- Self-service data preparation for business users
- Visual, drag-and-drop transformation designer
- Reusable data transformation logic
- Incremental refresh capabilities
- Data cleansing and enrichment
- Data integration without coding

**Key Concepts:**

**Power Query (M Language):**
- Visual transformation editor (like Power BI Power Query)
- 300+ built-in transformations
- Point-and-click interface
- Formula bar for advanced users
- Query folding optimization

**Common Use Cases:**
- Business users preparing data for reports
- Data cleansing (remove duplicates, fix formats)
- Simple joins and merges
- Column mapping and renaming
- Data type conversions
- Scheduled data refresh

---

**Dataflow Gen1 (Legacy - Power BI Dataflows)**

**Where It Lives:**
- Power BI Service (powerbi.com)
- Power BI Premium workspaces
- Not native to Microsoft Fabric

**Key Characteristics:**

**1. Storage:**
- Stores data in Azure Data Lake Storage Gen2 (ADLS Gen2)
- Requires separate storage account configuration
- Data stored as CSV or Parquet files
- Not automatically integrated with OneLake

**2. Destinations:**
- Limited to Power BI datasets
- Cannot write to Lakehouse or Warehouse directly
- Requires additional steps for broader consumption

**3. Refresh:**
- Scheduled refresh in Power BI
- Limited parallel refresh capabilities
- No advanced orchestration

**4. Compute:**
- Power BI capacity-based compute
- Limited compute scaling options
- Tied to Power BI Premium capacity

**5. Integration:**
- Primarily Power BI-focused
- Limited integration with other services
- Requires connectors to other Fabric items

**Example Use Case:**
```
Business Analyst Scenario (Gen1):
1. Connect to Excel file in SharePoint
2. Transform data using Power Query
3. Load to Power BI dataflow
4. Create Power BI dataset from dataflow
5. Build Power BI report
```

**Limitations:**
- ❌ No native Fabric integration
- ❌ Cannot write to Lakehouse/Warehouse directly
- ❌ Limited to Power BI ecosystem
- ❌ Separate storage management
- ❌ No pipeline integration
- ❌ Limited compute scalability

---

**Dataflow Gen2 (Modern - Fabric Dataflows)**

**Where It Lives:**
- Microsoft Fabric workspace
- Native Fabric item type
- Fully integrated with Fabric ecosystem

**Key Characteristics:**

**1. Storage - OneLake Native:**
- Data automatically stored in OneLake
- No separate storage configuration needed
- Seamless integration with Lakehouse
- Unified data lake experience

**2. Destinations - Multiple Options:**
```
Dataflow Gen2 can write to:
✅ Lakehouse tables
✅ Lakehouse files (Parquet, CSV)
✅ Data Warehouse tables
✅ KQL Database
✅ Custom destinations via Data Factory
✅ Power BI datasets (backward compatible)
```

**3. Refresh - Advanced Orchestration:**
- Can be triggered by Data Factory pipelines
- Parallel execution support
- Part of end-to-end orchestration
- Scheduled or event-based triggers

**4. Compute - Fabric Capacity:**
- Leverages Fabric capacity pools
- Better scaling and performance
- Optimized for large datasets
- Query folding to source when possible

**5. Integration - Fabric Ecosystem:**
- Native integration with all Fabric items
- Can be called from pipelines
- Supports staging areas
- Git integration for version control

**6. Advanced Features:**
```
Unique Gen2 Capabilities:
✅ Write to multiple destinations simultaneously
✅ Staging queries (intermediate transformations)
✅ Data destination settings per query
✅ Integration with Data Factory activities
✅ Enhanced monitoring in Fabric Monitoring Hub
✅ Git-based deployment
✅ Workspace-level security
```

**Example Configuration:**

```json
// Dataflow Gen2 destination configuration
{
  "query": "TransformedSales",
  "destination": {
    "type": "Lakehouse",
    "lakehouseName": "SalesLakehouse",
    "tableName": "SalesClean",
    "writeMode": "Append"  // Append, Overwrite, or Merge
  },
  "refresh": {
    "type": "Incremental",
    "incrementalColumn": "ModifiedDate"
  }
}
```

**Example Use Case:**
```
Data Engineer Scenario (Gen2):
1. Create Dataflow Gen2 in Fabric workspace
2. Connect to SQL Server source
3. Transform data using Power Query
4. Write to Lakehouse table (Bronze layer)
5. Trigger from Data Factory pipeline
6. Chain with Notebook for further processing
7. Load to Warehouse (Gold layer)
8. Power BI connects via DirectLake
```

---

**Side-by-Side Comparison:**

| Feature | Dataflow Gen1 (Legacy) | Dataflow Gen2 (Modern) |
|---------|----------------------|----------------------|
| **Platform** | Power BI Service | Microsoft Fabric |
| **Storage** | ADLS Gen2 (separate) | OneLake (automatic) |
| **Destinations** | Power BI datasets only | Lakehouse, Warehouse, KQL, Files, Power BI |
| **Orchestration** | Power BI refresh only | Data Factory pipelines + scheduled |
| **Integration** | Power BI-focused | Full Fabric ecosystem |
| **Compute** | Power BI Premium capacity | Fabric capacity pools |
| **Query Folding** | Limited | Enhanced |
| **Monitoring** | Power BI refresh history | Fabric Monitoring Hub |
| **Version Control** | Not available | Git integration |
| **Incremental Refresh** | Basic | Advanced with merge options |
| **Multiple Destinations** | No | Yes (write to multiple targets) |
| **Pipeline Integration** | No | Yes (called as activity) |
| **Staging Queries** | Basic | Advanced with data destinations |
| **Security** | Power BI RLS | Workspace RBAC + item-level |
| **Deployment** | Manual | Git + deployment pipelines |
| **Scale** | Limited by Power BI capacity | Fabric capacity scaling |
| **Cost Model** | Power BI Premium | Fabric capacity CU-based |

---

**Dataflow Gen2 Architecture:**

```
┌─────────────────────────────────────────────────────┐
│              Data Sources                            │
│  (SQL, APIs, Files, Cloud Storage, SaaS Apps)       │
└────────────────────┬────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────┐
│           Dataflow Gen2 (Power Query)                │
│  ┌──────────────────────────────────────────┐       │
│  │  Query 1: Extract & Transform            │       │
│  │  - Filter rows                            │       │
│  │  - Clean data                             │       │
│  │  - Join tables                            │       │
│  │  - Derive columns                         │       │
│  └──────────────────────────────────────────┘       │
│                                                      │
│  ┌──────────────────────────────────────────┐       │
│  │  Query 2: Staging (Reference Query 1)    │       │
│  │  - Aggregate                              │       │
│  │  - Pivot                                  │       │
│  └──────────────────────────────────────────┘       │
└────────────────┬─────────────────────────────────────┘
                 ↓
┌─────────────────────────────────────────────────────┐
│          Data Destinations (OneLake)                 │
│                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │ Lakehouse   │  │  Warehouse   │  │ KQL DB     │ │
│  │ - Tables    │  │  - Tables    │  │ - Tables   │ │
│  │ - Files     │  │              │  │            │ │
│  └─────────────┘  └──────────────┘  └────────────┘ │
└─────────────────────────────────────────────────────┘
```

---

**When to Use Dataflow Gen2:**

| Scenario | Use Dataflow Gen2 When... | Don't Use Dataflow Gen2 When... |
|----------|--------------------------|-------------------------------|
| **User Skill** | Business users, citizen data engineers | Complex Spark transformations needed |
| **Transformation Complexity** | Filter, join, aggregate, pivot, cleanse | Advanced ML, window functions, complex UDFs |
| **Data Volume** | Small to medium (<10GB) | Very large datasets (>100GB) |
| **Transformation Type** | Row-level operations, simple joins | Complex graph algorithms, advanced analytics |
| **Source** | Databases, APIs, files, SaaS apps | Streaming data (use Eventstream) |
| **Refresh Pattern** | Scheduled or triggered | Real-time continuous (use Eventstream) |
| **Team** | Business analysts, power users | Data engineers writing Spark code |
| **Iteration Speed** | Need quick iterations | Performance-critical workloads |

**✅ Perfect for Dataflow Gen2:**
- Sales data from CRM → clean → load to Lakehouse
- Excel files → standardize → append to Warehouse
- Multiple CSV files → merge → create gold table
- API data → flatten JSON → load to KQL Database
- Database tables → filter sensitive columns → staging area

**❌ Use Notebook/Spark Instead:**
- Processing 500GB files daily
- Complex window functions across billions of rows
- Machine learning model training
- Real-time streaming transformations
- Custom Python/Scala algorithms

---

**Dataflow Gen2 Best Practices:**

**1. Design Patterns:**

**Staging Queries:**
```
Pattern: Extract → Transform → Load (ETL)

Query 1: Extract (source connection)
  ↓ (reference)
Query 2: Transform (staging - no destination)
  ↓ (reference)
Query 3: Load (final - writes to destination)

Benefits:
- Reusable transformations
- Better performance (shared compute)
- Easier debugging
- Clear separation of concerns
```

**Incremental Refresh:**
```json
// Configure incremental refresh
{
  "query": "IncrementalSales",
  "incrementalRefresh": {
    "enabled": true,
    "sourceColumn": "OrderDate",
    "refreshType": "AppendOnly"  // or "Upsert"
  }
}
```

**2. Performance Optimization:**

**Query Folding:**
```
✅ DO: Keep transformations that fold to source
- Filters (WHERE clauses)
- Column selection (SELECT specific columns)
- Joins (when possible)
- Basic aggregations

❌ AVOID: Transformations that break folding
- Custom columns with M functions
- Merge queries (sometimes)
- Complex data type conversions
```

**Reduce Data Early:**
```
Step 1: Filter at source (query folding)
  SELECT * FROM Sales WHERE Year = 2026
  
Step 2: Select only needed columns
  Remove unused columns early
  
Step 3: Transform minimal dataset
  Work with smaller data volume
```

**3. Multiple Destinations:**

```
Use Case: Same transformation, different targets

Dataflow Gen2 Configuration:
- Query 1: TransformSales (staging, no destination)
  ↓
- Query 2: SalesLakehouse (reference Query 1)
  Destination: Lakehouse → Bronze.Sales
  
- Query 3: SalesWarehouse (reference Query 1)
  Destination: Warehouse → Gold.DimSales
  
- Query 4: SalesSummary (aggregate Query 1)
  Destination: Lakehouse → Gold.SalesSummary

Result: 
- Single transformation logic
- Multiple outputs
- Efficient compute usage
```

**4. Integration with Data Factory:**

```json
// Pipeline activity calling Dataflow Gen2
{
  "name": "RunDataflowGen2",
  "type": "DataflowActivity",
  "typeProperties": {
    "dataflow": {
      "referenceName": "CleanSalesData",
      "type": "DataflowReference"
    },
    "compute": {
      "computeType": "General",
      "coreCount": 8
    },
    "staging": {
      "linkedService": "OneLakeStorage",
      "folderPath": "Staging/Dataflow"
    }
  }
}

// Chaining: Dataflow → Notebook → Warehouse
Pipeline Steps:
1. Dataflow Gen2 → Clean & load to Bronze
2. Notebook → Transform Bronze → Silver
3. Script Activity → Load Silver → Gold Warehouse
4. Refresh Power BI dataset
```

**5. Error Handling:**

```
Configure in Dataflow Gen2:
- Data quality rules (remove nulls, validate formats)
- Error rows handling:
  ✅ Keep error rows (load to separate table)
  ✅ Remove error rows (log to file)
  ✅ Replace errors (with default values)
  
Example:
Query: CleanCustomers
- Remove rows where Email is invalid
- Log errors to Lakehouse.ErrorLog
- Continue processing valid rows
```

**6. Monitoring:**

```
Fabric Monitoring Hub:
- Dataflow refresh history
- Execution duration
- Rows processed
- Errors and warnings
- Compute usage (CU consumption)

Set up alerts for:
- Failed refreshes
- Long-running refreshes (>threshold)
- High error rates
- Compute spikes
```

---

**Migration Path: Gen1 → Gen2**

**Step 1: Assessment**
```
Inventory:
- List all Gen1 dataflows
- Document dependencies (which reports use them)
- Identify refresh schedules
- Map data sources
- Review transformations complexity
```

**Step 2: Recreate in Fabric**
```
For each Gen1 dataflow:
1. Create new Dataflow Gen2 in Fabric workspace
2. Copy Power Query M code from Gen1
3. Update destinations:
   - Gen1: Power BI dataset
   - Gen2: Lakehouse table + Power BI dataset (if needed)
4. Configure refresh (pipeline or schedule)
5. Test thoroughly
```

**Step 3: Update Consumers**
```
Power BI Reports:
- Option A: Use DirectLake from Lakehouse
  (Recommended - better performance)
  
- Option B: Keep dataset connection
  (Dataflow Gen2 can still output to Power BI dataset)

Other consumers:
- Update connection strings to Lakehouse/Warehouse
- Verify data freshness and quality
```

**Step 4: Cutover**
```
1. Run Gen1 and Gen2 in parallel (validate results)
2. Monitor for data quality issues
3. Update all consumers to Gen2 destinations
4. Disable Gen1 dataflow
5. Decommission after grace period
```

**Migration Considerations:**

| Aspect | Action Required |
|--------|-----------------|
| **M Code** | Copy-paste works (99% compatible) |
| **Connectors** | Verify same connectors available in Fabric |
| **Parameters** | Recreate (same functionality) |
| **Refresh Schedule** | Set up in Fabric (schedule or pipeline) |
| **Incremental Refresh** | Reconfigure (same column, better options) |
| **Permissions** | Set workspace roles in Fabric |
| **Monitoring** | Switch to Fabric Monitoring Hub |
| **Alerts** | Recreate in Fabric |

---

**Real-World Example: E-Commerce Sales Processing**

**Scenario:**
- Daily sales data from Shopify API
- Clean, transform, and load to data warehouse
- Power BI dashboard needs latest data every morning

**Dataflow Gen2 Implementation:**

**Query 1: ExtractSalesData (Source)**
```
Source:
- Connector: Web API (Shopify)
- Endpoint: /admin/api/2024-04/orders.json
- Authentication: OAuth 2.0

Transformations:
- Expand nested JSON (customer, line_items, shipping)
- Filter: created_at >= yesterday
- Select relevant columns (30 out of 100+)
```

**Query 2: StagingCleanSales (Staging - Reference Query 1)**
```
Transformations:
- Remove duplicates (by order_id)
- Fix data types (dates, decimals)
- Handle nulls (replace with defaults)
- Derive columns:
  * TotalRevenue = line_items_price + shipping_price
  * OrderYear, OrderMonth, OrderDay
- Validate email formats
- Filter cancelled orders
```

**Query 3: SalesBronze (Load to Lakehouse - Reference Query 2)**
```
Destination:
- Type: Lakehouse table
- Lakehouse: EcommerceLakehouse
- Table: Bronze.Sales
- Write Mode: Append
- Incremental: Yes (by created_at)
```

**Query 4: SalesWarehouse (Load to Warehouse - Reference Query 2)**
```
Destination:
- Type: Warehouse
- Warehouse: EcommerceWarehouse
- Schema: Gold
- Table: FactSales
- Write Mode: Append
```

**Query 5: DailySummary (Aggregation - Reference Query 2)**
```
Transformations:
- Group by: OrderDate, ProductCategory
- Aggregate:
  * TotalOrders = Count(OrderID)
  * TotalRevenue = Sum(TotalRevenue)
  * AvgOrderValue = Average(TotalRevenue)

Destination:
- Type: Lakehouse table
- Table: Gold.DailySalesSummary
- Write Mode: Overwrite (daily snapshot)
```

**Pipeline Orchestration:**
```json
{
  "name": "DailySalesProcessing",
  "activities": [
    {
      "name": "RunDataflowGen2",
      "type": "DataflowActivity",
      "typeProperties": {
        "dataflow": "ShopifySalesDataflow"
      }
    },
    {
      "name": "ValidateData",
      "type": "Script",
      "dependsOn": ["RunDataflowGen2"],
      "typeProperties": {
        "script": "SELECT COUNT(*) FROM Gold.FactSales WHERE OrderDate = CAST(GETDATE() AS DATE)"
      }
    },
    {
      "name": "RefreshPowerBI",
      "type": "PowerBIActivity",
      "dependsOn": ["ValidateData"]
    }
  ]
}

Schedule: Daily at 6 AM
```

**Results:**
- ✅ 500-1,000 orders processed daily
- ✅ 3-5 minute end-to-end processing time
- ✅ Zero code (business analyst maintains it)
- ✅ Multiple destinations from single dataflow
- ✅ Full audit trail in Fabric Monitoring Hub
- ✅ Power BI dashboard updated by 6:10 AM

---

**Interview Questions:**

**1. Q: What are the key differences between Dataflow Gen1 and Dataflow Gen2?**

**A:** Dataflow Gen1 and Gen2 represent different generations of self-service data transformation tools in Microsoft's data platform:

**Platform & Integration:**

**Gen1 (Legacy):**
- Lives in Power BI Service only
- Separate from broader data ecosystem
- Requires manual configuration for storage (ADLS Gen2)
- Limited to Power BI Premium workspaces

**Gen2 (Modern):**
- Native Microsoft Fabric item
- Fully integrated with Fabric ecosystem
- Automatic OneLake storage (no config needed)
- Available in all Fabric workspaces

**Destination Capabilities:**

**Gen1:**
- ❌ Can only output to Power BI datasets
- ❌ Cannot write to Lakehouse/Warehouse directly
- ❌ Requires additional pipelines for broader consumption

**Gen2:**
- ✅ Write to Lakehouse (tables and files)
- ✅ Write to Data Warehouse tables
- ✅ Write to KQL Database
- ✅ Write to multiple destinations simultaneously
- ✅ Power BI datasets (backward compatible)

**Example Difference:**
```
Gen1 Workflow:
Source → Dataflow Gen1 → Power BI Dataset → Power BI Report
(Data stuck in Power BI ecosystem)

Gen2 Workflow:
Source → Dataflow Gen2 → {
  ├─ Lakehouse (for data engineers)
  ├─ Warehouse (for analysts)
  ├─ KQL Database (for real-time analytics)
  └─ Power BI via DirectLake
}
(Data available to entire organization)
```

**Orchestration:**

**Gen1:**
- Only Power BI scheduled refresh
- No pipeline integration
- Limited parallel processing
- Manual refresh triggers

**Gen2:**
- Can be called from Data Factory pipelines
- Part of end-to-end orchestration
- Parallel execution support
- Event-based and scheduled triggers
- Chain with Notebooks, Scripts, etc.

**Example Pipeline:**
```
Data Factory Pipeline:
1. Dataflow Gen2 → Clean raw data → Lakehouse Bronze
2. Notebook → Transform → Lakehouse Silver
3. Dataflow Gen2 → Aggregate → Warehouse Gold
4. Refresh Power BI semantic model
```

**Compute & Performance:**

**Gen1:**
- Power BI Premium capacity
- Limited scaling options
- Tied to Power BI capacity limits

**Gen2:**
- Fabric capacity pools
- Better scaling and performance
- Optimized query folding
- Enhanced for large datasets

**Version Control & Deployment:**

**Gen1:**
- ❌ No Git integration
- ❌ Manual export/import
- ❌ Limited deployment automation

**Gen2:**
- ✅ Full Git integration
- ✅ Deployment pipelines
- ✅ CI/CD support
- ✅ Workspace-level version control

**When to Use Each:**

| Use Gen1 If... | Use Gen2 If... |
|---------------|---------------|
| Already invested in Power BI only | Building in Microsoft Fabric |
| Only need Power BI datasets | Need multiple destinations |
| No Fabric licensing yet | Want full ecosystem integration |
| Simple, isolated transformations | Part of larger data workflows |

**Migration Recommendation:**
If you're starting fresh, **always use Dataflow Gen2**. It's the future, fully supported, and provides significantly more capabilities. Gen1 is legacy and will eventually be deprecated.

---

**2. Q: When should you use Dataflow Gen2 vs Notebooks vs Data Pipelines for data transformations?**

**A:** Choosing the right transformation tool depends on complexity, user skill, data volume, and integration needs:

**Dataflow Gen2 - Best For:**

**Use When:**
- ✅ Business users or citizen data engineers (low/no code)
- ✅ Small to medium datasets (<10GB per refresh)
- ✅ Simple to moderate transformations
- ✅ Need visual, intuitive interface
- ✅ Power Query transformations sufficient
- ✅ Quick iterations and prototyping

**Ideal Scenarios:**
```
1. Clean CRM data (Salesforce → filter → standardize → Lakehouse)
2. Merge multiple Excel files (standardize columns → append)
3. API data ingestion (call REST API → flatten JSON → load)
4. Simple aggregations (group by, pivot, unpivot)
5. Data cleansing (remove duplicates, nulls, format dates)
6. Small dimensional tables (<1M rows)
```

**Example:**
```
Marketing Campaign Analysis:
- Source: Google Ads API
- Transform: Flatten nested JSON, calculate metrics
- Destination: Lakehouse table
- User: Marketing Analyst (no coding)
- Refresh: Daily
- Perfect for Dataflow Gen2!
```

**Strengths:**
- 🟢 No coding required
- 🟢 Visual interface (300+ transformations)
- 🟢 Quick development
- 🟢 Business user friendly
- 🟢 Great for self-service
- 🟢 Multiple destinations

**Limitations:**
- 🔴 Performance ceiling (~10-50GB max)
- 🔴 Limited advanced logic
- 🔴 No custom Python/Scala
- 🔴 Complex joins can be slow
- 🔴 Not ideal for ML workloads

---

**Notebooks (PySpark/Scala) - Best For:**

**Use When:**
- ✅ Large datasets (>10GB, up to petabytes)
- ✅ Complex transformations
- ✅ Data engineers writing code
- ✅ Need custom logic, UDFs
- ✅ Machine learning workflows
- ✅ Advanced analytics (window functions, arrays)

**Ideal Scenarios:**
```
1. Process 500GB daily fact table (partition by date)
2. Complex window functions (running totals, rank by partition)
3. Machine learning feature engineering
4. Custom business logic (complex calculations, multiple conditions)
5. Deduplication on billions of rows
6. Delta Lake MERGE operations (upserts)
7. Graph analysis, network processing
```

**Example:**
```
E-Commerce Fact Table Processing:
- Source: 200GB daily raw logs (Parquet)
- Transform: 
  * Deduplicate 1B+ rows
  * Complex sessionization logic
  * Window functions for customer analytics
  * Custom Python UDFs
- Destination: Delta Lake (partitioned by date)
- User: Data Engineer
- Refresh: Hourly
- MUST use Notebook!
```

**Code Example:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lag, datediff

# Complex transformation not possible in Dataflow Gen2
df = spark.read.parquet("Files/raw/weblogs/")

# Window function for sessionization
windowSpec = Window.partitionBy("UserID").orderBy("Timestamp")

result = df \
    .withColumn("PrevTimestamp", lag("Timestamp").over(windowSpec)) \
    .withColumn("TimeDiff", datediff("Timestamp", "PrevTimestamp")) \
    .withColumn("SessionID", 
        when(col("TimeDiff") > 30, monotonically_increasing_id())
        .otherwise(None)
    ) \
    .fillna(method='ffill', subset=['SessionID'])

# Write to Delta with partitioning
result.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("Date") \
    .save("Tables/UserSessions")
```

**Strengths:**
- 🟢 Handle massive datasets (petabyte-scale)
- 🟢 Unlimited transformation complexity
- 🟢 Full Spark ecosystem
- 🟢 Custom code (Python, Scala, R, SQL)
- 🟢 ML libraries (MLlib, scikit-learn)
- 🟢 Optimized for big data

**Limitations:**
- 🔴 Requires coding skills
- 🔴 Steeper learning curve
- 🔴 More development time
- 🔴 Harder to debug for non-coders

---

**Data Pipelines (Copy + Activities) - Best For:**

**Use When:**
- ✅ Orchestration and workflow management
- ✅ Simple data movement (no transformation)
- ✅ Bulk data loading
- ✅ Chaining multiple activities
- ✅ Error handling and retry logic
- ✅ Metadata-driven patterns

**Ideal Scenarios:**
```
1. Copy 50 tables from SQL Server to Lakehouse (no transform)
2. Orchestrate: Dataflow → Notebook → Warehouse Load → Refresh
3. File format conversion (CSV → Parquet)
4. Incremental load with watermark pattern
5. Parallel processing (ForEach 20 tables)
6. Error handling and logging framework
```

**Example:**
```
Orchestration Pattern:
Pipeline Activities:
1. Lookup → Get table list from control table
2. ForEach → Loop through tables
   a. Copy Activity → SQL to Lakehouse Bronze (raw data)
   b. Dataflow Gen2 → Clean and standardize
   c. Notebook → Complex transformations
   d. Script → Load to Warehouse Gold
3. Web Activity → Send completion notification
4. Power BI Refresh
```

**Strengths:**
- 🟢 Excellent orchestration
- 🟢 100+ connectors
- 🟢 Metadata-driven patterns
- 🟢 Visual workflow designer
- 🟢 Built-in error handling
- 🟢 Parallel execution

**Limitations:**
- 🔴 Limited transformation capabilities
- 🔴 Not for complex logic
- 🔴 Should call Dataflow/Notebook for transforms

---

**Decision Matrix:**

| Factor | Dataflow Gen2 | Notebook | Data Pipeline |
|--------|--------------|----------|--------------|
| **User Skill** | Business analyst | Data engineer | Data engineer |
| **Code Required** | No | Yes (Python/Scala) | Minimal (JSON/expressions) |
| **Data Volume** | <10GB | Unlimited | Any (orchestration) |
| **Complexity** | Simple-Moderate | Any complexity | N/A (orchestration) |
| **Dev Speed** | Fast | Moderate | Fast |
| **Transformations** | Power Query (300+) | Full Spark | Basic (copy, convert) |
| **ML Support** | No | Yes | No |
| **Custom Logic** | Limited | Full | Limited |
| **Best Use** | Self-service ETL | Big data, ML | Orchestration, bulk copy |

**Real-World Recommendation by Scenario:**

**Scenario 1: Small Business Sales Report**
```
Data: 50,000 rows from SQL Server
Need: Clean, aggregate, load to Power BI
User: Business Analyst
👉 Use: Dataflow Gen2
Why: Perfect size, simple logic, no code needed
```

**Scenario 2: Enterprise Data Warehouse Load**
```
Data: 100 tables, 5TB total, daily incremental
Need: Load → Bronze → Silver → Gold
User: Data Engineering Team
👉 Use: Pipeline (orchestrate) + Notebook (transform)
Why: Scale, complexity, parallelization needed
```

**Scenario 3: Customer 360 Analytics**
```
Data: 20GB customer data from 5 sources
Need: Merge, deduplicate, enrich, ML scoring
User: Data Scientist
👉 Use: Notebook
Why: ML, complex joins, custom algorithms
```

**Scenario 4: API to Lakehouse (Daily)**
```
Data: REST API, 10,000 records/day
Need: Extract JSON, flatten, append to table
User: Citizen Data Engineer
👉 Use: Dataflow Gen2
Why: API connector, JSON flattening, simple append
```

**Scenario 5: Multi-Source Migration**
```
Data: 200 tables from on-prem to Lakehouse
Need: One-time bulk migration
User: Migration Team
👉 Use: Pipeline with ForEach + Copy Activity
Why: Bulk operation, metadata-driven, no transforms
```

**Best Practice: Use All Three Together!**

```
Modern Data Platform Pattern:

1. PIPELINE (Orchestration Layer)
   - Trigger and schedule
   - Error handling
   - Logging framework
   
2. DATAFLOW GEN2 (Self-Service Layer)
   - Business users prep their data
   - Cleansing, standardization
   - Small-medium datasets
   
3. NOTEBOOK (Heavy Lifting Layer)
   - Large-scale transformations
   - Complex business logic
   - ML and advanced analytics

Complete Workflow:
Pipeline {
  → Dataflow Gen2: Extract & clean API data → Bronze
  → Notebook: Complex transformations → Silver
  → Dataflow Gen2: Create business-friendly views → Gold
  → Script: Load to Warehouse
  → Refresh Power BI
}
```

**Key Takeaway:**
- **Dataflow Gen2** = Self-service, visual, <10GB, business users
- **Notebook** = Code-based, unlimited scale, complex logic, data engineers
- **Pipeline** = Orchestrator that calls both, handles errors, manages workflow

Choose based on data volume, transformation complexity, and team skills. In enterprise scenarios, you'll use all three together!

---

**3. Q: How do you configure incremental refresh in Dataflow Gen2?**

**A:** Incremental refresh in Dataflow Gen2 allows you to load only new or changed data instead of reloading entire datasets, significantly improving performance and reducing compute costs.

**Step-by-Step Configuration:**

**1. Source Data Requirements:**

Your source must have a column to track changes:
```sql
-- Ideal columns for incremental refresh
ModifiedDate   (timestamp of last change)
CreatedDate    (for append-only data)
ID             (for sequential IDs)
TransactionDate (for time-series data)
```

**Example Source Table:**
```sql
CREATE TABLE Sales (
    SaleID INT PRIMARY KEY,
    CustomerID INT,
    Amount DECIMAL(10,2),
    SaleDate DATE,
    ModifiedDate DATETIME DEFAULT GETDATE()  -- ← Incremental column
);
```

**2. Dataflow Gen2 Configuration:**

**In Power Query Editor:**

**Step 1: Create Query**
```
Source: SQL Server table
Transformations: Your normal Power Query steps
```

**Step 2: Set Data Destination**
```
Click query → "Data destination" dropdown
Select: Lakehouse, Warehouse, or KQL Database

Configuration:
- Destination: SalesLakehouse
- Table name: Bronze.Sales
- Update method: Append  ← Important for incremental
```

**Step 3: Configure Incremental Refresh**

In destination settings panel:
```
☑ Enable incremental refresh

Incremental refresh settings:
┌─────────────────────────────────────┐
│ Incremental column: ModifiedDate    │  ← Column to track changes
│ Refresh type: Append                │  ← Or "Upsert"
│ Detect data changes: Yes            │  ← Optional: detect schema changes
└─────────────────────────────────────┘
```

**3. Refresh Type Options:**

**Option A: Append (Insert Only)**
```
Best for: Immutable data (logs, events, transactions)

Behavior:
- Only loads NEW rows (ModifiedDate > last watermark)
- Never updates existing rows
- Fastest option

Example:
Day 1: Load where ModifiedDate > '1900-01-01'
  → Loads 10,000 rows
  → Max ModifiedDate = '2026-04-24 23:59:00'
  
Day 2: Load where ModifiedDate > '2026-04-24 23:59:00'
  → Loads only new 500 rows
  → Appends to existing table
```

**SQL Equivalent:**
```sql
-- Day 1
INSERT INTO Lakehouse.Bronze.Sales
SELECT * FROM SourceDB.Sales
WHERE ModifiedDate > '1900-01-01';

-- Day 2 (automatic)
INSERT INTO Lakehouse.Bronze.Sales
SELECT * FROM SourceDB.Sales
WHERE ModifiedDate > (SELECT MAX(ModifiedDate) FROM Lakehouse.Bronze.Sales);
```

**Option B: Upsert (Insert + Update)**
```
Best for: Mutable data (customer profiles, product master data)

Behavior:
- Loads new AND changed rows
- Updates existing rows (based on key column)
- Uses MERGE logic under the hood
- Slower than Append, but handles updates

Example:
Day 1: Load all customers (100,000 rows)

Day 2: 
- 50 customers updated (email changed)
- 10 new customers added
→ Loads 60 rows
→ Updates 50 existing
→ Inserts 10 new
```

**Configuration:**
```
Update method: Upsert

Additional settings:
┌─────────────────────────────────────┐
│ Key column(s): CustomerID           │  ← Primary key for matching
│ Incremental column: ModifiedDate    │  ← Track changes
│ Compare all columns: No             │  ← Faster (trust ModifiedDate)
└─────────────────────────────────────┘
```

**Delta Lake MERGE Equivalent:**
```sql
MERGE INTO Lakehouse.Bronze.Customers AS target
USING (
  SELECT * FROM SourceDB.Customers
  WHERE ModifiedDate > (SELECT MAX(ModifiedDate) FROM Lakehouse.Bronze.Customers)
) AS source
ON target.CustomerID = source.CustomerID
WHEN MATCHED THEN
  UPDATE SET
    Name = source.Name,
    Email = source.Email,
    ModifiedDate = source.ModifiedDate
WHEN NOT MATCHED THEN
  INSERT (CustomerID, Name, Email, ModifiedDate)
  VALUES (source.CustomerID, source.Name, source.Email, source.ModifiedDate);
```

**4. Advanced Configuration:**

**Pattern 1: Sliding Window (Keep Last N Days)**
```
Use Case: Logs - keep only last 90 days

Configuration:
- Load: Last 90 days of data
- Each refresh: Drop old, load new

M Code (Advanced):
let
    CutoffDate = Date.AddDays(DateTime.LocalNow(), -90),
    Source = Sql.Database("server", "db"),
    FilteredRows = Table.SelectRows(Source, each [ModifiedDate] > CutoffDate)
in
    FilteredRows
```

**Pattern 2: Partition-Based Incremental**
```
Use Case: Daily partitioned data

Power Query:
- Parameter: LoadDate (default = today)
- Filter: Date = LoadDate
- Destination: Overwrite specific partition

Example:
Today: Load 2026-04-25 partition
Tomorrow: Load 2026-04-26 partition
(Each day is separate, no overlap)
```

**Pattern 3: Full Refresh Periodically**
```
Use Case: Weekly full, daily incremental

Solution: Two dataflows
1. Dataflow_Sales_Incremental (daily)
   - Incremental refresh
   - Upsert mode
   
2. Dataflow_Sales_Full (weekly)
   - Full refresh
   - Overwrite mode
   - Catches any missed updates
```

**5. Monitoring Incremental Refresh:**

**Fabric Monitoring Hub:**
```
Dataflow Refresh History:
┌──────────────────────────────────────────────────┐
│ Run Date: 2026-04-25 06:00:00                    │
│ Status: Succeeded                                 │
│ Duration: 45 seconds                              │
│ Rows Processed: 1,234                             │  ← Only incremental rows
│ Incremental Watermark: 2026-04-25 05:59:00       │  ← Max value loaded
│ Compute Used: 0.5 CU-hours                        │
└──────────────────────────────────────────────────┘

Compare to Full Refresh:
│ Rows Processed: 1,000,000                         │  ← All rows
│ Duration: 15 minutes                              │  ← Much slower
│ Compute Used: 5 CU-hours                          │  ← 10× more expensive
```

**6. Troubleshooting Common Issues:**

**Issue 1: Incremental column has NULLs**
```
Problem: Some rows have NULL ModifiedDate
Solution: Filter out NULLs or set default

Power Query:
= Table.SelectRows(Source, each [ModifiedDate] <> null)

Or update source:
UPDATE Sales SET ModifiedDate = CreatedDate WHERE ModifiedDate IS NULL;
```

**Issue 2: Clock skew / late-arriving data**
```
Problem: Source system clock ahead/behind
Solution: Add overlap buffer

M Code:
let
    LastWatermark = ...,
    BufferedWatermark = DateTime.AddHours(LastWatermark, -1),  // 1 hour overlap
    Source = ...
    FilteredRows = Table.SelectRows(Source, each [ModifiedDate] > BufferedWatermark)
in
    FilteredRows
    
Note: Handle duplicates with upsert or distinct
```

**Issue 3: Schema drift**
```
Problem: Source adds new column
Solution: Enable "Detect data changes"

Configuration:
☑ Detect data changes
→ Dataflow will add new columns automatically
→ Logs warning in refresh history
```

**Issue 4: Performance degradation over time**
```
Problem: Upsert gets slower as table grows
Solution: Partition the target table

Lakehouse table:
CREATE TABLE Bronze.Sales
PARTITIONED BY (SaleYear INT, SaleMonth INT);

Dataflow:
- Add columns: SaleYear, SaleMonth
- Destination: Append to specific partition
- Dramatically faster MERGE operations
```

**7. Best Practices:**

| Practice | Recommendation | Benefit |
|----------|---------------|---------|
| **Watermark Column** | Use DATETIME with milliseconds | Precision, no duplicates |
| **Source Index** | Index on incremental column | Fast source queries |
| **Buffer Overlap** | Add 1-5 minute overlap | Catch late-arriving data |
| **Key Column** | Use surrogate keys | Faster upserts |
| **Partition** | Partition target by date | Upsert performance |
| **Monitoring** | Alert on row count anomalies | Detect issues early |
| **Testing** | Test with backdated data | Verify logic works |
| **Full Refresh** | Weekly/monthly full refresh | Catch any drift |

**Complete Example:**

```
Scenario: E-commerce Orders (Incremental Upsert)

Source Table:
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderStatus VARCHAR(20),  -- Can change: Pending → Shipped → Delivered
    TotalAmount DECIMAL(10,2),
    OrderDate DATE,
    ModifiedDate DATETIME DEFAULT GETDATE()  -- Updated on any change
);

Dataflow Gen2 Configuration:
┌─────────────────────────────────────────────────┐
│ Query: Orders                                    │
│ Source: SQL Server                               │
│ Transformations: Clean, validate, derive columns │
│                                                  │
│ Data Destination:                                │
│   Type: Lakehouse table                          │
│   Table: Bronze.Orders                           │
│   Update method: Upsert                          │
│   Key column: OrderID                            │
│   Incremental column: ModifiedDate               │
│   Detect changes: Yes                            │
└─────────────────────────────────────────────────┘

Pipeline Schedule: Every hour

Result:
- Hour 1: Load all orders (100,000 rows) - 10 min
- Hour 2: Load only changed (150 rows) - 10 sec
- Hour 3: Load only changed (200 rows) - 12 sec
→ 60× faster after initial load!
```

**ROI Calculation:**
```
Without Incremental Refresh:
- 24 refreshes/day × 10 min = 240 min compute
- 240 min × 30 days = 7,200 min/month
- Cost: ~$500/month (capacity-based)

With Incremental Refresh:
- 1 full (10 min) + 23 incremental (10 sec each) = 14 min/day
- 14 min × 30 days = 420 min/month
- Cost: ~$35/month
→ 94% cost reduction! 💰
```

Incremental refresh is **essential** for production dataflows handling frequently updated data!

---

<!-- Additional terminologies will be added here as you learn them -->

---

### Dynamic Content & Pipeline Expression Builder

**Definition:**
Dynamic Content in Fabric Data Factory enables you to create flexible, parameterized pipelines by using expressions that evaluate at runtime. The Expression Builder is a visual editor that helps you construct expressions using system variables, functions, and parameters to dynamically control pipeline behavior, access activity outputs, manipulate data, and make decisions based on runtime conditions.

**Purpose:**
- Create reusable, parameterized pipelines
- Access runtime values (pipeline run ID, trigger time, activity outputs)
- Perform data transformations and calculations
- Implement conditional logic
- Build metadata-driven solutions
- Eliminate hardcoded values

**When to Use Dynamic Content:**

| Use Case | Example | Benefit |
|----------|---------|---------|
| **Parameterization** | `@pipeline().parameters.TableName` | Reusable pipelines |
| **Activity Outputs** | `@activity('Lookup1').output.firstRow.ID` | Chain activities |
| **Timestamps** | `@utcnow()` | Dynamic file naming |
| **Conditional Logic** | `@if(equals(variables('env'), 'prod'), 'url1', 'url2')` | Environment-specific |
| **String Manipulation** | `@concat('Table_', formatDateTime(utcnow(), 'yyyyMMdd'))` | Dynamic naming |
| **Array Operations** | `@activity('Lookup1').output.value` | ForEach loops |

---

**Expression Builder Interface:**

When you click the **"Add dynamic content"** link in any pipeline parameter field, the Expression Builder opens with four main sections:

```
┌─────────────────────────────────────────────────┐
│ Pipeline Expression Builder                      │
├─────────────────────────────────────────────────┤
│                                                  │
│ [Expression Editor]                              │
│ @pipeline().parameters.TableName                 │
│                                                  │
├─────────────────────────────────────────────────┤
│ System Variables | Functions | Parameters        │
├─────────────────────────────────────────────────┤
│ • @pipeline()        • concat()    • TableName  │
│ • @activity()        • if()        • StartDate  │
│ • @trigger()         • utcnow()    • Environment│
│ • @item()           • formatDateTime()           │
│ • @variables()       • split()                   │
│                                                  │
└─────────────────────────────────────────────────┘
```

**Tips for Using Expression Builder:**
- ✅ Click on suggested items to auto-insert
- ✅ Use IntelliSense for function autocomplete
- ✅ Validate expression before saving (green checkmark)
- ✅ Red X means syntax error - hover for details
- ✅ Use `@` symbol to start any expression

---

**System Variables:**

System variables provide access to pipeline metadata and activity outputs. All expressions must start with `@` symbol.

**1. @pipeline() - Pipeline Metadata**

```json
// Access pipeline properties
@pipeline().Pipeline          // Pipeline name (e.g., "LoadSalesData")
@pipeline().RunId            // Unique run identifier (GUID)
@pipeline().TriggerType      // "Manual", "Schedule", "Tumbling", "Event"
@pipeline().TriggerId        // Trigger identifier
@pipeline().TriggerName      // Trigger name
@pipeline().TriggerTime      // When trigger fired (UTC)
@pipeline().GroupId          // Workspace/Resource Group ID

// Access pipeline parameters
@pipeline().parameters.TableName          // String parameter
@pipeline().parameters.StartDate          // Date parameter
@pipeline().parameters.Environment        // Environment (dev/test/prod)
@pipeline().parameters.BatchSize          // Numeric parameter
@pipeline().parameters.TableList          // Array parameter

// Real-world example: Dynamic file path
Files/@{pipeline().parameters.Environment}/Sales/@{formatDateTime(utcnow(), 'yyyy/MM/dd')}/sales.parquet
// Result: Files/prod/Sales/2026/04/25/sales.parquet
```

**2. @activity() - Activity Outputs**

Access outputs from previous activities in the pipeline:

```json
// Lookup Activity outputs
@activity('GetMetadata').output                      // Full output object
@activity('GetMetadata').output.firstRow             // First row only
@activity('GetMetadata').output.firstRow.MaxDate     // Specific column value
@activity('GetMetadata').output.value                // All rows (array)
@activity('GetMetadata').output.count                // Number of rows returned

// Copy Activity outputs
@activity('CopyData').output.rowsCopied              // Number of rows copied
@activity('CopyData').output.rowsRead                // Number of rows read
@activity('CopyData').output.dataWritten             // Bytes written
@activity('CopyData').output.copyDuration            // Duration in seconds
@activity('CopyData').output.effectiveIntegrationRuntime  // Runtime used

// Activity execution metadata
@activity('CopyData').output.executionDetails[0].status   // "Succeeded", "Failed"
@activity('CopyData').output.executionDetails[0].start    // Start timestamp
@activity('CopyData').output.executionDetails[0].duration // Duration

// Error information (use in failure path)
@activity('CopyData').error.message                  // Error message
@activity('CopyData').error.errorCode                // Error code
@activity('CopyData').error.failureType              // Failure type

// Real-world example: Conditional processing based on rows copied
@if(
    greater(activity('CopyData').output.rowsCopied, 0),
    'ProcessData',
    'SkipProcessing'
)
```

**3. @trigger() - Trigger Information**

Access trigger metadata (only available when pipeline runs from trigger, not manual):

```json
// Schedule Trigger
@trigger().scheduledTime         // When trigger was supposed to run
@trigger().startTime            // When trigger actually started
@trigger().outputs.windowStartTime   // Tumbling window start
@trigger().outputs.windowEndTime     // Tumbling window end

// Event Trigger
@trigger().outputs.body.folderPath   // File path that triggered
@trigger().outputs.body.fileName     // File name that triggered

// Real-world example: Tumbling window watermark
WHERE ModifiedDate >= '@{trigger().outputs.windowStartTime}'
  AND ModifiedDate < '@{trigger().outputs.windowEndTime}'
```

**4. @item() - ForEach Current Item**

Access the current item in a ForEach loop:

```json
// When ForEach items is array of objects
{
  "items": "@activity('GetTableList').output.value",
  "activities": [{
    "name": "CopyTable",
    "type": "Copy",
    "inputs": [{
      "parameters": {
        "SchemaName": "@item().SchemaName",    // Current item's SchemaName
        "TableName": "@item().TableName",      // Current item's TableName
        "Query": "@item().SelectQuery"         // Current item's Query
      }
    }]
  }]
}

// When ForEach items is simple array
{
  "items": ["Table1", "Table2", "Table3"],
  "activities": [{
    "name": "ProcessTable",
    "typeProperties": {
      "tableName": "@item()"    // Current item: "Table1", "Table2", etc.
    }
  }]
}
```

**5. @variables() - Pipeline Variables**

Access pipeline variables (must be declared first):

```json
// Variable declaration
"variables": {
  "ErrorCount": {
    "type": "Integer",
    "defaultValue": 0
  },
  "CurrentTable": {
    "type": "String"
  },
  "ProcessedTables": {
    "type": "Array",
    "defaultValue": []
  }
}

// Variable usage
@variables('ErrorCount')              // Get variable value
@variables('CurrentTable')            
@variables('ProcessedTables')

// Set Variable activity
{
  "name": "IncrementErrorCount",
  "type": "SetVariable",
  "typeProperties": {
    "variableName": "ErrorCount",
    "value": "@add(variables('ErrorCount'), 1)"
  }
}
```

**6. @dataset() - Dataset Parameters**

Access dataset parameter values:

```json
@dataset().SchemaName
@dataset().TableName
@dataset().FilePath
```

---

**Functions:**

Fabric Data Factory provides 100+ built-in functions for data manipulation, string operations, logical comparisons, and more.

**1. String Functions:**

```json
// concat() - Combine strings
@concat('Table_', pipeline().parameters.TableName, '_', formatDateTime(utcnow(), 'yyyyMMdd'))
// Result: "Table_Sales_20260425"

// substring() - Extract part of string
@substring(pipeline().parameters.FileName, 0, 10)
// Input: "SalesData_20260425.csv" → Output: "SalesData_"

// replace() - Replace text
@replace(pipeline().parameters.FilePath, 'dev', 'prod')
// Input: "Files/dev/sales.csv" → Output: "Files/prod/sales.csv"

// toLower() / toUpper() - Case conversion
@toLower(pipeline().parameters.Environment)
// Input: "PROD" → Output: "prod"

@toUpper(pipeline().parameters.TableName)
// Input: "sales" → Output: "SALES"

// trim() - Remove whitespace
@trim(activity('Lookup1').output.firstRow.TableName)
// Input: "  Sales  " → Output: "Sales"

// split() - Split string into array
@split(pipeline().parameters.TableList, ',')
// Input: "Sales,Customer,Product" → Output: ["Sales", "Customer", "Product"]

// join() - Join array into string
@join(activity('GetTables').output.value, ', ')
// Input: ["Sales", "Customer"] → Output: "Sales, Customer"

// startsWith() / endsWith() - Check string start/end
@startsWith(pipeline().parameters.FileName, 'Sales')
// Returns: true or false

@endsWith(pipeline().parameters.FileName, '.csv')
// Returns: true or false

// indexOf() - Find position of substring
@indexOf(pipeline().parameters.FilePath, '/')
// Input: "Files/Sales/data.csv" → Output: 5

// lastIndexOf() - Find last occurrence
@lastIndexOf(pipeline().parameters.FilePath, '/')
// Input: "Files/Sales/data.csv" → Output: 11
```

**2. Logical Functions:**

```json
// if() - Conditional expression
@if(
    equals(pipeline().parameters.Environment, 'prod'),
    'https://prod.api.com',
    'https://dev.api.com'
)

// equals() - Equality check
@equals(variables('Status'), 'Success')

// greater() / less() - Numeric comparison
@greater(activity('CopyData').output.rowsCopied, 1000)
@less(variables('ErrorCount'), 5)

// greaterOrEquals() / lessOrEquals()
@greaterOrEquals(activity('Lookup1').output.count, 1)

// and() / or() / not() - Logical operators
@and(
    equals(pipeline().parameters.Environment, 'prod'),
    greater(activity('CopyData').output.rowsCopied, 0)
)

@or(
    equals(variables('Status'), 'Success'),
    equals(variables('Status'), 'Warning')
)

@not(equals(variables('HasError'), true))

// contains() - Check if string/array contains value
@contains(pipeline().parameters.TableName, 'Fact')
// Check if "Fact" appears in table name

@contains(activity('GetTables').output.value, 'Sales')
// Check if array contains "Sales"

// empty() - Check if value is empty
@empty(pipeline().parameters.OptionalParam)
// Returns true if parameter is null or empty string

// coalesce() - Return first non-null value
@coalesce(
    pipeline().parameters.CustomDate,
    utcnow()
)
// Use CustomDate if provided, otherwise use current time
```

**3. Conversion Functions:**

```json
// string() - Convert to string
@string(activity('CopyData').output.rowsCopied)
// Convert number to string: 1234 → "1234"

// int() - Convert to integer
@int(pipeline().parameters.BatchSize)
// Convert string to int: "100" → 100

// float() - Convert to float
@float(activity('Lookup1').output.firstRow.Price)

// bool() - Convert to boolean
@bool(pipeline().parameters.IsEnabled)
// "true" → true, "false" → false

// json() - Parse JSON string
@json(activity('WebActivity').output.Response)

// base64() - Encode to base64
@base64('username:password')

// base64ToString() - Decode base64
@base64ToString(pipeline().parameters.EncodedValue)

// uriComponent() - URL encode
@uriComponent(pipeline().parameters.SearchQuery)
// Input: "sales data" → Output: "sales%20data"

// uriComponentToString() - URL decode
@uriComponentToString(pipeline().parameters.EncodedUrl)
```

**4. Date & Time Functions:**

```json
// utcnow() - Current UTC timestamp
@utcnow()
// Result: "2026-04-25T14:30:15.1234567Z"

// formatDateTime() - Format date/time
@formatDateTime(utcnow(), 'yyyy-MM-dd')
// Result: "2026-04-25"

@formatDateTime(utcnow(), 'yyyyMMdd')
// Result: "20260425"

@formatDateTime(utcnow(), 'yyyy/MM/dd HH:mm:ss')
// Result: "2026/04/25 14:30:15"

@formatDateTime(pipeline().parameters.StartDate, 'yyyy-MM-dd')

// Common format patterns:
// yyyy-MM-dd          → 2026-04-25
// yyyyMMdd            → 20260425
// yyyy/MM/dd          → 2026/04/25
// dd-MMM-yyyy         → 25-Apr-2026
// HH:mm:ss            → 14:30:15
// yyyy-MM-ddTHH:mm:ss → 2026-04-25T14:30:15

// addDays() / addHours() / addMinutes() - Date arithmetic
@addDays(utcnow(), -1)
// Yesterday's date

@addDays(utcnow(), 7)
// 7 days from now

@addHours(utcnow(), -5)
// 5 hours ago

@addMinutes(pipeline().TriggerTime, 30)
// 30 minutes after trigger

// convertFromUtc() - Convert timezone
@convertFromUtc(utcnow(), 'Eastern Standard Time', 'yyyy-MM-dd HH:mm:ss')

// convertToUtc() - Convert to UTC
@convertToUtc(pipeline().parameters.LocalTime, 'Pacific Standard Time')

// dayOfWeek() - Get day of week (0=Sunday, 6=Saturday)
@dayOfWeek(utcnow())

// dayOfMonth() / dayOfYear()
@dayOfMonth(utcnow())  // 1-31
@dayOfYear(utcnow())   // 1-366

// ticks() - Get ticks since epoch
@ticks(utcnow())
// Result: 638494926151234567
```

**5. Array Functions:**

```json
// array() - Create array
@array('Sales', 'Customer', 'Product')

// length() - Get array length
@length(activity('GetTables').output.value)
// Returns number of items in array

// first() / last() - Get first/last element
@first(activity('GetTables').output.value)
@last(pipeline().parameters.TableList)

// take() - Get first N elements
@take(activity('GetTables').output.value, 5)
// Get first 5 items

// skip() - Skip first N elements
@skip(activity('GetTables').output.value, 10)
// Skip first 10 items

// union() - Combine arrays
@union(
    array('Sales', 'Customer'),
    array('Product', 'Employee')
)
// Result: ['Sales', 'Customer', 'Product', 'Employee']

// intersection() - Common elements
@intersection(
    activity('List1').output.value,
    activity('List2').output.value
)

// range() - Create number sequence
@range(1, 10)
// Result: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

**6. Math Functions:**

```json
// add() / sub() / mul() / div() - Arithmetic
@add(activity('Copy1').output.rowsCopied, activity('Copy2').output.rowsCopied)
@sub(100, variables('ErrorCount'))
@mul(pipeline().parameters.BatchSize, 10)
@div(activity('CopyData').output.dataWritten, 1024)  // Convert to KB

// mod() - Modulo
@mod(activity('Lookup1').output.count, 10)
// Get remainder when divided by 10

// max() / min() - Find maximum/minimum
@max(10, variables('ErrorCount'))
@min(activity('Copy1').output.rowsCopied, 1000)

// rand() - Random number
@rand()
// Returns random number between 0 and 2147483647

@rand(1, 100)
// Random number between 1 and 100
```

**7. Collection Functions:**

```json
// contains() - Check if collection contains item
@contains(activity('GetTables').output.value, 'Sales')

// join() - Join array elements
@join(activity('GetTables').output.value, ',')

// reverse() - Reverse array order
@reverse(activity('GetList').output.value)
```

---

**Common Patterns & Real-World Examples:**

**1. Dynamic File Naming with Timestamps:**

```json
// Pattern: TableName_YYYYMMDD_HHMMSS.parquet
{
  "fileName": {
    "value": "@concat(
      pipeline().parameters.TableName,
      '_',
      formatDateTime(utcnow(), 'yyyyMMdd'),
      '_',
      formatDateTime(utcnow(), 'HHmmss'),
      '.parquet'
    )",
    "type": "Expression"
  }
}
// Result: "Sales_20260425_143015.parquet"
```

**2. Environment-Specific Configuration:**

```json
// Different connection strings per environment
{
  "connectionString": {
    "value": "@if(
      equals(pipeline().parameters.Environment, 'prod'),
      'Server=prod-server.database.windows.net',
      if(
        equals(pipeline().parameters.Environment, 'test'),
        'Server=test-server.database.windows.net',
        'Server=dev-server.database.windows.net'
      )
    )",
    "type": "Expression"
  }
}
```

**3. Watermark-Based Incremental Load:**

```json
// SQL query with dynamic watermark
{
  "sqlReaderQuery": {
    "value": "@concat(
      'SELECT * FROM ',
      pipeline().parameters.SchemaName,
      '.',
      pipeline().parameters.TableName,
      ' WHERE ModifiedDate > ''',
      activity('GetWatermark').output.firstRow.MaxModifiedDate,
      ''' AND ModifiedDate <= ''',
      utcnow(),
      ''''
    )",
    "type": "Expression"
  }
}
// Result: "SELECT * FROM dbo.Sales WHERE ModifiedDate > '2026-04-24' AND ModifiedDate <= '2026-04-25'"
```

**4. Dynamic Folder Structure:**

```json
// OneLake path with date partitioning
{
  "folderPath": {
    "value": "@concat(
      'Files/',
      pipeline().parameters.Environment,
      '/',
      pipeline().parameters.TableName,
      '/year=',
      formatDateTime(utcnow(), 'yyyy'),
      '/month=',
      formatDateTime(utcnow(), 'MM'),
      '/day=',
      formatDateTime(utcnow(), 'dd')
    )",
    "type": "Expression"
  }
}
// Result: "Files/prod/Sales/year=2026/month=04/day=25"
```

**5. Conditional Activity Execution:**

```json
// Only run if data was copied
{
  "name": "ProcessData",
  "type": "SparkNotebook",
  "dependsOn": [{
    "activity": "CopyData",
    "dependencyConditions": ["Succeeded"]
  }],
  "typeProperties": {
    "notebook": {
      "value": "@if(
        greater(activity('CopyData').output.rowsCopied, 0),
        'ProcessFullData',
        'ProcessEmpty'
      )",
      "type": "Expression"
    }
  }
}
```

**6. Error Handling & Logging:**

```json
// Log activity details to table
{
  "name": "LogExecution",
  "type": "Script",
  "typeProperties": {
    "scripts": [{
      "text": {
        "value": "@concat(
          'INSERT INTO dbo.PipelineLog VALUES (',
          '''', pipeline().RunId, ''', ',
          '''', pipeline().Pipeline, ''', ',
          '''', activity('CopyData').output.rowsCopied, ''', ',
          '''', formatDateTime(utcnow(), 'yyyy-MM-dd HH:mm:ss'), '''',
          ')'
        )",
        "type": "Expression"
      }
    }]
  }
}
```

**7. ForEach with Lookup Pattern:**

```json
// Get list of tables from control table
{
  "name": "GetTableList",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "SELECT SchemaName, TableName FROM dbo.ControlTable WHERE IsActive = 1"
    },
    "firstRowOnly": false
  }
}

// Loop through each table
{
  "name": "ForEachTable",
  "type": "ForEach",
  "dependsOn": [{
    "activity": "GetTableList",
    "dependencyConditions": ["Succeeded"]
  }],
  "typeProperties": {
    "items": {
      "value": "@activity('GetTableList').output.value",
      "type": "Expression"
    },
    "isSequential": false,
    "batchCount": 10,
    "activities": [{
      "name": "CopyTable",
      "type": "Copy",
      "inputs": [{
        "parameters": {
          "SchemaName": {
            "value": "@item().SchemaName",
            "type": "Expression"
          },
          "TableName": {
            "value": "@item().TableName",
            "type": "Expression"
          }
        }
      }]
    }]
  }
}
```

**8. Window-Based Processing (Tumbling Window):**

```json
// Process data in time windows
{
  "query": {
    "value": "@concat(
      'SELECT * FROM Sales WHERE ',
      'OrderDate >= ''', trigger().outputs.windowStartTime, ''' ',
      'AND OrderDate < ''', trigger().outputs.windowEndTime, ''''
    )",
    "type": "Expression"
  }
}
// If window is 2026-04-25 00:00 to 2026-04-26 00:00:
// Result: "SELECT * FROM Sales WHERE OrderDate >= '2026-04-25 00:00' AND OrderDate < '2026-04-26 00:00'"
```

**9. File Pattern Matching:**

```json
// Process all files matching pattern
{
  "fileName": {
    "value": "@concat(
      'Sales_',
      formatDateTime(addDays(utcnow(), -1), 'yyyyMMdd'),
      '_*.csv'
    )",
    "type": "Expression"
  }
}
// Result: "Sales_20260424_*.csv"
// Matches: Sales_20260424_001.csv, Sales_20260424_002.csv, etc.
```

**10. Nested Expressions:**

```json
// Complex nested expression
{
  "value": "@concat(
    if(
      equals(pipeline().parameters.Environment, 'prod'),
      'https://prod.blob.core.windows.net/',
      'https://dev.blob.core.windows.net/'
    ),
    pipeline().parameters.ContainerName,
    '/',
    formatDateTime(
      if(
        empty(pipeline().parameters.CustomDate),
        utcnow(),
        pipeline().parameters.CustomDate
      ),
      'yyyy/MM/dd'
    ),
    '/',
    replace(pipeline().parameters.FileName, ' ', '_')
  )",
  "type": "Expression"
}
// Combines: environment logic, date handling, string manipulation
```

---

**Expression Validation & Debugging:**

**Common Errors:**

```json
// ❌ WRONG: Missing @ symbol
"value": "pipeline().parameters.TableName"  // Treated as literal string

// ✅ CORRECT: Always use @ for expressions
"value": "@pipeline().parameters.TableName"

// ❌ WRONG: Single quotes inside expression
"value": "@concat('Table', pipeline().parameters.Name, 'Data')"

// ✅ CORRECT: Use double quotes or escape
"value": "@concat('Table', pipeline().parameters.Name, 'Data')"

// ❌ WRONG: Missing type declaration
"tableName": {
  "value": "@pipeline().parameters.TableName"
  // Missing "type": "Expression"
}

// ✅ CORRECT: Include type
"tableName": {
  "value": "@pipeline().parameters.TableName",
  "type": "Expression"
}

// ❌ WRONG: Accessing non-existent output
"value": "@activity('Lookup1').output.result"  
// Should be .output.firstRow or .output.value

// ✅ CORRECT: Use correct output structure
"value": "@activity('Lookup1').output.firstRow.ColumnName"
```

**Debugging Tips:**

1. **Use Set Variable to Test Expressions:**
   ```json
   {
     "name": "TestExpression",
     "type": "SetVariable",
     "typeProperties": {
       "variableName": "TestResult",
       "value": "@concat('Result: ', string(activity('Copy1').output.rowsCopied))"
     }
   }
   ```
   Then check variable value in monitoring.

2. **Use If Condition to Validate:**
   ```json
   {
     "name": "CheckValue",
     "type": "IfCondition",
     "typeProperties": {
       "expression": {
         "value": "@greater(activity('Lookup1').output.count, 0)",
         "type": "Expression"
       }
     }
   }
   ```

3. **Enable Logging:**
   - Monitor pipeline runs
   - Check activity inputs/outputs
   - Review error messages
   - Use Script activity to log to table

4. **Test in Stages:**
   - Start with simple expressions
   - Build complexity gradually
   - Test each function separately
   - Combine only after validation

---

**Best Practices:**

**1. Naming Conventions:**
```json
// ✅ GOOD: Descriptive parameter names
"parameters": {
  "SourceSchemaName": "dbo",
  "SourceTableName": "Sales",
  "TargetLakehousePath": "Tables/Sales",
  "IncrementalLoadDate": "2026-04-25"
}

// ❌ BAD: Unclear names
"parameters": {
  "p1": "dbo",
  "p2": "Sales",
  "path": "Tables/Sales"
}
```

**2. Use Meaningful Defaults:**
```json
"parameters": {
  "Environment": {
    "type": "String",
    "defaultValue": "dev"  // Safe default
  },
  "BatchSize": {
    "type": "Integer",
    "defaultValue": 1000   // Reasonable default
  }
}
```

**3. Validate Early:**
```json
// Add validation at pipeline start
{
  "name": "ValidateParameters",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@or(
        empty(pipeline().parameters.TableName),
        equals(length(pipeline().parameters.TableName), 0)
      )",
      "type": "Expression"
    },
    "ifTrueActivities": [{
      "name": "Fail",
      "type": "Fail",
      "typeProperties": {
        "message": "TableName parameter is required",
        "errorCode": "400"
      }
    }]
  }
}
```

**4. Keep Expressions Readable:**
```json
// ❌ BAD: Complex, hard to read
"value": "@concat(if(equals(pipeline().parameters.Environment,'prod'),'https://prod.api.com',if(equals(pipeline().parameters.Environment,'test'),'https://test.api.com','https://dev.api.com')),'/api/',pipeline().parameters.Endpoint,'?date=',formatDateTime(utcnow(),'yyyy-MM-dd'))"

// ✅ GOOD: Use Set Variable for complex logic
{
  "name": "SetBaseUrl",
  "type": "SetVariable",
  "typeProperties": {
    "variableName": "BaseUrl",
    "value": "@if(
      equals(pipeline().parameters.Environment, 'prod'),
      'https://prod.api.com',
      if(
        equals(pipeline().parameters.Environment, 'test'),
        'https://test.api.com',
        'https://dev.api.com'
      )
    )"
  }
}

// Then use variable
"value": "@concat(
  variables('BaseUrl'),
  '/api/',
  pipeline().parameters.Endpoint,
  '?date=',
  formatDateTime(utcnow(), 'yyyy-MM-dd')
)"
```

**5. Document Complex Expressions:**
```json
{
  "description": "Builds dynamic file path: Environment/TableName/year=YYYY/month=MM/day=DD/file_YYYYMMDD.parquet",
  "folderPath": {
    "value": "@concat(
      pipeline().parameters.Environment, '/',
      pipeline().parameters.TableName, '/',
      'year=', formatDateTime(utcnow(), 'yyyy'), '/',
      'month=', formatDateTime(utcnow(), 'MM'), '/',
      'day=', formatDateTime(utcnow(), 'dd')
    )",
    "type": "Expression"
  }
}
```

**6. Use Consistent Patterns:**
```json
// Standard pattern for all pipelines
"parameters": {
  "Environment": "dev/test/prod",
  "RunDate": "YYYY-MM-DD",
  "SourceSystem": "System name",
  "TargetPath": "OneLake path"
}
```

**7. Avoid Hardcoding:**
```json
// ❌ BAD: Hardcoded values
"connectionString": "Server=prod-server.database.windows.net"

// ✅ GOOD: Use parameters
"connectionString": "@pipeline().parameters.ConnectionString"

// ✅ EVEN BETTER: Use linked service with parameterization
```

**8. Handle Nulls & Empty Values:**
```json
// Use coalesce for default values
"value": "@coalesce(
  pipeline().parameters.CustomDate,
  formatDateTime(utcnow(), 'yyyy-MM-dd')
)"

// Check for empty before using
"value": "@if(
  empty(pipeline().parameters.OptionalParam),
  'DefaultValue',
  pipeline().parameters.OptionalParam
)"
```

---

**Interview Questions:**

**1. Q: What is dynamic content in Fabric Data Factory, and why is it important?**

**A:** Dynamic content allows you to build flexible, parameterized pipelines using runtime expressions instead of hardcoded values. It's crucial for:

**Key Benefits:**
- **Reusability:** One pipeline can handle multiple tables/files/environments
- **Maintainability:** Change behavior via parameters, not code
- **Scalability:** Metadata-driven solutions that handle 100s of objects
- **Flexibility:** Adapt to different environments (dev/test/prod)
- **Efficiency:** Reduce from 50 static pipelines to 1 dynamic pipeline

**Example Impact:**
```
Static Approach:
- 50 pipelines (one per table)
- Any change = 50 updates
- Deployment nightmare
- Error-prone

Dynamic Approach:
- 1 pipeline
- Control table drives execution
- Change = update control table
- Single point of maintenance
```

**How It Works:**
1. Click "Add dynamic content" in any parameter field
2. Expression Builder opens
3. Use system variables (@pipeline, @activity, @trigger)
4. Apply functions (concat, formatDateTime, if, etc.)
5. Expression evaluates at runtime

**Real-World Usage:**
```json
// Dynamic file path with environment and date
@concat(
  'Files/',
  pipeline().parameters.Environment,  // dev/test/prod
  '/Sales/',
  formatDateTime(utcnow(), 'yyyy/MM/dd'),  // 2026/04/25
  '/sales.parquet'
)
// Result: "Files/prod/Sales/2026/04/25/sales.parquet"
```

Without dynamic content, you'd need separate hardcoded pipelines for each environment and manually update paths daily.

---

**2. Q: Explain the difference between @pipeline(), @activity(), and @trigger() system variables?**

**A:** These are the three main system variables for accessing pipeline metadata:

**@pipeline() - Pipeline Metadata:**
- Access pipeline-level information
- Available in all activities
- Common uses: parameters, run ID, pipeline name

```json
@pipeline().parameters.TableName     // Pipeline parameter
@pipeline().RunId                    // Unique execution ID
@pipeline().Pipeline                 // Pipeline name
@pipeline().TriggerType              // Manual/Schedule/Event
```

**Use Case:** Passing configuration to activities
```json
"tableName": "@pipeline().parameters.TableName"
```

---

**@activity() - Previous Activity Outputs:**
- Access outputs from earlier activities in pipeline
- Only available AFTER the activity has completed
- Must respect dependency chain

```json
@activity('Lookup1').output.firstRow.MaxDate    // Lookup result
@activity('Copy1').output.rowsCopied            // Copy statistics
@activity('Copy1').error.message                // Error info (in failure path)
```

**Use Case:** Chain activities together
```json
// Get watermark from Lookup, use in Copy
{
  "name": "GetWatermark",
  "type": "Lookup",
  "output": { "firstRow": { "MaxDate": "2026-04-24" } }
}

{
  "name": "CopyIncremental",
  "type": "Copy",
  "dependsOn": [{"activity": "GetWatermark"}],
  "typeProperties": {
    "source": {
      "query": "@concat(
        'SELECT * FROM Sales WHERE ModifiedDate > ''',
        activity('GetWatermark').output.firstRow.MaxDate,
        ''''
      )"
    }
  }
}
```

---

**@trigger() - Trigger Information:**
- Only available when pipeline runs from trigger (NOT manual runs)
- Access trigger metadata and outputs
- Critical for scheduled/event-based pipelines

```json
@trigger().scheduledTime              // When supposed to run
@trigger().startTime                  // When actually started
@trigger().outputs.windowStartTime    // Tumbling window start
@trigger().outputs.windowEndTime      // Tumbling window end
@trigger().outputs.body.fileName      // Event trigger file name
```

**Use Case:** Tumbling window incremental load
```json
{
  "query": "@concat(
    'SELECT * FROM Sales WHERE ',
    'OrderDate >= ''', trigger().outputs.windowStartTime, ''' ',
    'AND OrderDate < ''', trigger().outputs.windowEndTime, ''''
  )"
}
// Automatically processes each 24-hour window
```

**Key Differences:**

| Variable | Scope | When Available | Primary Use |
|----------|-------|----------------|-------------|
| @pipeline() | Pipeline-wide | Always | Configuration, parameters |
| @activity() | After activity | Only if activity completed | Chain activities, decisions |
| @trigger() | Trigger-based | Only when triggered | Scheduled/event processing |

**Common Mistake:**
```json
// ❌ WRONG: Using @trigger() in manual run
"query": "@trigger().outputs.windowStartTime"  // Fails if run manually

// ✅ CORRECT: Provide fallback
"query": "@if(
  equals(pipeline().TriggerType, 'Manual'),
  formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd'),
  trigger().outputs.windowStartTime
)"
```

---

**3. Q: How do you access Lookup Activity output in subsequent activities? Provide examples.**

**A:** Lookup Activity has two output modes, accessed differently:

**Output Structure:**
```json
{
  "count": 5,                    // Number of rows
  "value": [                     // Array of all rows
    {"ID": 1, "Name": "Sales", "Status": "Active"},
    {"ID": 2, "Name": "Customer", "Status": "Active"},
    ...
  ],
  "firstRow": {                  // First row (if firstRowOnly = true)
    "ID": 1,
    "Name": "Sales",
    "Status": "Active"
  }
}
```

**Mode 1: First Row Only (firstRowOnly = true)**

Lookup Configuration:
```json
{
  "name": "GetWatermark",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "SELECT MAX(ModifiedDate) AS MaxDate FROM dbo.Watermark"
    },
    "firstRowOnly": true  // ← Important!
  }
}
// Output: { "firstRow": { "MaxDate": "2026-04-24" } }
```

Access the output:
```json
// Access entire first row
@activity('GetWatermark').output.firstRow

// Access specific column
@activity('GetWatermark').output.firstRow.MaxDate

// Use in Copy Activity
{
  "name": "CopyIncremental",
  "type": "Copy",
  "typeProperties": {
    "source": {
      "query": "@concat(
        'SELECT * FROM Sales WHERE ModifiedDate > ''',
        activity('GetWatermark').output.firstRow.MaxDate,
        ''''
      )"
    }
  }
}
```

**Mode 2: All Rows (firstRowOnly = false)**

Lookup Configuration:
```json
{
  "name": "GetTableList",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "SELECT SchemaName, TableName FROM dbo.ControlTable WHERE IsActive = 1"
    },
    "firstRowOnly": false  // ← Get all rows
  }
}
// Output: { 
//   "count": 3,
//   "value": [
//     {"SchemaName": "dbo", "TableName": "Sales"},
//     {"SchemaName": "dbo", "TableName": "Customer"},
//     {"SchemaName": "dbo", "TableName": "Product"}
//   ]
// }
```

Access the output:
```json
// Access entire array
@activity('GetTableList').output.value

// Access count
@activity('GetTableList').output.count

// Use in ForEach (most common)
{
  "name": "ForEachTable",
  "type": "ForEach",
  "typeProperties": {
    "items": {
      "value": "@activity('GetTableList').output.value",  // Pass entire array
      "type": "Expression"
    },
    "activities": [{
      "name": "CopyTable",
      "type": "Copy",
      "inputs": [{
        "parameters": {
          "SchemaName": "@item().SchemaName",   // Current item from array
          "TableName": "@item().TableName"       // Current item from array
        }
      }]
    }]
  }
}
```

**Other Output Properties:**
```json
// Number of rows returned
@activity('GetTableList').output.count

// Check if any rows returned
@greater(activity('GetTableList').output.count, 0)

// Access first item in array (when firstRowOnly = false)
@first(activity('GetTableList').output.value)

// Access last item
@last(activity('GetTableList').output.value)
```

**Real-World Pattern: Watermark + ForEach + Copy**

```json
// Step 1: Get list of tables with their watermarks
{
  "name": "GetTablesWithWatermark",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "query": "SELECT SchemaName, TableName, LastLoadDate FROM dbo.ControlTable WHERE IsActive = 1"
    },
    "firstRowOnly": false
  }
}

// Step 2: Loop through each table
{
  "name": "ForEachTable",
  "type": "ForEach",
  "typeProperties": {
    "items": "@activity('GetTablesWithWatermark').output.value",
    "activities": [{
      "name": "CopyTableIncremental",
      "type": "Copy",
      "typeProperties": {
        "source": {
          "query": "@concat(
            'SELECT * FROM ',
            item().SchemaName, '.', item().TableName,
            ' WHERE ModifiedDate > ''',
            item().LastLoadDate,  // ← Watermark from Lookup
            ''''
          )"
        }
      }
    }]
  }
}
```

**Common Mistakes:**
```json
// ❌ WRONG: Using .firstRow when firstRowOnly = false
@activity('GetTableList').output.firstRow.TableName
// Will fail! Use .value[0] or first()

// ❌ WRONG: Using .value when firstRowOnly = true
@activity('GetWatermark').output.value
// Will fail! Use .firstRow

// ✅ CORRECT: Match access method to firstRowOnly setting
firstRowOnly = true  → Use .firstRow
firstRowOnly = false → Use .value (array)
```

**Best Practice - Defensive Coding:**
```json
// Check if Lookup returned data before using
{
  "name": "CheckLookupResult",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@greater(activity('GetWatermark').output.count, 0)",
      "type": "Expression"
    },
    "ifTrueActivities": [
      // Process normally
    ],
    "ifFalseActivities": [
      // Handle no data scenario
    ]
  }
}
```

---

**4. Q: What are some commonly used functions in pipeline expressions? Provide practical examples.**

**A:** Here are the most frequently used functions categorized by purpose:

**1. String Manipulation (Most Used):**

**concat() - Combine Strings**
```json
// Dynamic table name: Sales_20260425
@concat('Sales_', formatDateTime(utcnow(), 'yyyyMMdd'))

// Full file path: Files/prod/Sales/2026/04/25/data.parquet
@concat(
  'Files/',
  pipeline().parameters.Environment,
  '/Sales/',
  formatDateTime(utcnow(), 'yyyy/MM/dd'),
  '/data.parquet'
)

// SQL query with parameters
@concat(
  'SELECT * FROM ',
  pipeline().parameters.SchemaName,
  '.',
  pipeline().parameters.TableName,
  ' WHERE Date = ''',
  formatDateTime(utcnow(), 'yyyy-MM-dd'),
  ''''
)
```

**replace() - Replace Text**
```json
// Switch environment in path
@replace(
  pipeline().parameters.FilePath,
  'dev',
  pipeline().parameters.Environment
)
// Input: "Files/dev/Sales" → Output: "Files/prod/Sales" (if Environment = 'prod')

// Remove spaces from table names
@replace(pipeline().parameters.TableName, ' ', '_')
// Input: "Sales Data" → Output: "Sales_Data"
```

**split() - Split String into Array**
```json
// Split comma-separated list
@split(pipeline().parameters.TableList, ',')
// Input: "Sales,Customer,Product" 
// Output: ["Sales", "Customer", "Product"]

// Use in ForEach
{
  "items": "@split(pipeline().parameters.TableList, ',')",
  "activities": [...]
}

// Get file name from path
@last(split(pipeline().parameters.FilePath, '/'))
// Input: "Files/Sales/data.csv" → Output: "data.csv"
```

---

**2. Date/Time Operations (Critical for Data Pipelines):**

**formatDateTime() - Format Dates**
```json
// Standard formats
@formatDateTime(utcnow(), 'yyyy-MM-dd')        // 2026-04-25
@formatDateTime(utcnow(), 'yyyyMMdd')          // 20260425
@formatDateTime(utcnow(), 'yyyy/MM/dd')        // 2026/04/25
@formatDateTime(utcnow(), 'yyyy-MM-dd HH:mm:ss')  // 2026-04-25 14:30:15

// Use in file naming
@concat('Sales_', formatDateTime(utcnow(), 'yyyyMMdd'), '.csv')
// Result: Sales_20260425.csv

// Use in partition paths
@concat(
  'year=', formatDateTime(utcnow(), 'yyyy'),
  '/month=', formatDateTime(utcnow(), 'MM'),
  '/day=', formatDateTime(utcnow(), 'dd')
)
// Result: year=2026/month=04/day=25
```

**addDays() - Date Arithmetic**
```json
// Yesterday
@formatDateTime(addDays(utcnow(), -1), 'yyyy-MM-dd')
// 2026-04-24

// Last 7 days window
WHERE Date >= '@{formatDateTime(addDays(utcnow(), -7), 'yyyy-MM-dd')}'
  AND Date < '@{formatDateTime(utcnow(), 'yyyy-MM-dd')}'

// Next month
@formatDateTime(addDays(utcnow(), 30), 'yyyy-MM-dd')

// Beginning of month (trick: go to day 1)
@formatDateTime(addDays(utcnow(), sub(1, dayOfMonth(utcnow()))), 'yyyy-MM-dd')
```

**utcnow() - Current Timestamp**
```json
// Current UTC time
@utcnow()
// 2026-04-25T14:30:15.1234567Z

// Use in logging
INSERT INTO dbo.PipelineLog VALUES (
  '@{pipeline().RunId}',
  '@{utcnow()}'
)
```

---

**3. Conditional Logic (Control Flow):**

**if() - Ternary Operator**
```json
// Environment-specific URLs
@if(
  equals(pipeline().parameters.Environment, 'prod'),
  'https://prod.api.com',
  'https://dev.api.com'
)

// Nested if for multiple environments
@if(
  equals(pipeline().parameters.Environment, 'prod'),
  'https://prod.api.com',
  if(
    equals(pipeline().parameters.Environment, 'test'),
    'https://test.api.com',
    'https://dev.api.com'
  )
)

// Use default if parameter empty
@if(
  empty(pipeline().parameters.CustomDate),
  formatDateTime(utcnow(), 'yyyy-MM-dd'),
  pipeline().parameters.CustomDate
)
```

**equals() - Equality Check**
```json
// Check environment
@equals(pipeline().parameters.Environment, 'prod')

// Check activity status
@equals(activity('CopyData').output.status, 'Succeeded')

// Use in If Condition activity
{
  "expression": {
    "value": "@equals(pipeline().parameters.LoadType, 'Full')",
    "type": "Expression"
  },
  "ifTrueActivities": [/* Full load */],
  "ifFalseActivities": [/* Incremental load */]
}
```

**greater() / less() - Numeric Comparison**
```json
// Check if data was copied
@greater(activity('CopyData').output.rowsCopied, 0)

// Check if under threshold
@less(activity('GetCount').output.firstRow.RecordCount, 1000000)

// Use in branching
{
  "expression": {
    "value": "@greater(activity('CopyData').output.rowsCopied, 10000)",
    "type": "Expression"
  },
  "ifTrueActivities": [/* Run complex processing */],
  "ifFalseActivities": [/* Skip or simple processing */]
}
```

---

**4. Data Type Conversion:**

**string() - Convert to String**
```json
// Convert number to string for concatenation
@concat('Copied ', string(activity('CopyData').output.rowsCopied), ' rows')
// Result: "Copied 1234 rows"

// Log metrics
INSERT INTO dbo.Log VALUES (
  '@{pipeline().RunId}',
  '@{string(activity('CopyData').output.rowsCopied)}'
)
```

**int() - Convert to Integer**
```json
// Convert string parameter to int for math
@add(
  int(pipeline().parameters.BatchSize),
  100
)

// Use in comparisons
@greater(
  int(activity('Lookup1').output.firstRow.RecordCount),
  1000
)
```

---

**5. Array Operations:**

**length() - Array/String Length**
```json
// Count items from Lookup
@length(activity('GetTableList').output.value)

// Check if array has items
@greater(length(activity('GetTableList').output.value), 0)

// String length
@length(pipeline().parameters.TableName)
```

**first() / last() - Array Endpoints**
```json
// Get first table from list
@first(activity('GetTableList').output.value)

// Get last file in folder
@last(activity('GetMetadata').output.childItems)

// Extract filename from path
@last(split(pipeline().parameters.FilePath, '/'))
// Input: "Files/Sales/data.csv" → Output: "data.csv"

// Extract extension
@last(split(pipeline().parameters.FileName, '.'))
// Input: "sales.csv" → Output: "csv"
```

**join() - Combine Array to String**
```json
// Convert array to comma-separated string
@join(activity('GetTableList').output.value, ', ')
// Input: ["Sales", "Customer", "Product"]
// Output: "Sales, Customer, Product"

// Create WHERE IN clause
@concat(
  'WHERE TableName IN (''',
  join(activity('GetTableList').output.value, ''', '''),
  ''')'
)
// Result: "WHERE TableName IN ('Sales', 'Customer', 'Product')"
```

---

**6. Null/Empty Handling:**

**coalesce() - First Non-Null Value**
```json
// Use custom date or default to today
@coalesce(
  pipeline().parameters.CustomDate,
  formatDateTime(utcnow(), 'yyyy-MM-dd')
)

// Use provided path or default
@coalesce(
  pipeline().parameters.CustomPath,
  'Files/default/path'
)
```

**empty() - Check if Empty**
```json
// Check if parameter provided
@empty(pipeline().parameters.OptionalParam)

// Use with if
@if(
  empty(pipeline().parameters.TableName),
  'DefaultTable',
  pipeline().parameters.TableName
)
```

---

**Real-World Combined Example:**

```json
// Complete dynamic file path with all concepts
{
  "folderPath": {
    "value": "@concat(
      'Files/',
      
      // Environment (dev/test/prod)
      toLower(pipeline().parameters.Environment),
      '/',
      
      // Table name with spaces replaced
      replace(pipeline().parameters.TableName, ' ', '_'),
      '/',
      
      // Date partition (use custom date or today)
      'year=',
      formatDateTime(
        coalesce(
          pipeline().parameters.CustomDate,
          utcnow()
        ),
        'yyyy'
      ),
      '/month=',
      formatDateTime(
        coalesce(
          pipeline().parameters.CustomDate,
          utcnow()
        ),
        'MM'
      ),
      '/day=',
      formatDateTime(
        coalesce(
          pipeline().parameters.CustomDate,
          utcnow()
        ),
        'dd'
      )
    )",
    "type": "Expression"
  },
  
  "fileName": {
    "value": "@concat(
      replace(pipeline().parameters.TableName, ' ', '_'),
      '_',
      formatDateTime(
        coalesce(pipeline().parameters.CustomDate, utcnow()),
        'yyyyMMdd_HHmmss'
      ),
      '.parquet'
    )",
    "type": "Expression"
  }
}

// Example result:
// Files/prod/Sales_Data/year=2026/month=04/day=25/Sales_Data_20260425_143015.parquet
```

**Function Frequency in Production:**
1. **concat()** - 95% of pipelines
2. **formatDateTime()** - 90% of pipelines
3. **pipeline().parameters.X** - 90% of pipelines
4. **activity().output.X** - 80% of pipelines
5. **if()** / **equals()** - 70% of pipelines
6. **addDays()** - 60% of pipelines
7. **replace()** / **split()** - 50% of pipelines
8. **greater()** / **less()** - 40% of pipelines
9. **coalesce()** / **empty()** - 30% of pipelines
10. **string()** / **int()** - 20% of pipelines

---

**5. Q: How would you implement environment-specific configuration (dev/test/prod) using dynamic expressions?**

**A:** Environment-specific configuration ensures pipelines work across environments without code changes. Here are multiple approaches:

**Approach 1: Pipeline Parameter with If() Function (Simple)**

```json
// Pipeline parameter
{
  "parameters": {
    "Environment": {
      "type": "String",
      "defaultValue": "dev"
    }
  }
}

// Use in connection string
{
  "serverName": {
    "value": "@if(
      equals(pipeline().parameters.Environment, 'prod'),
      'prod-sql-server.database.windows.net',
      if(
        equals(pipeline().parameters.Environment, 'test'),
        'test-sql-server.database.windows.net',
        'dev-sql-server.database.windows.net'
      )
    )",
    "type": "Expression"
  },
  
  "databaseName": {
    "value": "@if(
      equals(pipeline().parameters.Environment, 'prod'),
      'ProductionDB',
      'DevelopmentDB'
    )",
    "type": "Expression"
  }
}

// Use in file paths
{
  "folderPath": {
    "value": "@concat(
      'Files/',
      toLower(pipeline().parameters.Environment),
      '/Sales/',
      formatDateTime(utcnow(), 'yyyy/MM/dd')
    )",
    "type": "Expression"
  }
}
// Result: Files/prod/Sales/2026/04/25
```

**Approach 2: Parameterized Linked Services (Recommended)**

```json
// Define parameterized linked service
{
  "name": "SQL_Server_Parameterized",
  "type": "LinkedService",
  "properties": {
    "type": "AzureSqlDatabase",
    "parameters": {
      "ServerName": {
        "type": "String"
      },
      "DatabaseName": {
        "type": "String"
      }
    },
    "typeProperties": {
      "connectionString": "@{concat(
        'Server=', linkedService().ServerName,
        ';Database=', linkedService().DatabaseName,
        ';Authentication=MSI'
      )}"
    }
  }
}

// Use in pipeline with environment-specific values
{
  "name": "CopyActivity",
  "linkedServiceName": {
    "referenceName": "SQL_Server_Parameterized",
    "type": "LinkedServiceReference",
    "parameters": {
      "ServerName": {
        "value": "@if(
          equals(pipeline().parameters.Environment, 'prod'),
          'prod-server.database.windows.net',
          'dev-server.database.windows.net'
        )",
        "type": "Expression"
      },
      "DatabaseName": {
        "value": "@if(
          equals(pipeline().parameters.Environment, 'prod'),
          'ProdDB',
          'DevDB'
        )",
        "type": "Expression"
      }
    }
  }
}
```

**Approach 3: Configuration Lookup Table (Enterprise Pattern)**

```sql
-- Create configuration table
CREATE TABLE dbo.EnvironmentConfig (
    Environment VARCHAR(10),
    ConfigKey VARCHAR(100),
    ConfigValue VARCHAR(500)
);

-- Insert environment-specific configs
INSERT INTO dbo.EnvironmentConfig VALUES
('dev', 'SqlServer', 'dev-server.database.windows.net'),
('test', 'SqlServer', 'test-server.database.windows.net'),
('prod', 'SqlServer', 'prod-server.database.windows.net'),
('dev', 'ApiUrl', 'https://dev.api.company.com'),
('test', 'ApiUrl', 'https://test.api.company.com'),
('prod', 'ApiUrl', 'https://api.company.com'),
('dev', 'StorageAccount', 'devstorageaccount'),
('test', 'StorageAccount', 'teststorageaccount'),
('prod', 'StorageAccount', 'prodstorageaccount');
```

```json
// In pipeline: Lookup configuration
{
  "name": "GetEnvironmentConfig",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "sqlReaderQuery": "@concat(
        'SELECT ConfigKey, ConfigValue FROM dbo.EnvironmentConfig WHERE Environment = ''',
        pipeline().parameters.Environment,
        ''''
      )"
    },
    "firstRowOnly": false
  }
}

// Use configuration in subsequent activities
{
  "name": "CopyData",
  "type": "Copy",
  "dependsOn": [{
    "activity": "GetEnvironmentConfig",
    "dependencyConditions": ["Succeeded"]
  }],
  "typeProperties": {
    "source": {
      "type": "SqlSource",
      "connectionString": "@concat(
        'Server=',
        activity('GetEnvironmentConfig').output.value[0].ConfigValue,  // SqlServer
        ';Database=SalesDB'
      )"
    }
  }
}
```

**Approach 4: Global Parameters (Fabric-Wide)**

```json
// Define global parameters at workspace level
{
  "globalParameters": {
    "DevSqlServer": "dev-server.database.windows.net",
    "TestSqlServer": "test-server.database.windows.net",
    "ProdSqlServer": "prod-server.database.windows.net",
    "DevStorageAccount": "devstorageaccount",
    "ProdStorageAccount": "prodstorageaccount"
  }
}

// Use in pipelines
{
  "serverName": {
    "value": "@if(
      equals(pipeline().parameters.Environment, 'prod'),
      pipeline().globalParameters.ProdSqlServer,
      pipeline().globalParameters.DevSqlServer
    )",
    "type": "Expression"
  }
}
```

**Approach 5: Complete Environment Pattern (Production-Ready)**

```json
{
  "name": "EnvironmentAwarePipeline",
  "properties": {
    "parameters": {
      "Environment": {
        "type": "String",
        "defaultValue": "dev"
      }
    },
    "variables": {
      "SqlServer": { "type": "String" },
      "ApiUrl": { "type": "String" },
      "StorageAccount": { "type": "String" },
      "ParallelBatchSize": { "type": "Integer" }
    },
    "activities": [
      
      // Step 1: Set environment-specific variables
      {
        "name": "SetSqlServer",
        "type": "SetVariable",
        "typeProperties": {
          "variableName": "SqlServer",
          "value": "@if(
            equals(pipeline().parameters.Environment, 'prod'),
            'prod-server.database.windows.net',
            if(
              equals(pipeline().parameters.Environment, 'test'),
              'test-server.database.windows.net',
              'dev-server.database.windows.net'
            )
          )"
        }
      },
      
      {
        "name": "SetApiUrl",
        "type": "SetVariable",
        "typeProperties": {
          "variableName": "ApiUrl",
          "value": "@if(
            equals(pipeline().parameters.Environment, 'prod'),
            'https://api.company.com',
            concat('https://', toLower(pipeline().parameters.Environment), '.api.company.com')
          )"
        }
      },
      
      {
        "name": "SetStorageAccount",
        "type": "SetVariable",
        "typeProperties": {
          "variableName": "StorageAccount",
          "value": "@concat(
            toLower(pipeline().parameters.Environment),
            'storageaccount'
          )"
        }
      },
      
      {
        "name": "SetBatchSize",
        "type": "SetVariable",
        "typeProperties": {
          "variableName": "ParallelBatchSize",
          "value": "@if(
            equals(pipeline().parameters.Environment, 'prod'),
            20,  // Production: high parallelism
            if(
              equals(pipeline().parameters.Environment, 'test'),
              10,  // Test: medium
              5    // Dev: low
            )
          )"
        }
      },
      
      // Step 2: Use variables in activities
      {
        "name": "CopyData",
        "type": "Copy",
        "dependsOn": [
          {"activity": "SetSqlServer"},
          {"activity": "SetStorageAccount"}
        ],
        "typeProperties": {
          "source": {
            "type": "SqlSource",
            "connectionString": "@concat(
              'Server=', variables('SqlServer'),
              ';Database=SalesDB;Authentication=MSI'
            )"
          },
          "sink": {
            "type": "LakehouseSink",
            "folderPath": "@concat(
              'Files/',
              toLower(pipeline().parameters.Environment),
              '/Sales/',
              formatDateTime(utcnow(), 'yyyy/MM/dd')
            )"
          }
        }
      },
      
      {
        "name": "CallAPI",
        "type": "WebActivity",
        "dependsOn": [{"activity": "SetApiUrl"}],
        "typeProperties": {
          "url": "@concat(
            variables('ApiUrl'),
            '/api/data/upload'
          )",
          "method": "POST"
        }
      }
    ]
  }
}
```

**Best Practices:**

| Practice | Recommendation | Reason |
|----------|---------------|--------|
| **Default to Dev** | `defaultValue: "dev"` | Safety - prevent accidental prod runs |
| **Use toLower()** | `toLower(parameters.Environment)` | Consistency in comparisons |
| **Validate Early** | Check env at pipeline start | Fail fast on invalid environment |
| **Use Variables** | Set variables for complex expressions | Readability and reusability |
| **Document Values** | Add descriptions to parameters | Team clarity |
| **Centralize Config** | Use config table or global params | Single source of truth |
| **Test All Envs** | CI/CD pipeline tests dev/test/prod | Catch environment-specific issues |

**Trigger Configuration:**

```json
// Different triggers per environment
{
  "name": "Schedule_Dev",
  "properties": {
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "schedule": {
          "hours": [2]  // 2 AM
        }
      }
    },
    "pipelines": [{
      "pipelineReference": {
        "referenceName": "LoadData"
      },
      "parameters": {
        "Environment": "dev"
      }
    }]
  }
}

{
  "name": "Schedule_Prod",
  "properties": {
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Hour",
        "interval": 1  // Every hour in prod
      }
    },
    "pipelines": [{
      "pipelineReference": {
        "referenceName": "LoadData"
      },
      "parameters": {
        "Environment": "prod"
      }
    }]
  }
}
```

**Validation Pattern:**

```json
// Validate environment parameter
{
  "name": "ValidateEnvironment",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@or(
        or(
          equals(toLower(pipeline().parameters.Environment), 'dev'),
          equals(toLower(pipeline().parameters.Environment), 'test')
        ),
        equals(toLower(pipeline().parameters.Environment), 'prod')
      )",
      "type": "Expression"
    },
    "ifFalseActivities": [{
      "name": "FailInvalidEnvironment",
      "type": "Fail",
      "typeProperties": {
        "message": "@concat(
          'Invalid environment: ',
          pipeline().parameters.Environment,
          '. Must be dev, test, or prod.'
        )",
        "errorCode": "InvalidEnvironment"
      }
    }]
  }
}
```

**Result:** One codebase, three environments, zero duplication!

---

## Interview Questions by Topic

<!-- Consolidated interview questions organized by topic areas -->

---

## Scenario-Based Interview Questions

### Scenario 1: Performance Optimization

**Scenario:**
You have a Fabric Data Warehouse with a fact table containing 500 million rows. Business users report that dashboard queries are slow, taking 2-3 minutes to load. The dashboard filters data by Date, Region, and ProductCategory. How would you approach this performance issue?

**Answer:**

**Step 1: Diagnose the Problem**
```sql
-- Check query execution details
SELECT 
    query_id,
    command,
    total_elapsed_time_ms,
    row_count,
    result_cache_hit
FROM sys.dm_exec_requests
WHERE session_id = SESSION_ID();

-- Check if statistics are up to date
SELECT 
    t.name AS TableName,
    s.name AS StatName,
    STATS_DATE(s.object_id, s.stats_id) AS LastUpdated,
    sp.rows AS RowCount
FROM sys.stats s
INNER JOIN sys.tables t ON s.object_id = t.object_id
INNER JOIN sys.dm_db_partition_stats sp ON t.object_id = sp.object_id
WHERE t.name = 'FactSales'
ORDER BY LastUpdated;
```

**Step 2: Solutions**

**A. Create Statistics on Filter Columns:**
```sql
-- Create statistics on frequently filtered columns
CREATE STATISTICS stat_FactSales_Date ON Sales.FactSales(DateKey);
CREATE STATISTICS stat_FactSales_Region ON Sales.FactSales(RegionKey);
CREATE STATISTICS stat_FactSales_Category ON Sales.FactSales(CategoryKey);

-- Update statistics
UPDATE STATISTICS Sales.FactSales;
```

**B. Create Aggregated Summary Tables:**
```sql
-- Pre-aggregate common queries
CREATE TABLE Reports.DailySalesSummary AS
SELECT 
    d.Date,
    d.Year,
    d.Month,
    r.RegionName,
    c.CategoryName,
    COUNT(DISTINCT f.CustomerKey) AS Customers,
    SUM(f.Quantity) AS TotalQuantity,
    SUM(f.Amount) AS TotalRevenue
FROM Sales.FactSales f
INNER JOIN Dim.DimDate d ON f.DateKey = d.DateKey
INNER JOIN Dim.DimRegion r ON f.RegionKey = r.RegionKey
INNER JOIN Dim.DimCategory c ON f.CategoryKey = c.CategoryKey
GROUP BY d.Date, d.Year, d.Month, r.RegionName, c.CategoryName;

-- Refresh nightly via pipeline
```

**C. Implement Partitioning Strategy (if supported):**
```sql
-- Consider date-based partitioning for time-series queries
-- (Note: Check current Fabric limitations on partitioning)
```

**D. Use Power BI Aggregations:**
- Create aggregation tables in Power BI dataset
- Enable result caching
- Use DirectQuery with aggregations

**E. Optimize Query Patterns:**
```sql
-- ❌ BAD: SELECT *
SELECT * FROM Sales.FactSales WHERE Year = 2026;

-- ✅ GOOD: Only needed columns
SELECT 
    DateKey,
    ProductKey,
    CustomerKey,
    Quantity,
    Amount
FROM Sales.FactSales
WHERE DateKey BETWEEN 20260101 AND 20261231;
```

**Expected Outcome:**
- Query time reduced from 2-3 minutes to 5-10 seconds
- Dashboard loads under 15 seconds
- Improved user experience

---

### Scenario 2: Data Quality & Validation

**Scenario:**
Your team ingests customer data from multiple sources (CRM, ERP, Web) into a Fabric Lakehouse daily. Recently, business users discovered duplicate customer records and missing email addresses in reports. How do you implement data quality checks?

**Answer:**

**Step 1: Implement Data Quality Framework**

**A. Pre-Ingestion Validation (Pipeline Level):**
```json
{
  "activities": [
    {
      "name": "GetMetadata_SourceFile",
      "type": "GetMetadata",
      "typeProperties": {
        "dataset": "SourceCustomerData",
        "fieldList": ["itemName", "size", "lastModified"]
      }
    },
    {
      "name": "If_ValidateFileSize",
      "type": "IfCondition",
      "dependsOn": [{"activity": "GetMetadata_SourceFile"}],
      "typeProperties": {
        "expression": {
          "@greater(activity('GetMetadata_SourceFile').output.size, 0)"
        },
        "ifTrueActivities": [...],
        "ifFalseActivities": [
          {
            "name": "SendAlertEmail",
            "type": "WebActivity",
            "typeProperties": {
              "method": "POST",
              "url": "https://prod-123.logic.azure.com:443/workflows/...",
              "body": {
                "message": "Empty file detected: @{activity('GetMetadata_SourceFile').output.itemName}"
              }
            }
          }
        ]
      }
    }
  ]
}
```

**B. Notebook Validation Logic:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("DataQuality").getOrCreate()

# Read raw data
df_raw = spark.read.format("csv").option("header", "true").load("Files/raw/customers/*.csv")

# Data Quality Checks
dq_results = {}

# 1. Check for duplicates
duplicate_count = df_raw.groupBy("CustomerID").count().filter(col("count") > 1).count()
dq_results['duplicates'] = duplicate_count

# 2. Check for null/missing values
null_check = df_raw.select([
    count(when(col(c).isNull() | isnan(c), c)).alias(c) 
    for c in df_raw.columns
])
dq_results['null_counts'] = null_check.collect()[0].asDict()

# 3. Email format validation
invalid_emails = df_raw.filter(
    ~col("Email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
).count()
dq_results['invalid_emails'] = invalid_emails

# 4. Check for required fields
missing_required = df_raw.filter(
    col("CustomerID").isNull() | 
    col("CustomerName").isNull() |
    col("Email").isNull()
).count()
dq_results['missing_required'] = missing_required

# Log results to quality table
from datetime import datetime
quality_df = spark.createDataFrame([{
    'run_date': datetime.now(),
    'source': 'CRM',
    'total_rows': df_raw.count(),
    'duplicate_count': duplicate_count,
    'invalid_emails': invalid_emails,
    'missing_required': missing_required,
    'status': 'PASS' if duplicate_count == 0 and invalid_emails == 0 else 'FAIL'
}])

quality_df.write.format("delta").mode("append").save("Tables/data_quality_log")

# If quality checks fail, stop pipeline
if dq_results['duplicates'] > 0 or dq_results['invalid_emails'] > 0:
    raise Exception(f"Data quality check failed: {dq_results}")
```

**C. Data Cleansing & Deduplication:**
```python
# Remove duplicates (keep most recent)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window_spec = Window.partitionBy("CustomerID").orderBy(desc("LastModified"))

df_deduped = df_raw.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# Fill missing emails with placeholder
df_clean = df_deduped.fillna({'Email': 'no-email@placeholder.com'})

# Write to clean zone
df_clean.write.format("delta").mode("overwrite").save("Tables/customers_clean")
```

**D. Create Data Quality Dashboard:**
```sql
-- Query quality metrics
CREATE VIEW Reports.vw_DataQualityMetrics AS
SELECT 
    run_date,
    source,
    total_rows,
    duplicate_count,
    invalid_emails,
    missing_required,
    status,
    CAST(duplicate_count AS FLOAT) / NULLIF(total_rows, 0) * 100 AS duplicate_pct,
    CAST(invalid_emails AS FLOAT) / NULLIF(total_rows, 0) * 100 AS invalid_email_pct
FROM data_quality_log
WHERE run_date >= DATEADD(DAY, -30, GETDATE());
```

**Step 2: Implement Alerts**
- Email notification if quality checks fail
- Slack/Teams integration for real-time alerts
- Stop pipeline execution on critical failures

**Expected Outcome:**
- 100% data quality before loading to warehouse
- Proactive alerts on data issues
- Audit trail of all quality checks

---

### Scenario 3: Incremental Data Loading

**Scenario:**
You need to load transactional sales data from an on-premises SQL Server database to Fabric Warehouse. The source table has 100 million rows and grows by ~500K rows daily. Full loads take 6+ hours. How do you implement efficient incremental loading?

**Answer:**

**Solution: Watermark-Based Incremental Load**

**Step 1: Setup Watermark Table**
```sql
-- Fabric Warehouse
CREATE TABLE ETL.Watermark (
    TableName VARCHAR(100) PRIMARY KEY NONCLUSTERED NOT ENFORCED,
    LastLoadTime DATETIME,
    LastLoadedID BIGINT,
    UpdatedDate DATETIME
);

INSERT INTO ETL.Watermark VALUES
('Sales', '1900-01-01', 0, GETDATE());
```

**Step 2: Pipeline Implementation**

**A. Lookup Last Watermark:**
```json
{
  "name": "Lookup_GetWatermark",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "DataWarehouseSource",
      "sqlReaderQuery": "SELECT LastLoadTime, LastLoadedID FROM ETL.Watermark WHERE TableName = 'Sales'"
    },
    "dataset": "FabricWarehouse"
  }
}
```

**B. Copy Incremental Data:**
```json
{
  "name": "Copy_IncrementalSales",
  "type": "Copy",
  "dependsOn": [{"activity": "Lookup_GetWatermark"}],
  "typeProperties": {
    "source": {
      "type": "SqlServerSource",
      "sqlReaderQuery": "
        SELECT * 
        FROM Sales.Transactions
        WHERE ModifiedDate > '@{activity('Lookup_GetWatermark').output.firstRow.LastLoadTime}'
           OR TransactionID > @{activity('Lookup_GetWatermark').output.firstRow.LastLoadedID}
        ORDER BY ModifiedDate, TransactionID
      "
    },
    "sink": {
      "type": "DataWarehouseSink",
      "preCopyScript": "-- No pre-copy needed for incremental"
    },
    "enableStaging": true,
    "translator": {
      "type": "TabularTranslator",
      "mappings": [...]
    }
  }
}
```

**C. Update Watermark:**
```json
{
  "name": "StoredProcedure_UpdateWatermark",
  "type": "SqlServerStoredProcedure",
  "dependsOn": [{"activity": "Copy_IncrementalSales"}],
  "typeProperties": {
    "storedProcedureName": "ETL.usp_UpdateWatermark",
    "storedProcedureParameters": {
      "TableName": "Sales",
      "LastLoadTime": "@{utcnow()}",
      "LastLoadedID": "@{activity('Copy_IncrementalSales').output.rowsCopied}",
      "RowsCopied": "@{activity('Copy_IncrementalSales').output.rowsCopied}"
    }
  }
}
```

**Step 3: Alternative - Change Data Capture (CDC)**

If source supports CDC (SQL Server 2016+):

```sql
-- Enable CDC on source database
USE SourceDB;
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on table
EXEC sys.sp_cdc_enable_table
    @source_schema = 'Sales',
    @source_name = 'Transactions',
    @role_name = NULL,
    @supports_net_changes = 1;
```

**Pipeline with CDC:**
```json
{
  "source": {
    "type": "SqlServerSource",
    "sqlReaderQuery": "
      SELECT * 
      FROM cdc.fn_cdc_get_net_changes_Sales_Transactions(
        @{activity('GetWatermark').output.firstRow.StartLSN},
        sys.fn_cdc_get_max_lsn(),
        'all'
      )
    "
  }
}
```

**Step 4: Handle Deletes (SCD Type 2)**
```sql
-- Merge logic to handle updates and soft deletes
MERGE INTO Sales.FactSales AS target
USING #StagingSales AS source
ON target.TransactionID = source.TransactionID
WHEN MATCHED AND target.IsCurrent = 1 AND source.IsDeleted = 1 THEN
    UPDATE SET 
        IsCurrent = 0,
        EndDate = GETDATE(),
        IsDeleted = 1
WHEN MATCHED AND target.IsCurrent = 1 THEN
    UPDATE SET 
        IsCurrent = 0,
        EndDate = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (TransactionID, Amount, Date, IsCurrent, StartDate, EndDate)
    VALUES (source.TransactionID, source.Amount, source.Date, 1, GETDATE(), '9999-12-31');
```

**Expected Outcome:**
- Load time reduced from 6 hours to 15-30 minutes
- Only new/changed records processed
- Full audit trail via watermark table
- Minimal impact on source system

---

### Scenario 4: Cross-Domain Data Integration

**Scenario:**
Your organization has data spread across multiple domains:
- Finance: Azure SQL Database (transactional data)
- Sales: Salesforce (CRM data via API)
- Logistics: On-premises SQL Server (behind firewall)
- Marketing: Google Analytics (web analytics)

You need to build a unified analytics solution in Fabric. How do you architect this?

**Answer:**

**Architecture: Medallion Pattern with Multi-Source Integration**

```
┌─────────────────── BRONZE LAYER (Raw) ──────────────────┐
│                                                           │
│  Finance:          Sales:          Logistics:  Marketing:│
│  Azure SQL ──┐     Salesforce ─┐   On-Prem ─┐  GA ──┐   │
│              │                 │            │       │   │
│              ▼                 ▼            ▼       ▼   │
│         [Lakehouse: raw_finance]  [raw_sales]  etc.     │
│                                                           │
└───────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────── SILVER LAYER (Clean) ────────────────┐
│                                                           │
│  [Lakehouse: clean_data]                                 │
│  - Validated, deduplicated                               │
│  - Standardized schemas                                  │
│  - Business rules applied                                │
│                                                           │
└───────────────────────────────────────────────────────────┘
                            │
                            ▼
┌────────────────── GOLD LAYER (Curated) ─────────────────┐
│                                                           │
│  [Warehouse: EnterpriseWarehouse]                        │
│  - Star schema                                           │
│  - Fact & dimension tables                               │
│  - Aggregated metrics                                    │
│  - Power BI datasets                                     │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

**Step 1: Bronze Layer - Data Ingestion**

**A. Azure SQL Database (Finance):**
```json
{
  "name": "Pipeline_Ingest_Finance",
  "activities": [
    {
      "name": "Copy_FinancialTransactions",
      "type": "Copy",
      "typeProperties": {
        "source": {
          "type": "AzureSqlSource",
          "sqlReaderQuery": "SELECT * FROM Finance.Transactions WHERE ModifiedDate > '@{pipeline().parameters.WatermarkTime}'"
        },
        "sink": {
          "type": "LakehouseSink",
          "writeBatchSize": 100000,
          "format": "delta",
          "location": "Files/raw/finance/transactions"
        }
      }
    }
  ],
  "parameters": {
    "WatermarkTime": "2026-04-01"
  }
}
```

**B. Salesforce (CRM via REST API):**
```json
{
  "name": "Copy_SalesforceCRM",
  "type": "Copy",
  "typeProperties": {
    "source": {
      "type": "RestSource",
      "httpRequestTimeout": "00:01:40",
      "requestInterval": "00.00:00:00.010",
      "requestMethod": "GET",
      "additionalHeaders": {
        "Authorization": "@{linkedService().apiKey}"
      },
      "paginationRules": {
        "supportRFC5988": "true"
      }
    },
    "sink": {
      "type": "LakehouseSink",
      "format": "parquet",
      "location": "Files/raw/salesforce/opportunities"
    }
  }
}
```

**C. On-Premises SQL Server (Self-Hosted IR):**
```json
{
  "name": "Copy_OnPremLogistics",
  "type": "Copy",
  "typeProperties": {
    "source": {
      "type": "SqlServerSource",
      "sqlReaderQuery": "SELECT * FROM Logistics.Shipments"
    },
    "sink": {
      "type": "LakehouseSink",
      "format": "delta",
      "location": "Tables/raw_logistics"
    },
    "enableStaging": true,
    "stagingSettings": {
      "linkedServiceName": "AzureDataLakeStorage",
      "path": "staging"
    }
  },
  "linkedServiceName": {
    "referenceName": "OnPremSQLServer_SelfHostedIR",
    "type": "LinkedServiceReference"
  }
}
```

**D. Google Analytics (HTTP + Custom Connector):**
```python
# Notebook: Ingest Google Analytics
import requests
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest

# Initialize client
client = BetaAnalyticsDataClient()

# Request
request = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    dimensions=[
        {"name": "date"},
        {"name": "country"},
        {"name": "deviceCategory"}
    ],
    metrics=[
        {"name": "activeUsers"},
        {"name": "sessions"}
    ],
    date_ranges=[{"start_date": "30daysAgo", "end_date": "today"}]
)

response = client.run_report(request)

# Convert to DataFrame
data = []
for row in response.rows:
    data.append({
        'date': row.dimension_values[0].value,
        'country': row.dimension_values[1].value,
        'device': row.dimension_values[2].value,
        'active_users': row.metric_values[0].value,
        'sessions': row.metric_values[1].value
    })

df = spark.createDataFrame(data)
df.write.format("delta").mode("overwrite").save("Tables/raw_google_analytics")
```

**Step 2: Silver Layer - Data Transformation**

```python
# Notebook: Transform to Silver
from pyspark.sql import functions as F

# Finance: Standardize currency
df_finance = spark.read.format("delta").load("Tables/raw_finance")
df_finance_clean = df_finance \
    .withColumn("Amount_USD", 
        F.when(F.col("Currency") == "EUR", F.col("Amount") * 1.1)
         .when(F.col("Currency") == "GBP", F.col("Amount") * 1.25)
         .otherwise(F.col("Amount"))
    ) \
    .dropDuplicates(["TransactionID"]) \
    .filter(F.col("Amount") > 0)

# Sales: Flatten JSON from Salesforce
df_sales = spark.read.format("delta").load("Tables/raw_salesforce")
df_sales_clean = df_sales \
    .select(
        F.col("Id").alias("OpportunityID"),
        F.col("AccountId").alias("AccountID"),
        F.col("Amount"),
        F.get_json_object(F.col("metadata"), "$.owner").alias("Owner"),
        F.to_timestamp(F.col("CloseDate")).alias("CloseDate")
    ) \
    .filter(F.col("Amount").isNotNull())

# Logistics: Handle nulls
df_logistics = spark.read.format("delta").load("Tables/raw_logistics")
df_logistics_clean = df_logistics \
    .fillna({'DeliveryDate': '9999-12-31'}) \
    .withColumn("IsDelivered", F.when(F.col("DeliveryDate") != '9999-12-31', True).otherwise(False))

# Write to Silver
df_finance_clean.write.format("delta").mode("overwrite").save("Tables/clean_finance")
df_sales_clean.write.format("delta").mode("overwrite").save("Tables/clean_sales")
df_logistics_clean.write.format("delta").mode("overwrite").save("Tables/clean_logistics")
```

**Step 3: Gold Layer - Data Warehouse (Star Schema)**

```sql
-- Create conformed dimension (shared across domains)
CREATE TABLE Warehouse.DimDate AS
SELECT DISTINCT
    CAST(FORMAT(Date, 'yyyyMMdd') AS INT) AS DateKey,
    Date,
    YEAR(Date) AS Year,
    MONTH(Date) AS Month,
    DAY(Date) AS Day
FROM (
    SELECT DISTINCT TransactionDate AS Date FROM clean_finance
    UNION
    SELECT DISTINCT CloseDate FROM clean_sales
    UNION
    SELECT DISTINCT ShipDate FROM clean_logistics
) AS AllDates;

-- Create integrated fact table
CREATE TABLE Warehouse.FactRevenue AS
SELECT 
    f.TransactionID,
    d.DateKey,
    'Finance' AS Source,
    f.Amount_USD AS Revenue,
    NULL AS OpportunityID
FROM clean_finance f
INNER JOIN Warehouse.DimDate d ON CAST(f.TransactionDate AS DATE) = d.Date
UNION ALL
SELECT 
    NULL,
    d.DateKey,
    'Sales' AS Source,
    s.Amount,
    s.OpportunityID
FROM clean_sales s
INNER JOIN Warehouse.DimDate d ON CAST(s.CloseDate AS DATE) = d.Date;
```

**Step 4: Orchestration (Master Pipeline)**

```json
{
  "name": "Pipeline_Master_DailyETL",
  "activities": [
    {
      "name": "Execute_BronzeIngestion",
      "type": "ExecutePipeline",
      "typeProperties": {
        "pipeline": {
          "referenceName": "Pipeline_IngestAll_Bronze"
        },
        "waitOnCompletion": true
      }
    },
    {
      "name": "Execute_SilverTransformation",
      "type": "Notebook",
      "dependsOn": [{"activity": "Execute_BronzeIngestion"}],
      "typeProperties": {
        "notebookPath": "/Notebooks/Transform_Silver"
      }
    },
    {
      "name": "Execute_GoldAggregation",
      "type": "Script",
      "dependsOn": [{"activity": "Execute_SilverTransformation"}],
      "typeProperties": {
        "scripts": [
          {
            "type": "Query",
            "text": "EXEC ETL.usp_LoadFactRevenue"
          }
        ]
      }
    },
    {
      "name": "Send_Success_Notification",
      "type": "WebActivity",
      "dependsOn": [{"activity": "Execute_GoldAggregation"}],
      "typeProperties": {
        "method": "POST",
        "url": "https://outlook.office.com/webhook/...",
        "body": {
          "text": "Daily ETL completed successfully"
        }
      }
    }
  ]
}
```

**Expected Outcome:**
- Unified view of data across all domains
- Single source of truth in Gold layer
- Reusable conformed dimensions
- Automated daily refresh

---

### Scenario 5: Migration from Azure Synapse to Fabric

**Scenario:**
Your organization currently has an Azure Synapse Analytics dedicated SQL pool with:
- 50+ TB of data
- 200+ tables
- 50+ stored procedures
- 20+ Power BI datasets connected
- 100+ users with various permissions

Leadership wants to migrate to Fabric Warehouse. How do you plan and execute this migration?

**Answer:**

**Migration Strategy: Phased Approach**

**Phase 1: Assessment (Week 1-2)**

```sql
-- Inventory existing objects
SELECT 
    SCHEMA_NAME(schema_id) AS SchemaName,
    name AS TableName,
    CAST(ROUND(SUM(row_count) / 1000000.0, 2) AS DECIMAL(18,2)) AS MillionRows,
    CAST(ROUND(SUM(reserved_page_count) * 8.0 / 1024 / 1024, 2) AS DECIMAL(18,2)) AS GB
FROM sys.dm_pdw_nodes_db_partition_stats
WHERE object_id > 100
GROUP BY SCHEMA_NAME(schema_id), name
ORDER BY GB DESC;

-- Identify incompatible features
-- Fabric doesn't support:
-- - Columnstore clustered indexes
-- - Hash/Round-robin distribution
-- - Partitioning (limited)
-- - Identity columns
-- - Computed columns

-- Find tables with identity columns
SELECT 
    SCHEMA_NAME(t.schema_id) AS SchemaName,
    t.name AS TableName,
    c.name AS ColumnName
FROM sys.tables t
INNER JOIN sys.columns c ON t.object_id = c.object_id
WHERE c.is_identity = 1;

-- Find distributed tables (not supported in Fabric)
SELECT 
    t.name AS TableName,
    d.distribution_policy_desc
FROM sys.tables t
INNER JOIN sys.pdw_table_distribution_properties d ON t.object_id = d.object_id;
```

**Phase 2: Design Target Schema (Week 3-4)**

**Mapping Document:**
```markdown
| Synapse Object | Fabric Equivalent | Changes Required |
|----------------|-------------------|------------------|
| Hash distributed table | Standard table | Remove distribution |
| Clustered columnstore | Standard table | Fabric auto-optimizes |
| Identity column | Sequence/app logic | Generate IDs in ETL |
| Partitioned table | Non-partitioned | Fabric handles internally |
| Indexed views | Standard views | Recreate as tables if perf critical |
```

**Phase 3: Setup Fabric Environment (Week 5)**

```sql
-- Create Fabric Warehouse
-- (Via Fabric Portal or API)

-- Create schemas matching Synapse
CREATE SCHEMA Sales;
CREATE SCHEMA Finance;
CREATE SCHEMA Staging;
CREATE SCHEMA ETL;

-- Create security roles matching Synapse
CREATE ROLE DataReader;
CREATE ROLE DataWriter;
CREATE ROLE ReportViewer;

-- Grant permissions
GRANT SELECT ON SCHEMA::Sales TO DataReader;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::Sales TO DataWriter;
```

**Phase 4: Data Migration (Week 6-8)**

**Option A: COPY INTO from Synapse → OneLake → Fabric**

```sql
-- Step 1: Export from Synapse to ADLS
-- In Synapse:
COPY INTO 'https://storage.blob.core.windows.net/migration/sales_factorders/*.parquet'
FROM Sales.FactOrders
WITH (
    FILE_TYPE = 'PARQUET',
    COMPRESSION = 'SNAPPY'
);

-- Step 2: Load into Fabric Warehouse
-- In Fabric:
COPY INTO Sales.FactOrders
FROM 'https://onelake.blob.fabric.microsoft.com/migration/sales_factorders/*.parquet'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
);
```

**Option B: Azure Data Factory / Fabric Pipeline**

```json
{
  "name": "Pipeline_Migrate_FactOrders",
  "activities": [
    {
      "name": "Copy_FactOrders_Synapse_to_Fabric",
      "type": "Copy",
      "typeProperties": {
        "source": {
          "type": "SqlDWSource",
          "sqlReaderQuery": "SELECT * FROM Sales.FactOrders",
          "queryTimeout": "02:00:00"
        },
        "sink": {
          "type": "SqlDWSink",
          "preCopyScript": "TRUNCATE TABLE Sales.FactOrders",
          "writeBatchSize": 100000,
          "tableOption": "autoCreate"
        },
        "enableStaging": true,
        "stagingSettings": {
          "linkedServiceName": "AzureDataLakeStorage",
          "path": "staging/migration"
        },
        "parallelCopies": 32,
        "dataIntegrationUnits": 128
      }
    }
  ]
}
```

**Phase 5: Code Migration (Week 9-10)**

**Convert Stored Procedures:**

```sql
-- Synapse Stored Procedure (incompatible features)
CREATE PROCEDURE Sales.usp_LoadDailySales
AS
BEGIN
    -- Synapse uses CTAS with distributions
    CREATE TABLE Sales.FactSales_New
    WITH (
        DISTRIBUTION = HASH(CustomerKey),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT * FROM Staging.Sales;
    
    -- Swap tables
    RENAME OBJECT Sales.FactSales TO FactSales_Old;
    RENAME OBJECT Sales.FactSales_New TO FactSales;
    DROP TABLE Sales.FactSales_Old;
END;

-- Fabric Equivalent (simplified)
CREATE PROCEDURE Sales.usp_LoadDailySales
AS
BEGIN
    -- Fabric: Use CTAS or INSERT...SELECT
    TRUNCATE TABLE Sales.FactSales;
    
    INSERT INTO Sales.FactSales
    SELECT * FROM Staging.Sales;
    
    -- Or use CTAS
    -- DROP TABLE IF EXISTS Sales.FactSales;
    -- CREATE TABLE Sales.FactSales AS
    -- SELECT * FROM Staging.Sales;
END;
```

**Phase 6: Power BI Connection Update (Week 11)**

```powerquery
// Old Synapse connection
let
    Source = Sql.Database("synapse-workspace.sql.azuresynapse.net", "SynapseDW")
in
    Source

// New Fabric connection
let
    Source = Fabric.Warehouse("workspace-guid", "FabricWarehouse")
in
    Source
```

**Migration Script for Multiple Reports:**
```powershell
# PowerShell script to update connections
Connect-PowerBIServiceAccount

$workspaces = Get-PowerBIWorkspace -Scope Organization

foreach ($workspace in $workspaces) {
    $reports = Get-PowerBIReport -WorkspaceId $workspace.Id
    
    foreach ($report in $reports) {
        # Update data source
        $params = @{
            DataSource = @{
                DatasourceType = "Fabric"
                ConnectionDetails = @{
                    Server = "workspace-guid"
                    Database = "FabricWarehouse"
                }
            }
        }
        
        Set-PowerBIDataSource -WorkspaceId $workspace.Id `
            -DatasetId $report.DatasetId `
            @params
    }
}
```

**Phase 7: Parallel Run & Validation (Week 12-14)**

```sql
-- Data reconciliation query
WITH SynapseData AS (
    SELECT COUNT(*) AS RowCount, SUM(Amount) AS TotalAmount
    FROM [synapse-server].Sales.FactOrders
),
FabricData AS (
    SELECT COUNT(*) AS RowCount, SUM(Amount) AS TotalAmount
    FROM Sales.FactOrders
)
SELECT 
    'Synapse' AS Source,
    s.RowCount,
    s.TotalAmount
FROM SynapseData s
UNION ALL
SELECT 
    'Fabric',
    f.RowCount,
    f.TotalAmount
FROM FabricData f;

-- Row-by-row comparison for critical tables
SELECT 
    'Synapse' AS Source,
    *
FROM [synapse-server].Sales.FactOrders
EXCEPT
SELECT 
    'Fabric',
    *
FROM Sales.FactOrders;
```

**Phase 8: Cutover (Week 15)**

**Cutover Checklist:**
- [ ] All tables migrated and validated
- [ ] Stored procedures converted and tested
- [ ] Power BI reports updated and tested
- [ ] User permissions replicated
- [ ] ETL pipelines updated
- [ ] Backup of Synapse data
- [ ] Communication to users
- [ ] Rollback plan ready

**Cutover Steps:**
1. Freeze Synapse (read-only)
2. Final incremental sync
3. Switch Power BI to Fabric
4. Update application connection strings
5. Monitor for 48 hours
6. Decommission Synapse

**Expected Outcome:**
- Zero data loss
- <2 hour downtime for cutover
- 30-40% cost reduction (Fabric capacity vs Synapse DW)
- Improved performance with auto-optimization

---

### Scenario 6: Real-Time Data Pipeline

**Scenario:**
You need to build a real-time dashboard showing website clickstream data. Events are generated at 10,000 events/second. The dashboard should show metrics with <5 second latency. How do you architect this in Fabric?

**Answer:**

**Architecture: Event Streaming → KQL → Power BI**

```
Event Sources                    Fabric Components                  Consumption
┌──────────────┐                                                   ┌──────────────┐
│ Website      │──┐                                              ┌─│ Power BI     │
│ (10K evt/s)  │  │                                              │ │ (Real-time)  │
└──────────────┘  │             ┌─────────────┐                  │ └──────────────┘
                  ├────────────►│ Event Hub   │──────────────┐   │
┌──────────────┐  │             └─────────────┘              │   │ ┌──────────────┐
│ Mobile App   │──┘                                          │   └─│ Fabric       │
│ (5K evt/s)   │                                             ▼     │ Notebooks    │
└──────────────┘                                       ┌──────────┐│ (Analysis)   │
                                                       │ Eventstream│└──────────────┘
                                                       └─────┬──────┘
                                                             │
                                                             ▼
                                                       ┌──────────┐
                                                       │   KQL    │
                                                       │ Database │
                                                       └──────────┘
```

**Step 1: Setup Event Hub**

```bash
# Azure CLI - Create Event Hub
az eventhubs namespace create \
  --name clickstream-events \
  --resource-group fabric-rg \
  --location eastus \
  --sku Standard

az eventhubs eventhub create \
  --name website-clicks \
  --namespace-name clickstream-events \
  --resource-group fabric-rg \
  --partition-count 32 \
  --message-retention 7
```

**Step 2: Ingest Events (Application Code)**

```javascript
// Node.js - Send events to Event Hub
const { EventHubProducerClient } = require("@azure/event-hubs");

const producer = new EventHubProducerClient(
  "Endpoint=sb://clickstream-events.servicebus.windows.net/;...",
  "website-clicks"
);

// Capture click event
window.addEventListener('click', async (event) => {
  const clickEvent = {
    timestamp: new Date().toISOString(),
    userId: getCurrentUserId(),
    sessionId: getSessionId(),
    pageUrl: window.location.href,
    elementId: event.target.id,
    elementClass: event.target.className,
    xCoordinate: event.clientX,
    yCoordinate: event.clientY,
    userAgent: navigator.userAgent,
    country: await getGeoLocation()
  };
  
  const batch = await producer.createBatch();
  batch.tryAdd({ body: clickEvent });
  await producer.sendBatch(batch);
});
```

**Step 3: Create Fabric Eventstream**

```json
// Fabric Eventstream Configuration
{
  "name": "Clickstream_Eventstream",
  "source": {
    "type": "EventHub",
    "connectionString": "Endpoint=sb://clickstream-events...",
    "eventHubName": "website-clicks",
    "consumerGroup": "$Default"
  },
  "destinations": [
    {
      "type": "KQLDatabase",
      "database": "ClickstreamDB",
      "table": "RawClicks",
      "mappingRule": "ClickstreamMapping"
    },
    {
      "type": "Lakehouse",
      "lakehouse": "ClickstreamLakehouse",
      "table": "raw_clicks",
      "format": "delta"
    }
  ]
}
```

**Step 4: Setup KQL Database**

```kql
// Create table in KQL Database
.create table RawClicks (
    Timestamp: datetime,
    UserId: string,
    SessionId: string,
    PageUrl: string,
    ElementId: string,
    ElementClass: string,
    XCoordinate: int,
    YCoordinate: int,
    UserAgent: string,
    Country: string
)

// Create ingestion mapping
.create table RawClicks ingestion json mapping 'ClickstreamMapping' ```
[
    {"column":"Timestamp", "path":"$.timestamp", "datatype":"datetime"},
    {"column":"UserId", "path":"$.userId"},
    {"column":"SessionId", "path":"$.sessionId"},
    {"column":"PageUrl", "path":"$.pageUrl"},
    {"column":"ElementId", "path":"$.elementId"},
    {"column":"ElementClass", "path":"$.elementClass"},
    {"column":"XCoordinate", "path":"$.xCoordinate", "datatype":"int"},
    {"column":"YCoordinate", "path":"$.yCoordinate", "datatype":"int"},
    {"column":"UserAgent", "path":"$.userAgent"},
    {"column":"Country", "path":"$.country"}
]
```

// Create real-time aggregation
.create-or-alter function with (folder = "RealTimeMetrics") ClickMetrics_1Minute() {
    RawClicks
    | where Timestamp > ago(1m)
    | summarize 
        TotalClicks = count(),
        UniqueUsers = dcount(UserId),
        UniqueSessions = dcount(SessionId),
        AvgX = avg(XCoordinate),
        AvgY = avg(YCoordinate)
      by 
        bin(Timestamp, 10s),
        Country,
        PageUrl
}

// Create materialized view for performance
.create materialized-view with (backfill=true) ClicksByCountry on table RawClicks {
    RawClicks
    | summarize 
        TotalClicks = count(),
        UniqueUsers = dcount(UserId)
      by Country, bin(Timestamp, 1m)
}
```

**Step 5: Power BI Real-Time Dashboard**

```powerquery
// Power BI - Connect to KQL Database
let
    Source = AzureDataExplorer.Contents(
        "https://clickstreamdb.kusto.fabric.microsoft.com", 
        "ClickstreamDB", 
        "ClickMetrics_1Minute()", 
        [MaxRows=null, MaxSize=null, NoTruncate=null, AdditionalSetStatements=null]
    )
in
    Source
```

**Enable Auto-Refresh:**
- Set refresh interval to 5 seconds
- Enable DirectQuery mode
- Use streaming datasets for tile updates

**Step 6: Alerts & Monitoring**

```kql
// Create alert for anomalies
.create-or-alter function AnomalyDetection() {
    let baseline = RawClicks
        | where Timestamp between (ago(7d) .. ago(1d))
        | summarize AvgClicks = avg(count()) by bin(Timestamp, 1m)
        | summarize BaselineAvg = avg(AvgClicks);
    
    let current = RawClicks
        | where Timestamp > ago(5m)
        | summarize CurrentClicks = count() by bin(Timestamp, 1m);
    
    current
    | extend Baseline = toscalar(baseline)
    | extend DeviationPct = (CurrentClicks - Baseline) / Baseline * 100
    | where abs(DeviationPct) > 50  // Alert if >50% deviation
    | project Timestamp, CurrentClicks, Baseline, DeviationPct
}

// Schedule alert (via Fabric Data Activator)
```

**Step 7: Cost Optimization**

```kql
// Implement data retention policy
.alter-merge table RawClicks policy retention softdelete = 7d
.alter-merge table RawClicks policy caching hot = 1d

// Partition by ingestion time
.alter table RawClicks policy partitioning ```
{
  "PartitionKeys": [
    {
      "ColumnName": "Timestamp",
      "Kind": "UniformRange",
      "Properties": {
        "Reference": "1970-01-01T00:00:00",
        "RangeSize": "1.00:00:00",
        "OverrideCreationTime": false
      }
    }
  ]
}
```
```

**Expected Outcome:**
- <5 second end-to-end latency
- Real-time dashboard with auto-refresh
- 10,000+ events/second throughput
- Scalable to millions of events/day
- Cost-optimized with retention policies

---

## Components & Services

### Main Components:
1. **Data Factory** - Data integration and ETL
2. **Synapse Data Engineering** - Spark-based data engineering
3. **Synapse Data Warehouse** - SQL-based analytics
4. **Synapse Data Science** - ML and AI capabilities
5. **Synapse Real-Time Analytics** - Real-time data processing
6. **Power BI** - Business intelligence and visualization

---

## Best Practices

### 1. Data Architecture Best Practices
- **Implement Medallion Architecture:** Bronze (raw) → Silver (cleaned) → Gold (curated)
- **Use Delta Lake Format:** ACID transactions, time travel, schema evolution
- **Apply Partition Strategy:** Optimize queries with proper partitioning
- **Enable V-Order:** Automatic write-time optimization for Parquet files
- **Implement Data Quality Checks:** Validate data at ingestion and transformation layers

### 2. Performance Best Practices
- **Optimize File Sizes:** Target 128MB-1GB per file
- **Use Statistics:** Create and maintain statistics on filter columns
- **Implement Caching:** Leverage result cache for repeated queries
- **Z-Order Clustering:** Co-locate related data for filter performance
- **Minimize Data Movement:** Use cross-database queries instead of data copies
- **Pre-Aggregate:** Create summary tables for common dashboard queries

### 3. Security & Governance Best Practices
- **Principle of Least Privilege:** Grant minimum necessary permissions
- **Use Workspace Roles:** Assign roles to AD groups, not individuals
- **Apply Sensitivity Labels:** Classify data with Purview Information Protection
- **Implement RLS/CLS:** Row-level and column-level security where needed
- **Enable Audit Logging:** Track all data access and modifications
- **Document Data Lineage:** Maintain clear understanding of data flow

### 4. Development & Deployment Best Practices
- **Use Git Integration:** Version control for notebooks, pipelines, and SQL
- **Implement CI/CD:** Automated testing and deployment pipelines
- **Separate Environments:** DEV → TEST → PROD progression
- **Parameterize Pipelines:** Avoid hard-coding values
- **Error Handling:** Implement try-catch and alerting in pipelines
- **Document Code:** Add comments and maintain README files

### 5. Cost Optimization Best Practices
- **Right-Size Capacity:** Monitor usage and adjust Fabric capacity
- **Implement Data Retention:** Archive or delete old data
- **Use VACUUM:** Clean up old Delta Lake versions
- **Schedule Off-Peak:** Run heavy workloads during low-cost periods
- **Optimize Queries:** Avoid SELECT *, use column pruning
- **Monitor Capacity Metrics:** Use Capacity Metrics app regularly

---

## 💡 Key Takeaways for Interview Success

### 🎯 Must-Know Concepts
1. **OneLake** = Unified data lake across all Fabric workloads
2. **Delta Lake** = ACID transactions + time travel + versioning
3. **Medallion Architecture** = Bronze → Silver → Gold data quality progression
4. **CTAS** = 2-3x faster than CREATE + INSERT
5. **Cross-Database Queries** = Zero-copy data access across items
6. **Star Schema** = Preferred dimensional model (fast, simple queries)
7. **SCD Type 2** = Track historical changes with effective dates
8. **V-Order** = Automatic write-time optimization
9. **Capacity Model** = Pay-as-you-go based on CU consumption
10. **Data Governance** = Admin Portal + Purview + Sensitivity Labels + RLS/CLS

### 🔑 Architecture Patterns to Remember
- **Lakehouse:** Flexibility + raw data storage + Spark processing
- **Warehouse:** SQL analytics + star schema + business intelligence
- **Real-Time:** Event Hub → Eventstream → KQL Database → Power BI
- **Incremental Load:** Watermark-based or CDC for efficient ETL
- **Data Quality:** Bronze (raw) → Silver (validated) → Gold (business-ready)

### ⚡ Performance Optimization Checklist
- [ ] Statistics created on filter columns
- [ ] V-Order enabled for Parquet files
- [ ] Z-Order applied to frequently filtered columns
- [ ] File sizes optimized (128MB-1GB)
- [ ] Unnecessary columns removed from queries
- [ ] Pre-aggregated tables for common queries
- [ ] Result cache enabled
- [ ] VACUUM run to clean old versions

### 🛡️ Security Implementation Checklist
- [ ] Workspace roles assigned to AD groups
- [ ] Sensitivity labels applied to all items
- [ ] Row-level security implemented where needed
- [ ] Column-level security for PII/sensitive data
- [ ] Dynamic data masking for non-privileged users
- [ ] Audit logging enabled
- [ ] DLP policies configured
- [ ] Item permissions reviewed quarterly

---

## 📚 Additional Resources

### Official Microsoft Documentation
- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Fabric Data Warehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/)
- [Fabric Data Engineering](https://learn.microsoft.com/en-us/fabric/data-engineering/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [OneLake Overview](https://learn.microsoft.com/en-us/fabric/onelake/)

### Learning Paths
- [Fabric Learning Path (Microsoft Learn)](https://learn.microsoft.com/en-us/training/fabric/)
- [DP-600: Implementing Analytics Solutions Using Microsoft Fabric](https://learn.microsoft.com/en-us/credentials/certifications/exams/dp-600/)

### Community Resources
- [Fabric Community](https://community.fabric.microsoft.com/)
- [Fabric Blog](https://blog.fabric.microsoft.com/)
- [Fabric YouTube Channel](https://www.youtube.com/@MicrosoftFabric)

---

## 🎓 Exam & Certification Information

**Certification:** Microsoft Certified: Fabric Data Engineer Associate  
**Exam:** DP-600 - Implementing Analytics Solutions Using Microsoft Fabric

**Exam Topics:**
1. **Plan, implement, and manage a solution for data analytics (10-15%)**
2. **Prepare and serve data (40-45%)**
3. **Implement and manage semantic models (15-20%)**
4. **Explore and analyze data (20-25%)**

**Study Recommendations:**
- Complete all hands-on labs in this guide
- Build end-to-end solutions in trial capacity
- Practice scenario-based questions
- Review official Microsoft Learn modules
- Join study groups and communities

---

## 📝 Document Update Log

| Date | Update | Sections Added/Modified |
|------|--------|------------------------|
| 2026-04-24 | Initial Creation | OneLake, Delta Lake, ADLS Gen2 |
| 2026-04-25 | Data Warehouse Section | Complete DW coverage (9,500+ lines) |
| 2026-04-25 | Interview Questions | Q1-Q7: ADF, Medallion, SCD, OneLake, Delta, Cross-DB, CTAS |
| 2026-04-25 | Advanced Topics | Q8-Q10: Dimensional Modeling, Views, Data Governance |
| 2026-04-25 | Scenario-Based Questions | 6 real-world scenarios with solutions |
| 2026-04-25 | TOC & Structure | Clickable navigation, best practices, takeaways |

---

## 📊 Document Statistics

- **Total Lines:** ~29,000+
- **Interview Questions:** 10 conceptual + 6 scenario-based
- **Code Examples:** 200+ SQL, Python, JSON snippets
- **Topics Covered:** 50+ terminologies and concepts
- **Diagrams:** 30+ ASCII architecture diagrams
- **Tables:** 25+ comparison and reference tables

---

## ✅ Pre-Interview Checklist

### Day Before Interview
- [ ] Review all 10 conceptual interview questions
- [ ] Practice explaining 3-4 scenario solutions verbally
- [ ] Memorize key differences (Lakehouse vs Warehouse, Star vs Snowflake, etc.)
- [ ] Review architecture diagrams
- [ ] Know your resume projects - map to Fabric concepts
- [ ] Prepare 2-3 questions to ask interviewer

### Core Concepts to Explain Clearly
- [ ] OneLake architecture and benefits
- [ ] Delta Lake ACID transactions
- [ ] Medallion architecture (Bronze/Silver/Gold)
- [ ] Star schema vs Snowflake schema
- [ ] SCD Type 1 vs Type 2
- [ ] CTAS performance benefits
- [ ] Data governance framework
- [ ] Cross-database queries

### Scenario Troubleshooting Framework
When asked "How would you solve X?":
1. **Clarify Requirements** - Ask questions about scale, latency, budget
2. **Propose Architecture** - Draw diagram, explain components
3. **Identify Challenges** - Performance, security, cost considerations
4. **Present Solutions** - Multiple options with trade-offs
5. **Discuss Monitoring** - How to measure success, alerts
6. **Mention Best Practices** - Security, governance, optimization

---

## 🎯 Final Tips for Interview Success

### Technical Interview Tips
1. **Draw Diagrams:** Visualize architectures on whiteboard/paper
2. **Think Aloud:** Explain your reasoning process
3. **Ask Clarifying Questions:** Don't assume requirements
4. **Consider Trade-offs:** Discuss pros/cons of approaches
5. **Use Real Examples:** Reference this guide's scenarios
6. **Admit Unknowns:** "I don't know, but here's how I'd find out"

### Behavioral Interview Tips
1. **Prepare STAR Stories:** Situation, Task, Action, Result
2. **Highlight Impact:** Quantify results (X% faster, $Y saved)
3. **Show Learning:** Discuss failures and lessons learned
4. **Demonstrate Collaboration:** Cross-team projects
5. **Express Curiosity:** Continuous learning mindset

### Common Interview Questions (Non-Technical)
- "Tell me about a challenging data project you worked on"
- "How do you handle tight deadlines?"
- "Describe a time you optimized performance"
- "How do you ensure data quality?"
- "What's your approach to learning new technologies?"

---

## 🏆 You're Ready!

You've completed comprehensive coverage of:
✅ Microsoft Fabric architecture and components  
✅ Data warehousing with dimensional modeling  
✅ Performance optimization techniques  
✅ Data governance and security  
✅ Real-world scenario problem-solving  
✅ 16 detailed interview questions with answers  
✅ Best practices and key takeaways  

**Good luck with your interview! 🚀**

---

*End of Document*

---

## Examples & Use Cases

<!-- Examples and use cases will be added here -->

---

## Questions & Notes

<!-- Your learning questions and notes will be tracked here -->

---

## Format for Adding New Terminologies

When you ask about a terminology, I'll add it in this format:

```markdown
### [Terminology Name]

**Definition:**
Clear, concise explanation of what it is.

**Key Concepts:**
- Important point 1
- Important point 2
- How it works

**Use Cases:**
When and why you'd use this.

**Interview Questions:**
1. **Q:** Common interview question?
   **A:** Detailed answer with examples.

2. **Q:** Follow-up or related question?
   **A:** Answer connecting concepts.

3. **Q:** Practical/scenario-based question?
   **A:** Real-world application answer.
```
