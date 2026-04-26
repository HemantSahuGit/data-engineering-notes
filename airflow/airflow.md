# Apache Airflow — Complete Interview & Study Guide

> **Coverage:** Core Concepts → Architecture → Scheduling → Executors → Advanced Features → Production Best Practices → Scenario-Based Questions
> **Format:** Theory + Diagrams + Code Examples + Interview Q&A

---

## 📌 Navigation Index

| # | Section | Topics |
|---|---------|--------|
| 1 | [What is Apache Airflow](#1-what-is-apache-airflow) | Overview, history, use cases |
| 2 | [Core Concepts](#2-core-concepts) | DAG, Task, Operator, Task Instance |
| 3 | [Airflow Architecture](#3-airflow-architecture) | Scheduler, Webserver, Metadata DB, Executor, Worker, Triggerer |
| 4 | [DAG Authoring](#4-dag-authoring) | DAG parameters, dependencies, Jinja, TaskFlow API |
| 5 | [Operators](#5-operators) | Types of operators, Sensors, Hooks |
| 6 | [Scheduling & Execution](#6-scheduling--execution) | Cron, catchup, backfill, logical_date |
| 7 | [Executors](#7-executors) | Sequential, Local, Celery, Kubernetes |
| 8 | [XCom & Data Passing](#8-xcom--data-passing) | Push/pull, best practices |
| 9 | [Advanced Features](#9-advanced-features) | Branching, Trigger Rules, Pools, TaskGroups, Dynamic Mapping |
| 10 | [Production Best Practices](#10-production-best-practices) | Secrets, logging, idempotency, CI/CD |
| 11 | [Troubleshooting](#11-troubleshooting) | Task states, debugging, zombie tasks |
| 12 | [Interview Q&A — Beginner](#12-interview-qa--beginner) | 20 foundational questions |
| 13 | [Interview Q&A — Intermediate](#13-interview-qa--intermediate) | 20 intermediate questions |
| 14 | [Interview Q&A — Advanced](#14-interview-qa--advanced) | 20 advanced questions |
| 15 | [Scenario-Based Questions](#15-scenario-based-questions) | 15 real-world design scenarios |
| 16 | [Quick Reference Cheat Sheet](#16-quick-reference-cheat-sheet) | Tables, states, macros |

---

## 1. What is Apache Airflow

### Overview

Apache Airflow is an **open-source workflow orchestration platform** for programmatically authoring, scheduling, and monitoring data pipelines.

- Originally built at **Airbnb in 2014** to manage increasingly complex ETL workflows.
- Donated to the **Apache Software Foundation** and became a top-level project.
- Workflows are defined as **Python code**, making them version-controlled, testable, and maintainable.

### Problems Airflow Solves

| Problem (Without Airflow) | Solution (With Airflow) |
|---|---|
| Cron jobs with no dependency management | DAG-based dependency graph |
| Manual re-runs on failure | Automatic retries with configurable delay |
| No visibility into pipeline status | Centralized Web UI |
| Scattered scripts across servers | Code-defined, version-controlled pipelines |
| No audit trail | Every run recorded in Metadata DB |

### When to Use Airflow

- **ETL/ELT pipelines** (Extract → Transform → Load)
- **ML model training pipelines** (data prep → train → evaluate → deploy)
- **Data quality checks** (run tests after ingestion)
- **Cross-system orchestration** (S3 → Spark → Snowflake → Slack alert)
- **Scheduled report generation**

### When NOT to Use Airflow

- **Real-time streaming** → use Kafka, Flink
- **Sub-minute scheduling** → Airflow overhead is too high
- **Simple single-step jobs** → overkill

---

## 2. Core Concepts

### 2.1 DAG — Directed Acyclic Graph

A DAG is the central concept of Airflow. It represents the **entire workflow**.

```
Definition:
  Directed  → Tasks flow in one direction (no bidirectional links)
  Acyclic   → No cycles / loops
  Graph     → A collection of nodes (tasks) and edges (dependencies)
```

**Visual DAG Example:**
```
                    ┌──────────────┐
                    │   extract    │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  transform   │
                    └──────┬───────┘
                           │
              ┌────────────┴─────────────┐
              │                          │
     ┌────────▼────────┐       ┌─────────▼────────┐
     │  load_to_db     │       │  notify_slack     │
     └─────────────────┘       └──────────────────┘
```

This is a valid DAG — no task depends on itself or on a downstream task.

**Invalid (Cyclic) — Would cause infinite loop:**
```
[A] ──► [B] ──► [C] ──► [A]   ← ❌ CYCLE — NOT allowed
```

### 2.2 Task

A **Task** is a single unit of work inside a DAG. It is an instance of an Operator within a DAG definition.

```python
from airflow.operators.python import PythonOperator

# This is a Task — a PythonOperator used inside a DAG
process_data = PythonOperator(
    task_id='process_data',
    python_callable=my_function,
    dag=dag
)
```

### 2.3 Operator

An **Operator** is a class that defines what a task does. Think of it as a template.

```
Operator  ──► defines the TYPE of work (e.g., run Python, run Bash, send Email)
Task      ──► a specific USE of that operator in a DAG
Task Instance ──► a specific RUN of that task for a specific date
```

### 2.4 Task Instance

A **Task Instance** is one execution of a Task for a specific `logical_date`.

```
DAG: daily_etl
  Task: extract
    Task Instance: extract | 2024-01-01  ← specific run
    Task Instance: extract | 2024-01-02
    Task Instance: extract | 2024-01-03
```

### 2.5 DAG Run

A **DAG Run** is one execution of the full DAG for a specific `logical_date`. It contains all the task instances for that run.

---

## 3. Airflow Architecture

### 3.1 Component Overview

```
┌──────────────────────────────────────────────────────────────┐
│                   APACHE AIRFLOW ARCHITECTURE                │
│                                                              │
│  ┌─────────────┐    parse DAGs    ┌─────────────────────┐   │
│  │             │◄─────────────────│     DAG Files       │   │
│  │  Scheduler  │                  │  (Python .py files) │   │
│  │             │                  └─────────────────────┘   │
│  │  - Monitors │                                            │
│  │    DAGs     │  read/write state                          │
│  │  - Triggers │◄────────────────►┌─────────────────────┐   │
│  │    tasks    │                  │   Metadata Database  │   │
│  └──────┬──────┘                  │   (PostgreSQL/MySQL) │   │
│         │                        │                      │   │
│         │ sends tasks             │  Stores:             │   │
│         ▼                        │  - DAG runs          │   │
│  ┌─────────────┐                  │  - Task states       │   │
│  │  Executor   │                  │  - Variables         │   │
│  │             │                  │  - Connections       │   │
│  │  - Celery   │                  │  - XComs             │   │
│  │  - K8s      │                  └──────────▲───────────┘   │
│  │  - Local    │                             │               │
│  └──────┬──────┘                             │ reads state   │
│         │                                   │               │
│         ├──► Worker 1 ──────────────────────►│               │
│         ├──► Worker 2 ──────────────────────►│               │
│         └──► Worker 3 ──────────────────────►│               │
│                                              │               │
│  ┌─────────────┐                             │               │
│  │  Webserver  │◄────────────────────────────┘               │
│  │  (Flask UI) │    reads serialized DAGs + task states      │
│  └─────────────┘                                             │
│         ▲                                                    │
│         │  User browses UI                                   │
│       [Browser]                                              │
└──────────────────────────────────────────────────────────────┘
```

### 3.2 Scheduler

- **Heart of Airflow** — runs continuously.
- Parses DAG files (every `min_file_process_interval` seconds).
- Evaluates which task instances are ready to run.
- Submits ready tasks to the Executor.
- Writes state changes to the Metadata DB.

### 3.3 Webserver

- Flask-based web UI.
- Reads from the Metadata Database (NOT directly from DAG files in modern Airflow — uses serialized DAGs).
- Lets users: visualize DAGs, trigger runs, view logs, manage Variables/Connections.

### 3.4 Metadata Database

- Usually **PostgreSQL** (recommended for production) or MySQL.
- Stores everything: DAG runs, task states, XComs, Variables, Connections, users.
- **Critical bottleneck** — a slow DB = slow scheduler.
- SQLite is only for development/testing.

### 3.5 Executor

- Defines **how and where** tasks are run.
- Does NOT execute tasks itself; it sends them to workers.
- Types: `SequentialExecutor`, `LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor`.

### 3.6 Worker

- The actual process that runs task code.
- In Celery setup: separate machines listening to a message queue (Redis/RabbitMQ).
- In K8s setup: a Pod spun up per task.

### 3.7 Triggerer (Airflow 2.2+)

- Manages **Deferrable Operators**.
- Runs an async event loop.
- Instead of a Sensor holding a worker slot for hours, it defers to the Triggerer.
- Triggerer wakes the task when the condition is met.

```
WITHOUT Triggerer:
  [Sensor on Worker] ──► polls every 60s for 6 hours ──► holds worker slot entire time

WITH Triggerer (Deferrable):
  [Sensor] ──► defers to Triggerer ──► worker slot FREED
  [Triggerer] ──► condition met ──► wakes task ──► worker picks it up
```

### 3.8 DAG Serialization

- Scheduler serializes DAG Python files → JSON → stored in Metadata DB.
- Webserver reads JSON, not raw Python.
- **Benefit:** Reduces Webserver CPU load; faster UI load.

---

## 4. DAG Authoring

### 4.1 Basic DAG Structure

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments applied to all tasks
default_args = {
    'owner': 'hemant',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['hemant@company.com']
}

# DAG definition
with DAG(
    dag_id='my_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline',
    schedule_interval='@daily',       # or '0 6 * * *' (cron)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'production']
) as dag:

    def extract():
        return {"rows": 1000}

    def transform(**context):
        data = context['ti'].xcom_pull(task_ids='extract_task')
        print(f"Transforming {data['rows']} rows")

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True
    )

    # Define dependency
    extract_task >> transform_task
```

### 4.2 Key DAG Parameters

| Parameter | Description | Example |
|---|---|---|
| `dag_id` | Unique identifier | `'daily_etl'` |
| `start_date` | When scheduling begins | `datetime(2024, 1, 1)` |
| `schedule_interval` | How often to run | `'@daily'`, `'0 6 * * *'` |
| `catchup` | Run missed intervals? | `False` |
| `max_active_runs` | Max parallel DAG runs | `1` |
| `concurrency` | Max parallel tasks in one run | `4` |
| `default_args` | Default task arguments | `{'retries': 3}` |
| `tags` | UI filter labels | `['etl', 'prod']` |
| `on_failure_callback` | Function on DAG failure | `alert_slack` |

### 4.3 Task Dependency Syntax

```python
# Method 1: Bitshift operators (most common)
extract >> transform >> load

# Method 2: Method calls
extract.set_downstream(transform)
transform.set_downstream(load)

# Method 3: Multiple dependencies
[extract_users, extract_orders] >> transform >> load

# Method 4: Cross dependencies
from airflow.utils.helpers import cross_downstream
cross_downstream([A, B], [C, D])
# All of [C, D] depend on all of [A, B]

# Method 5: Chain
from airflow.models.baseoperator import chain
chain(A, B, C, D)   # A >> B >> C >> D
```

### 4.4 TaskFlow API (Airflow 2.0+)

The modern, decorator-based approach to writing DAGs.

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def my_etl_pipeline():

    @task
    def extract() -> dict:
        return {"rows": 1000, "source": "postgres"}

    @task
    def transform(raw_data: dict) -> dict:
        # XCom push/pull is handled AUTOMATICALLY
        return {"processed_rows": raw_data["rows"] * 2}

    @task
    def load(processed_data: dict):
        print(f"Loading {processed_data['processed_rows']} rows to warehouse")

    # Wire them up like regular Python functions
    raw = extract()
    processed = transform(raw)
    load(processed)

dag = my_etl_pipeline()
```

**TaskFlow vs. Traditional:**

```
Traditional:
  task1 = PythonOperator(task_id='t1', python_callable=fn1)
  task2 = PythonOperator(task_id='t2', python_callable=fn2)
  # Manual XCom: ti.xcom_push / ti.xcom_pull
  task1 >> task2

TaskFlow API:
  @task
  def t1(): return data       # auto XCom push

  @task
  def t2(data): ...           # auto XCom pull

  t2(t1())                    # dependency defined naturally
```

### 4.5 Jinja Templating

Airflow uses Jinja2 for dynamic values in templateable fields.

```python
# Common Jinja macros
{{ ds }}              # execution date as YYYY-MM-DD string
{{ ds_nodash }}       # YYYYMMDD
{{ execution_date }}  # datetime object
{{ next_ds }}         # next execution date
{{ prev_ds }}         # previous execution date
{{ dag.dag_id }}      # DAG ID
{{ task.task_id }}    # Task ID
{{ dag_run.conf }}    # Runtime config dict

# Example usage in SQL operator
t = PostgresOperator(
    task_id='load_daily',
    sql="""
        INSERT INTO sales_summary
        SELECT * FROM raw_sales
        WHERE sale_date = '{{ ds }}'
    """
)
```

---

## 5. Operators

### 5.1 Operator Types

```
┌────────────────────────────────────────────────────────┐
│                  AIRFLOW OPERATORS                     │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │  ACTION OPERATORS                                │  │
│  │  BashOperator, PythonOperator, EmailOperator,   │  │
│  │  HttpOperator, SparkSubmitOperator, etc.        │  │
│  └──────────────────────────────────────────────────┘  │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │  SENSOR OPERATORS                                │  │
│  │  S3KeySensor, FileSensor, HttpSensor,           │  │
│  │  SqlSensor, ExternalTaskSensor, etc.            │  │
│  └──────────────────────────────────────────────────┘  │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │  TRANSFER OPERATORS                              │  │
│  │  S3ToSnowflakeOperator, MySqlToHiveOperator,    │  │
│  │  GCSToBigQueryOperator, etc.                    │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

### 5.2 Common Operators Reference

| Operator | Import | Use Case |
|---|---|---|
| `BashOperator` | `airflow.operators.bash` | Run shell commands |
| `PythonOperator` | `airflow.operators.python` | Run Python functions |
| `EmailOperator` | `airflow.operators.email` | Send emails |
| `PostgresOperator` | `airflow.providers.postgres.operators.postgres` | Run SQL on Postgres |
| `S3KeySensor` | `airflow.providers.amazon.aws.sensors.s3` | Wait for S3 file |
| `BranchPythonOperator` | `airflow.operators.python` | Conditional branching |
| `EmptyOperator` | `airflow.operators.empty` | Placeholder task |
| `SparkSubmitOperator` | `airflow.providers.apache.spark.operators.spark_submit` | Submit Spark job |
| `DatabricksRunNowOperator` | `airflow.providers.databricks.operators.databricks` | Trigger Databricks job |

### 5.3 BashOperator

```python
from airflow.operators.bash import BashOperator

run_script = BashOperator(
    task_id='run_etl_script',
    bash_command='python /opt/scripts/etl.py --date {{ ds }}',
    env={'ENV': 'production'}
)
```

### 5.4 PythonOperator

```python
from airflow.operators.python import PythonOperator

def process_data(ds, **kwargs):
    print(f"Processing data for {ds}")
    return "done"

task = PythonOperator(
    task_id='process',
    python_callable=process_data,
    op_kwargs={'custom_param': 'value'},
    provide_context=True    # passes ds, ti, etc.
)
```

### 5.5 Sensors

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_file = S3KeySensor(
    task_id='wait_for_s3_file',
    bucket_name='my-data-bucket',
    bucket_key='data/{{ ds }}/input.csv',
    aws_conn_id='aws_default',
    mode='reschedule',     # ✅ efficient — frees worker between checks
    poke_interval=300,     # check every 5 minutes
    timeout=3600           # fail after 1 hour
)
```

**Sensor Mode Comparison:**

```
POKE mode:
  [Sensor Task] ──► holds worker slot ──► polls every 60s ──► condition met ──► done
                         ▲
                         └── Worker slot occupied entire time (resource-heavy)

RESCHEDULE mode:
  [Sensor Task] ──► checks condition ──► NOT MET ──► releases worker ──► sleeps
                         │
                         └── checks again after poke_interval ──► MET ──► resumes
                                 ▲
                                 └── Worker slot only used during actual check
```

### 5.6 Hooks

Hooks provide reusable, abstracted connections to external systems.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

def query_db(**context):
    hook = PostgresHook(postgres_conn_id='my_postgres')
    records = hook.get_records("SELECT * FROM orders LIMIT 10")
    return records

# Available hooks: S3Hook, SlackHook, HttpHook, SnowflakeHook, etc.
```

---

## 6. Scheduling & Execution

### 6.1 Schedule Interval Options

```python
# Preset strings
schedule_interval='@once'      # Run once
schedule_interval='@hourly'    # 0 * * * *
schedule_interval='@daily'     # 0 0 * * *
schedule_interval='@weekly'    # 0 0 * * 0
schedule_interval='@monthly'   # 0 0 1 * *
schedule_interval='@yearly'    # 0 0 1 1 *
schedule_interval=None         # Only trigger manually

# Cron expressions
schedule_interval='0 6 * * *'     # Every day at 6 AM
schedule_interval='0 */4 * * *'   # Every 4 hours
schedule_interval='0 9 * * MON'   # Every Monday at 9 AM
schedule_interval='30 8 1 * *'    # 1st of every month at 8:30 AM
```

**Cron Syntax:**
```
┌────────── minute (0-59)
│ ┌──────── hour (0-23)
│ │ ┌────── day of month (1-31)
│ │ │ ┌──── month (1-12)
│ │ │ │ ┌── day of week (0-6, Sun=0)
│ │ │ │ │
* * * * *
```

### 6.2 start_date Behavior

```
start_date = 2024-01-01, schedule = @daily

Timeline:
  2024-01-01 ────► 2024-01-02 ────► 2024-01-03
       ▲                 ▲                 ▲
  1st interval     2nd interval      3rd interval
  starts           runs at           runs at
                   2024-01-02        2024-01-03

NOTE: First DAG run triggers AFTER start_date + one interval.
```

### 6.3 logical_date vs. data_interval vs. actual runtime

```
For a @daily DAG with start_date=2024-01-01:

  Run for Jan 1st data:
    data_interval_start : 2024-01-01 00:00:00
    data_interval_end   : 2024-01-02 00:00:00
    logical_date        : 2024-01-01 00:00:00  ← the "label" of this run
    actual_run_time     : 2024-01-02 00:00:00  ← when it actually executes

KEY INSIGHT: Airflow runs AFTER the interval completes.
```

### 6.4 catchup

```python
# catchup=True (default)
# If today is 2024-06-01 and start_date is 2024-01-01:
# Airflow will try to run ALL 150+ daily runs since Jan 1!

# catchup=False (recommended for most pipelines)
# Only runs from the next scheduled interval going forward
dag = DAG('daily_pipeline', catchup=False, ...)
```

### 6.5 Backfilling

Used to re-run historical data for a specific date range.

```bash
# CLI backfill
airflow dags backfill \
    --start-date 2024-01-01 \
    --end-date 2024-03-31 \
    my_dag_id

# Run in dry-run mode first to see what would execute
airflow dags backfill --dry-run -s 2024-01-01 -e 2024-03-31 my_dag_id
```

### 6.6 depends_on_past

```python
task = PythonOperator(
    task_id='cumulative_load',
    python_callable=load_cumulative,
    depends_on_past=True    # Won't run if previous date's run of THIS task failed
)
```

```
SCENARIO:
  Jan 1 run ──► SUCCESS ──► Jan 2 run can proceed
  Jan 2 run ──► FAILED  ──► Jan 3 run is BLOCKED (even if Jan 3 upstream tasks succeed)
```

---

## 7. Executors

### 7.1 Executor Comparison

```
┌────────────────────────────────────────────────────────────────────┐
│                        EXECUTOR TYPES                              │
│                                                                    │
│  SequentialExecutor                                                │
│  ─────────────────                                                 │
│  [Task A] → [Task B] → [Task C]   ← One at a time, NO parallelism │
│  ✅ Simple dev setup                                               │
│  ❌ Only for testing, uses SQLite                                  │
│                                                                    │
│  LocalExecutor                                                     │
│  ─────────────                                                     │
│  Scheduler ──► [Task A] ┐                                          │
│              ──► [Task B] ├─ Run in parallel on same machine       │
│              ──► [Task C] ┘                                        │
│  ✅ Small/medium teams, single server                              │
│  ❌ Limited by single machine resources                            │
│                                                                    │
│  CeleryExecutor                                                    │
│  ──────────────                                                    │
│  Scheduler ──► [Redis/RabbitMQ Queue] ──► Worker 1 (Task A)       │
│                                       ──► Worker 2 (Task B)       │
│                                       ──► Worker 3 (Task C)       │
│  ✅ Horizontal scaling, production standard                        │
│  ❌ Requires Redis + Flower + more infra                           │
│                                                                    │
│  KubernetesExecutor                                                │
│  ────────────────                                                  │
│  Scheduler ──► [K8s API] ──► Pod(Task A) ──► terminates           │
│                          ──► Pod(Task B) ──► terminates           │
│                          ──► Pod(Task C) ──► terminates           │
│  ✅ Best isolation, each task gets its own env, auto-scales        │
│  ❌ Cold start latency per task, complex setup                     │
└────────────────────────────────────────────────────────────────────┘
```

### 7.2 CeleryExecutor Deep Dive

```
Architecture:
  ┌───────────┐    submit    ┌─────────────┐    pick up    ┌──────────┐
  │ Scheduler │ ───────────► │    Redis    │ ─────────────► │ Worker1 │
  └───────────┘              │  (Queue)    │               └──────────┘
                             └─────────────┘
                                    │                      ┌──────────┐
                                    └──────────────────────► │ Worker2 │
                                                            └──────────┘

Components needed:
  - Airflow Scheduler
  - Airflow Workers (1 or more)
  - Redis or RabbitMQ (message broker)
  - Flower (optional: Celery monitoring UI)
  - Shared filesystem or remote storage for logs
```

### 7.3 KubernetesExecutor Deep Dive

```
Per-task pod lifecycle:
  Task Ready ──► Scheduler calls K8s API ──► Pod Created ──► Task Runs ──► Pod Deleted

Benefits:
  - Each task can use a different Docker image
  - No idle workers consuming resources
  - True resource isolation
  - Natural autoscaling

Trade-offs:
  - Pod startup time (10-30 seconds per task)
  - Requires K8s cluster
  - More complex debugging
```

### 7.4 Executor Selection Guide

```
Dev/Testing           → SequentialExecutor
Single Machine Prod   → LocalExecutor
Multi-machine Prod    → CeleryExecutor
Cloud-native / K8s    → KubernetesExecutor
Best of both worlds   → CeleryKubernetesExecutor
```

---

## 8. XCom & Data Passing

### 8.1 What is XCom?

XCom (Cross-Communication) allows tasks within a DAG to pass small pieces of data to each other.

```
Task A ──► pushes value to XCom ──► stored in Metadata DB
Task B ──► pulls value from XCom ──► uses it
```

### 8.2 Using XCom (Traditional)

```python
def push_fn(ti, **kwargs):
    # Push data
    ti.xcom_push(key='file_path', value='/data/output/2024-01-01.parquet')
    # Or simply return a value (auto-pushed with key='return_value')
    return {'rows': 1000, 'status': 'success'}

def pull_fn(ti, **kwargs):
    # Pull by key
    file_path = ti.xcom_pull(task_ids='upload_task', key='file_path')

    # Pull return value
    result = ti.xcom_pull(task_ids='extract_task')
    print(result['rows'])
```

### 8.3 Using XCom (TaskFlow API — Recommended)

```python
@dag(schedule_interval='@daily', start_date=datetime(2024,1,1))
def pipeline():

    @task
    def extract() -> dict:
        return {'rows': 1000, 'path': '/tmp/data.csv'}   # auto XCom push

    @task
    def transform(data: dict) -> dict:                   # auto XCom pull
        return {'processed': data['rows'] * 2}

    @task
    def load(result: dict):
        print(f"Loading {result['processed']} rows")

    load(transform(extract()))
```

### 8.4 XCom Best Practices

```
✅ DO use XCom for:
   - File paths ('/data/2024-01-01/output.parquet')
   - Row counts (1000)
   - Job IDs ('job_abc_123')
   - Status flags ('success', 'no_data')
   - Small metadata dicts

❌ NEVER use XCom for:
   - DataFrames
   - Large JSON blobs
   - File contents
   - Anything > ~48KB (bloats Metadata DB)

For large data: write to S3/GCS/HDFS, pass the PATH via XCom.
```

---

## 9. Advanced Features

### 9.1 Branching

Used for conditional pipeline paths.

```python
from airflow.operators.python import BranchPythonOperator

def decide_path(**context):
    env = Variable.get('environment')
    if env == 'production':
        return 'load_to_prod'    # returns task_id to execute
    else:
        return 'load_to_dev'     # all other paths are auto-skipped

branch = BranchPythonOperator(
    task_id='decide_environment',
    python_callable=decide_path
)

load_prod = PythonOperator(task_id='load_to_prod', ...)
load_dev  = PythonOperator(task_id='load_to_dev', ...)
notify    = EmptyOperator(task_id='notify', trigger_rule='none_failed_min_one_success')

branch >> [load_prod, load_dev] >> notify
```

**Branch Flow Diagram:**
```
                  ┌────────────────┐
                  │  decide_path   │
                  └───────┬────────┘
                          │
              ┌───────────┴───────────┐
              │                       │
    ┌─────────▼────────┐   ┌──────────▼────────┐
    │  load_to_prod    │   │   load_to_dev     │
    │  (chosen ✅)     │   │   (skipped ⏭️)    │
    └─────────┬────────┘   └──────────┬────────┘
              │                       │
              └───────────┬───────────┘
                          │
                  ┌───────▼────────┐
                  │    notify      │
                  └────────────────┘
```

### 9.2 Trigger Rules

Define when a task should run based on the state of its upstream tasks.

| Trigger Rule | Description |
|---|---|
| `all_success` | All parents succeeded (**default**) |
| `all_failed` | All parents failed |
| `all_done` | All parents finished (success OR failure) |
| `one_success` | At least one parent succeeded |
| `one_failed` | At least one parent failed |
| `none_failed` | No parent has failed (some may be skipped) |
| `none_skipped` | No parent was skipped |
| `none_failed_min_one_success` | No failure AND at least one success |

```python
# Notification task that runs whether pipeline succeeded or failed
notify = PythonOperator(
    task_id='send_notification',
    python_callable=notify_team,
    trigger_rule='all_done'   # runs regardless of upstream success/failure
)
```

### 9.3 Pools

Limit concurrency for specific resource-constrained tasks.

```python
# Create pool in UI: Admin > Pools > Create pool "api_pool" with 3 slots

api_task_1 = PythonOperator(
    task_id='call_api_1',
    python_callable=call_rate_limited_api,
    pool='api_pool',          # max 3 of these run simultaneously
    pool_slots=1
)
```

**Pool Diagram:**
```
api_pool: 3 slots available

Running:
  [api_call_1] ──► uses slot 1
  [api_call_2] ──► uses slot 2
  [api_call_3] ──► uses slot 3

Waiting:
  [api_call_4] ──► QUEUED (no slots available)
  [api_call_5] ──► QUEUED
```

### 9.4 TaskGroups (Airflow 2.0+)

Group tasks visually in the UI without SubDAG overhead.

```python
from airflow.utils.task_group import TaskGroup

with DAG('grouped_pipeline', ...) as dag:

    with TaskGroup('ingestion', tooltip='Data ingestion tasks') as ingestion:
        extract_users = PythonOperator(task_id='extract_users', ...)
        extract_orders = PythonOperator(task_id='extract_orders', ...)

    with TaskGroup('transformation') as transformation:
        transform_users = PythonOperator(task_id='transform_users', ...)
        transform_orders = PythonOperator(task_id='transform_orders', ...)

    load = PythonOperator(task_id='load_to_warehouse', ...)

    ingestion >> transformation >> load
```

```
UI View:
  ┌──────────────────────┐     ┌──────────────────────┐
  │   [ingestion] ▸      │────►│  [transformation] ▸  │────► [load]
  │  (collapsed group)   │     │  (collapsed group)   │
  └──────────────────────┘     └──────────────────────┘
```

**TaskGroups vs. SubDAGs:**

| Feature | SubDAGs (Old) | TaskGroups (Modern) |
|---|---|---|
| UI grouping | ✅ | ✅ |
| Executor issues | Known deadlocks | None |
| Separate DAG entry | Yes (pollutes UI) | No |
| Performance | Heavy | Lightweight |
| Recommendation | ❌ Avoid | ✅ Use this |

### 9.5 Dynamic Task Mapping (Airflow 2.3+)

Create a dynamic number of parallel task instances at runtime.

```python
from airflow.decorators import dag, task

@dag(schedule_interval='@daily', start_date=datetime(2024,1,1))
def dynamic_pipeline():

    @task
    def get_files() -> list:
        # Returns list of files to process
        return ['file_a.csv', 'file_b.csv', 'file_c.csv', 'file_d.csv']

    @task
    def process_file(filename: str):
        print(f"Processing {filename}")

    # Creates one task instance per file — DYNAMICALLY at runtime!
    process_file.expand(filename=get_files())
```

```
At runtime:
  get_files() returns ['a.csv', 'b.csv', 'c.csv', 'd.csv']

  Dynamic tasks created:
  ├── process_file[0] ──► a.csv
  ├── process_file[1] ──► b.csv
  ├── process_file[2] ──► c.csv
  └── process_file[3] ──► d.csv
  (all run in parallel, up to parallelism limits)
```

### 9.6 SLA (Service Level Agreements)

Define maximum acceptable task duration. Alert if exceeded.

```python
from datetime import timedelta

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    # Send Slack/PagerDuty alert
    send_alert(f"SLA missed! Tasks: {task_list}")

task = PythonOperator(
    task_id='critical_etl',
    python_callable=run_etl,
    sla=timedelta(hours=2)    # Must complete within 2 hours
)

dag = DAG(
    'daily_pipeline',
    sla_miss_callback=sla_miss_callback,
    ...
)
```

### 9.7 ExternalTaskSensor

Wait for a task or DAG in a *different* DAG.

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id='wait_for_ingestion_dag',
    external_dag_id='ingestion_pipeline',
    external_task_id='final_load',        # if None, waits for entire DAG
    execution_delta=timedelta(hours=1),   # if upstream runs 1 hour earlier
    mode='reschedule',
    timeout=3600
)
```

---

## 10. Production Best Practices

### 10.1 Secrets Management

```python
# ❌ NEVER do this
password = "my_secret_password_123"

# ✅ Method 1: Airflow Variables (basic)
from airflow.models import Variable
api_key = Variable.get('my_api_key', deserialize_json=False)

# ✅ Method 2: Airflow Connections
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('my_postgres_conn')
db_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}/{conn.schema}"

# ✅ Method 3: AWS Secrets Manager backend (production recommended)
# Configure in airflow.cfg:
# [secrets]
# backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
# backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}
```

### 10.2 Idempotency

```python
# ❌ NOT idempotent — inserts duplicate rows on retry
def load_data(**context):
    df = read_data()
    df.to_sql('sales', engine, if_exists='append')  # duplicates on re-run

# ✅ Idempotent — safe to re-run
def load_data(**context):
    ds = context['ds']
    # Delete existing data for this date first
    engine.execute(f"DELETE FROM sales WHERE sale_date = '{ds}'")
    # Then insert fresh
    df = read_data(date=ds)
    df.to_sql('sales', engine, if_exists='append')
```

### 10.3 DAG File Best Practices

```python
# ❌ WRONG — top-level DB connection (runs on every parse!)
conn = psycopg2.connect("postgresql://...")   # ← spams DB every few seconds

# ✅ CORRECT — connection inside the function
def my_task():
    conn = psycopg2.connect("postgresql://...")
    # do work
    conn.close()
```

### 10.4 Remote Logging

```python
# airflow.cfg
[logging]
remote_logging = True
remote_base_log_folder = s3://my-airflow-logs/logs
remote_log_conn_id = aws_default
```

```
Without remote logging:
  Worker 1 logs → /var/log/airflow/worker1/
  Worker 2 logs → /var/log/airflow/worker2/
  UI can't find them! ❌

With remote logging (S3):
  Worker 1 logs → s3://airflow-logs/dag/task/2024-01-01.log
  Worker 2 logs → s3://airflow-logs/dag/task/2024-01-02.log
  UI fetches from S3 ✅
```

### 10.5 High Availability Setup

```
Production HA Architecture:
  ┌──────────────────────────────────────────────────┐
  │               LOAD BALANCER                      │
  └───────────────────┬──────────────────────────────┘
                      │
          ┌───────────┴────────────┐
          │                        │
  ┌───────▼──────┐        ┌────────▼─────┐
  │  Webserver 1 │        │  Webserver 2 │
  └──────────────┘        └──────────────┘
          │                        │
          └────────────┬───────────┘
                       │
          ┌────────────┼────────────┐
          │            │            │
  ┌───────▼──┐  ┌──────▼──┐  ┌─────▼───┐
  │Scheduler1│  │Scheduler2│  │Triggerer│
  └──────────┘  └──────────┘  └─────────┘
       (HA — if one fails, other takes over)
                       │
               ┌───────▼────────┐
               │  PostgreSQL    │
               │  (RDS/Cloud)   │
               └───────┬────────┘
                       │
          ┌────────────┼────────────┐
  ┌───────▼──┐  ┌──────▼──┐  ┌─────▼───┐
  │ Worker 1 │  │ Worker 2 │  │ Worker 3│
  └──────────┘  └──────────┘  └─────────┘
```

### 10.6 CI/CD for DAGs

```
Git Repository
    │
    ├── dags/           ← DAG files
    ├── tests/          ← Unit tests
    └── plugins/        ← Custom operators

CI Pipeline:
  [Git Push]
    └──► [Lint: flake8, black]
    └──► [Test: pytest dag_tests/]
         └── test DAG imports
         └── test task count
         └── test no cycles
    └──► [Deploy to S3/DAG folder if tests pass]
```

```python
# Example DAG unit test
import pytest
from airflow.models import DagBag

def test_dag_loads():
    dagbag = DagBag(dag_folder='./dags', include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG errors: {dagbag.import_errors}"

def test_dag_task_count():
    dagbag = DagBag()
    dag = dagbag.get_dag('daily_etl')
    assert len(dag.tasks) == 5

def test_no_cycles():
    dagbag = DagBag()
    dag = dagbag.get_dag('daily_etl')
    assert dag.test_cycle() is False
```

---

## 11. Troubleshooting

### 11.1 Task States

```
State Flow:

  [none] ──► [scheduled] ──► [queued] ──► [running] ──► [success]
                                                │
                                                └──► [failed] ──► [up_for_retry]
                                                │                      │
                                                │              (after retry_delay)
                                                │                      │
                                                │              [scheduled again]
                                                │
                                                └──► [upstream_failed]
                                                └──► [skipped]
                                                └──► [up_for_reschedule]
```

| State | Meaning |
|---|---|
| `none` | Not yet scheduled |
| `scheduled` | Ready to run, waiting for executor |
| `queued` | Sent to executor, waiting for worker |
| `running` | Currently executing on a worker |
| `success` | Completed successfully |
| `failed` | Raised an unhandled exception |
| `up_for_retry` | Failed but has retries remaining |
| `up_for_reschedule` | Sensor in reschedule mode, waiting |
| `skipped` | Bypassed due to branching |
| `upstream_failed` | Parent task failed |
| `removed` | Task removed from DAG definition |
| `shutdown` | Killed by user or system |
| `deferred` | Task deferred to Triggerer |

### 11.2 Zombie and Undead Tasks

```
ZOMBIE Tasks:
  Airflow thinks task is running, but the worker process died.
  
  Cause: Worker crash, OOM kill, EC2 termination
  Fix: Airflow scheduler auto-detects and marks as failed after
       `scheduler_zombie_task_threshold` (default: 300s)

UNDEAD Tasks:
  Task is running on a worker, but Airflow DB says it should be dead.
  
  Cause: User cleared the task in UI while it was running
  Fix: Worker will eventually notice and stop the process
```

### 11.3 Common Issues & Fixes

| Problem | Likely Cause | Fix |
|---|---|---|
| DAG not appearing in UI | Import error in DAG file | Check `airflow dags list-import-errors` |
| Task stuck in `queued` | No worker available / pool full | Check worker health, pool slots |
| Task stuck in `running` | Zombie task | Wait for zombie detection or clear task |
| Scheduler not heartbeating | Scheduler process died | Restart scheduler |
| UI slow / timeout | Metadata DB is slow | Tune DB, add indexes |
| `DagFileProcessorManager` lag | Too many DAG files | Increase `min_file_process_interval` |

### 11.4 Debugging Commands

```bash
# List all DAGs
airflow dags list

# Check for import errors
airflow dags list-import-errors

# Test a specific task without running the whole DAG
airflow tasks test my_dag my_task 2024-01-01

# Run a DAG manually
airflow dags trigger my_dag

# Check task logs
airflow tasks logs my_dag my_task 2024-01-01

# Backfill
airflow dags backfill -s 2024-01-01 -e 2024-01-31 my_dag

# Pause/unpause a DAG
airflow dags pause my_dag
airflow dags unpause my_dag

# Clear task instances
airflow tasks clear -s 2024-01-01 -e 2024-01-31 my_dag
```

---

## 12. Interview Q&A — Beginner

**Q1. What is Apache Airflow?**
Apache Airflow is an open-source platform to programmatically author, schedule, and monitor workflows. Originally built at Airbnb in 2014, it allows engineers to define complex data pipelines as Python code (DAGs), with dependency management, retries, and a centralized monitoring UI.

---

**Q2. What is a DAG?**
DAG stands for Directed Acyclic Graph. It's the core abstraction in Airflow representing a workflow. Directed means tasks flow in one direction; Acyclic means no circular dependencies; Graph means it's a network of tasks with edges representing dependencies.

---

**Q3. What are the four core components of Airflow?**
1. **Scheduler** — monitors DAGs, triggers ready tasks
2. **Webserver** — Flask UI for monitoring and management
3. **Metadata Database** — stores all state (PostgreSQL/MySQL)
4. **Executor** — defines how tasks are executed

---

**Q4. What is the difference between Operator, Task, and Task Instance?**
- **Operator** = class that defines *what* to do (e.g., `PythonOperator`)
- **Task** = instantiated operator inside a DAG
- **Task Instance** = one execution of a task for a specific `logical_date`

---

**Q5. How do you define dependencies between tasks?**
Using bitshift operators: `task_a >> task_b` means B depends on A. You can also use `set_downstream()` / `set_upstream()` methods.

---

**Q6. What is schedule_interval and give examples?**
It defines how often a DAG runs. Examples: `@daily`, `@hourly`, `'0 6 * * *'` (daily at 6 AM), `None` (manual only).

---

**Q7. What does catchup=False mean?**
It prevents Airflow from running all the historical DAG runs from `start_date` to now. Without it, a new DAG could trigger hundreds of backfill runs.

---

**Q8. What is an Airflow Sensor?**
A special operator that waits for an external condition to be true (e.g., file in S3, database row, API response) before proceeding. It supports `poke` and `reschedule` modes.

---

**Q9. What is an Airflow Hook?**
A Hook is a reusable interface to an external system (S3, Postgres, Slack). Operators use Hooks internally. You can use Hooks directly in PythonOperator functions for custom logic.

---

**Q10. What is XCom?**
Cross-Communication. A mechanism allowing tasks to exchange small amounts of data (metadata, file paths, counts) through the Metadata Database.

---

**Q11. What is the start_date in Airflow?**
The date from which Airflow begins scheduling runs. Important: the first actual DAG run happens **after** `start_date` + one `schedule_interval` has passed.

---

**Q12. What is the default_args dict?**
A dictionary of default task arguments (retries, email_on_failure, retry_delay, etc.) applied to all tasks in the DAG unless overridden at the task level.

---

**Q13. What database does Airflow use and why does it matter?**
PostgreSQL (recommended for production) or MySQL. SQLite only for dev/testing. The Metadata DB stores all state, and a slow or poorly tuned DB directly impacts Scheduler performance.

---

**Q14. What are the two ways to write a DAG?**
1. **Traditional** — using `with DAG(...) as dag:` and `PythonOperator`, `BashOperator`, etc.
2. **TaskFlow API** — using `@dag` and `@task` decorators (Airflow 2.0+).

---

**Q15. How do you trigger a DAG manually?**
Via the UI (click "Trigger DAG"), via CLI (`airflow dags trigger dag_id`), or via the REST API.

---

**Q16. What is the Airflow Web UI used for?**
Visualizing DAGs (Graph View, Grid View), triggering runs, viewing task logs, managing Variables and Connections, monitoring scheduler health, and clearing/rerunning tasks.

---

**Q17. What is a Variable in Airflow?**
A key-value store in the Metadata DB for configuration values. Access with `Variable.get('key')`. In production, backed by a secrets manager.

---

**Q18. What is a Connection in Airflow?**
A stored set of credentials for connecting to external systems (DBs, APIs, cloud services). Referenced by `conn_id` in operators and hooks.

---

**Q19. How do you add retries to a task?**
```python
task = PythonOperator(
    task_id='my_task',
    python_callable=my_fn,
    retries=3,
    retry_delay=timedelta(minutes=5)
)
```

---

**Q20. What does the `provide_context=True` argument do?**
(Airflow 1.x style) Injects the Airflow context (including `ds`, `execution_date`, `ti`, etc.) as keyword arguments to the Python callable. In Airflow 2.x with `@task`, context is accessed via `get_current_context()`.

---

## 13. Interview Q&A — Intermediate

**Q21. Compare LocalExecutor, CeleryExecutor, and KubernetesExecutor.**

| | LocalExecutor | CeleryExecutor | KubernetesExecutor |
|---|---|---|---|
| Parallelism | Multi-process on one machine | Distributed across workers | One Pod per task |
| Scaling | Vertical only | Horizontal | Horizontal + K8s autoscale |
| Infrastructure | Simple | Redis/RabbitMQ needed | K8s cluster needed |
| Resource isolation | None | Shared worker env | Full (per-task image) |
| Best for | Small teams | Medium-large production | Cloud-native, diverse envs |

---

**Q22. Explain the logical_date (formerly execution_date).**
The `logical_date` is the start of the data interval that the DAG run is processing — it does NOT mean when the task actually ran. For a `@daily` DAG, the run for logical_date `2024-01-01` processes data from `2024-01-01 00:00` to `2024-01-02 00:00`, and actually executes at `2024-01-02 00:00`.

---

**Q23. What are Trigger Rules and why are they needed?**
Trigger rules define when a task executes based on its upstream tasks' states. The default is `all_success`. Other rules like `all_done` or `none_failed` allow tasks to run for notifications, cleanup, or partial-success scenarios — critical for building robust pipelines.

---

**Q24. What is a Pool and when do you use it?**
A Pool limits concurrency for tasks that share a constrained resource (rate-limited API, expensive DB, etc.). You create a pool with N slots; only N tasks from that pool run at any time.

---

**Q25. What is DAG Serialization?**
The Scheduler parses DAG Python files and stores a JSON representation in the Metadata DB. The Webserver reads JSON instead of running Python directly — reducing Webserver CPU load and improving UI performance.

---

**Q26. What is the difference between SubDAGs and TaskGroups?**
SubDAGs are an older mechanism that creates child DAGs, causing performance issues, deadlocks, and UI clutter. TaskGroups are a lightweight, UI-only grouping mechanism introduced in Airflow 2.0 — same DAG, just visually collapsed groups. Always prefer TaskGroups.

---

**Q27. Explain Jinja templating in Airflow.**
Airflow fields marked as templateable support Jinja2 syntax. You can inject `{{ ds }}` (date string), `{{ dag_run.conf['key'] }}` (runtime config), `{{ var.value.my_var }}` (Variables), and custom macros into SQL, Bash commands, and Python operator arguments.

---

**Q28. How does depends_on_past work?**
When `True`, a task instance will only start if the same task's instance for the **previous** logical date succeeded. Useful for cumulative processes where each day's job depends on the previous day having completed correctly.

---

**Q29. What is the Triggerer and what problem does it solve?**
Without the Triggerer, Sensors occupy a worker slot for their entire wait duration. The Triggerer (Airflow 2.2+) runs an async event loop. Deferrable Operators "hand off" their waiting to the Triggerer, freeing the worker slot. This allows a single Triggerer to manage thousands of waits without consuming worker resources.

---

**Q30. How do you pass runtime config to a DAG?**
Trigger the DAG with a JSON config via UI or API. Access it with `{{ dag_run.conf['key'] }}` in templates or `context['dag_run'].conf['key']` in Python.
```bash
airflow dags trigger my_dag --conf '{"env": "production", "date": "2024-01-01"}'
```

---

**Q31. What are Zombie tasks and how does Airflow handle them?**
Zombies are task instances that appear as `running` in the DB but whose actual worker process has died. The Scheduler detects them by checking heartbeats and automatically marks them as `failed` after `scheduler_zombie_task_threshold` (default: 300 seconds).

---

**Q32. Why should you never put connection/import logic at the top-level of a DAG file?**
The Airflow Scheduler parses DAG files every few seconds (configurable via `min_file_process_interval`). Any top-level code (connections, API calls, heavy imports) runs on every parse — not just when tasks execute. This can spam external systems and slow the Scheduler significantly.

---

**Q33. What is `sensor_mode='reschedule'` and why is it important?**
In `poke` mode (default), a sensor holds a worker slot the entire time it waits. In `reschedule` mode, it checks the condition, and if not met, releases the worker slot and reschedules itself. For long waits (hours), this can free dozens of worker slots for other tasks.

---

**Q34. How do you do unit testing in Airflow?**
```python
def test_dag_integrity():
    dagbag = DagBag()
    assert 'my_dag' in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_task_count():
    dag = DagBag().get_dag('my_dag')
    assert len(dag.tasks) == 5

# Run a task locally for testing
airflow tasks test my_dag my_task 2024-01-01
```

---

**Q35. What is an SLA miss and how do you configure it?**
SLA (Service Level Agreement) miss occurs when a task takes longer than its `sla` timedelta. Configure with `sla=timedelta(hours=2)` on the task and `sla_miss_callback` on the DAG. Airflow sends an email or calls the callback function.

---

**Q36. What is the ExternalTaskSensor?**
A sensor that waits for a task or entire DAG in a **different** DAG to complete before proceeding. Used to create cross-DAG dependencies without merging DAGs together.

---

**Q37. How does CeleryExecutor work internally?**
1. Scheduler determines task is ready → writes to Metadata DB
2. Scheduler sends task to message queue (Redis/RabbitMQ)
3. Celery Worker picks up the task from the queue
4. Worker executes the task code
5. Worker writes success/failure state to Metadata DB

---

**Q38. What is Dynamic Task Mapping?**
Introduced in Airflow 2.3+, it allows creating a variable number of task instances at runtime using `.expand()`. The number of tasks is determined by the output of a previous task — useful for processing a dynamic list of files, accounts, or regions.

---

**Q39. How do you handle secrets in production?**
Use Airflow Connections and Variables backed by a Secrets Backend:
- AWS Secrets Manager
- HashiCorp Vault
- GCP Secret Manager
- Azure Key Vault
Configure in `airflow.cfg` under `[secrets]` backend.

---

**Q40. What happens when the Scheduler crashes in Airflow 2.0+?**
Airflow 2.0+ supports multiple Schedulers running simultaneously (HA mode). They coordinate via database locking. If one crashes, another takes over immediately. Running tasks on workers continue to completion; the surviving Scheduler will detect and handle zombies.

---

## 14. Interview Q&A — Advanced

**Q41. How would you design Airflow for 10,000+ DAGs?**

Key strategies:
1. Use KubernetesExecutor (no idle workers, infinite scalability)
2. Enable DAG Serialization (reduce Webserver load)
3. Increase Scheduler instances (HA mode, 2-3 Schedulers)
4. Store DAGs in S3 + sync to workers (Git-Sync or S3 sidecar)
5. Tune `min_file_process_interval` and `dag_dir_list_interval`
6. Use connection pooling for the Metadata DB (PgBouncer)
7. Run Metadata DB on managed service (RDS Aurora, Cloud SQL)

---

**Q42. Explain Airflow's HA (High Availability) architecture.**
In Airflow 2.0+:
- Multiple Scheduler instances run concurrently
- Each Scheduler uses a "scheduler lock" in the DB to avoid double-scheduling
- Multiple Webservers behind a load balancer
- Metadata DB on a replicated managed service (RDS Multi-AZ)
- Workers can be autoscaled (K8s HPA or KEDA)

---

**Q43. How do you implement an audit trail in Airflow?**
Airflow's Metadata DB stores every DAG run and task instance. For custom audit trails:
- Add `on_success_callback` and `on_failure_callback` to tasks
- Write audit records to a separate audit table
- Use Airflow's REST API to export run history

---

**Q44. What is the difference between `max_active_runs` and `concurrency`?**
- `max_active_runs`: Maximum number of DAG runs that can be active simultaneously (e.g., backfill + scheduled run at the same time). Default: 16.
- `concurrency` (now `max_active_tasks`): Maximum number of task instances that can run simultaneously within a single DAG run.

---

**Q45. How does Airflow handle task isolation for environment dependencies?**
- **CeleryExecutor**: All tasks share the worker's Python environment. Use virtual environments or Docker for isolation.
- **KubernetesExecutor**: Each task gets its own Pod with a custom Docker image — full isolation.
- **DockerOperator**: Run any task in a Docker container regardless of executor.

---

**Q46. Explain the difference between Airflow Variables and Connections. When to use each?**
- **Variables**: Configuration values (environment name, S3 bucket name, feature flags). Accessed in Python or Jinja templates.
- **Connections**: External system credentials and endpoints (DB host/user/password, API keys). Accessed by Hooks and Operators via `conn_id`.

For both: use a Secrets Backend in production instead of storing in the Metadata DB directly.

---

**Q47. What is TaskFlow API and what problems does it solve over the traditional approach?**
Traditional Airflow requires manual XCom push/pull and boilerplate `PythonOperator` definitions. TaskFlow API (`@task` decorator) auto-handles XCom, makes dependencies obvious through Python function calls, and reduces code volume by ~50%. It also integrates with type hints for better IDE support.

---

**Q48. How would you implement retry logic with exponential backoff?**
```python
from airflow.utils.dates import days_ago
from datetime import timedelta

task = PythonOperator(
    task_id='api_call',
    python_callable=call_api,
    retries=5,
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=True,   # each retry waits 2x longer
    max_retry_delay=timedelta(hours=1)
)
```

---

**Q49. How do you pass large data between tasks safely?**
Never use XCom for large data. Pattern:
1. Task A writes data to S3/GCS/HDFS/Delta Lake
2. Task A pushes the **file path** to XCom
3. Task B pulls the **file path** from XCom
4. Task B reads the data from S3/GCS/HDFS

---

**Q50. What is the difference between `airflow tasks test` and a real DAG run?**
`airflow tasks test` runs a task locally in the current Python environment without writing to the Metadata DB, without respecting dependencies, and without triggering any callbacks. It's purely for debugging. A real DAG run respects state, writes to the DB, and runs on workers.

---

**Q51. What is KEDA and how does it work with Airflow?**
KEDA (Kubernetes Event-Driven Autoscaling) scales Airflow Workers on Kubernetes based on the number of messages in the Celery queue. When the queue is empty, workers scale to 0. When tasks are queued, workers scale up automatically — enabling cost-effective, demand-driven scaling.

---

**Q52. How would you implement cross-DAG dependencies?**
Three approaches:
1. **ExternalTaskSensor** — wait for a specific task/DAG in another DAG
2. **TriggerDagRunOperator** — programmatically trigger a downstream DAG
3. **Dataset-based scheduling** (Airflow 2.4+) — DAGs triggered when a Dataset is updated by another DAG

```python
# Dataset-based (modern approach)
from airflow import Dataset

my_dataset = Dataset("s3://bucket/data/output.csv")

# Producer DAG
with DAG('producer') as dag:
    task = PythonOperator(..., outlets=[my_dataset])

# Consumer DAG — triggers automatically when my_dataset is updated
with DAG('consumer', schedule=[my_dataset]) as dag:
    ...
```

---

**Q53. What are Deferrable Operators?**
Operators that can "defer" their wait to the Triggerer component instead of holding a worker slot. They push their waiting logic into an async trigger, release the worker, and resume when the trigger fires. Any sensor can be made deferrable.

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Regular sensor — holds worker slot
sensor = S3KeySensor(task_id='wait', ...)

# Deferrable — frees worker slot
sensor = S3KeySensor(task_id='wait', ..., deferrable=True)
```

---

**Q54. How do you implement data quality checks in Airflow?**
```python
# Pattern 1: SQLCheckOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

quality_check = SQLCheckOperator(
    task_id='check_row_count',
    conn_id='warehouse',
    sql="SELECT COUNT(*) > 0 FROM daily_sales WHERE sale_date = '{{ ds }}'"
)

# Pattern 2: Great Expectations
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

ge_task = GreatExpectationsOperator(
    task_id='validate_data',
    expectation_suite_name='sales_suite',
    batch_kwargs={'table': 'daily_sales', 'datasource': 'warehouse'}
)

# Flow: extract >> transform >> quality_check >> load_to_prod
```

---

**Q55. Explain the Airflow REST API and its uses.**
Airflow 2.0+ provides a stable REST API for:
- Triggering DAGs (CI/CD pipelines)
- Getting DAG/task status (external monitoring)
- Updating Variables/Connections programmatically
- Clearing and re-running task instances

```bash
# Trigger DAG via API
curl -X POST "http://airflow:8080/api/v1/dags/my_dag/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{"conf": {"env": "production"}}' \
  -u "admin:admin"
```

---

**Q56. What are Datasets in Airflow 2.4+?**
Datasets are logical representations of data assets. DAGs can declare:
- **outlets** — data they produce
- **schedule** based on Datasets — run when a dataset is updated

This enables event-driven, data-aware scheduling without polling.

---

**Q57. How do you implement a fan-out / fan-in pattern?**
```python
# Fan-out: one task triggers multiple parallel tasks
# Fan-in: multiple tasks must complete before proceeding

extract >> [process_users, process_orders, process_products]
[process_users, process_orders, process_products] >> load_to_warehouse
```

```
               ┌──► process_users   ──┐
               │                      │
  [extract] ───┼──► process_orders  ──┼──► [load_to_warehouse]
               │                      │
               └──► process_products ─┘
```

---

**Q58. How do you handle a scenario where some tasks in a group can fail but the pipeline should continue?**
Use Trigger Rules:
```python
risky_task = PythonOperator(task_id='risky', ...)
cleanup = PythonOperator(
    task_id='cleanup',
    trigger_rule='all_done'    # runs whether risky_task succeeded or failed
)
risky_task >> cleanup
```

---

**Q59. What is the role of the Metadata DB and how do you optimize it?**
It stores all Airflow state. Optimization strategies:
1. Use PostgreSQL (not MySQL for large setups)
2. Run `airflow db clean` to purge old logs/runs
3. Configure connection pooling (PgBouncer)
4. Add DB indexes on frequently queried columns
5. Use managed DB services (RDS, Cloud SQL) with read replicas
6. Tune `max_db_retries` and connection pool size in `airflow.cfg`

---

**Q60. What is Git-Sync in Kubernetes Airflow deployments?**
A sidecar container that continuously syncs DAG files from a Git repository to the local filesystem shared with the Scheduler and Workers. This enables GitOps-style DAG deployment — merge to main branch → DAGs automatically update without restarting Airflow.

---

## 15. Scenario-Based Questions

---

### Scenario 1: Daily ETL Pipeline Design

**Question:** Design an Airflow DAG for a retail company that daily extracts sales data from Postgres, transforms it with Python, loads to Snowflake, and sends a Slack alert on completion or failure.

**Answer:**

```python
from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta

def send_failure_alert(context):
    SlackWebhookOperator(
        task_id='slack_fail',
        http_conn_id='slack_webhook',
        message=f"❌ DAG Failed: {context['dag'].dag_id} | Task: {context['task'].task_id}"
    ).execute(context)

@dag(
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': send_failure_alert
    }
)
def daily_retail_etl():

    @task
    def extract_sales(ds: str) -> str:
        # Write to S3, return path
        path = f's3://raw/sales/{ds}/data.parquet'
        # extract logic...
        return path

    @task
    def transform(raw_path: str, ds: str) -> str:
        out_path = f's3://processed/sales/{ds}/data.parquet'
        # transform logic...
        return out_path

    @task
    def load_to_snowflake(processed_path: str):
        # load logic using SnowflakeHook
        pass

    @task
    def notify_success():
        # Slack success message
        pass

    path = extract_sales()
    processed = transform(path)
    loaded = load_to_snowflake(processed)
    loaded >> notify_success()

dag = daily_retail_etl()
```

```
Flow:
  [extract_sales] ──► [transform] ──► [load_to_snowflake] ──► [notify_success]
         │                  │                  │
         └──────────────────┴──────────────────┘
                   on_failure_callback ──► Slack alert
```

---

### Scenario 2: Late-Arriving Data

**Question:** Your pipeline runs at 6 AM but source data sometimes arrives at 7-8 AM. How do you handle this?

**Answer:**
Use a Sensor at the start of the DAG:

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_data = S3KeySensor(
    task_id='wait_for_source_data',
    bucket_name='raw-data',
    bucket_key='sales/{{ ds }}/done.flag',
    mode='reschedule',      # don't hold worker slot
    poke_interval=300,      # check every 5 min
    timeout=7200,           # fail after 2 hours
    soft_fail=True          # mark as skipped instead of failed on timeout
)

wait_for_data >> extract >> transform >> load
```

---

### Scenario 3: Re-processing Failed Historical Data

**Question:** 15 days of your pipeline ran but had a bug in the transform step. How do you reprocess?

**Answer:**
1. Fix the bug in the transform function.
2. Clear only the `transform` and `load` task instances for those 15 days:
```bash
airflow tasks clear \
    --task-regex 'transform|load' \
    --start-date 2024-01-01 \
    --end-date 2024-01-15 \
    my_dag
```
3. The Scheduler automatically re-queues cleared tasks.

Ensure transform is **idempotent** (deletes target partition before rewriting).

---

### Scenario 4: Processing 1000 Files in Parallel

**Question:** You receive 1000 CSV files daily. How do you process them in parallel in Airflow?

**Answer:**
Use Dynamic Task Mapping:

```python
@dag(schedule_interval='@daily', ...)
def process_files():

    @task
    def list_files(ds: str) -> list:
        s3 = S3Hook()
        return s3.list_keys(bucket_name='raw-data', prefix=f'uploads/{ds}/')

    @task(pool='file_processing_pool')  # limit to N concurrent
    def process_file(key: str):
        # process single file
        pass

    @task
    def merge_results():
        pass

    files = list_files()
    process_file.expand(key=files) >> merge_results()
```

Create pool `file_processing_pool` with 50 slots to process 50 files concurrently.

---

### Scenario 5: Cross-DAG Dependency

**Question:** Your reporting DAG should only run after both the sales pipeline and the inventory pipeline have completed for the same day.

**Answer:**

```python
from airflow.sensors.external_task import ExternalTaskSensor

with DAG('reporting_pipeline', schedule_interval='0 8 * * *', ...) as dag:

    wait_sales = ExternalTaskSensor(
        task_id='wait_for_sales',
        external_dag_id='sales_pipeline',
        external_task_id=None,    # wait for entire DAG
        mode='reschedule',
        timeout=3600
    )

    wait_inventory = ExternalTaskSensor(
        task_id='wait_for_inventory',
        external_dag_id='inventory_pipeline',
        external_task_id=None,
        mode='reschedule',
        timeout=3600
    )

    generate_report = PythonOperator(task_id='generate_report', ...)

    [wait_sales, wait_inventory] >> generate_report
```

---

### Scenario 6: Rate-Limited API

**Question:** You need to call a third-party API that only allows 5 concurrent requests. How do you handle this?

**Answer:**
Create an Airflow Pool with 5 slots:

```python
# Create in UI: Admin > Pools > "api_rate_limit_pool" with 5 slots

@task(pool='api_rate_limit_pool', pool_slots=1)
def call_api(entity_id: str):
    # Only 5 of these run at the same time
    return requests.get(f'https://api.example.com/data/{entity_id}').json()

call_api.expand(entity_id=entity_ids)
```

---

### Scenario 7: Environment-Based Conditional Logic

**Question:** Your DAG should load to `prod_warehouse` in production and `dev_warehouse` in development. How do you implement this?

**Answer:**

```python
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator

def choose_env(**context):
    env = Variable.get('deployment_env', default_var='dev')
    return 'load_to_prod' if env == 'production' else 'load_to_dev'

branch = BranchPythonOperator(task_id='choose_environment', python_callable=choose_env)
load_prod = PythonOperator(task_id='load_to_prod', python_callable=load, op_kwargs={'target': 'prod_warehouse'})
load_dev  = PythonOperator(task_id='load_to_dev', python_callable=load, op_kwargs={'target': 'dev_warehouse'})
done      = EmptyOperator(task_id='done', trigger_rule='none_failed_min_one_success')

transform >> branch >> [load_prod, load_dev] >> done
```

---

### Scenario 8: Handling Upstream Pipeline Delays

**Question:** Your DAG is scheduled for 6 AM but some days the upstream system is delayed by 2 hours. The DAG fails because data isn't ready. How do you fix this?

**Answer:**
Set `soft_fail=True` on the sensor + add downstream skip handling, OR shift the schedule by 2 hours. Better approach:

```python
from airflow.providers.http.sensors.http import HttpSensor

check_upstream = HttpSensor(
    task_id='check_upstream_ready',
    http_conn_id='upstream_api',
    endpoint='/status?date={{ ds }}',
    response_check=lambda r: r.json()['status'] == 'ready',
    mode='reschedule',
    poke_interval=600,   # check every 10 minutes
    timeout=10800,       # wait up to 3 hours
    soft_fail=False      # hard fail if not ready after 3 hours → PagerDuty alert
)
```

---

### Scenario 9: Monitoring and Alerting

**Question:** How do you set up monitoring and alerts for a production Airflow environment?

**Answer:**

```python
# 1. Task-level failure callback
def on_failure(context):
    slack_msg = f"""
    ❌ Task Failed!
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution: {context['execution_date']}
    Log: {context['task_instance'].log_url}
    """
    SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack_alerts',
        message=slack_msg
    ).execute(context)

# 2. SLA miss callback
def on_sla_miss(dag, task_list, **kwargs):
    send_pagerduty_alert(f"SLA missed for {task_list}")

# 3. DAG-level config
dag = DAG(
    'critical_pipeline',
    sla_miss_callback=on_sla_miss,
    default_args={'on_failure_callback': on_failure}
)

# 4. External: Push Airflow metrics to Prometheus/Datadog via StatsD
```

---

### Scenario 10: Incremental vs. Full Refresh

**Question:** How do you implement incremental data loading in Airflow?

**Answer:**

```python
@task
def incremental_load(ds: str, data_interval_start, data_interval_end):
    # Use the logical interval for incremental extraction
    query = f"""
        SELECT * FROM source_table
        WHERE updated_at >= '{data_interval_start}'
          AND updated_at < '{data_interval_end}'
    """
    df = pd.read_sql(query, engine)

    # Idempotent: delete-then-insert
    target.execute(f"DELETE FROM target WHERE date = '{ds}'")
    df.to_sql('target', target_engine, if_exists='append')
```

---

### Scenario 11: DAG Parameterization

**Question:** You need to run the same pipeline for 10 different clients. How do you avoid writing 10 DAGs?

**Answer:**

```python
# Option 1: Factory function
def create_client_dag(client_id: str, schedule: str):
    @dag(
        dag_id=f'pipeline_{client_id}',
        schedule_interval=schedule,
        start_date=datetime(2024, 1, 1)
    )
    def client_pipeline():
        @task
        def extract():
            return fetch_data(client_id=client_id)
        @task
        def load(data):
            write_data(client_id=client_id, data=data)
        load(extract())
    return client_pipeline()

clients = {'client_a': '@daily', 'client_b': '@hourly', 'client_c': '@weekly'}
for client_id, schedule in clients.items():
    globals()[f'dag_{client_id}'] = create_client_dag(client_id, schedule)
```

---

### Scenario 12: Zero-Downtime DAG Updates

**Question:** You need to update a production DAG that is currently running. How do you avoid disrupting running instances?

**Answer:**
1. Pause the DAG: `airflow dags pause my_dag`
2. Wait for running task instances to complete.
3. Deploy the new DAG code (via Git-Sync or S3 sync).
4. Unpause: `airflow dags unpause my_dag`
5. If the update is structural (added/removed tasks), clear old task instances if needed.

---

### Scenario 13: Long-Running Tasks

**Question:** A Spark job submitted via Airflow takes 3 hours. Worker slots are consumed the whole time. How do you optimize?

**Answer:**
Use `DatabricksRunNowOperator` or `SparkSubmitOperator` with deferrable mode:

```python
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

spark_job = DatabricksRunNowOperator(
    task_id='run_spark_job',
    databricks_conn_id='databricks_default',
    job_id=12345,
    deferrable=True   # frees worker slot while job runs in Databricks
)
```

The Triggerer monitors the job status async; worker slot is freed during the 3-hour runtime.

---

### Scenario 14: Multi-Team Airflow

**Question:** Your company has 5 data teams sharing one Airflow instance. How do you isolate and manage their workloads?

**Answer:**
1. **Separate DAG folders** per team (use Git-Sync with separate repos).
2. **RBAC** — different Airflow roles per team (Viewer, User, Op, Admin).
3. **Separate Pools** per team with slot quotas.
4. **Tags** for filtering DAGs in the UI by team.
5. **Naming conventions** for DAG IDs (`team_a.pipeline_name`).
6. For stronger isolation: separate Airflow environments per team (Helm chart per namespace in K8s).

---

### Scenario 15: Debugging a Broken Pipeline

**Question:** Your pipeline ran successfully yesterday but failed today. Walk through your debugging process.

**Answer:**
```
Step 1: Check the Airflow UI Grid View
  └── Which task failed? What is the state?

Step 2: View task logs in UI
  └── Scroll to the bottom for the exception traceback

Step 3: Check if it's a data issue
  └── Did upstream data arrive? Check with source team.

Step 4: Check if it's an environment issue
  └── Did any package versions change? Was a secret rotated?

Step 5: Check infrastructure
  └── Is the worker healthy? Is the Metadata DB responding?
  └── Check worker logs: docker logs / kubectl logs

Step 6: Check for schema changes
  └── Did the source table add/remove columns?

Step 7: Reproduce locally
  └── airflow tasks test my_dag failed_task 2024-01-15

Step 8: Fix → redeploy → clear the failed task → let it retry
  └── airflow tasks clear --task-regex 'failed_task' my_dag -s 2024-01-15 -e 2024-01-15
```

---

## 16. Quick Reference Cheat Sheet

### Common Cron Expressions

| Expression | Meaning |
|---|---|
| `@daily` / `0 0 * * *` | Every day at midnight |
| `@hourly` / `0 * * * *` | Every hour |
| `0 6 * * MON-FRI` | Weekdays at 6 AM |
| `0 */4 * * *` | Every 4 hours |
| `0 8 1 * *` | 1st of every month at 8 AM |
| `@once` | Run one time only |
| `None` | Manual trigger only |

### Jinja Macros

| Macro | Value |
|---|---|
| `{{ ds }}` | `2024-01-15` (execution date) |
| `{{ ds_nodash }}` | `20240115` |
| `{{ ts }}` | `2024-01-15T00:00:00+00:00` |
| `{{ next_ds }}` | Next execution date |
| `{{ prev_ds }}` | Previous execution date |
| `{{ dag.dag_id }}` | DAG ID string |
| `{{ task.task_id }}` | Task ID string |
| `{{ dag_run.conf }}` | Runtime config dict |
| `{{ var.value.KEY }}` | Airflow Variable value |

### Task State Quick Reference

| State | Meaning | Action |
|---|---|---|
| `queued` | Waiting for worker | Check executor/worker health |
| `running` | Executing | Normal — wait |
| `success` | Done ✅ | — |
| `failed` | Exception occurred | Check logs |
| `up_for_retry` | Waiting to retry | Wait for retry_delay |
| `skipped` | Branching bypassed it | Normal — check branch logic |
| `upstream_failed` | Parent failed | Fix parent task |
| `zombie` | Process died | Auto-detected, marked failed |
| `deferred` | Waiting in Triggerer | Normal for deferrable operators |

### Executor Selection Matrix

| Requirement | Recommended Executor |
|---|---|
| Local development | SequentialExecutor |
| Small production (1 machine) | LocalExecutor |
| Standard production (multi-node) | CeleryExecutor |
| Cloud-native / K8s | KubernetesExecutor |
| Mixed requirements | CeleryKubernetesExecutor |

### DAG Checklist for Production

```
☐ catchup=False (unless explicitly needed)
☐ Explicit start_date (no relative dates)
☐ retries and retry_delay configured
☐ No top-level I/O or connections in DAG file
☐ All secrets via Connections/Variables/Secret Backend
☐ Sensors using mode='reschedule'
☐ Remote logging configured (S3/GCS)
☐ on_failure_callback for alerting
☐ Tasks are idempotent
☐ DAG passes unit tests in CI/CD
☐ Tags added for easy filtering
☐ max_active_runs set to prevent overlap
```

---

*End of Apache Airflow Interview & Study Guide*
*Topics: Architecture · DAG Authoring · Operators · Scheduling · Executors · XCom · Advanced Features · Production Practices · 60 Interview Questions · 15 Real Scenarios*
