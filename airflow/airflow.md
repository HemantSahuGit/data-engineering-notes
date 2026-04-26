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
| 17 | [Custom Operators & Hooks](#17-custom-operators--hooks) | BaseOperator, template_fields, custom sensors, hooks |
| 18 | [Airflow Plugins](#18-airflow-plugins) | Plugin registration, listener plugins |
| 19 | [RBAC & Security](#19-rbac--security) | Roles, DAG-level access, security best practices |
| 20 | [Configuration Deep Dive](#20-airflow-configuration-deep-dive) | airflow.cfg, concurrency hierarchy, tuning checklist |
| 21 | [Managed Airflow Services](#21-managed-airflow-services) | MWAA, Cloud Composer, Astronomer |
| 22 | [Airflow 1.x → 2.x Migration](#22-airflow-1x--2x-migration) | Key differences, migration checklist |
| 23 | [Setup & Teardown Tasks](#23-setup--teardown-tasks-airflow-27) | Infrastructure lifecycle, guaranteed cleanup |
| 24 | [Custom Timetables](#24-custom-timetables-airflow-22) | Business day, fiscal calendar scheduling |
| 25 | [Priority Weights & Queues](#25-priority-weights--task-queues) | weight_rule, Celery queue routing |
| 26 | [Monitoring & Observability](#26-monitoring--observability) | StatsD, Prometheus, OpenLineage |
| 27 | [Integration: dbt, Spark, Databricks](#27-integration-guide-dbt-spark-databricks) | Integration patterns, deferrable usage |
| 28 | [Cluster Policies](#28-cluster-policies) | Enforce organizational standards |
| 29 | [DAG Lifecycle & Scheduler Internals](#29-dag-lifecycle--scheduler-internals) | Scheduler loop, LocalTaskJob, state machine |
| 30 | [Interview Q&A — Expert Level](#30-interview-qa--expert-level) | Q61–Q90, advanced concepts |
| 31 | [Additional Scenarios (16-25)](#31-additional-scenario-based-questions) | ML pipelines, multi-tenant, cost optimization |
| 32 | [Updated Quick Reference](#32-updated-quick-reference-cheat-sheet) | Imports, config reference, interview checklist |

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

## 17. Custom Operators & Hooks

### 17.1 Why Create a Custom Operator?

Built-in operators cover common cases, but real-world pipelines often need custom logic. Custom operators:
- Encapsulate reusable business logic
- Are testable, maintainable, and shareable across teams
- Can be published as Python packages or added to `plugins/`

### 17.2 Anatomy of a Custom Operator

```python
# plugins/operators/my_api_operator.py
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class MyAPIOperator(BaseOperator):
    """
    Calls a custom internal API and returns the JSON response.
    
    :param endpoint: The API endpoint path (e.g., '/data/export')
    :param method: HTTP method (default: GET)
    :param conn_id: Airflow Connection ID for the base URL + auth
    """

    # 1. Declare templateable fields (Jinja will render these)
    template_fields = ('endpoint', 'method')

    # 2. Optional: customize UI appearance
    ui_color = '#4CAF50'
    ui_fgcolor = '#FFFFFF'

    # 3. Constructor — initialize parameters ONLY (no heavy I/O here!)
    def __init__(
        self,
        endpoint: str,
        method: str = 'GET',
        conn_id: str = 'my_api_default',
        **kwargs
    ):
        super().__init__(**kwargs)   # ← ALWAYS call super().__init__
        self.endpoint = endpoint
        self.method = method
        self.conn_id = conn_id

    # 4. execute() — this is what runs when the task executes
    def execute(self, context):
        from airflow.providers.http.hooks.http import HttpHook

        hook = HttpHook(method=self.method, http_conn_id=self.conn_id)
        self.log.info(f"Calling API: {self.endpoint}")
        response = hook.run(self.endpoint)
        response.raise_for_status()
        result = response.json()
        self.log.info(f"API returned: {result}")
        return result  # auto-pushed to XCom as 'return_value'
```

```python
# Using the custom operator in a DAG
from operators.my_api_operator import MyAPIOperator

with DAG('api_pipeline', ...) as dag:
    fetch_data = MyAPIOperator(
        task_id='fetch_sales_data',
        endpoint='/api/sales/{{ ds }}',   # Jinja works because template_fields
        conn_id='sales_api'
    )
```

**Critical Rules for Custom Operators:**

```
✅ DO:
  - Only put configuration in __init__
  - All heavy logic goes in execute()
  - Use self.log (not print) for logging
  - Call super().__init__(**kwargs) first
  - Declare template_fields for Jinja support
  - Use Hooks for external connections

❌ DON'T:
  - Open DB connections in __init__ (runs every parse cycle!)
  - Make API calls in __init__
  - Import heavy libraries at top-level of DAG file
  - Hardcode credentials inside the operator
```

**Operator Lifecycle Diagram:**
```
┌─────────────────────────────────────────────────────────────┐
│                  OPERATOR LIFECYCLE                         │
│                                                             │
│  [Scheduler parses DAG] ──► __init__() called               │
│    (every few seconds)       (lightweight config only!)     │
│                                                             │
│  [Task ready to run]  ──► execute(context) called           │
│    (once per task run)       (heavy logic here)             │
│                                                             │
│  execute() returns value ──► auto XCom push                 │
│  execute() raises exception ──► task fails, retry if set    │
└─────────────────────────────────────────────────────────────┘
```

### 17.3 Custom Sensor

```python
# plugins/sensors/my_database_sensor.py
from airflow.sensors.base import BaseSensorOperator

class DatabaseRowSensor(BaseSensorOperator):
    """
    Waits until a specific row appears in a database table.
    """
    template_fields = ('table', 'filter_date')

    def __init__(self, table: str, filter_date: str, conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.table = table
        self.filter_date = filter_date
        self.conn_id = conn_id

    def poke(self, context) -> bool:
        """Returns True when condition is met, False to keep polling."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        sql = f"SELECT COUNT(*) FROM {self.table} WHERE load_date = '{self.filter_date}'"
        count = hook.get_first(sql)[0]
        self.log.info(f"Row count for {self.filter_date}: {count}")
        return count > 0   # returns True → task succeeds; False → polls again
```

### 17.4 Custom Hook

```python
# plugins/hooks/my_crm_hook.py
from airflow.hooks.base import BaseHook
import requests

class MyCRMHook(BaseHook):
    """
    Hook to interact with the internal CRM REST API.
    Reads base_url and api_key from an Airflow Connection.
    """
    conn_name_attr = 'crm_conn_id'
    default_conn_name = 'crm_default'
    conn_type = 'http'
    hook_name = 'My CRM API'

    def __init__(self, crm_conn_id: str = 'crm_default'):
        super().__init__()
        self.crm_conn_id = crm_conn_id

    def get_conn(self):
        conn = self.get_connection(self.crm_conn_id)
        self.base_url = f"https://{conn.host}"
        self.api_key = conn.password
        return requests.Session()

    def get_customers(self, page: int = 1) -> list:
        session = self.get_conn()
        r = session.get(
            f"{self.base_url}/api/customers",
            headers={'Authorization': f'Bearer {self.api_key}'},
            params={'page': page}
        )
        r.raise_for_status()
        return r.json()['customers']
```

---

## 18. Airflow Plugins

### 18.1 What is an Airflow Plugin?

A Plugin lets you extend Airflow's functionality by hooking into the plugin manager. You can add:
- Custom operators, sensors, hooks
- Custom views (Flask Blueprints in the Web UI)
- Custom macros
- Listener callbacks (observe task/DAG lifecycle events)

### 18.2 Plugin Directory Structure

```
airflow_home/
├── dags/
│   └── my_dag.py
├── plugins/               ← Plugin root (auto-loaded by Airflow)
│   ├── __init__.py
│   ├── operators/
│   │   └── my_api_operator.py
│   ├── sensors/
│   │   └── my_db_sensor.py
│   ├── hooks/
│   │   └── my_crm_hook.py
│   └── my_plugin.py       ← Plugin registration file
└── config/
    └── airflow_local_settings.py
```

### 18.3 Plugin Registration

```python
# plugins/my_plugin.py
from airflow.plugins_manager import AirflowPlugin
from operators.my_api_operator import MyAPIOperator
from hooks.my_crm_hook import MyCRMHook

class MyCompanyPlugin(AirflowPlugin):
    name = "my_company_plugin"
    operators = [MyAPIOperator]
    hooks = [MyCRMHook]
    macros = []          # custom Jinja macros
    flask_blueprints = []   # custom UI pages
```

### 18.4 Listener Plugin (Observability)

```python
# plugins/audit_listener.py
from airflow.listeners import hookimpl

class AuditListener:
    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance, session):
        log_audit(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            state='success',
            duration=task_instance.duration
        )

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, error, session):
        log_audit(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            state='failed',
            error=str(error)
        )

# Register in plugin
from airflow.plugins_manager import AirflowPlugin

class AuditPlugin(AirflowPlugin):
    name = "audit_plugin"
    listeners = [AuditListener()]
```

---

## 19. RBAC & Security

### 19.1 Airflow Roles

Airflow uses Flask-AppBuilder for RBAC (Role-Based Access Control). Built-in roles:

```
┌─────────────────────────────────────────────────────────────┐
│                    AIRFLOW RBAC ROLES                       │
│                                                             │
│  Admin                                                      │
│  ─────                                                      │
│  Full access: manage users, roles, connections, variables,  │
│  DAGs, task runs, configurations                            │
│                                                             │
│  Op (Operator)                                              │
│  ──────────────                                             │
│  Can: view/trigger/clear DAGs, manage pools/connections     │
│  Cannot: manage users, system config                        │
│                                                             │
│  User                                                       │
│  ────                                                       │
│  Can: view DAGs, trigger/clear DAGs                         │
│  Cannot: manage connections, variables, pools               │
│                                                             │
│  Viewer                                                     │
│  ──────                                                     │
│  Read-only: view DAGs, runs, logs                           │
│  Cannot: trigger or modify anything                         │
│                                                             │
│  Public (no auth)                                           │
│  ─────────────────                                          │
│  Access only to public pages (if auth_backend allows)       │
└─────────────────────────────────────────────────────────────┘
```

### 19.2 DAG-Level Access Control

```python
# Restrict which roles can access a DAG
with DAG(
    'sensitive_finance_dag',
    access_control={
        'Finance_Team': {'can_read', 'can_edit', 'can_delete'},
        'Auditors': {'can_read'},
    }
) as dag:
    ...
```

### 19.3 Security Best Practices

```
1. Authentication:
   ├── Use OAuth2 / SSO (Azure AD, Okta, Google)
   ├── Enable LDAP authentication
   └── Never use the default 'admin/admin' credentials in production

2. Secret Management:
   ├── Never store secrets in DAG code or Variables directly
   ├── Use Secrets Backends: AWS Secrets Manager, HashiCorp Vault
   └── Rotate credentials regularly

3. Network:
   ├── Webserver behind a reverse proxy (NGINX) with HTTPS only
   ├── Metadata DB not exposed to the public internet
   └── Workers in private subnets

4. API Security:
   ├── Enable API authentication (basic_auth or Kerberos)
   └── Use HTTPS for all API calls

5. Audit:
   ├── Enable Airflow audit logs
   └── Use Listener plugins to log all task events
```

### 19.4 Webserver Authentication Configuration

```ini
# airflow.cfg
[webserver]
authenticate = True
auth_backend = airflow.providers.google.common.auth_backend.google_openid
                # or airflow.contrib.auth.backends.ldap_auth
                # or airflow.api.auth.backend.basic_auth

# For OAuth2
[oauth]
providers = [{'name': 'google', 'icon': 'fa-google', 
              'token_key': 'access_token',
              'remote_app': {...}}]
```

---

## 20. Airflow Configuration Deep Dive

### 20.1 Key airflow.cfg Sections

```ini
[core]
# Total parallelism across all DAGs and tasks
parallelism = 32

# Max task instances per DAG at a time
max_active_tasks_per_dag = 16

# Max concurrent DAG runs across all DAGs
max_active_runs_per_dag = 16

# Executor class
executor = CeleryExecutor

# DAG folder location
dags_folder = /opt/airflow/dags

# Metadata DB connection string
sql_alchemy_conn = postgresql+psycopg2://user:pass@host/airflow_db

# Load example DAGs (disable in production!)
load_examples = False

[scheduler]
# How often to parse new DAG files (seconds)
min_file_process_interval = 30

# How often to scan the DAGs folder (seconds)
dag_dir_list_interval = 300

# Zombie task detection threshold (seconds)
scheduler_zombie_task_threshold = 300

# HA: how many task instances per scheduling loop
max_dagruns_per_loop_to_schedule = 20

# How long scheduler sleeps between loops when idle
scheduler_idle_sleep_time = 1

[celery]
# Redis or RabbitMQ broker
broker_url = redis://redis:6379/0
result_backend = db+postgresql://user:pass@host/airflow_db

# Tasks each Celery worker can run simultaneously
worker_concurrency = 16

[logging]
remote_logging = True
remote_base_log_folder = s3://my-bucket/airflow-logs
remote_log_conn_id = aws_default

[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {"connections_prefix": "airflow/connections", 
                  "variables_prefix": "airflow/variables",
                  "sep": "/"}
```

### 20.2 Concurrency Settings Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│              CONCURRENCY CONTROLS (outer → inner)               │
│                                                                 │
│  parallelism (airflow.cfg)                                      │
│  ─────────────────────────                                      │
│  Global max tasks running across ALL dags at once               │
│  Default: 32                                                    │
│                                                                 │
│    ↳ max_active_runs_per_dag (airflow.cfg / per DAG)            │
│      Max simultaneous DAG runs for ONE dag                      │
│      Default: 16                                                │
│                                                                 │
│        ↳ max_active_tasks_per_dag (airflow.cfg / per DAG)       │
│          Max tasks running inside ONE DAG run                   │
│          Default: 16                                            │
│                                                                 │
│              ↳ Pool slots                                       │
│                Task-specific resource limits                    │
│                User-configured                                  │
│                                                                 │
│                  ↳ worker_concurrency (Celery)                  │
│                    Max tasks per individual worker process      │
│                    Default: 16                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 20.3 Performance Tuning Checklist

```
Database:
  ☐ Use PostgreSQL 12+ (not MySQL/SQLite for production)
  ☐ Add PgBouncer connection pooler for PostgreSQL
  ☐ Run airflow db clean periodically to purge old records
  ☐ Add database indexes on dag_id, execution_date columns

Scheduler:
  ☐ Run 2-3 Scheduler instances (HA mode)
  ☐ Tune min_file_process_interval (higher = less CPU on parsing)
  ☐ Increase max_dagruns_per_loop_to_schedule for many small DAGs
  ☐ Use DAG serialization (enabled by default in 2.x)
  ☐ Move complex Python out of DAG files into importable modules

Workers:
  ☐ Tune worker_concurrency based on CPU/memory available
  ☐ Use Pools to prevent resource oversubscription
  ☐ Enable autoscaling (KEDA for K8s, ASG for EC2 Celery workers)

DAG Design:
  ☐ No top-level DB connections or API calls in DAG files
  ☐ Use reschedule mode for all sensors
  ☐ Use deferrable operators for long-running external jobs
  ☐ Limit XCom payload size (< 48KB)
```

---

## 21. Managed Airflow Services

### 21.1 Comparison: MWAA vs Cloud Composer vs Astronomer

```
┌───────────────────────────────────────────────────────────────────────────┐
│              MANAGED AIRFLOW SERVICES COMPARISON                          │
├─────────────────┬──────────────┬──────────────────┬──────────────────────┤
│ Feature         │ AWS MWAA     │ Cloud Composer   │ Astronomer           │
│                 │ (Amazon)     │ (GCP)            │ (SaaS/Hybrid)        │
├─────────────────┼──────────────┼──────────────────┼──────────────────────┤
│ Airflow version │ Lags behind  │ Lags behind      │ Latest stable        │
│ Deployment      │ VPC-native   │ GKE-native       │ Kubernetes (any)     │
│ DAG deploy      │ S3 sync      │ GCS sync         │ CI/CD push           │
│ Custom plugins  │ S3 zip       │ GCS zip          │ Docker image         │
│ Scaling         │ Auto (limits)│ Auto (GKE)       │ KEDA autoscale       │
│ Observability   │ CloudWatch   │ Cloud Logging    │ Astronomer UI        │
│ Cost model      │ Per environment│ Per vCPU hour  │ Per worker/month     │
│ Multi-tenancy   │ Separate env │ Separate env     │ Workspaces + RBAC    │
│ Best for        │ AWS shops    │ GCP shops        │ Multi-cloud/serious  │
└─────────────────┴──────────────┴──────────────────┴──────────────────────┘
```

### 21.2 AWS MWAA (Managed Workflows for Apache Airflow)

```
Architecture:
  ┌─────────────────────────────────────────┐
  │                  VPC                    │
  │  ┌──────────┐  ┌──────────┐            │
  │  │ Scheduler│  │Webserver │            │
  │  └────┬─────┘  └────┬─────┘            │
  │       │              │                 │
  │  ┌────▼──────────────▼─────┐           │
  │  │      RDS (Aurora)       │           │
  │  │      Metadata DB        │           │
  │  └─────────────────────────┘           │
  │                                        │
  │  ┌─────────────────────────┐           │
  │  │   Fargate Workers       │           │
  │  │   (auto-scaling)        │           │
  │  └─────────────────────────┘           │
  └─────────────────────────────────────────┘

DAG Deployment:
  git push ──► S3 bucket ──► MWAA auto-syncs every ~1 min

Limitations:
  - No SSH into workers/scheduler
  - Limited Airflow version options
  - No custom executor configuration
```

### 21.3 GCP Cloud Composer

```
Architecture:
  - Built on GKE (Google Kubernetes Engine)
  - Scheduler + Webserver run as GKE pods
  - Workers scale via GKE autoscaling
  - Uses Cloud SQL (PostgreSQL) for Metadata DB
  - Logs to Cloud Logging

DAG Deployment:
  git push ──► Cloud Build ──► GCS bucket ──► Composer auto-syncs

Composer 2 (latest):
  - Uses KubernetesPodOperator for full task isolation
  - Environment-level autoscaling (scale workers to 0)
  - Per-task resource allocation
```

### 21.4 Astronomer

```
Key Features:
  - Astro CLI for local development (docker-compose based)
  - Deployments on Astronomer SaaS or private GKE/EKS/AKS
  - KEDA-based autoscaling (workers scale to 0)
  - Built-in Lineage (OpenLineage integration)
  - Deployment API for CI/CD integration
  - Astronomer-certified operators with support

astro CLI:
  $ astro dev init          # create project
  $ astro dev start         # run locally
  $ astro deploy            # push to Astronomer cloud
  $ astro deployment list   # manage deployments
```

---

## 22. Airflow 1.x → 2.x Migration

### 22.1 Key Differences

```
┌───────────────────────────────────────────────────────────┐
│            AIRFLOW 1.x vs 2.x KEY DIFFERENCES             │
├─────────────────────────┬─────────────────────────────────┤
│ Feature                 │ 1.x            │ 2.x            │
├─────────────────────────┼────────────────┼────────────────┤
│ TaskFlow API            │ No             │ @dag/@task ✅   │
│ Scheduler HA            │ No             │ Multi-scheduler │
│ DAG Serialization       │ Optional       │ Default ✅      │
│ REST API                │ Experimental   │ Stable ✅       │
│ execution_date          │ Primary name   │ logical_date   │
│ SubDAGs                 │ Common pattern │ Deprecated ❌   │
│ TaskGroups              │ No             │ ✅              │
│ Triggerer               │ No             │ ✅ (2.2+)       │
│ Dynamic Task Mapping    │ No             │ ✅ (2.3+)       │
│ Datasets                │ No             │ ✅ (2.4+)       │
│ Setup/Teardown          │ No             │ ✅ (2.7+)       │
│ Timetables              │ No             │ ✅ (2.2+)       │
│ provide_context         │ Required       │ Deprecated      │
│ import path changes     │ airflow.contrib │ airflow.providers│
└─────────────────────────┴────────────────┴────────────────┘
```

### 22.2 Common Migration Steps

```python
# ❌ Airflow 1.x style
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

t = PythonOperator(
    task_id='my_task',
    python_callable=my_fn,
    provide_context=True    # deprecated in 2.x
)

# ✅ Airflow 2.x style
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# provide_context no longer needed; context injected automatically
t = PythonOperator(
    task_id='my_task',
    python_callable=my_fn
)

# Access context with:
def my_fn(**context):
    ds = context['ds']
    ti = context['ti']
```

### 22.3 Migration Checklist

```
Pre-migration:
  ☐ Run: airflow upgrade_check (Airflow upgrade check plugin)
  ☐ Update all provider packages (airflow-providers-*)
  ☐ Update imports from airflow.contrib.* to airflow.providers.*
  ☐ Remove provide_context=True from all PythonOperators
  ☐ Replace SubDAGs with TaskGroups
  ☐ Update execution_date references to logical_date
  ☐ Re-test all DAGs in staging environment
  ☐ Upgrade Metadata DB schema: airflow db upgrade
```

---

## 23. Setup & Teardown Tasks (Airflow 2.7+)

### 23.1 What is Setup/Teardown?

Setup and Teardown tasks let you define infrastructure provisioning and cleanup tasks that:
- **Setup** runs before work tasks
- **Teardown** runs after work tasks — EVEN if work tasks fail
- Teardown tasks do not affect the DAG's final state

```python
from airflow.decorators import dag, task, setup, teardown

@dag(schedule='@daily', start_date=datetime(2024, 1, 1))
def etl_with_infrastructure():

    @setup
    def create_cluster():
        """Provision EMR/Spark cluster"""
        cluster_id = provision_emr_cluster()
        return cluster_id

    @task
    def run_job(cluster_id: str):
        """Run the actual ETL"""
        submit_spark_job(cluster_id)

    @teardown
    def destroy_cluster(cluster_id: str):
        """Always clean up, even if run_job fails"""
        terminate_emr_cluster(cluster_id)

    cluster = create_cluster()
    job = run_job(cluster)
    destroy_cluster(cluster) >> job   # teardown linked to setup
```

**Flow Diagram:**
```
[create_cluster (setup)] ──► [run_job] ──► [destroy_cluster (teardown)]
         │                       │                   ▲
         │                       └── fails ───────────┘
         │                                  teardown STILL runs
         └────────────────────────────────────────────┘
              setup → work → teardown (guaranteed cleanup)
```

**Why it matters:**
```
Without setup/teardown:
  - Developers use on_failure_callback hacks for cleanup
  - Infrastructure leaks when tasks fail (expensive idle clusters!)

With setup/teardown:
  - Teardown always runs regardless of work task outcome
  - Cluster lifecycle is explicitly modeled in the DAG
  - No orphaned cloud resources
```

---

## 24. Custom Timetables (Airflow 2.2+)

### 24.1 What is a Timetable?

A Timetable is a class that defines WHEN a DAG runs. It replaces `schedule_interval` for complex scheduling needs:

- Business days only
- Quarterly runs
- Fiscal calendar
- Run on 15th and last day of each month

### 24.2 Standard Timetables

```python
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
from airflow.timetables.trigger import CronTriggerTimetable

# Standard interval (covers a time window)
schedule = CronDataIntervalTimetable('0 6 * * MON-FRI', timezone='US/Eastern')

# Trigger-based (no data interval, just triggers at this time)
schedule = CronTriggerTimetable('0 9 * * *', timezone='UTC')

with DAG('business_daily', schedule=schedule, ...):
    ...
```

### 24.3 Custom Timetable Example

```python
# plugins/timetables/business_day_timetable.py
from datetime import timedelta
from typing import Optional
from pendulum import DateTime, instance
import pendulum

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class BusinessDayTimetable(Timetable):
    """
    Runs the DAG every business day (Mon-Fri) at 7 AM UTC.
    Skips weekends automatically.
    """

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        delta = timedelta(days=1)
        start = run_after - delta
        return DataInterval(start=start, end=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is None:
            next_start = restriction.earliest
        else:
            next_start = last_automated_data_interval.end

        # Skip weekends
        while next_start.day_of_week in (5, 6):  # 5=Sat, 6=Sun
            next_start = next_start.add(days=1)

        run_at = next_start.set(hour=7, minute=0, second=0)
        return DagRunInfo.interval(
            start=run_at - timedelta(days=1),
            end=run_at,
        )


class BusinessDayPlugin(AirflowPlugin):
    name = "business_day_plugin"
    timetables = [BusinessDayTimetable]
```

```python
# Using the custom timetable in a DAG
from timetables.business_day_timetable import BusinessDayTimetable

with DAG(
    'business_pipeline',
    timetable=BusinessDayTimetable(),
    start_date=datetime(2024, 1, 1)
) as dag:
    ...
```

---

## 25. Priority Weights & Task Queues

### 25.1 Priority Weight

When many tasks are queued and executor slots are limited, Airflow uses `priority_weight` to determine which task runs first.

```python
# Higher number = higher priority
urgent_task = PythonOperator(
    task_id='critical_sla_task',
    python_callable=critical_fn,
    priority_weight=100,     # runs before tasks with lower weights
    weight_rule='absolute'
)

normal_task = PythonOperator(
    task_id='routine_task',
    python_callable=routine_fn,
    priority_weight=1        # default
)
```

**Weight Rules:**

| Rule | Description |
|---|---|
| `downstream` | Weight = task's own weight + sum of all downstream task weights (default) |
| `upstream` | Weight = task's own weight + sum of all upstream task weights |
| `absolute` | Weight = exactly the task's `priority_weight` value |

```
Example with downstream (default):
  [A(1)] ──► [B(2)] ──► [C(3)] ──► [D(4)]

  Effective weights:
  D = 4
  C = 3 + 4 = 7
  B = 2 + 7 = 9
  A = 1 + 9 = 10   ← A has highest effective priority

  This means upstream tasks that enable more downstream work get higher priority.
```

### 25.2 Task Queues (Celery)

Queues route tasks to specific worker pools — useful for tasks requiring special hardware or isolation.

```python
# Assign tasks to named queues
gpu_task = PythonOperator(
    task_id='train_model',
    python_callable=train,
    queue='gpu_workers'     # only workers listening to 'gpu_workers' queue run this
)

cpu_task = PythonOperator(
    task_id='preprocess',
    python_callable=preprocess,
    queue='default'         # default workers
)
```

```bash
# Start a worker listening to a specific queue
airflow celery worker --queues gpu_workers

# Start a worker for multiple queues
airflow celery worker --queues default,gpu_workers
```

**Queue Architecture:**
```
Scheduler ──► Redis Broker ──┬──► Queue: default ──► Worker A, Worker B
                              │
                              └──► Queue: gpu_workers ──► GPU Worker 1
                              │
                              └──► Queue: high_memory ──► High-RAM Worker
```

---

## 26. Monitoring & Observability

### 26.1 StatsD Metrics

Airflow emits metrics via StatsD that can be forwarded to Prometheus, Grafana, Datadog.

```ini
# airflow.cfg
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
```

**Key metrics to monitor:**

| Metric | What It Tells You |
|---|---|
| `airflow.scheduler.heartbeat` | Scheduler is alive |
| `airflow.dag.loading-duration.*` | DAG parse time (high = slow DAG files) |
| `airflow.ti.start.*` | Task instance start rate |
| `airflow.ti.finish.*.success` | Task success rate |
| `airflow.ti.finish.*.failed` | Task failure rate |
| `airflow.executor.open_slots` | Available executor slots |
| `airflow.executor.queued_tasks` | Backlog size |
| `airflow.pool.open_slots.*` | Pool availability |
| `airflow.pool.used_slots.*` | Pool utilization |

### 26.2 Monitoring Architecture

```
┌──────────────────────────────────────────────────────────────┐
│               AIRFLOW MONITORING STACK                       │
│                                                              │
│  Airflow ──► StatsD ──► Prometheus ──► Grafana dashboards   │
│                              │                               │
│                              └──► Alertmanager ──► PagerDuty │
│                                                              │
│  Airflow logs ──► Fluentd/Fluentbit ──► Elasticsearch       │
│                                              │               │
│                                         Kibana dashboard     │
│                                                              │
│  Airflow events ──► Listener Plugin ──► Audit DB            │
│                                       ──► Slack/Teams alert │
└──────────────────────────────────────────────────────────────┘
```

### 26.3 OpenLineage Integration

OpenLineage provides data lineage tracking across Airflow tasks.

```python
# Install: pip install openlineage-airflow

# airflow.cfg or environment variable:
# OPENLINEAGE_URL=http://marquez:5000
# OPENLINEAGE_NAMESPACE=production

# Operators like PostgresOperator, SparkSubmitOperator, BigQueryOperator
# automatically emit lineage events when OpenLineage is configured.
# No code changes needed for supported operators!
```

```
Lineage Flow:
  [Source Table] ──► [Transform Task] ──► [Target Table]
                          │
                   emits lineage events
                          │
                    [Marquez / OpenMetadata]
                          │
                    Lineage graph & impact analysis
```

### 26.4 Prometheus Exporter

```yaml
# docker-compose.yml snippet
prometheus:
  image: prom/prometheus
  volumes:
    - ./prometheus.yml:/etc/prometheus/prometheus.yml

# prometheus.yml
scrape_configs:
  - job_name: 'airflow-statsd'
    static_configs:
      - targets: ['statsd-exporter:9102']
```

---

## 27. Integration Guide: dbt, Spark, Databricks

### 27.1 Airflow + dbt

dbt (data build tool) handles SQL transformations; Airflow orchestrates when they run.

```python
from airflow.operators.bash import BashOperator

# Option 1: BashOperator (simple)
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/dbt && dbt run --profiles-dir /opt/dbt --target prod',
    env={'DBT_PROFILES_DIR': '/opt/dbt'}
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/dbt && dbt test --profiles-dir /opt/dbt',
)

dbt_run >> dbt_test
```

```python
# Option 2: Cosmos (Astronomer's dbt-airflow integration)
# pip install astronomer-cosmos

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="my_project",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default"
    ),
)

with DAG('dbt_pipeline', ...):
    transform = DbtTaskGroup(
        group_id="transform",
        project_config=ProjectConfig("/opt/dbt/my_project"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt"),
    )
```

**Airflow + dbt Flow:**
```
[Extract (Airbyte/Fivetran)] ──► [dbt run (transform)] ──► [dbt test (quality)] ──► [Report]
         (Airflow triggers)          (Airflow runs dbt)        (Airflow asserts)
```

### 27.2 Airflow + Apache Spark

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_job = SparkSubmitOperator(
    task_id='process_large_dataset',
    application='/opt/spark/jobs/etl_job.py',
    conn_id='spark_default',
    conf={
        'spark.driver.memory': '4g',
        'spark.executor.instances': '10',
        'spark.executor.memory': '8g',
        'spark.sql.shuffle.partitions': '200'
    },
    py_files='/opt/spark/dependencies.zip',
    application_args=['--date', '{{ ds }}'],
)
```

### 27.3 Airflow + Databricks

```python
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator
)

# Option 1: Trigger an existing Databricks Job
run_existing_job = DatabricksRunNowOperator(
    task_id='run_databricks_job',
    databricks_conn_id='databricks_default',
    job_id=12345,
    notebook_params={'date': '{{ ds }}', 'env': 'prod'},
    deferrable=True    # ← frees worker slot during 3-hour job!
)

# Option 2: Submit an ad-hoc notebook run
run_notebook = DatabricksSubmitRunOperator(
    task_id='run_notebook',
    databricks_conn_id='databricks_default',
    new_cluster={
        'spark_version': '13.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 5
    },
    notebook_task={
        'notebook_path': '/Shared/ETL/transform_data',
        'base_parameters': {'date': '{{ ds }}'}
    },
    deferrable=True
)
```

---

## 28. Cluster Policies

### 28.1 What are Cluster Policies?

Cluster Policies allow platform administrators to enforce standards across ALL DAGs without requiring DAG authors to manually configure them. They run when Airflow loads DAGs.

```python
# config/airflow_local_settings.py  (or plugin)

def dag_policy(dag):
    """Enforce organizational standards on every DAG."""
    # 1. Require all DAGs to have a tag
    if not dag.tags:
        raise AirflowClusterPolicyViolation(
            f"DAG {dag.dag_id} must have at least one tag."
        )

    # 2. Auto-set owner if missing
    if dag.default_args.get('owner') == 'airflow':
        dag.default_args['owner'] = 'platform-team'

    # 3. Limit max_active_runs for all DAGs
    if dag.max_active_runs > 5:
        raise AirflowClusterPolicyViolation(
            f"DAG {dag.dag_id} max_active_runs ({dag.max_active_runs}) exceeds limit of 5."
        )


def task_policy(task):
    """Enforce standards on every task in every DAG."""
    # 1. Require retries on all tasks
    if task.retries is None or task.retries < 1:
        task.retries = 2
        task.retry_delay = timedelta(minutes=5)

    # 2. Require all tasks to have an SLA
    # if not task.sla:
    #     raise AirflowClusterPolicyViolation("All tasks must have an SLA.")

    # 3. Force all sensors to use reschedule mode
    from airflow.sensors.base import BaseSensorOperator
    if isinstance(task, BaseSensorOperator) and task.mode == 'poke':
        task.mode = 'reschedule'
```

**When to use Cluster Policies:**
```
✅ Great for:
  - Enforcing retry standards across 100+ DAGs
  - Auto-adding tags, owner, SLAs
  - Preventing resource abuse (too many active runs)
  - Centralizing platform governance

❌ Not for:
  - Task-specific business logic
  - Anything that changes frequently
```

---

## 29. DAG Lifecycle & Scheduler Internals

### 29.1 The Scheduler Loop (Simplified)

```
┌──────────────────────────────────────────────────────────────┐
│                   SCHEDULER LOOP                             │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Step 1: Parse DAG files                             │   │
│  │  DagFileProcessorManager reads *.py from dags/       │   │
│  │  Serializes to JSON → stores in Metadata DB          │   │
│  └────────────────────┬─────────────────────────────────┘   │
│                       │                                      │
│  ┌────────────────────▼─────────────────────────────────┐   │
│  │  Step 2: Create DagRuns                              │   │
│  │  Check: is a new DagRun needed? (schedule met?)      │   │
│  │  If yes → create DagRun with state = RUNNING         │   │
│  └────────────────────┬─────────────────────────────────┘   │
│                       │                                      │
│  ┌────────────────────▼─────────────────────────────────┐   │
│  │  Step 3: Evaluate TaskInstances                      │   │
│  │  For each active DagRun:                             │   │
│  │    - Check task dependencies (all upstream done?)    │   │
│  │    - Check trigger rules                             │   │
│  │    - Check pool slots available                      │   │
│  │    - Check priority weights                          │   │
│  │  Mark qualifying tasks as SCHEDULED                  │   │
│  └────────────────────┬─────────────────────────────────┘   │
│                       │                                      │
│  ┌────────────────────▼─────────────────────────────────┐   │
│  │  Step 4: Submit to Executor                          │   │
│  │  Send SCHEDULED tasks to the Executor queue          │   │
│  │  Executor sends to Worker (Celery/K8s/Local)         │   │
│  │  Task state: SCHEDULED → QUEUED → RUNNING            │   │
│  └────────────────────┬─────────────────────────────────┘   │
│                       │                                      │
│  ┌────────────────────▼─────────────────────────────────┐   │
│  │  Step 5: Detect Zombies                              │   │
│  │  Check heartbeats of running tasks                   │   │
│  │  Tasks with stale heartbeats → marked FAILED         │   │
│  └────────────────────┬─────────────────────────────────┘   │
│                       │                                      │
│                  ← Loop repeats every 1-5 seconds →          │
└──────────────────────────────────────────────────────────────┘
```

### 29.2 LocalTaskJob

When Airflow runs a task, it creates a **LocalTaskJob** — a wrapper process on the worker that:
1. Starts the actual task subprocess
2. Sends periodic heartbeats to the Metadata DB (every ~5 seconds)
3. Monitors the subprocess for unexpected deaths
4. Updates the task state to SUCCESS or FAILED in the DB

```
Worker receives task ──► LocalTaskJob starts
                              │
                              ├── Starts task subprocess
                              ├── Sends heartbeat every 5s to DB
                              └── Waits for subprocess to complete
                                      │
                              task exits with 0 ──► SUCCESS in DB
                              task exits with non-0 ──► FAILED in DB
                              LocalTaskJob crashes ──► Zombie detected by Scheduler
```

### 29.3 Task State Machine (Complete)

```
                    ┌─────────────┐
                    │    none     │
                    │ (not sched) │
                    └──────┬──────┘
                           │ scheduler creates TI
                    ┌──────▼──────┐
                    │  scheduled  │
                    └──────┬──────┘
                           │ sent to executor
                    ┌──────▼──────┐
                    │   queued    │
                    └──────┬──────┘
                           │ worker picks up
                    ┌──────▼──────┐
              ┌────►│   running   │
              │     └──────┬──────┘
              │            │
              │     ┌──────▼─────┐    max retries reached
              │     │   failed   │──────────────────────────────────────┐
              │     └──────┬─────┘                                      │
              │            │ retries left                                │
              │     ┌──────▼──────────┐                                 │
              └─────┤  up_for_retry   │                                 │
                    └─────────────────┘                                 │
                                                                        │
              ┌────────────────────────────────────────────────────────▼──┐
              │                   terminal states                          │
              │  success │ failed │ upstream_failed │ skipped │ shutdown   │
              └──────────────────────────────────────────────────────────┘

Additional states:
  deferred         ──► waiting in Triggerer (async)
  up_for_reschedule ──► sensor in reschedule mode between polls
  removed          ──► task removed from DAG definition
```

---

## 30. Interview Q&A — Expert Level

**Q61. How do you create a custom operator and what are the critical rules?**

Creating a custom operator means extending `BaseOperator`. Critical rules:
1. **Constructor (`__init__`)** must be lightweight — it runs every time the Scheduler parses the DAG (every few seconds). Never open DB connections or make API calls here.
2. **`execute(context)`** is where actual work happens — called only when the task runs.
3. Declare `template_fields` for any field you want Jinja templating support on.
4. Always call `super().__init__(**kwargs)` as the first line of `__init__`.
5. Use `self.log` (not `print`) for logging.

```python
class NotifySlackOperator(BaseOperator):
    template_fields = ('message',)   # Jinja will render {{ ds }}, etc.

    def __init__(self, message: str, channel: str, conn_id: str = 'slack', **kwargs):
        super().__init__(**kwargs)
        self.message = message
        self.channel = channel
        self.conn_id = conn_id

    def execute(self, context):
        hook = SlackHook(slack_conn_id=self.conn_id)   # Hook goes in execute()
        hook.call('chat.postMessage', json={'channel': self.channel, 'text': self.message})
```

---

**Q62. What is the difference between `parallelism`, `max_active_tasks_per_dag`, and `worker_concurrency`?**

- **`parallelism`** (`airflow.cfg [core]`): Global cap on total simultaneously running task instances across ALL DAGs and all workers. If this is 32, a maximum of 32 tasks run at any moment cluster-wide.
- **`max_active_tasks_per_dag`** (`airflow.cfg [core]` or per-DAG `concurrency`): Max tasks running at once within a SINGLE DAG (regardless of how many DAG runs are active). Prevents one busy DAG from consuming all parallelism slots.
- **`worker_concurrency`** (Celery): Max tasks a single Celery worker process will pick up simultaneously. Multiply this by the number of workers to understand the practical ceiling.

```
Global ceiling:     parallelism = 32
  │
  ├── DAG A can use at most: max_active_tasks_per_dag = 16
  └── DAG B can use at most: max_active_tasks_per_dag = 16

Worker 1:           worker_concurrency = 8  (picks up max 8 tasks)
Worker 2:           worker_concurrency = 8
Worker 3:           worker_concurrency = 8
Worker 4:           worker_concurrency = 8
                    Total = 32 ← matches parallelism
```

---

**Q63. What is RBAC in Airflow and what are the built-in roles?**

Airflow uses Role-Based Access Control (RBAC) via Flask-AppBuilder. Five built-in roles:
- **Admin**: Full access including user/role management
- **Op**: Can manage connections, variables, pools, trigger/clear DAGs
- **User**: Can trigger and clear DAGs they have access to
- **Viewer**: Read-only access to DAGs, runs, logs
- **Public**: No authentication required (if enabled)

Custom roles can be created in the UI (Security → List Roles) and DAGs can have `access_control` to restrict which roles can interact with them.

---

**Q64. What is a Custom Timetable and when do you need one?**

When `schedule_interval`'s cron syntax is insufficient — e.g., run only on business days, fiscal quarters, or custom calendar intervals — you create a `Timetable` subclass. Introduced in Airflow 2.2+.

You override two methods:
- `next_dagrun_info()` — returns when the next run should happen
- `infer_manual_data_interval()` — defines data interval for manual triggers

Register the timetable via an Airflow Plugin.

---

**Q65. What is `priority_weight` and `weight_rule` in Airflow?**

`priority_weight` assigns a numeric priority to a task. When multiple tasks compete for limited executor slots, higher-weight tasks run first.

`weight_rule` determines how the effective weight is calculated:
- `downstream` (default): task weight + sum of all downstream weights → upstream tasks get boosted priority since they unlock more work
- `upstream`: task weight + sum of upstream weights
- `absolute`: use exactly the `priority_weight` value; no cascade

---

**Q66. What are the key differences between Airflow 1.x and 2.x?**

Major changes in 2.x:
1. **TaskFlow API** (`@dag`, `@task`) — no more `PythonOperator` + manual XCom
2. **HA Scheduler** — multiple schedulers can run simultaneously
3. **DAG Serialization** on by default
4. **Stable REST API** (2.0+)
5. **TaskGroups** replace SubDAGs
6. **Triggerer** for deferrable operators (2.2+)
7. **Dynamic Task Mapping** (2.3+)
8. **Datasets** for event-driven scheduling (2.4+)
9. **Setup/Teardown tasks** (2.7+)
10. Provider packages split from core (`airflow.contrib.*` → `airflow.providers.*`)

---

**Q67. What is `soft_fail` in sensors and when would you use it?**

`soft_fail=True` on a sensor means: if the sensor times out or fails, mark the task as `skipped` instead of `failed`. This prevents the entire DAG run from failing due to missing source data.

```python
wait_for_file = S3KeySensor(
    task_id='wait_for_optional_data',
    bucket_key='data/{{ ds }}/optional_file.csv',
    timeout=3600,
    soft_fail=True   # skip this path if file never arrives
)
```

Use when: the upstream data is optional, and downstream tasks can handle its absence via trigger rules (`none_failed_min_one_success`).

---

**Q68. What is the Airflow REST API and what can you do with it?**

The stable REST API (introduced in Airflow 2.0) enables programmatic control:

```bash
# Authentication
curl -u admin:password ...

# List DAGs
GET /api/v1/dags

# Trigger a DAG
POST /api/v1/dags/{dag_id}/dagRuns
Body: {"conf": {"param": "value"}}

# Get task instance status
GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}

# Clear a task instance
POST /api/v1/dags/{dag_id}/clearTaskInstances
Body: {"task_ids": ["task_a"], "start_date": "2024-01-01"}

# Update variable
PATCH /api/v1/variables/{variable_key}
Body: {"value": "new_value"}
```

**Use cases:** CI/CD pipeline triggers, external monitoring systems, ITSM integrations, Airflow-to-Airflow orchestration.

---

**Q69. How does Airflow's DAG Serialization work and why was it introduced?**

Before serialization, the Webserver parsed all Python DAG files directly — running Python code for each page view. This caused high CPU usage and security concerns (arbitrary Python execution in the UI process).

With serialization (default in Airflow 2.x):
1. Scheduler parses DAG Python files
2. Converts DAG structure to JSON
3. Stores JSON in `serialized_dag` table in Metadata DB
4. Webserver reads JSON from DB — never touches Python files

Benefits: Webserver is faster, more secure, and can scale independently. Webserver nodes don't need access to the DAG files at all.

---

**Q70. Explain how ExternalTaskSensor handles execution_delta.**

`ExternalTaskSensor` waits for a task in a different DAG. Since DAGs may run at different schedules, `execution_delta` (or `execution_date_fn`) aligns which run of the external DAG to wait for.

```python
# Your reporting DAG runs at 8 AM
# The upstream ingestion DAG runs at 6 AM (2 hours earlier)

wait_for_ingestion = ExternalTaskSensor(
    task_id='wait',
    external_dag_id='ingestion_pipeline',
    external_task_id='final_load',
    execution_delta=timedelta(hours=2),  # look for the run 2 hours before THIS run's logical_date
    mode='reschedule'
)
```

Without `execution_delta`, it would look for a run with the SAME `logical_date` as the current run — which may not exist.

---

**Q71. What are Datasets in Airflow 2.4+ and how do they enable event-driven scheduling?**

Datasets represent logical data assets (an S3 path, a DB table). DAGs can declare:
- **`outlets=[dataset]`** — "I produce this dataset"
- **`schedule=[dataset]`** — "Run me when this dataset is updated"

```python
sales_dataset = Dataset("s3://data-lake/sales/{{ds}}/")

# Producer DAG
@dag(schedule='@daily')
def ingest_sales():
    @task(outlets=[sales_dataset])   # marks dataset as updated on success
    def load_to_s3():
        pass
    load_to_s3()

# Consumer DAG — NO schedule_interval, triggers when dataset is updated!
@dag(schedule=[sales_dataset])
def generate_report():
    @task
    def build_report():
        pass
    build_report()
```

This decouples producer and consumer DAGs — no ExternalTaskSensor polling required.

---

**Q72. What is a Cluster Policy and how does it enforce organizational standards?**

A Cluster Policy is a function defined in `airflow_local_settings.py` that runs when Airflow loads every DAG/task. The platform team can use it to:
- Enforce tagging requirements
- Force retry settings
- Change sensor modes (poke → reschedule)
- Block DAGs violating resource limits

```python
# config/airflow_local_settings.py
from airflow.exceptions import AirflowClusterPolicyViolation

def task_policy(task):
    # Force all sensors to use reschedule mode to save worker slots
    from airflow.sensors.base import BaseSensorOperator
    if isinstance(task, BaseSensorOperator):
        task.mode = 'reschedule'
    
    # Enforce retries on all tasks
    if task.retries < 1:
        task.retries = 2
```

---

**Q73. How would you implement a circuit breaker pattern in Airflow?**

The circuit breaker pattern stops the pipeline if a critical check fails — preventing partial loads from corrupting downstream systems.

```python
from airflow.exceptions import AirflowSkipException, AirflowFailException

@task
def circuit_breaker_check(ds: str) -> str:
    """Fails the pipeline if source data quality is unacceptable."""
    row_count = get_source_row_count(ds)
    
    if row_count == 0:
        raise AirflowSkipException("No data available — skipping entire pipeline")
    
    if row_count < MIN_EXPECTED_ROWS:
        raise AirflowFailException(
            f"Only {row_count} rows found, expected >= {MIN_EXPECTED_ROWS}. "
            "Failing to prevent corrupted load."
        )
    
    return f"OK: {row_count} rows"

# DAG flow
check = circuit_breaker_check()
transform = transform_data()
load = load_to_warehouse()

check >> transform >> load
```

---

**Q74. What are the main managed Airflow offerings and when would you choose each?**

- **AWS MWAA**: Best for AWS-centric shops that want zero infrastructure management. Tightly integrated with IAM, S3, CloudWatch. DAGs deployed via S3.
- **GCP Cloud Composer**: Best for GCP shops. Built on GKE with tight BigQuery/GCS integration. Composer 2 offers autoscaling workers.
- **Astronomer**: Best for teams wanting the latest Airflow features, multi-cloud support, strong RBAC, CI/CD integration, and enterprise support. Uses Docker + Kubernetes.

For greenfield: if you're already on AWS/GCP, use the native managed service. For multi-cloud or needing cutting-edge Airflow features, Astronomer.

---

**Q75. How do you pass configuration to a DAG at runtime?**

Three methods:

```python
# Method 1: Trigger with conf via CLI
airflow dags trigger my_dag --conf '{"env": "prod", "date": "2024-01-15"}'

# Method 2: Trigger via REST API
POST /api/v1/dags/my_dag/dagRuns
{"conf": {"env": "prod"}}

# Method 3: Trigger via UI
# Click "Trigger DAG w/ config" → enter JSON

# Access inside a task
@task
def process(**context):
    conf = context['dag_run'].conf
    env = conf.get('env', 'dev')
    date = conf.get('date', context['ds'])

# Access in Jinja template
'{{ dag_run.conf.get("env", "dev") }}'
```

---

**Q76. How do you handle a long-running task (e.g., 3-hour Spark job) without blocking worker slots?**

Use **deferrable operators** with the Triggerer:

```python
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

spark_job = DatabricksRunNowOperator(
    task_id='long_spark_job',
    job_id=12345,
    deferrable=True    # releases worker slot; Triggerer polls Databricks async
)
```

Flow:
```
Task starts ──► submits job to Databricks ──► defers to Triggerer
Worker slot FREED ──► Triggerer polls Databricks every 30s (async, no worker needed)
Job finishes ──► Triggerer fires ──► Worker picks up task ──► marks SUCCESS
```

Without deferrable: one worker slot held for 3 hours per active job.
With deferrable: Triggerer manages 1000s of waits with near-zero resource overhead.

---

**Q77. What is `depends_on_past` and how does it interact with backfilling?**

`depends_on_past=True` means a task instance will only start if the same task's previous logical date instance **succeeded**. 

During backfill, this creates a strict sequential dependency:
```
Jan 1 run ──► MUST succeed ──► Jan 2 run starts
Jan 2 run ──► MUST succeed ──► Jan 3 run starts

If Jan 2 fails:
  Jan 3 is blocked — even if all of Jan 3's upstream tasks succeed.
```

During backfill with `--reset-dagruns`, you can override this to process all dates independently.

**When to use:** Cumulative aggregations where day N depends on day N-1 being complete.

---

**Q78. Explain the difference between `on_failure_callback` at DAG vs task level.**

- **Task-level `on_failure_callback`**: Called when THAT specific task fails. Receives `context` with details about the failed task.
- **DAG-level `on_failure_callback`**: Called when ANY task in the DAG fails (actually it's called when the DAG run itself fails — the scheduler calls it after it determines the DAG run state is FAILED).

```python
# Task level — sends alert for each failed task
task = PythonOperator(
    on_failure_callback=alert_task_failure  # called per-task
)

# DAG level — notifies when the entire DAG is considered failed
dag = DAG(
    on_failure_callback=alert_dag_failure   # called once per DAG run failure
)
```

For SLA, use `sla_miss_callback` on the DAG — it fires when a task exceeds its `sla` timedelta.

---

**Q79. How does the KubernetesExecutor differ from the CeleryExecutor in terms of resource isolation and cost?**

| Aspect | CeleryExecutor | KubernetesExecutor |
|---|---|---|
| Task isolation | Shared worker process | Each task gets its own Pod |
| Environment | All tasks share worker's Python env | Each task can use a different Docker image |
| Idle cost | Workers always running (paying for idle) | Workers scale to 0 (pay per task) |
| Cold start | Instant (worker already running) | 10-30s per task (Pod startup) |
| Debug | Harder (task output in queue logs) | Pod logs accessible via `kubectl` |
| Best for | High-frequency short tasks | Variable environments, cost-sensitive |

```
CeleryExecutor:
  Workers always running ──► low latency, high idle cost

KubernetesExecutor:
  No idle workers ──► cold start per task, pay only for usage

KubernetesPodOperator with CeleryExecutor:
  Best of both — fast task pickup via Celery + isolated container per task
```

---

**Q80. What is `airflow db clean` and when should you run it?**

`airflow db clean` purges old data from the Metadata DB:
- Old DAG runs
- Old task instances  
- Old XCom entries
- Old import errors

```bash
# Delete records older than 90 days
airflow db clean --clean-before-timestamp "2024-01-01 00:00:00" --yes

# Dry run to see what would be deleted
airflow db clean --clean-before-timestamp "2024-01-01 00:00:00" --dry-run
```

Run it as a scheduled maintenance DAG:
```python
maintenance = BashOperator(
    task_id='clean_metadata_db',
    bash_command='airflow db clean --clean-before-timestamp {{ macros.ds_add(ds, -90) }} --yes'
)
```

Without regular cleaning, the Metadata DB grows unbounded → slower queries → slower Scheduler.

---

**Q81. What happens to running tasks if the Scheduler crashes in Airflow 2.x?**

In Airflow 2.x with HA mode (multiple Schedulers):
1. Running tasks on workers continue executing — workers don't depend on the Scheduler to run
2. The surviving Scheduler(s) detect the crashed Scheduler via heartbeat timeouts
3. The surviving Scheduler "adopts" orphaned task instances and monitors their heartbeats
4. Any tasks whose LocalTaskJob heartbeat goes stale are detected as Zombie tasks and marked FAILED

If running only ONE Scheduler and it crashes:
- Running tasks continue on workers but state won't be updated until Scheduler restarts
- New DAG runs won't be created
- Zombie detection pauses

This is why HA mode (2+ Schedulers) is recommended for production.

---

**Q82. How do you implement a retry with exponential backoff?**

```python
task = PythonOperator(
    task_id='call_external_api',
    python_callable=call_api,
    retries=5,
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=True,   # 1min, 2min, 4min, 8min, 16min...
    max_retry_delay=timedelta(hours=1)  # cap the maximum wait
)
```

Retry timeline:
```
Attempt 1 fails ──► wait 1 min  ──► Attempt 2
Attempt 2 fails ──► wait 2 min  ──► Attempt 3
Attempt 3 fails ──► wait 4 min  ──► Attempt 4
Attempt 4 fails ──► wait 8 min  ──► Attempt 5
Attempt 5 fails ──► wait 16 min ──► Attempt 6 (max retries → FAILED)
```

---

**Q83. What is the Triggerer process and how does it save resources?**

The Triggerer is an async event loop process (Python `asyncio`) that manages **deferrable operators**. When a task defers:

1. Task writes a `Trigger` record to the Metadata DB
2. Worker slot is freed immediately
3. Triggerer reads pending Triggers from DB
4. Runs all triggers as async coroutines (no blocking)
5. When a trigger fires, Triggerer writes a `TriggerEvent` to DB
6. Scheduler picks up the event, queues the task again
7. Worker picks up and completes the task

```
Resource comparison for 100 waiting S3 sensors:

  Poke mode:      100 tasks × 1 worker slot = 100 workers occupied for hours
  Reschedule:     Workers used only during poke (efficient but still has overhead)
  Deferrable:     1 Triggerer handles 1000s of waits with async I/O (near zero overhead)
```

---

**Q84. How do you structure a large-scale Airflow project with many teams?**

```
monorepo/
├── dags/
│   ├── team_finance/
│   │   ├── daily_revenue_dag.py
│   │   └── monthly_report_dag.py
│   ├── team_analytics/
│   │   └── user_metrics_dag.py
│   └── team_platform/
│       └── infrastructure_cleanup_dag.py
├── plugins/
│   ├── operators/
│   │   └── company_api_operator.py
│   └── hooks/
│       └── internal_db_hook.py
├── tests/
│   ├── test_finance_dags.py
│   └── test_analytics_dags.py
├── Makefile
└── .github/workflows/
    └── ci.yml       # lint → test → deploy

Conventions:
  - DAG IDs: {team}.{pipeline_name}  (e.g., 'finance.daily_revenue')
  - All DAGs tagged with team name
  - Separate pools per team for quota management
  - RBAC: each team gets a role with access only to their DAGs
```

---

**Q85. What is the difference between AirflowException, AirflowSkipException, and AirflowFailException?**

```python
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException

# AirflowException (or any Exception):
# Task fails → retries if configured → eventually FAILED state
raise AirflowException("Something went wrong")

# AirflowSkipException:
# Task is marked as SKIPPED — no retries triggered
# Useful when a task has nothing to do (no data for this date)
raise AirflowSkipException("No data available for {{ ds }}")

# AirflowFailException (Airflow 2.2+):
# Task fails IMMEDIATELY — retries are NOT attempted
# Useful when retrying won't help (schema mismatch, unrecoverable error)
raise AirflowFailException("Schema mismatch — human intervention required")
```

---

**Q86. How do you test DAGs in a CI/CD pipeline?**

```python
# tests/test_dag_integrity.py
import pytest
from airflow.models import DagBag

@pytest.fixture
def dagbag():
    return DagBag(dag_folder='./dags', include_examples=False)

def test_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"

def test_all_dags_have_tags(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG {dag_id} has no tags"

def test_all_dags_have_retries(dagbag):
    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            assert task.retries >= 1, f"Task {dag_id}.{task.task_id} has no retries"

def test_dag_task_count(dagbag):
    dag = dagbag.get_dag('daily_etl')
    assert len(dag.tasks) == 5

def test_dag_structure(dagbag):
    dag = dagbag.get_dag('daily_etl')
    # Verify dependency structure
    extract = dag.get_task('extract')
    assert 'transform' in [t.task_id for t in extract.downstream_list]
```

```yaml
# .github/workflows/ci.yml
- name: Test DAGs
  run: |
    pip install apache-airflow pytest
    pytest tests/ -v --tb=short
```

---

**Q87. What is Git-Sync and how does it work in Kubernetes Airflow?**

Git-Sync is a sidecar container pattern for Kubernetes deployments. It continuously syncs a Git repository to a shared volume that Airflow reads DAGs from.

```yaml
# Kubernetes Pod spec (simplified Helm chart values)
scheduler:
  extraContainers:
    - name: git-sync
      image: registry.k8s.io/git-sync/git-sync:v4.1.0
      args:
        - --repo=https://github.com/myorg/airflow-dags
        - --branch=main
        - --root=/git
        - --period=30s    # sync every 30 seconds
      volumeMounts:
        - name: dags-volume
          mountPath: /git
```

```
Flow:
  [Git push to main] ──► 30 seconds ──► git-sync detects change
                                         ──► pulls latest DAGs
                                         ──► Scheduler detects new files
                                         ──► Refreshes DAG definitions
```

**Benefits:** DAG updates without restarting Airflow. Zero-downtime deployments. GitOps-compatible.

---

**Q88. How does Airflow handle Variables caching and what is the performance implication?**

Airflow 2.x caches Variables with a TTL (default: 30 seconds). This means:

```python
# This hits the DB
value = Variable.get('my_config')

# Cached — no DB hit for ~30 seconds
value = Variable.get('my_config')   # returned from cache

# In Jinja templates, Variables are fetched at template render time
# (not cached the same way)
# {{ var.value.my_config }}
```

**Performance implications:**
1. Reading Variables in `__init__` (DAG parse time) hits DB every parse cycle — very bad
2. Reading Variables inside `execute()` is fine — happens only at task runtime
3. Variables with sensitive values should use a Secrets Backend (not DB cache)

```python
# ❌ BAD — reads Variable on every DAG parse (every few seconds)
my_var = Variable.get('config')  # top-level DAG file code

with DAG(...) as dag:
    t = PythonOperator(task_id='t', python_callable=lambda: my_var)

# ✅ GOOD — reads Variable only when task executes
@task
def my_task():
    config = Variable.get('config')  # inside execute()
```

---

**Q89. What is the `max_active_runs` parameter and what problem does it solve?**

`max_active_runs` limits how many concurrent DAG runs of the same DAG can be active simultaneously.

```python
dag = DAG(
    'daily_pipeline',
    max_active_runs=1,    # only ONE active run at a time
    ...
)
```

**Why it matters:**
```
Without limit (max_active_runs=16):
  Backfill creates 365 DAG runs simultaneously
  → 365 × tasks competing for resources
  → Metadata DB overwhelmed
  → Other DAGs starved of resources

With max_active_runs=1:
  Backfill runs one day at a time → orderly processing
  
With depends_on_past=True + max_active_runs=1:
  Strictly sequential processing — perfect for cumulative loads
```

---

**Q90. How would you debug a task that is stuck in the "running" state but not doing anything?**

Step-by-step debugging:

```
Step 1: Check if it's a Zombie
  ├── Is the worker process actually running?
  └── Check worker logs: kubectl logs <worker-pod> / docker logs <worker>

Step 2: Verify the task's heartbeat
  └── DB query: SELECT heartbeat FROM task_instance 
                WHERE dag_id='x' AND task_id='y' AND execution_date='z'
  └── If heartbeat is stale (> 5 min old) → it's a zombie

Step 3: For Kubernetes workers
  └── kubectl get pods → is the task pod still Running?
  └── kubectl logs <task-pod> → any error output?
  └── kubectl describe pod <task-pod> → OOMKilled? Evicted?

Step 4: For Celery workers
  └── Check Flower UI (port 5555): is the task in active tasks?
  └── Check Celery worker logs for the specific task

Step 5: Force resolution
  └── If confirmed zombie: airflow tasks clear my_dag stuck_task -s <date> -e <date>
  └── Or mark failed: airflow tasks state-failed my_dag stuck_task <date>
  └── Or kill worker pod (K8s) / worker process (Celery)
```

---

## 31. Additional Scenario-Based Questions

---

### Scenario 16: Building a Custom Monitoring DAG

**Question:** Create an Airflow DAG that monitors other DAGs and sends a daily health summary.

**Answer:**

```python
from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from datetime import datetime, timedelta

@dag(
    schedule_interval='0 8 * * *',   # 8 AM daily health report
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def airflow_health_monitor():

    @task
    def check_failed_dags(ds: str) -> dict:
        """Find all DAGs that had failures in the last 24 hours."""
        yesterday = datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=1)
        
        from airflow.utils.session import create_session
        with create_session() as session:
            failed_runs = session.query(DagRun).filter(
                DagRun.state == DagRunState.FAILED,
                DagRun.start_date >= yesterday
            ).all()
        
        return {
            'failed_count': len(failed_runs),
            'failed_dags': [r.dag_id for r in failed_runs]
        }

    @task
    def send_health_report(health_data: dict):
        failed = health_data['failed_count']
        status = '🔴 ISSUES DETECTED' if failed > 0 else '🟢 ALL HEALTHY'
        message = f"""
        Daily Airflow Health Report ({datetime.now().strftime('%Y-%m-%d')})
        Status: {status}
        Failed DAG runs: {failed}
        Failed DAGs: {', '.join(health_data['failed_dags']) or 'None'}
        """
        # send via Slack/Email
        notify(message)

    report = send_health_report(check_failed_dags())

dag = airflow_health_monitor()
```

---

### Scenario 17: Database Migration Pipeline

**Question:** Design a DAG that migrates data from an on-premise MySQL database to Snowflake, with row count validation.

**Answer:**

```python
@dag(schedule_interval='@once', start_date=datetime(2024,1,1), catchup=False)
def db_migration():

    @task
    def extract_from_mysql(table: str) -> str:
        hook = MySqlHook(mysql_conn_id='onprem_mysql')
        df = hook.get_pandas_df(f"SELECT * FROM {table}")
        path = f"s3://migration-bucket/{table}/data.parquet"
        df.to_parquet(path)
        return path

    @task
    def validate_source(table: str) -> int:
        hook = MySqlHook(mysql_conn_id='onprem_mysql')
        return hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]

    @task
    def load_to_snowflake(s3_path: str, table: str) -> int:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        hook.run(f"""
            COPY INTO {table}
            FROM '{s3_path}'
            FILE_FORMAT = (TYPE = PARQUET)
        """)
        return hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]

    @task
    def validate_counts(source_count: int, target_count: int, table: str):
        if source_count != target_count:
            raise AirflowFailException(
                f"Row count mismatch for {table}: "
                f"source={source_count}, target={target_count}"
            )
        print(f"✅ Migration validated: {source_count} rows match")

    tables = ['customers', 'orders', 'products']
    for table in tables:
        path = extract_from_mysql(table)
        src_count = validate_source(table)
        tgt_count = load_to_snowflake(path, table)
        validate_counts(src_count, tgt_count, table)
```

---

### Scenario 18: ML Pipeline Orchestration

**Question:** Design an Airflow DAG for an end-to-end ML pipeline: data prep → feature engineering → training → evaluation → conditional deployment.

**Answer:**

```python
@dag(
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def ml_training_pipeline():

    @task
    def prepare_data(ds: str) -> str:
        path = f"s3://ml-data/prepared/{ds}/"
        # extract and clean raw data
        return path

    @task
    def engineer_features(data_path: str) -> str:
        features_path = data_path.replace('prepared', 'features')
        # compute features
        return features_path

    @task
    def train_model(features_path: str) -> dict:
        # train and log to MLflow
        run_id = mlflow_train(features_path)
        metrics = get_metrics(run_id)
        return {'run_id': run_id, 'accuracy': metrics['accuracy'], 'f1': metrics['f1']}

    @task.branch
    def evaluate_model(model_info: dict) -> str:
        """Decide whether to deploy based on metrics."""
        if model_info['accuracy'] >= 0.90 and model_info['f1'] >= 0.85:
            return 'deploy_model'
        else:
            return 'reject_model'

    @task
    def deploy_model(model_info: dict):
        deploy_to_production(model_info['run_id'])
        notify_team(f"Model deployed: accuracy={model_info['accuracy']:.2%}")

    @task
    def reject_model(model_info: dict):
        notify_team(
            f"Model rejected: accuracy={model_info['accuracy']:.2%} < 90% threshold. "
            "Manual review required."
        )

    done = EmptyOperator(task_id='done', trigger_rule='none_failed_min_one_success')

    data = prepare_data()
    features = engineer_features(data)
    model = train_model(features)
    decision = evaluate_model(model)
    [deploy_model(model), reject_model(model)] >> done
```

```
Flow:
  [prepare_data] ──► [engineer_features] ──► [train_model] ──► [evaluate_model]
                                                                       │
                                                         ┌─────────────┴─────────────┐
                                                   accuracy ≥ 90%           accuracy < 90%
                                                         │                         │
                                               [deploy_model]           [reject_model]
                                                         │                         │
                                                         └──────────[done]──────────┘
```

---

### Scenario 19: Multi-Region Data Aggregation

**Question:** You need to aggregate data from 5 regional databases (US, EU, APAC, LATAM, MEA) into a global warehouse daily. Design the DAG.

**Answer:**

```python
@dag(schedule_interval='0 2 * * *', start_date=datetime(2024,1,1), catchup=False)
def global_data_aggregation():

    REGIONS = {
        'us': 'us_postgres',
        'eu': 'eu_postgres',
        'apac': 'apac_postgres',
        'latam': 'latam_postgres',
        'mea': 'mea_postgres'
    }

    @task
    def extract_region(region: str, conn_id: str, ds: str) -> str:
        hook = PostgresHook(postgres_conn_id=conn_id)
        df = hook.get_pandas_df(
            f"SELECT * FROM sales WHERE sale_date = '{ds}'"
        )
        df['region'] = region
        path = f"s3://global-data/{ds}/{region}/sales.parquet"
        df.to_parquet(path)
        return path

    @task
    def merge_regions(paths: list, ds: str) -> str:
        """Fan-in: merge all 5 regional extracts."""
        import pandas as pd
        dfs = [pd.read_parquet(p) for p in paths]
        merged = pd.concat(dfs)
        merged_path = f"s3://global-data/{ds}/merged/sales.parquet"
        merged.to_parquet(merged_path)
        return merged_path

    @task
    def load_global_warehouse(merged_path: str, ds: str):
        hook = SnowflakeHook(snowflake_conn_id='global_warehouse')
        hook.run(f"DELETE FROM global_sales WHERE sale_date = '{ds}'")
        # load from S3

    # Fan-out: extract all regions in parallel
    regional_paths = [
        extract_region.override(task_id=f'extract_{region}')(region, conn_id, '{{ ds }}')
        for region, conn_id in REGIONS.items()
    ]

    # Fan-in: merge after all regions complete
    merged = merge_regions(regional_paths)
    load_global_warehouse(merged)
```

```
Fan-Out / Fan-In Pattern:
                        ┌──► extract_us   ──┐
                        ├──► extract_eu   ──┤
  [start] ─────────────┼──► extract_apac ──┼──► [merge_regions] ──► [load_warehouse]
                        ├──► extract_latam──┤
                        └──► extract_mea  ──┘
  (all 5 run in parallel)                 (fan-in after all complete)
```

---

### Scenario 20: Automated DAG Documentation

**Question:** How do you ensure every DAG in a large team has proper documentation?

**Answer:**

```python
# 1. Use Cluster Policy to enforce doc_md on all DAGs
def dag_policy(dag):
    if not dag.doc_md:
        raise AirflowClusterPolicyViolation(
            f"DAG '{dag.dag_id}' must have doc_md documentation."
        )

# 2. DAG documentation template (in each DAG file)
with DAG(
    dag_id='daily_revenue_pipeline',
    doc_md="""
    # Daily Revenue Pipeline
    
    **Owner:** Finance Team  
    **Schedule:** Daily at 2 AM UTC  
    **SLA:** Must complete by 6 AM UTC  
    
    ## Description
    Extracts daily revenue data from PostgreSQL, applies business rules,
    and loads aggregated metrics to Snowflake for BI reporting.
    
    ## Data Sources
    - `financial_db.daily_transactions`
    
    ## Outputs  
    - `snowflake.finance.daily_revenue_summary`
    
    ## Runbook
    - On failure: check Slack #data-alerts
    - Manual trigger: use `env=backfill` config key
    """,
    ...
):
    ...

# 3. In the Airflow UI: click on a DAG → "Docs" tab shows this markdown
```

---

### Scenario 21: Cost Optimization with KubernetesExecutor

**Question:** Your team runs 500 DAGs on Celery with 20 workers always running. How would you migrate to KubernetesExecutor to reduce costs?

**Answer:**

```
Migration Plan:

Phase 1: Analysis
  - Profile task resource requirements (CPU/memory distribution)
  - Identify tasks that need special environments (GPU, high-memory)
  - Calculate current idle worker time percentage

Phase 2: Infrastructure
  - Set up Kubernetes cluster (EKS/GKE/AKS)
  - Deploy Airflow with Helm chart (KubernetesExecutor)
  - Configure default Pod template with standard resources

Phase 3: Configuration
```
```python
# airflow.cfg
[kubernetes]
executor = KubernetesExecutor

# Default pod template
pod_template_file = /opt/airflow/pod_templates/default.yaml

# In DAGs: override pod config per task
spark_task = PythonOperator(
    task_id='heavy_compute',
    python_callable=compute,
    executor_config={
        'pod_override': k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='base',
                        resources=k8s.V1ResourceRequirements(
                            requests={'memory': '16Gi', 'cpu': '4'},
                            limits={'memory': '32Gi', 'cpu': '8'}
                        )
                    )
                ]
            )
        )
    }
)
```
```
Cost comparison:
  Celery (20 workers always on):  20 × $0.20/hr × 8760hr = ~$35,000/yr
  KubernetesExecutor (on-demand): Pay only during task execution
  
  If tasks run 20% of the time: ~$7,000/yr (80% cost reduction)

Tradeoffs:
  ✅ Zero idle cost
  ✅ Per-task resource profiles
  ✅ Better isolation
  ❌ Pod startup adds 10-30s latency per task
  ❌ More complex debugging
```

---

### Scenario 22: Implementing Data Contracts in Airflow

**Question:** How do you enforce data contracts (schema + quality checks) between producer and consumer DAGs?

**Answer:**

```python
# data_contracts.py — shared contract definitions
from dataclasses import dataclass
from typing import List

@dataclass
class ColumnContract:
    name: str
    dtype: str
    nullable: bool
    min_value: float = None
    max_value: float = None

SALES_CONTRACT = [
    ColumnContract('sale_id', 'int64', False),
    ColumnContract('amount', 'float64', False, min_value=0),
    ColumnContract('sale_date', 'datetime64[ns]', False),
    ColumnContract('customer_id', 'int64', False),
]

# validation_task.py
@task
def validate_contract(data_path: str, contract: list) -> str:
    import pandas as pd

    df = pd.read_parquet(data_path)
    violations = []

    for col in contract:
        # Check column exists
        if col.name not in df.columns:
            violations.append(f"Missing column: {col.name}")
            continue

        # Check nullable constraint
        if not col.nullable and df[col.name].isna().any():
            violations.append(f"Null values in non-nullable column: {col.name}")

        # Check value ranges
        if col.min_value is not None and (df[col.name] < col.min_value).any():
            violations.append(f"Values below minimum in {col.name}: min={col.min_value}")

    if violations:
        raise AirflowFailException(f"Contract violations: {violations}")

    return data_path   # pass through to next task

# In DAG
raw_path = extract()
validated_path = validate_contract(raw_path, SALES_CONTRACT)
transform(validated_path)
```

---

### Scenario 23: Building a Self-Healing Pipeline

**Question:** How would you design a pipeline that automatically retries with different strategies based on the type of failure?

**Answer:**

```python
@task(retries=3, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True)
def call_external_api(endpoint: str) -> dict:
    import requests
    from requests.exceptions import Timeout, ConnectionError

    try:
        response = requests.get(endpoint, timeout=30)
        response.raise_for_status()
        return response.json()

    except Timeout:
        # Retryable — network timeout, exponential backoff handles it
        raise

    except requests.HTTPError as e:
        if e.response.status_code == 429:
            # Rate limited — wait longer
            raise AirflowException("Rate limited — will retry with backoff")
        elif e.response.status_code in (400, 401, 403, 404):
            # Non-retryable — bad request, wrong credentials
            raise AirflowFailException(f"Non-retryable HTTP error: {e.response.status_code}")
        else:
            # Server errors — retryable
            raise

    except Exception as e:
        # Unknown error — log and retry
        logger.error(f"Unexpected error: {e}")
        raise
```

```python
# Global circuit breaker via Airflow Variable
@task
def process_with_circuit_breaker():
    circuit_state = Variable.get('api_circuit_state', default_var='closed')
    
    if circuit_state == 'open':
        raise AirflowSkipException("Circuit breaker is OPEN — skipping to prevent cascade failures")
    
    try:
        result = call_api()
        Variable.set('api_failure_count', 0)
        return result
    except Exception:
        count = int(Variable.get('api_failure_count', default_var=0)) + 1
        Variable.set('api_failure_count', count)
        if count >= 5:
            Variable.set('api_circuit_state', 'open')
            notify_on_call("Circuit breaker OPENED after 5 consecutive failures")
        raise
```

---

### Scenario 24: Multi-Tenant Data Pipeline

**Question:** You need to build an Airflow setup where 10 clients each have completely isolated data pipelines with separate credentials, separate compute, and separate monitoring.

**Answer:**

```python
# Option 1: Factory pattern — one DAG per client
CLIENT_CONFIGS = {
    'client_acme': {
        'schedule': '0 2 * * *',
        'db_conn': 'acme_postgres',
        'storage': 's3://acme-data/',
        'slack_channel': '#acme-alerts'
    },
    'client_globex': {
        'schedule': '0 3 * * *',
        'db_conn': 'globex_postgres',
        'storage': 's3://globex-data/',
        'slack_channel': '#globex-alerts'
    },
    # ... 8 more clients
}

def create_client_dag(client_id: str, config: dict):
    @dag(
        dag_id=f'pipeline.{client_id}',
        schedule_interval=config['schedule'],
        start_date=datetime(2024, 1, 1),
        tags=[client_id, 'client-pipeline'],
        default_args={'on_failure_callback': lambda ctx: alert(config['slack_channel'], ctx)}
    )
    def client_pipeline():
        @task(pool=f'{client_id}_pool')  # dedicated pool per client
        def extract():
            hook = PostgresHook(postgres_conn_id=config['db_conn'])
            ...

        @task
        def load(data_path: str):
            # Write to client-specific storage
            write_to_storage(config['storage'], data_path)

        load(extract())
    return client_pipeline()

# Create all client DAGs
for client_id, config in CLIENT_CONFIGS.items():
    globals()[f'dag_{client_id}'] = create_client_dag(client_id, config)
```

```
Isolation Strategy:
  ├── Separate Airflow Connections per client (no cross-client credential access)
  ├── Separate Pools per client (quota enforcement)
  ├── DAG access_control (only client team can see their DAGs)
  ├── Separate S3/GCS paths per client
  └── Client-specific failure alerting (separate Slack channels)
```

---

### Scenario 25: Zero-Downtime DAG Migration

**Question:** You need to rename a DAG (from `old_etl` to `new_etl`) without losing history and without downtime. How?

**Answer:**

```python
# Step 1: Create new DAG pointing to same logic
# BUT keep the old DAG running for now

# new_etl.py (new file)
with DAG('new_etl', schedule_interval='@daily', ...) as dag:
    # identical tasks to old_etl
    ...

# Step 2: Pause old DAG
# airflow dags pause old_etl

# Step 3: Verify new DAG is running correctly for 2-3 days

# Step 4: If history continuity needed — use dag_id aliasing pattern
# In old_etl.py, update to redirect:
with DAG('old_etl', schedule_interval=None, ...) as dag:  # set to None (manual only)
    redirect = EmptyOperator(task_id='migrated_to_new_etl')
    # Add doc_md explaining migration

# Step 5: Archive old DAG (keep in repo for history reference)
# Step 6: Delete old DAG after retention period

# Alternative: Use airflow dags delete (destructive — loses history!)
# airflow dags delete old_etl  ← DO NOT use in production without backing up history
```

```
Migration Timeline:
  Day 0: Deploy new_etl alongside old_etl
  Day 1: Pause old_etl, verify new_etl
  Day 3: Confirm new_etl stable — mark old_etl as deprecated
  Day 30: Archive old_etl code, delete from active dags/
```

---

## 32. Updated Quick Reference Cheat Sheet

### Airflow 2.x Key Imports

```python
# Core
from airflow import DAG
from airflow.decorators import dag, task, setup, teardown
from airflow.models import Variable, XCom, DagRun
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException

# Operators
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Sensors
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Hooks
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Utilities
from airflow.utils.task_group import TaskGroup
from airflow import Dataset
```

### airflow.cfg Key Settings Reference

| Setting | Section | Description | Default |
|---|---|---|---|
| `parallelism` | `core` | Global task parallelism | 32 |
| `max_active_tasks_per_dag` | `core` | Tasks per DAG | 16 |
| `max_active_runs_per_dag` | `core` | DAG runs per DAG | 16 |
| `load_examples` | `core` | Load example DAGs | True |
| `executor` | `core` | Executor type | SequentialExecutor |
| `min_file_process_interval` | `scheduler` | DAG reparse interval (s) | 30 |
| `dag_dir_list_interval` | `scheduler` | DAG folder scan interval (s) | 300 |
| `scheduler_zombie_task_threshold` | `scheduler` | Zombie detection (s) | 300 |
| `worker_concurrency` | `celery` | Tasks per Celery worker | 16 |
| `remote_logging` | `logging` | Enable remote log storage | False |

### Operator Categories Quick Reference

```
┌─────────────────────────────────────────────────────────────────┐
│                    OPERATOR QUICK REFERENCE                     │
│                                                                 │
│  COMPUTE                                                        │
│  PythonOperator, BashOperator, DockerOperator                   │
│  SparkSubmitOperator, KubernetesPodOperator                     │
│                                                                 │
│  DATABASE                                                       │
│  PostgresOperator, MySqlOperator, SnowflakeOperator             │
│  BigQueryOperator, MsSqlOperator                                │
│                                                                 │
│  CLOUD                                                          │
│  S3CopyObjectOperator, GCSToGCSOperator                         │
│  EMRCreateJobFlowOperator, DataprocSubmitJobOperator            │
│  AzureDataFactoryRunPipelineOperator                            │
│                                                                 │
│  COMMUNICATION                                                  │
│  EmailOperator, SlackWebhookOperator                            │
│  HttpOperator                                                   │
│                                                                 │
│  CONTROL FLOW                                                   │
│  BranchPythonOperator, EmptyOperator                            │
│  TriggerDagRunOperator, ExternalTaskSensor                      │
│                                                                 │
│  DATA VALIDATION                                                │
│  SQLCheckOperator, SQLValueCheckOperator                        │
│  SQLThresholdCheckOperator, SQLIntervalCheckOperator            │
└─────────────────────────────────────────────────────────────────┘
```

### Complete Interview Preparation Checklist

```
Beginner Level:
  ☐ What is Airflow and what problem does it solve?
  ☐ What is a DAG, Task, Operator, Task Instance?
  ☐ What are the 4 core components of Airflow?
  ☐ How does scheduling work (start_date, schedule_interval, catchup)?
  ☐ What is XCom? What are its limits?
  ☐ What is a Sensor? Poke vs Reschedule?
  ☐ What is a Hook?
  ☐ How do you define task dependencies?
  ☐ What is the Metadata Database?
  ☐ What is default_args?

Intermediate Level:
  ☐ Compare LocalExecutor, CeleryExecutor, KubernetesExecutor
  ☐ Explain logical_date vs execution_date vs actual runtime
  ☐ What are Trigger Rules?
  ☐ What are Pools?
  ☐ What is DAG Serialization and why is it useful?
  ☐ TaskGroups vs SubDAGs
  ☐ What is Dynamic Task Mapping?
  ☐ What is the Triggerer and Deferrable Operators?
  ☐ How do you handle secrets in production?
  ☐ What are Datasets (data-aware scheduling)?

Advanced Level:
  ☐ How do you create a custom operator?
  ☐ What are Cluster Policies?
  ☐ How does the Scheduler loop work internally?
  ☐ What is a LocalTaskJob?
  ☐ How does HA Scheduler work?
  ☐ What is Git-Sync?
  ☐ What are Custom Timetables?
  ☐ How do you implement RBAC?
  ☐ How do you tune Airflow performance?
  ☐ How do you integrate Airflow with dbt, Spark, Databricks?

Design Scenarios:
  ☐ Design a daily ETL pipeline with alerting
  ☐ Handle late-arriving data
  ☐ Process 1000 files in parallel
  ☐ Cross-DAG dependencies
  ☐ Rate-limited API processing
  ☐ Multi-environment (dev/prod) pipelines
  ☐ ML training pipeline with conditional deployment
  ☐ Cost optimization with KubernetesExecutor
  ☐ Multi-tenant Airflow setup
  ☐ Zero-downtime DAG migration
```

---

*End of Apache Airflow Interview & Study Guide*
*Topics: Architecture · DAG Authoring · Operators · Scheduling · Executors · XCom · Advanced Features · Custom Operators/Plugins · RBAC & Security · Configuration · Managed Services (MWAA/Composer/Astronomer) · Airflow 1→2 Migration · Setup/Teardown · Timetables · Priority Weights · Monitoring & OpenLineage · dbt/Spark/Databricks Integration · Cluster Policies · Scheduler Internals · 90 Interview Questions · 25 Real Scenarios*
