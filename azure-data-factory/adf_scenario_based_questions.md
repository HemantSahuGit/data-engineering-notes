# Azure Data Factory — Scenario-Based Interview Q&A

---

## Q1. How do you perform validation on rows and handle error rows in Mapping Data Flows?

### Overview

In ADF Mapping Data Flows, row validation is primarily done using the **Assert Transformation**, combined with **Conditional Split**, **Derived Column**, and **Sink error-row handling**. The goal is to:
1. Identify rows that violate business/data-quality rules.
2. Route valid rows to the target sink.
3. Route invalid (error) rows to an error sink (dead-letter table, blob file, etc.) without stopping the pipeline.

---

### 1. Assert Transformation (Primary Validation Mechanism)

The **Assert** transformation lets you define data quality rules directly in the data flow. Each assertion attaches metadata to rows — marking them as error rows — rather than immediately dropping them.

#### Assert Types

| Type | Description |
|---|---|
| `expect true` | A boolean expression must evaluate to `true` for the row. |
| `expect unique` | The specified column(s) must be unique within the stream. |
| `expect exists` | Rows must have a matching record in another stream (like a lookup). |

#### Key Properties

- **Assert type** — one of the three above.
- **Condition** — expression builder condition (e.g., `!isNull(customer_id) && amount > 0`).
- **Error code** — custom string tag (e.g., `NULL_CUSTOMER_ID`), stored in `_errorCode`.
- **Error message** — expression for a human-readable message, stored in `_errorMessage`.
- **Filter assert failures** — if enabled, removes error rows from the stream entirely; if disabled, marks them and keeps them flowing downstream.

#### How Assert Tags Rows

Internally, Assert adds two metadata columns to every row:
- `isError()` — boolean function: returns `true` if any assertion failed on that row.
- `_errorCode` — the custom error code string you defined.
- `_errorMessage` — the custom error message.

These columns are **virtual** (not written to output unless explicitly mapped) and accessible via built-in functions in downstream transformations.

#### Example Assertions

```
# Assert 1 — customer_id must not be null
Type: expect true
Condition: !isNull(customer_id)
Error code: 'NULL_CUSTOMER_ID'
Error message: 'customer_id cannot be null'

# Assert 2 — amount must be positive
Type: expect true
Condition: amount > 0
Error code: 'INVALID_AMOUNT'
Error message: concat('Amount must be > 0, got: ', toString(amount))

# Assert 3 — email must be unique
Type: expect unique
Columns: email
Error code: 'DUPLICATE_EMAIL'
Error message: 'Duplicate email found'
```

---

### 2. Routing Valid vs. Error Rows — Conditional Split

After the Assert transformation, use a **Conditional Split** to fork the stream:

```
Condition 1 (valid rows):   !isError()
Default stream (error rows): isError()
```

Each fork is then connected to a separate Sink.

---

### 3. Full Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Mapping Data Flow                          │
│                                                                 │
│  Source ──► Derived Column ──► Assert ──► Conditional Split     │
│               (add flags)    (validate)       │                 │
│                                           ┌───┴───┐            │
│                                     valid rows  error rows      │
│                                           │           │         │
│                                       Target       Error Sink   │
│                                        Sink     (Blob / Table)  │
└─────────────────────────────────────────────────────────────────┘
```

**Detailed flow:**

```
[Source]
   │
   ▼
[Derived Column]          ← optional: add computed/helper columns
   │
   ▼
[Assert #1: NULL checks]
   │
   ▼
[Assert #2: Range checks]
   │
   ▼
[Assert #3: Uniqueness]
   │
   ▼
[Conditional Split]
   ├── stream: "valid"   condition: !isError()
   │       │
   │       ▼
   │   [Select / Map columns]
   │       │
   │       ▼
   │   [Sink → Target DB / Blob]
   │
   └── stream: "errors"  condition: isError()  (default)
           │
           ▼
       [Derived Column]  ← capture _errorCode, _errorMessage, timestamp
           │
           ▼
       [Sink → Error Table / error_rows.csv]
```

---

### 4. Error Row Sink — What to Capture

In the error sink's **Derived Column** (before writing), add:

| New Column | Expression |
|---|---|
| `error_code` | `_errorCode` |
| `error_message` | `_errorMessage` |
| `ingestion_timestamp` | `currentTimestamp()` |
| `source_file` | `$sourceFileName` (pipeline parameter) |

This gives full audit context for every bad row.

---

### 5. Sink-Level Error Row Handling (Alternative / Complementary)

Each **Sink** transformation has an **Error row handling** tab with three modes:

| Mode | Behavior |
|---|---|
| **Fail on first error** | Pipeline fails immediately when any write error occurs. |
| **Continue on error** | Skips rows that fail to write; pipeline succeeds. |
| **Redirect error rows** | Writes failed rows to a separate output path (CSV/Parquet). |

> **Important:** Sink-level error handling catches *write-time* errors (e.g., type mismatch on insert, constraint violations at DB level). Assert-level errors are *transform-time* errors. Use **both** for complete coverage.

---

### 6. Monitoring & Alerting

- In the **Data Flow debug / run output**, each Assert transformation shows a row count for assertions passed vs. failed.
- Use **pipeline parameters** to pass the error file path dynamically (e.g., `@concat('errors/', pipeline().RunId, '.csv')`).
- After the Data Flow activity, add a **Web Activity** or **Logic App** call to notify teams if the error sink has rows (check via a subsequent `Get Metadata` or stored proc count).

---

### End-to-End Example Scenario

**Scenario:** You receive a daily CSV of customer orders. Rules:
- `order_id` must not be null.
- `amount` must be > 0.
- `customer_email` must be unique per file.

**Implementation Steps:**
1. Source → CSV file in ADLS Gen2.
2. Assert #1: `!isNull(order_id)` → error code `NULL_ORDER_ID`.
3. Assert #2: `amount > 0` → error code `NEGATIVE_AMOUNT`.
4. Assert #3: expect unique on `customer_email` → error code `DUPLICATE_EMAIL`.
5. Conditional Split → `!isError()` to valid stream, default to error stream.
6. Valid sink → Azure SQL DB `orders` table.
7. Error sink → ADLS `errors/orders_errors_@{pipeline().RunId}.csv` with columns: all original columns + `error_code`, `error_message`, `ingestion_timestamp`.
8. Post-activity → If error file row count > 0, send alert email.

---

### Cross Questions & Answers

**Q: What is the difference between using Assert vs. Conditional Split with Filter for validation?**

> Assert is the recommended approach because it:
> - Accumulates *multiple* assertion failures per row (a row can fail Assert #1 and Assert #2 simultaneously).
> - Stores structured metadata (`_errorCode`, `_errorMessage`) for auditing.
> - Allows a single stream to carry both valid and invalid rows until you explicitly split them.
>
> A plain `Conditional Split + Filter` can only capture one condition per branch and loses the failure reason metadata.

---

**Q: Can a single row fail multiple Assert transformations? How is that handled?**

> Yes. If a row fails multiple Assert transformations, ADF captures the *last* assertion failure's `_errorCode` and `_errorMessage` by default (the most recent assert that failed "wins"). To capture all failures, use a **Derived Column** after each Assert to append to a running error log column: `concat(error_log, '|', _errorCode)`.

---

**Q: What happens if you enable "Filter assert failures" on the Assert transformation?**

> The row is **removed from the stream entirely** at that Assert step and does not flow to the Conditional Split. You lose the ability to route it to an error sink. Only use this when you intentionally want to discard invalid rows with no audit trail.

---

**Q: How do you handle error rows in the sink when inserting into Azure SQL and there are DB-level constraint violations?**

> Use the **Redirect error rows** option in the Sink's Error row handling tab. Specify an output path (blob/ADLS) where rejected rows will land. These are write-time errors (PK violations, NOT NULL DB constraints, type overflows) and are separate from Assert transform-time errors. A complete solution uses Assert for business rules + Redirect error rows for DB-level constraint violations.

---

**Q: How can you alert the team when error rows are found?**

> After the Data Flow activity in the pipeline:
> 1. Add a **Get Metadata** activity on the error output folder.
> 2. Add an **If Condition** activity: `@greater(activity('GetMetadata').output.size, 0)`.
> 3. In the true branch, add a **Web Activity** calling a Logic App / Teams webhook / Azure Function to send an alert with the error file path and row count.

---

**Q: Can you use Assert with streaming sources (Event Hub / Kafka)?**

> No. Assert transformation is available only in **batch** Mapping Data Flows. For streaming validation in ADF, you would need to post-process using Azure Stream Analytics or handle validation in the downstream system.

---

**Q: How does the Assert transformation affect data flow performance?**

> Assert adds minimal overhead since it evaluates expressions row-by-row during the Spark execution. The `expect unique` type is more expensive as it requires a shuffle/group-by operation across the partition. For very large datasets, place uniqueness asserts after other filters to reduce the dataset size first.

---

## Q2. How do you dynamically pick file names from a source folder, filter only files whose names start with a specific string, and copy them to a sink with the same file name?

### Overview

This is a very common real-world pattern. The solution uses four ADF pipeline activities chained together:

1. **Get Metadata** — list all child items (files) in the source folder.
2. **Filter** — keep only items whose name starts with the required prefix.
3. **ForEach** — iterate over the filtered file list.
4. **Copy Activity** (inside ForEach) — copy each file using its dynamic name for both source and sink.

Datasets for both source and sink must be **parameterized** so the file name can be injected at runtime.

---

### Step 1 — Parameterize the Source and Sink Datasets

Both datasets must accept `fileName` and `folderPath` as parameters so they can be driven dynamically.

**Source Dataset Parameters:**

| Parameter | Type | Default |
|---|---|---|
| `sourceFolder` | String | `raw/input` |
| `sourceFileName` | String | (none) |

In the dataset's connection tab:
- **File path (folder):** `@dataset().sourceFolder`
- **File path (file):** `@dataset().sourceFileName`

**Sink Dataset Parameters:**

| Parameter | Type | Default |
|---|---|---|
| `sinkFolder` | String | `processed/output` |
| `sinkFileName` | String | (none) |

In the dataset's connection tab:
- **File path (folder):** `@dataset().sinkFolder`
- **File path (file):** `@dataset().sinkFileName`

---

### Step 2 — Pipeline Parameters

Define these at the pipeline level:

| Parameter | Example Value | Purpose |
|---|---|---|
| `p_sourceFolder` | `raw/input` | Source ADLS folder path |
| `p_sinkFolder` | `processed/output` | Sink ADLS folder path |
| `p_filePrefix` | `SALES_` | Only files starting with this are processed |

---

### Step 3 — Get Metadata Activity

**Purpose:** Retrieve the list of all files in the source folder.

| Setting | Value |
|---|---|
| Dataset | Source dataset (folder-level, no file name parameter yet) |
| Field list | `childItems` |

> `childItems` returns an array of objects: `[{ "name": "SALES_2024_01.csv", "type": "File" }, ...]`

**Activity output reference:** `@activity('GetMetadata').output.childItems`

---

### Step 4 — Filter Activity

**Purpose:** Retain only files whose name starts with `p_filePrefix` AND whose type is `File` (excludes sub-folders).

| Setting | Value |
|---|---|
| Items | `@activity('GetMetadata').output.childItems` |
| Condition | `@and(startsWith(item().name, pipeline().parameters.p_filePrefix), equals(item().type, 'File'))` |

**Output reference:** `@activity('Filter').output.value`
- This is the filtered array of file objects.

---

### Step 5 — ForEach Activity

**Purpose:** Loop over every file in the filtered list.

| Setting | Value |
|---|---|
| Items | `@activity('Filter').output.value` |
| Sequential | `false` (parallel; set batch count, e.g., 5) |

Inside ForEach, `@item()` refers to the current file object, so `@item().name` is the file name.

---

### Step 6 — Copy Activity (Inside ForEach)

**Source tab:**

| Setting | Value |
|---|---|
| Dataset | Parameterized source dataset |
| `sourceFolder` parameter | `@pipeline().parameters.p_sourceFolder` |
| `sourceFileName` parameter | `@item().name` |

**Sink tab:**

| Setting | Value |
|---|---|
| Dataset | Parameterized sink dataset |
| `sinkFolder` parameter | `@pipeline().parameters.p_sinkFolder` |
| `sinkFileName` parameter | `@item().name` |

> Using `@item().name` for both source and sink ensures the file is written with the **exact same name** to the destination.

---

### Full Pipeline Architecture

```
[Pipeline Parameters]
  p_sourceFolder = "raw/input"
  p_sinkFolder   = "processed/output"
  p_filePrefix   = "SALES_"

        │
        ▼
┌─────────────────────────────┐
│  Get Metadata Activity      │
│  Field list: childItems     │
│  Output: array of all files │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────┐
│  Filter Activity                                    │
│  Condition:                                         │
│  startsWith(item().name, p_filePrefix)              │
│    AND item().type == 'File'                        │
│  Output: filtered file array                        │
└────────────┬────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────┐
│  ForEach Activity                                   │
│  Items: @activity('Filter').output.value            │
│  Parallel (batch count: 5)                          │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │  Copy Activity                               │   │
│  │  Source file: @item().name                   │   │
│  │  Sink file:   @item().name  (same name)      │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
             │
             ▼
  Files copied to p_sinkFolder
  with identical names
```

---

### Alternative Approach: Wildcard in Copy Activity (Simpler, Less Control)

If you just need to copy matching files in one shot **without iterating individually**, use the **wildcard path** directly in the Copy Activity source:

| Setting | Value |
|---|---|
| Wildcard folder path | `@pipeline().parameters.p_sourceFolder` |
| Wildcard file name | `@concat(pipeline().parameters.p_filePrefix, '*')` |
| Copy behavior | Preserve hierarchy |

**Sink:**

| Setting | Value |
|---|---|
| File path | `@pipeline().parameters.p_sinkFolder` |

> **Limitation:** With the wildcard approach you cannot manipulate individual file names, add per-file logging, or apply per-file conditional logic. Use Get Metadata + ForEach when you need per-file control.

---

### Bonus: Appending Timestamp to Sink File Name (Common Variant)

Sometimes you need to preserve the original name but append a timestamp:

```
@concat(
  replace(item().name, '.csv', ''),
  '_',
  formatDateTime(utcNow(), 'yyyyMMddHHmmss'),
  '.csv'
)
```

This produces: `SALES_2024_01_20240427143022.csv`

---

### Cross Questions & Answers

**Q: What does `childItems` return and what fields does each item have?**

> `childItems` returns an array of objects. Each object has:
> - `name` — file or folder name (string)
> - `type` — `"File"` or `"Folder"`
>
> Example:
> ```json
> [
>   { "name": "SALES_Jan.csv", "type": "File" },
>   { "name": "SALES_Feb.csv", "type": "File" },
>   { "name": "HR_Jan.csv",    "type": "File" },
>   { "name": "archive",       "type": "Folder" }
> ]
> ```
> After the Filter with `startsWith(item().name, 'SALES_') AND type == 'File'`, only the first two remain.

---

**Q: Why use a separate Filter activity instead of filtering inside ForEach?**

> The **Filter** activity is a dedicated, efficient activity that outputs a clean filtered array. Filtering inside ForEach (using an If Condition to skip) wastes iterations — ForEach still loops over all items, spawning activity instances for skipped files too. Filter is cleaner and faster.

---

**Q: What happens if the source folder is empty or no files match the prefix? Does the pipeline fail?**

> No. If `Get Metadata` finds no children, `childItems` is an empty array `[]`. The `Filter` output is also `[]`. `ForEach` with an empty array simply executes zero iterations — the pipeline succeeds with nothing copied. To explicitly alert on this, add a check after Filter:
> ```
> If Condition: @equals(length(activity('Filter').output.value), 0)
> True branch: Fail activity or Web Activity (send alert)
> ```

---

**Q: Can the ForEach run in parallel? What is the maximum batch count?**

> Yes. By default ForEach is sequential. Enable **parallel** mode and set a **batch count** (1–50). ADF caps parallel inner activities at **50 concurrent iterations**. For file copy workloads, 5–20 is a practical sweet spot to avoid throttling on the linked service.

---

**Q: How do you preserve folder sub-structure when copying nested folders?**

> In the Copy Activity sink, set **Copy behavior** to `PreserveHierarchy` or `FlattenHierarchy`. For recursive folder traversal, enable **Recursive** on the source dataset and use `PreserveHierarchy` to replicate the same folder tree in the sink.

---

**Q: How do you capture which files were successfully copied and which failed?**

> Pattern:
> 1. Inside ForEach, wrap the Copy Activity in a **Try-Catch** pattern using activity dependency (on success vs. on failure).
> 2. On success → append `@item().name` to a success log array variable.
> 3. On failure → append `@item().name` + `@activity('CopyFile').error.message` to a failure log array variable.
> 4. After ForEach, write these arrays to a log table or blob using a stored procedure or another Copy Activity.
>
> Note: ADF variables cannot be set concurrently inside a parallel ForEach (race condition). Use **sequential** ForEach for logging, or log to an external store (e.g., Azure SQL) using a Stored Procedure activity directly inside the ForEach.

---

**Q: What is the difference between Get Metadata on a file vs. on a folder?**

> - **On a folder:** use `childItems` to list contents, `itemName`, `itemType`, `lastModified` of the folder itself.
> - **On a file:** use `itemName`, `itemType`, `size`, `lastModified`, `contentMD5` to get individual file metadata.
>
> For listing files, always point Get Metadata at the **folder** and request `childItems`. For validating a single known file exists before copying, point it at the **file** and check `itemType == 'File'`.

---

**Q: How would you filter files modified in the last 24 hours in addition to the prefix check?**

> Get Metadata with `childItems` does **not** return `lastModified` per child item (it only returns `name` and `type`). To filter by date, you need to either:
> 1. Call Get Metadata **on each individual file** inside a ForEach to read its `lastModified` — expensive.
> 2. Use an **Azure Function** or **Logic App** triggered by blob events to maintain a manifest file listing new arrivals.
> 3. Use **event-based triggers** (Storage Event Trigger) in ADF that fire when a blob is created/modified, so you only process files as they arrive — no date filtering needed.

---

## Q3. How do you incrementally copy new and changed files from Azure Blob Storage to Azure Blob Storage based on `lastModifiedDate`?

### Overview

This is the **incremental file ingestion** pattern — only files that are new or modified since the last successful run are copied. ADF supports this natively via the **Copy Activity's built-in `lastModifiedDatetimeStart` / `lastModifiedDatetimeEnd` filter** on blob sources, combined with a **watermark** stored in a persistent store (Azure SQL, Azure Table Storage, or a blob file).

Two main approaches:

| Approach | Best For |
|---|---|
| **Native `lastModifiedDate` filter in Copy Activity** | Simple blob-to-blob incremental copy; no custom code |
| **Watermark table in Azure SQL + Lookup + Copy** | Fine-grained control, auditing, multi-table patterns |

---

### Approach 1 — Native `lastModifiedDate` Filter (Recommended for Blob-to-Blob)

ADF's blob/ADLS source has built-in date filter settings:

| Property | Description |
|---|---|
| `modifiedDatetimeStart` | Include files with `lastModified >= this value` |
| `modifiedDatetimeEnd` | Include files with `lastModified < this value` |

Both accept dynamic expressions, making it straightforward to implement a sliding window.

#### Pipeline Design

```
[Pipeline Parameters]
  p_sourceContainer  = "raw"
  p_sourceFolder     = "input"
  p_sinkContainer    = "processed"
  p_sinkFolder       = "output"

[Variables]
  v_watermark_start  (String) — populated from watermark store
  v_watermark_end    (String) — set to utcNow() at pipeline start
```

#### Activity Chain

```
┌──────────────────────────────────────────┐
│  Lookup Activity                         │
│  → Read last watermark from SQL table    │
│    SELECT MAX(last_run_time)             │
│    FROM watermark_table                  │
│    WHERE pipeline_name = 'BlobIncremental│
└──────────────┬───────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│  Set Variable: v_watermark_end           │
│  Value: @utcNow()                        │
└──────────────┬───────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────┐
│  Copy Activity                                                   │
│  Source: Blob/ADLS dataset                                       │
│    Wildcard folder: p_sourceFolder                               │
│    Wildcard file:   *                                            │
│    modifiedDatetimeStart: @activity('LookupWatermark').output    │
│                            .firstRow.last_run_time               │
│    modifiedDatetimeEnd:   @variables('v_watermark_end')          │
│  Sink: Blob/ADLS dataset                                         │
│    Folder: p_sinkFolder                                          │
│    Copy behavior: PreserveHierarchy                              │
└──────────────┬───────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│  Stored Procedure Activity               │
│  → Update watermark table:              │
│    UPDATE watermark_table               │
│    SET last_run_time = v_watermark_end  │
│    WHERE pipeline_name = '...'          │
└──────────────────────────────────────────┘
```

---

### Watermark Table Schema (Azure SQL)

```sql
CREATE TABLE watermark_table (
    pipeline_name   VARCHAR(100) PRIMARY KEY,
    last_run_time   DATETIME2   NOT NULL,
    updated_at      DATETIME2   DEFAULT GETUTCDATE()
);

-- Seed the initial watermark (first-ever run picks up all files)
INSERT INTO watermark_table (pipeline_name, last_run_time)
VALUES ('BlobIncrementalCopy', '1900-01-01 00:00:00');
```

---

### Copy Activity Source Configuration (Key Settings)

In the source dataset's **Additional columns** / Copy Activity source tab:

```json
{
  "modifiedDatetimeStart": "@activity('LookupWatermark').output.firstRow.last_run_time",
  "modifiedDatetimeEnd":   "@variables('v_watermark_end')",
  "recursive": true,
  "wildcardFolderPath": "@pipeline().parameters.p_sourceFolder",
  "wildcardFileName": "*"
}
```

> Setting `recursive: true` picks up files in sub-folders as well. Remove it or set `false` to restrict to the top-level folder only.

---

### Watermark Update — Stored Procedure

Always update the watermark **only after a successful copy**. Link the Stored Procedure activity with **"On success"** dependency from the Copy Activity.

```sql
CREATE PROCEDURE usp_update_watermark
    @pipeline_name  VARCHAR(100),
    @new_watermark  DATETIME2
AS
BEGIN
    UPDATE watermark_table
    SET    last_run_time = @new_watermark,
           updated_at    = GETUTCDATE()
    WHERE  pipeline_name = @pipeline_name;
END
```

Stored Procedure Activity parameters:

| Parameter | Value |
|---|---|
| `@pipeline_name` | `BlobIncrementalCopy` |
| `@new_watermark` | `@variables('v_watermark_end')` |

---

### Full End-to-End Flow Diagram

```
  ┌─────────────────────────────────────────────────────────────────┐
  │  PIPELINE: BlobIncrementalCopy  (scheduled: every 1 hour)      │
  │                                                                 │
  │  ① Lookup Watermark                                            │
  │     SQL: SELECT last_run_time FROM watermark_table             │
  │     Result: e.g., "2026-04-27 08:00:00"                        │
  │                       │                                         │
  │                       ▼                                         │
  │  ② Set Variable: v_watermark_end = utcNow()                    │
  │     e.g., "2026-04-27 09:00:00"                                │
  │                       │                                         │
  │                       ▼                                         │
  │  ③ Copy Activity                                               │
  │     Source filter:                                              │
  │       lastModified >= "2026-04-27 08:00:00"   (watermark_start)│
  │       lastModified <  "2026-04-27 09:00:00"   (watermark_end)  │
  │     → Only files modified in that 1-hour window are copied     │
  │                       │                                         │
  │              [On Success]                                       │
  │                       │                                         │
  │                       ▼                                         │
  │  ④ Update Watermark                                            │
  │     SET last_run_time = "2026-04-27 09:00:00"                  │
  │                                                                 │
  │  Next run: window = 09:00 → 10:00                              │
  └─────────────────────────────────────────────────────────────────┘

  Azure Blob (source)              Azure Blob (sink)
  ┌─────────────────┐              ┌─────────────────┐
  │ raw/input/      │   Copy only  │ processed/      │
  │   file_A.csv    │──(modified ──│   file_A.csv    │
  │   file_B.csv    │   in window) │   file_C.csv    │
  │   file_C.csv    │─────────────►│                 │
  └─────────────────┘              └─────────────────┘
```

---

### Approach 2 — No SQL Watermark (Stateless Window via Pipeline Parameters)

If you don't have an Azure SQL linked service, pass the window explicitly as pipeline parameters (driven by a **tumbling window trigger**):

| Parameter | Value (from trigger) |
|---|---|
| `p_windowStart` | `@trigger().outputs.windowStartTime` |
| `p_windowEnd` | `@trigger().outputs.windowEndTime` |

Copy Activity source:
```
modifiedDatetimeStart: @pipeline().parameters.p_windowStart
modifiedDatetimeEnd:   @pipeline().parameters.p_windowEnd
```

The **Tumbling Window Trigger** automatically manages non-overlapping time windows and handles backfill/rerun natively — no watermark table needed.

---

### Approach 3 — Event-Based Trigger (Real-Time, No Polling)

For near-real-time incremental copy, use a **Storage Event Trigger**:

- Fires when a blob is **Created** or **Deleted** in a specified container/folder.
- The trigger passes `@triggerBody().fileName` and `@triggerBody().folderPath` to the pipeline.
- The Copy Activity copies that exact single file — no date filtering needed.

> **Limitation:** Only fires on new blobs. If an existing blob is overwritten, it triggers only if the blob service raises a `BlobCreated` event (which it does on overwrite too in Azure).

---

### Choosing the Right Approach

```
Is your pipeline scheduled (batch)?
  ├── YES → Do you have Azure SQL available?
  │           ├── YES → Use Watermark Table + Lookup + Copy + Stored Proc (Approach 1)
  │           └── NO  → Use Tumbling Window Trigger (Approach 2)
  └── NO  → Do you need near-real-time copy?
              └── YES → Use Storage Event Trigger (Approach 3)
```

---

### Cross Questions & Answers

**Q: What is the risk of using `utcNow()` as the watermark end, and how do you handle it?**

> `utcNow()` is evaluated at the moment the activity runs. If the Copy Activity takes 30 minutes, files created *during* the copy run fall between `watermark_end` and the actual copy-finish time — they may be missed in the next window if the watermark is updated to `watermark_end` (the time *before* copy started), not to *now*.
>
> **Solution:** Capture `utcNow()` **once at pipeline start** in a Set Variable activity and use that fixed value throughout the pipeline for both the copy filter and the watermark update. Never call `utcNow()` twice in the same pipeline run.

---

**Q: What happens if the pipeline fails during the Copy Activity — does the watermark get updated?**

> No — and that is by design. The Stored Procedure (watermark update) is linked with **"On success"** dependency. If Copy fails, the watermark stays at the previous value. On the next run, the same time window is re-processed, effectively providing **at-least-once** delivery semantics.

---

**Q: What if a file is uploaded twice within the same watermark window — does it get copied twice to the sink?**

> The Copy Activity copies each file once per run (based on its path). If the same file is overwritten in the source within the window, ADF copies the latest version. The sink blob is overwritten. You do not get duplicates — only the current version at the time of copy.

---

**Q: Can the native `lastModifiedDate` filter work with ADLS Gen2, or only Azure Blob Storage?**

> It works with both **Azure Blob Storage** and **ADLS Gen2** (Azure Data Lake Storage Gen2) linked services. It does **not** work with on-premise file systems or SFTP sources natively — for those, use Get Metadata + ForEach with per-file `lastModified` checks.

---

**Q: How does a Tumbling Window Trigger differ from a Schedule Trigger for incremental loads?**

| Feature | Schedule Trigger | Tumbling Window Trigger |
|---|---|---|
| Window boundaries exposed | No | Yes (`windowStartTime`, `windowEndTime`) |
| Handles backfill/rerun | No (runs at current time) | Yes (replays missed windows) |
| Concurrency control | No | Yes (max concurrent runs configurable) |
| Best for | Simple scheduled runs | Incremental loads, time-partitioned data |

> For incremental copy, **always prefer Tumbling Window Trigger** — it gives you reliable, non-overlapping windows and automatic backfill.

---

**Q: How do you handle the very first run (cold start) where no watermark exists yet?**

> Seed the watermark table with a sentinel date far in the past (e.g., `'1900-01-01'`) during deployment. The first pipeline run will use this as `modifiedDatetimeStart`, which effectively means "all files ever modified" — a full initial load. All subsequent runs are incremental.

---

**Q: What is the difference between `modifiedDatetimeStart` / `modifiedDatetimeEnd` and using a Filter activity on `lastModified`?**

> - **`modifiedDatetimeStart/End` in Copy Activity source:** Pushed down to the storage service — ADF requests only matching files from the API. More efficient; no extra activity needed.
> - **Filter activity on `lastModified`:** Requires Get Metadata → ForEach → per-file Get Metadata (to get `lastModified`) → Filter. Much more overhead; only use when you also need other per-file metadata actions.
>
> Always prefer the native source filter for simple blob-to-blob incremental copy.

---

**Q: How do you handle deleted files — i.e., files removed from source that should also be removed from sink?**

> The native `lastModifiedDate` filter does **not** detect deletions (deleted blobs have no metadata). Options:
> 1. **Enable Delete Activity** after copy — list source files and compare with sink using Get Metadata + set difference logic.
> 2. **Soft-delete pattern** — mark files as deleted in a metadata table; a separate cleanup pipeline reads the table and issues Delete Activity on the sink.
> 3. **Storage lifecycle policies** — use Azure Blob lifecycle management to expire/archive old sink blobs independently.

---

## Q4. How do you delete old files from an Azure Storage Account using ADF?

### Overview

ADF provides a dedicated **Delete Activity** for removing files from supported storage stores (Azure Blob Storage, ADLS Gen2, Amazon S3, file system, etc.). Deletion patterns in ADF typically fall into three categories:

| Pattern | Use Case |
|---|---|
| **Delete after successful copy** | Archive/move pattern — copy then clean source |
| **Delete files older than N days** | Retention/housekeeping — purge stale files |
| **Delete specific files by name** | Targeted cleanup based on a manifest or list |

---

### The Delete Activity — Key Properties

| Property | Description |
|---|---|
| **Dataset** | Points to the storage location (blob container + folder) |
| **Recursive** | If `true`, deletes files in all sub-folders |
| **Max concurrent connections** | Throttle parallel delete calls to the storage API |
| **Enable logging** | Writes a log of deleted file names to a specified blob path |
| **Log storage linked service** | Where the delete log is written |
| **Log folder path** | Path for the delete audit log |

> **Important:** The Delete Activity permanently removes blobs. It does **not** move them to the recycle bin. Always enable logging so you have an audit trail of what was deleted.

---

### Pattern 1 — Delete Files After Successful Copy (Move Pattern)

Copy a file to the destination, then delete it from the source only if the copy succeeded.

#### Pipeline Design

```
[Copy Activity] ──(On Success)──► [Delete Activity]
                ──(On Failure)──► [Fail / Alert]
```

#### Delete Activity Configuration

| Setting | Value |
|---|---|
| Dataset | Source dataset (same as Copy source) |
| Folder path | `@pipeline().parameters.p_sourceFolder` |
| File name | `@item().name` (inside ForEach) or wildcard `*` |
| Recursive | `false` (single folder) |
| Enable logging | `true` |
| Log path | `logs/delete/@{pipeline().RunId}/` |

#### Full Move Pattern (ForEach-based)

```
[Get Metadata]
    │ childItems
    ▼
[Filter]  ← optional prefix/type filter
    │
    ▼
[ForEach] (parallel, batch: 5)
    │
    ├─ [Copy Activity]  source → sink
    │       │
    │   (On Success)
    │       │
    └─ [Delete Activity]  delete source file
```

---

### Pattern 2 — Delete Files Older Than N Days (Retention Cleanup)

ADF's Delete Activity itself does not have a built-in age filter. You must combine **Get Metadata** (per-file `lastModified`) or the **native `modifiedDatetimeEnd`** filter to identify old files, then delete them.

#### Sub-approach A — Using `modifiedDatetimeEnd` (Simplest)

Set the dataset's `modifiedDatetimeEnd` to `N days ago`. The Delete Activity will only delete files whose `lastModified` is **before** that cutoff.

```
Cutoff expression (30-day retention):
@addDays(utcNow(), -30)
```

Delete Activity source dataset settings:
```
modifiedDatetimeEnd: @addDays(utcNow(), -30)
wildcardFolderPath:  @pipeline().parameters.p_folder
wildcardFileName:    *
recursive:           true
```

This is the cleanest and most efficient approach — no ForEach needed.

#### Sub-approach B — Get Metadata + ForEach + If Condition + Delete

Use this when you need **conditional logic per file** (e.g., different retention per file type):

```
[Get Metadata: childItems]
         │
         ▼
[ForEach all files]
         │
    [Get Metadata: lastModified of @item().name]
         │
    [If Condition]
    lastModified < addDays(utcNow(), -30)
         │
    (true branch)
         │
    [Delete Activity: delete @item().name]
```

> This approach makes one Get Metadata API call per file — expensive for large folders. Prefer Sub-approach A unless per-file logic is required.

---

### Pattern 3 — Delete Specific Files by Name (Manifest-Driven)

When you have a list of file names to delete (e.g., from a SQL table or a metadata file):

```
[Lookup Activity]
SQL: SELECT file_name FROM files_to_delete WHERE status = 'processed'
         │
         ▼
[ForEach: @activity('Lookup').output.value]
         │
    [Delete Activity]
    file: @item().file_name
    folder: @pipeline().parameters.p_folder
```

---

### Full Architecture — Retention Cleanup Pipeline

```
┌───────────────────────────────────────────────────────────────┐
│  PIPELINE: BlobRetentionCleanup  (scheduled: daily at 02:00)  │
│                                                               │
│  Parameters:                                                  │
│    p_folder         = "raw/input"                             │
│    p_retentionDays  = 30                                      │
│                                                               │
│  ① Set Variable: v_cutoff_date                               │
│     = @addDays(utcNow(), -pipeline().parameters.p_retentionDays)│
│                                                               │
│  ② Delete Activity                                            │
│     Dataset folder:         p_folder                          │
│     Wildcard file:          *                                 │
│     modifiedDatetimeEnd:    v_cutoff_date                     │
│     Recursive:              true                              │
│     Enable logging:         true                              │
│     Log path: logs/cleanup/@{pipeline().RunId}/               │
│                                                               │
│  ③ (On Success) Get Metadata on log file                     │
│     → Read count of deleted files                             │
│                                                               │
│  ④ Web Activity → send Teams/email alert with deleted count  │
└───────────────────────────────────────────────────────────────┘

  Azure Blob Storage
  ┌──────────────────────────────────────────────────┐
  │  raw/input/                                      │
  │    file_jan.csv   (lastModified: 30+ days ago) ──┼──► DELETED
  │    file_feb.csv   (lastModified: 30+ days ago) ──┼──► DELETED
  │    file_apr.csv   (lastModified: 5 days ago)     │    kept
  └──────────────────────────────────────────────────┘
```

---

### Delete Activity — Logging Output

When **Enable logging** is turned on, ADF writes a CSV/JSON file to the specified log path after deletion. Each row contains:

| Field | Description |
|---|---|
| `FileName` | Full path of the deleted blob |
| `Status` | `Deleted` or `Failed` |
| `ErrorCode` | Error code if deletion failed |
| `ErrorMessage` | Error detail if deletion failed |

Log path example: `logs/delete/pipeline-run-id/delete_log.csv`

> Always enable logging in production. It is your only audit record of what was permanently removed.

---

### Safety Best Practices

| Practice | Reason |
|---|---|
| Always enable Delete Activity logging | Permanent operation — need audit trail |
| Use `modifiedDatetimeEnd` filter precisely | Avoid accidentally deleting recent files |
| Test with a dry run (copy instead of delete first) | Validate the file selection before actual deletion |
| Add an `If Condition` before Delete | Confirm file count > 0 before proceeding |
| Use "On Success" dependency from upstream Copy | Never delete source until copy is confirmed |
| Store `p_retentionDays` as a pipeline parameter | Easy to adjust without code changes |

---

### Cross Questions & Answers

**Q: Does the Delete Activity support all storage types?**

> Yes, the Delete Activity supports:
> - Azure Blob Storage
> - Azure Data Lake Storage Gen1 and Gen2
> - Amazon S3
> - Google Cloud Storage
> - File System (on-premise via Self-hosted IR)
> - FTP / SFTP
>
> It does **not** support deleting rows from databases (use a Stored Procedure or Script Activity for that).

---

**Q: What happens if the Delete Activity tries to delete a file that no longer exists?**

> By default it **does not fail** — ADF skips files that are not found. The log will show `Status: Skipped` or simply omit that file. This makes the Delete Activity idempotent — safe to re-run.

---

**Q: Can you delete an entire folder (not just files) with the Delete Activity?**

> No. The Delete Activity deletes **files** (blobs), not folder objects. In Azure Blob Storage, folders are virtual (they don't exist as independent objects). Setting `recursive: true` and pointing to a folder path deletes all blobs within it, which effectively makes the folder disappear since it has no contents. You cannot delete a folder while keeping files inside it.

---

**Q: How do you implement an archive-then-delete pattern (move files to archive before deleting from source)?**

> Use this pipeline sequence:
> ```
> [Copy Activity]  source → archive container
>       │ (On Success)
>       ▼
> [Delete Activity]  delete from source container
> ```
> Both activities share the same file name parameter (`@item().name`), giving you a safe move with a copy-then-delete guarantee. If Copy fails, Delete never runs.

---

**Q: What is the maximum number of files the Delete Activity can handle in a single run?**

> There is no hard documented limit on the number of files. Performance depends on:
> - The `maxConcurrentConnections` setting (controls parallel API calls to storage)
> - The storage account's request rate limits (Azure Blob: ~20,000 requests/sec per account)
> - For very large file counts (millions of blobs), consider batching with pagination via Lookup + ForEach rather than a single wildcard delete.

---

**Q: How do you prevent accidental deletion of files currently being written to (in-flight files)?**

> Set `modifiedDatetimeEnd` to at least **1 hour ago** (not `utcNow()`). This creates a buffer window so files being actively written are never in scope:
> ```
> @addHours(utcNow(), -1)
> ```
> For stricter guarantees, add a file-age minimum (e.g., only delete files older than 24 hours):
> ```
> @addDays(utcNow(), -1)
> ```

---

**Q: Can you use Azure Blob Lifecycle Management policies instead of ADF Delete Activity?**

> Yes, and for pure retention/expiry use cases it is often **better** — no pipeline needed, no ADF costs, native to storage:
> - Lifecycle policies can automatically **tier** blobs (Hot → Cool → Archive) or **delete** them based on age, last accessed time, or prefix.
> - **Limitation:** Lifecycle policies run on Azure's schedule (not your pipeline schedule) and cannot integrate with your pipeline's success/failure logic.
>
> **Rule of thumb:** Use Lifecycle Management for passive background retention. Use ADF Delete Activity when deletion must be coordinated with pipeline steps (e.g., delete only after copy succeeds).

---

## Q5. How do you process fixed-length text files in ADF Mapping Data Flows?

### Overview

A **fixed-length (flat) file** is a text file where each field occupies a predetermined, fixed number of characters — there is no delimiter. Each record is a fixed-width row and fields are identified purely by their **start position** and **length**.

Example fixed-length record (40 chars per row):

```
JOHN      SMITH     19850312000123456USD
JANE      DOE       19920715000987654GBP
```

Field layout:
```
Position  1–10   : first_name  (10 chars)
Position 11–20   : last_name   (10 chars)
Position 21–28   : dob         (8 chars, YYYYMMDD)
Position 29–38   : account_no  (10 chars)
Position 39–41   : currency    (3 chars)
```

ADF **does not have a native fixed-width file format**. The standard approach is:

1. Read the file as a **single-column raw text** (each row = one string).
2. Use **`substring()`** in a Derived Column transformation to slice each field out by position and length.
3. Cast and transform columns as needed.
4. Write to the target sink.

---

### Step 1 — Source Dataset Configuration

Set up a **DelimitedText** dataset but configure it to treat each line as a single raw column.

| Setting | Value |
|---|---|
| Format | DelimitedText |
| Column delimiter | **None** (or use a character that never appears, e.g., `~`) |
| Row delimiter | `\n` (or `\r\n` for Windows files) |
| First row as header | `false` |
| Escape character | None |
| Quote character | None |

> This loads each line as a single column named `Column_1` (ADF's auto-generated name). Every row value is the entire raw line as a string.

In the Data Flow source settings:
- Enable **"Allow schema drift"** — since there are no headers, schema is inferred as one column.
- Set the source **column name** (in the projection tab) to something meaningful, e.g., `raw_line`.

---

### Step 2 — Derived Column Transformation (Field Extraction)

Use the **`substring(str, start, length)`** expression function to extract each field.

> **Important:** ADF's `substring()` is **1-based** (first character is position 1).

#### Expression Syntax

```
substring(column_name, start_position, length)
```

#### Field Extraction for the Example Above

| New Column | Expression | Notes |
|---|---|---|
| `first_name` | `trim(substring(raw_line, 1, 10))` | trim trailing spaces |
| `last_name` | `trim(substring(raw_line, 11, 10))` | trim trailing spaces |
| `dob_raw` | `substring(raw_line, 21, 8)` | raw string `19850312` |
| `account_no` | `trim(substring(raw_line, 29, 10))` | |
| `currency` | `trim(substring(raw_line, 39, 3))` | |

Add all these in a single **Derived Column** transformation.

---

### Step 3 — Data Type Casting

After extraction, fields are still strings. Use another **Derived Column** (or the same one) to cast:

| Column | Cast Expression |
|---|---|
| `dob` | `toDate(dob_raw, 'yyyyMMdd')` |
| `account_no_int` | `toLong(account_no)` |

Or use a **Cast** transformation (available in newer ADF versions) to apply type conversions cleanly.

---

### Step 4 — Filter Blank / Header Rows

Fixed-length files sometimes have:
- **Header rows** (positional field names in row 1)
- **Trailer rows** (record counts, checksums in last row)
- **Blank rows**

Filter these out using a **Filter** transformation:

```
# Remove blank rows
length(trim(raw_line)) > 0

# Remove header row if first field is literal "FIRSTNAME"
AND trim(substring(raw_line, 1, 10)) != 'FIRSTNAME '

# Remove trailer row if it starts with 'TRAILER'
AND left(raw_line, 7) != 'TRAILER'
```

Place the **Filter** transformation **before** the Derived Column to avoid parsing errors on non-data rows.

---

### Full Data Flow Architecture

```
[Source: DelimitedText]
  No delimiter → each line = raw_line (one column)
         │
         ▼
[Filter Transformation]
  Remove blank lines, header row, trailer row
         │
         ▼
[Derived Column #1: Extract Fields]
  first_name  = trim(substring(raw_line, 1, 10))
  last_name   = trim(substring(raw_line, 11, 10))
  dob_raw     = substring(raw_line, 21, 8)
  account_no  = trim(substring(raw_line, 29, 10))
  currency    = trim(substring(raw_line, 39, 3))
         │
         ▼
[Derived Column #2: Cast Types]
  dob         = toDate(dob_raw, 'yyyyMMdd')
  account_no  = toLong(account_no)
         │
         ▼
[Select Transformation]
  Drop: raw_line, dob_raw  (intermediate columns)
  Keep: first_name, last_name, dob, account_no, currency
         │
         ▼
[Assert Transformation]  ← optional data quality checks
  !isNull(account_no)
  length(currency) == 3
         │
         ▼
[Conditional Split]
  valid  → Target Sink (Azure SQL / ADLS Parquet)
  errors → Error Sink (CSV with error_code, error_message)
```

---

### Handling Variable Record Types in the Same File

Some fixed-length files have **multiple record types** identified by a **record type indicator** (RTI) — a flag in a fixed position that tells you which layout to apply.

Example:
```
H2026042700001                      ← Header record (starts with 'H')
D JOHN      SMITH     19850312...   ← Detail record (starts with 'D')
D JANE      DOE       19920715...   ← Detail record
T000002                             ← Trailer record (starts with 'T')
```

#### Processing Steps

```
[Source] → [Derived Column: extract record_type = substring(raw_line, 1, 1)]
                │
                ▼
         [Conditional Split]
           ├── record_type == 'H'  → [Header Stream]  → parse header fields
           ├── record_type == 'D'  → [Detail Stream]  → parse detail fields → Sink
           └── record_type == 'T'  → [Trailer Stream] → parse control totals → validate counts
```

Each stream applies its own Derived Column transformation with the correct field positions for that record type.

---

### Trailer Record Validation (Common Pattern)

The trailer row often contains a **record count** for reconciliation. Validate it in the data flow:

```
[Detail Stream]
    │
    ▼
[Aggregate: count all detail rows → v_actual_count]
    │
    ▼
[Join with Trailer Stream on constant key]
    │
    ▼
[Assert: v_actual_count == trailer_count]
  Error if mismatch → fail pipeline
```

---

### Pipeline-Level Setup

```
[Pipeline]
  │
  ▼
[Get Metadata]  → verify file exists + size > 0
  │ (On Success)
  ▼
[Data Flow Activity]
  Parameters:
    p_sourceFile: @pipeline().parameters.p_sourceFile
    p_sinkTable:  @pipeline().parameters.p_sinkTable
  │
  ▼
[Stored Procedure]  → log run status, row counts
```

---

### Key ADF Expression Functions for Fixed-Width Parsing

| Function | Syntax | Description |
|---|---|---|
| `substring` | `substring(str, start, length)` | Extract by position (1-based) |
| `trim` | `trim(str)` | Remove leading/trailing spaces |
| `ltrim` / `rtrim` | `ltrim(str)` / `rtrim(str)` | Remove left/right spaces only |
| `left` | `left(str, n)` | First n characters |
| `right` | `right(str, n)` | Last n characters |
| `length` | `length(str)` | String length |
| `toDate` | `toDate(str, 'yyyyMMdd')` | Parse date string |
| `toLong` | `toLong(str)` | Cast to long integer |
| `toDecimal` | `toDecimal(str, precision, scale)` | Cast to decimal |
| `regexReplace` | `regexReplace(str, pattern, replacement)` | Regex-based cleanup |
| `iif` | `iif(condition, trueVal, falseVal)` | Inline conditional |

---

### Cross Questions & Answers

**Q: Why can't you just use a DelimitedText dataset with a fixed-width format directly?**

> ADF's DelimitedText format requires a **delimiter character** to separate fields. Fixed-length files have no delimiter — fields are defined purely by position. There is no native fixed-width format connector in ADF (unlike SSIS, which has a built-in fixed-width flat file connection manager). The workaround is to ingest the raw line as a single string and use `substring()` to extract fields.

---

**Q: What if the file has EBCDIC encoding (mainframe files)?**

> ADF does not natively decode EBCDIC. Steps:
> 1. Pre-process the file using an **Azure Function** or **Databricks notebook** to convert EBCDIC → UTF-8 before ADF ingests it.
> 2. Or use a **Self-Hosted Integration Runtime** with a custom converter script as a pre-step.
>
> Once converted to UTF-8, the standard fixed-width substring approach applies.

---

**Q: How do you handle packed decimal (computational-3) fields in mainframe fixed-length files?**

> Packed decimal is a binary encoding where two decimal digits are stored per byte. ADF cannot decode binary packed fields with `substring()` (which works on text characters). This again requires pre-processing via Azure Function, Databricks, or a custom SSIS package before ADF can work with the file.

---

**Q: The fixed-length file has 1 million rows. Will Mapping Data Flows handle this efficiently?**

> Yes. Mapping Data Flows run on **Apache Spark** clusters. One million rows is well within its capability — Spark partitions the file and processes rows in parallel across the cluster. Key performance tips:
> - Use **Auto Resolve Integration Runtime** and let ADF scale the Spark cluster.
> - Avoid row-by-row ForEach + Copy patterns for this — always use Data Flows for column-level transformation at scale.
> - Use **Parquet or Delta** as the sink format for downstream consumption.

---

**Q: How do you validate that every row in the fixed-length file is exactly the expected length?**

> Add an **Assert transformation** before the Derived Column:
> ```
> Type: expect true
> Condition: length(raw_line) == 41
> Error code: 'INVALID_ROW_LENGTH'
> Error message: concat('Expected 41 chars, got: ', toString(length(raw_line)))
> ```
> Route failed rows to the error sink; only rows with the correct length proceed to field extraction.

---

**Q: How do you handle numeric fields that are right-justified and zero-padded (e.g., `0000123456`)?**

> Use `toLong()` or `toInteger()` directly — these functions automatically strip leading zeros when casting:
> ```
> toLong(trim(substring(raw_line, 29, 10)))
> ```
> `trim()` removes spaces; `toLong()` converts `"0000123456"` → `123456`.

---

**Q: Can you use a pipeline parameter to pass the field layout (positions and lengths) dynamically?**

> Not natively — ADF expressions are static at design time; you cannot pass an array of `{fieldName, start, length}` and have the Derived Column generate columns dynamically at runtime (the column names must be known at design time).
>
> Workarounds:
> 1. **Parameterize by file type:** Use separate data flows per layout (one per record type/version) and switch between them using an If Condition or Switch activity in the pipeline.
> 2. **Use Databricks/Synapse Spark:** If layouts change frequently, use a metadata-driven Spark job where the layout is stored in a config table and applied dynamically.
> 3. **Schema drift + dynamic expressions:** If only values change (not field counts), use pipeline parameters to drive `substring` start/length values inside expressions.

---

## Q6. How do you log pipeline execution details to an Azure SQL table in ADF?

### Overview

ADF does not persist execution details (row counts, durations, errors) outside of Azure Monitor / Activity Runs API by default. To build a **custom audit/logging framework**, you write execution metadata to an Azure SQL table using **Stored Procedure Activity** or **Script Activity** at strategic points in the pipeline:

- **Pipeline start** → insert a "running" row.
- **Pipeline success** → update the row with success status, row counts, duration.
- **Pipeline failure** → update the row with failure status and error message.

This gives you a queryable, persistent execution history independent of ADF's 45-day retention limit.

---

### Step 1 — Create the Log Table in Azure SQL

```sql
CREATE TABLE pipeline_execution_log (
    log_id              INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name       VARCHAR(200)    NOT NULL,
    pipeline_run_id     VARCHAR(100)    NOT NULL,
    trigger_name        VARCHAR(200),
    trigger_type        VARCHAR(50),
    start_time          DATETIME2       NOT NULL,
    end_time            DATETIME2,
    status              VARCHAR(20)     NOT NULL,   -- RUNNING | SUCCESS | FAILED
    rows_read           BIGINT          DEFAULT 0,
    rows_written        BIGINT          DEFAULT 0,
    rows_skipped        BIGINT          DEFAULT 0,
    error_code          VARCHAR(100),
    error_message       NVARCHAR(MAX),
    source_name         VARCHAR(200),
    sink_name           VARCHAR(200),
    additional_info     NVARCHAR(MAX),  -- JSON blob for extra metadata
    created_at          DATETIME2       DEFAULT GETUTCDATE()
);
```

---

### Step 2 — Stored Procedures

#### 2a. Insert Log on Pipeline Start

```sql
CREATE PROCEDURE usp_log_pipeline_start
    @pipeline_name    VARCHAR(200),
    @pipeline_run_id  VARCHAR(100),
    @trigger_name     VARCHAR(200),
    @trigger_type     VARCHAR(50),
    @start_time       DATETIME2,
    @source_name      VARCHAR(200),
    @sink_name        VARCHAR(200)
AS
BEGIN
    INSERT INTO pipeline_execution_log
        (pipeline_name, pipeline_run_id, trigger_name, trigger_type,
         start_time, status, source_name, sink_name)
    VALUES
        (@pipeline_name, @pipeline_run_id, @trigger_name, @trigger_type,
         @start_time, 'RUNNING', @source_name, @sink_name);
END;
```

#### 2b. Update Log on Pipeline Success

```sql
CREATE PROCEDURE usp_log_pipeline_success
    @pipeline_run_id  VARCHAR(100),
    @end_time         DATETIME2,
    @rows_read        BIGINT,
    @rows_written     BIGINT,
    @rows_skipped     BIGINT,
    @additional_info  NVARCHAR(MAX)
AS
BEGIN
    UPDATE pipeline_execution_log
    SET    end_time       = @end_time,
           status         = 'SUCCESS',
           rows_read      = @rows_read,
           rows_written   = @rows_written,
           rows_skipped   = @rows_skipped,
           additional_info = @additional_info
    WHERE  pipeline_run_id = @pipeline_run_id;
END;
```

#### 2c. Update Log on Pipeline Failure

```sql
CREATE PROCEDURE usp_log_pipeline_failure
    @pipeline_run_id  VARCHAR(100),
    @end_time         DATETIME2,
    @error_code       VARCHAR(100),
    @error_message    NVARCHAR(MAX)
AS
BEGIN
    UPDATE pipeline_execution_log
    SET    end_time      = @end_time,
           status        = 'FAILED',
           error_code    = @error_code,
           error_message = @error_message
    WHERE  pipeline_run_id = @pipeline_run_id;
END;
```

---

### Step 3 — Pipeline Design Pattern

Every pipeline that needs logging follows this structure:

```
┌─────────────────────────────────────────────────────────────────────┐
│  PIPELINE: MyDataPipeline                                           │
│                                                                     │
│  [Stored Procedure: usp_log_pipeline_start]   ← first activity     │
│    @pipeline_name   = pipeline().Pipeline                           │
│    @pipeline_run_id = pipeline().RunId                              │
│    @trigger_name    = pipeline().TriggerName                        │
│    @trigger_type    = pipeline().TriggerType                        │
│    @start_time      = utcNow()                                      │
│    @source_name     = 'raw/input'                                   │
│    @sink_name       = 'processed/output'                            │
│           │                                                         │
│           ▼                                                         │
│  [Core Activities]                                                  │
│  (Copy / Data Flow / ForEach / etc.)                                │
│           │                                                         │
│     ┌─────┴──────┐                                                  │
│  (On Success) (On Failure)                                          │
│     │              │                                                │
│     ▼              ▼                                                │
│  [SP: Success]  [SP: Failure]                                       │
│  usp_log_       usp_log_                                            │
│  pipeline_      pipeline_                                           │
│  success        failure                                             │
└─────────────────────────────────────────────────────────────────────┘
```

#### Activity Dependency Configuration

| Activity | Depends On | Condition |
|---|---|---|
| Start Log SP | — | — |
| Core Activities | Start Log SP | On Success |
| Success Log SP | Core Activities | On Success |
| Failure Log SP | Core Activities | On Failure |

> Both the Success and Failure SPs depend on the **core activities**, not on each other. Only one will execute per run.

---

### Step 4 — Stored Procedure Activity Parameters (ADF Expressions)

#### Start Log SP Parameters

| SP Parameter | ADF Expression |
|---|---|
| `@pipeline_name` | `@pipeline().Pipeline` |
| `@pipeline_run_id` | `@pipeline().RunId` |
| `@trigger_name` | `@pipeline().TriggerName` |
| `@trigger_type` | `@pipeline().TriggerType` |
| `@start_time` | `@utcNow()` |
| `@source_name` | `@pipeline().parameters.p_sourceFolder` |
| `@sink_name` | `@pipeline().parameters.p_sinkFolder` |

#### Success Log SP Parameters

| SP Parameter | ADF Expression |
|---|---|
| `@pipeline_run_id` | `@pipeline().RunId` |
| `@end_time` | `@utcNow()` |
| `@rows_read` | `@activity('CopyActivity').output.dataRead` |
| `@rows_written` | `@activity('CopyActivity').output.rowsCopied` |
| `@rows_skipped` | `@activity('CopyActivity').output.rowsSkipped` |
| `@additional_info` | `@string(activity('CopyActivity').output)` |

#### Failure Log SP Parameters

| SP Parameter | ADF Expression |
|---|---|
| `@pipeline_run_id` | `@pipeline().RunId` |
| `@end_time` | `@utcNow()` |
| `@error_code` | `@activity('CopyActivity').error.errorCode` |
| `@error_message` | `@activity('CopyActivity').error.message` |

---

### Common Pipeline System Variables

| Variable | Description |
|---|---|
| `@pipeline().RunId` | Unique GUID for this pipeline run |
| `@pipeline().Pipeline` | Pipeline name |
| `@pipeline().TriggerName` | Name of the trigger that fired |
| `@pipeline().TriggerType` | `Manual`, `ScheduleTrigger`, `TumblingWindowTrigger`, `BlobEventsTrigger` |
| `@pipeline().TriggerTime` | Time the trigger fired |
| `@pipeline().GroupId` | Group run ID (for re-runs) |

---

### Common Activity Output Properties

| Activity Type | Expression | Description |
|---|---|---|
| Copy Activity | `@activity('name').output.rowsCopied` | Rows written to sink |
| Copy Activity | `@activity('name').output.dataRead` | Bytes read from source |
| Copy Activity | `@activity('name').output.dataWritten` | Bytes written to sink |
| Copy Activity | `@activity('name').output.rowsSkipped` | Rows skipped (error rows) |
| Copy Activity | `@activity('name').output.copyDuration` | Duration in seconds |
| Data Flow | `@activity('name').output.runStatus.metrics` | Spark metrics JSON |
| Any Activity | `@activity('name').error.errorCode` | Error code on failure |
| Any Activity | `@activity('name').error.message` | Error message on failure |

---

### Full Architecture Diagram

```
                    ┌─────────────────────┐
                    │  ADF Pipeline Start  │
                    └──────────┬──────────┘
                               │
                               ▼
               ┌───────────────────────────────┐
               │  SP Activity: Log Start       │
               │  → INSERT INTO log table      │
               │    status = 'RUNNING'         │
               │    run_id, pipeline, trigger  │
               │    start_time = utcNow()      │
               └──────────────┬────────────────┘
                              │ (On Success)
                              ▼
               ┌───────────────────────────────┐
               │   Core Pipeline Activities    │
               │   (Copy / DataFlow / etc.)    │
               └──────┬───────────────┬────────┘
                      │               │
               (On Success)      (On Failure)
                      │               │
                      ▼               ▼
      ┌───────────────────┐  ┌─────────────────────┐
      │ SP: Log Success   │  │ SP: Log Failure      │
      │ UPDATE log table  │  │ UPDATE log table     │
      │ status='SUCCESS'  │  │ status='FAILED'      │
      │ rows_read         │  │ error_code           │
      │ rows_written      │  │ error_message        │
      │ end_time          │  │ end_time             │
      └───────────────────┘  └─────────────────────┘
```

---

### Advanced: Logging Inside ForEach (Per-File Logging)

When iterating over multiple files with ForEach, log each file's result individually:

```sql
CREATE TABLE file_processing_log (
    log_id            INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id   VARCHAR(100),
    file_name         VARCHAR(500),
    status            VARCHAR(20),
    rows_copied       BIGINT,
    error_message     NVARCHAR(MAX),
    processed_at      DATETIME2 DEFAULT GETUTCDATE()
);
```

Inside ForEach:
```
[Copy Activity: copy @item().name]
    │ (On Success)                   │ (On Failure)
    ▼                                ▼
[SP: INSERT file_processing_log]  [SP: INSERT file_processing_log]
  file_name = @item().name           file_name = @item().name
  status    = 'SUCCESS'              status    = 'FAILED'
  rows      = @activity('Copy')      error     = @activity('Copy')
              .output.rowsCopied                .error.message
```

> Use **sequential** ForEach when writing to a SQL log inside the loop — parallel ForEach can cause deadlocks on the log table.

---

### Cross Questions & Answers

**Q: Why not just use Azure Monitor / ADF built-in monitoring instead of a custom log table?**

> Azure Monitor and ADF's built-in Activity Runs are useful for real-time monitoring but have limitations:
> - **45-day retention** — historical data is lost after 45 days.
> - **No custom business metrics** — you can't store domain-specific data (file names, business row counts, validation summaries).
> - **No easy SQL querying** — Azure Monitor uses KQL (Kusto), not SQL; stakeholders prefer SQL dashboards.
> - A custom log table gives unlimited retention, custom fields, and integrates with Power BI / SSRS / any SQL tool.

---

**Q: Can you use a Script Activity instead of Stored Procedure Activity for logging?**

> Yes. The **Script Activity** (available in ADF for SQL-based linked services) lets you write inline SQL directly without creating a stored procedure:
> ```sql
> INSERT INTO pipeline_execution_log (pipeline_name, pipeline_run_id, status, start_time)
> VALUES ('MyPipeline', '@{pipeline().RunId}', 'RUNNING', '@{utcNow()}')
> ```
> **Trade-offs:**
> - Script Activity: faster to set up, no SP needed, SQL is visible in the pipeline JSON.
> - Stored Procedure Activity: cleaner separation of concerns, SQL is version-controlled in the DB, easier to update without touching pipeline.

---

**Q: What if the Start Log SP itself fails — does the pipeline continue?**

> By default, if the Start Log SP fails, downstream activities that depend on it with "On Success" will not run — the pipeline fails. To make logging non-blocking:
> - Set the core activity dependency to **"On Completion"** (runs regardless of the SP outcome).
> - Or wrap the Start Log SP in a try-catch pattern using **If Condition + activity status check**.
>
> However, for most production scenarios, if you can't write to the log table it likely indicates a connectivity issue that warrants failing the pipeline.

---

**Q: How do you capture the error details when a pipeline has multiple activities and any one of them could fail?**

> Use a **Set Variable** activity in the failure branch of each individual activity to capture its error:
> ```
> @activity('ActivityName').error.message
> ```
> Or, use the failure dependency at the pipeline level and iterate through activity statuses using a **Web Activity** calling the ADF REST API (`GET /activityruns`) to retrieve all activity statuses for the current run, then parse and log them.
>
> A simpler pattern: use `@string(activity('ActivityName').error)` to serialize the full error object to JSON and store it in the `error_message` NVARCHAR(MAX) column.

---

**Q: How do you log Data Flow activity metrics (row counts) since they are nested in a JSON metrics object?**

> The Data Flow activity output has a `runStatus.metrics` object. Extract specific metrics using the `activity().output` expressions:
> ```
> @string(activity('DataFlowActivity').output.runStatus.metrics)
> ```
> Store the full JSON in the `additional_info` NVARCHAR(MAX) column, then parse it with SQL `JSON_VALUE()` or downstream tools. Common metrics inside:
> - `sources.SourceName.rowsRead`
> - `sinks.SinkName.rowsWritten`
> - `sources.SourceName.bytesRead`

---

**Q: How do you handle re-runs — does the same `RunId` get reused?**

> No. Every pipeline run (including re-runs triggered manually or by retry) gets a **new `RunId`** GUID. Re-runs are independent entries in the log table. If you need to correlate a re-run with its original run, use `@pipeline().GroupId` — this stays the same across re-runs of the same logical execution.

---

**Q: Can you log to the SQL table from inside a Mapping Data Flow?**

> Not directly — Data Flows do not have a built-in SQL Stored Procedure output. Options:
> 1. Use a **SQL sink** in the Data Flow and write a summary/log row alongside the main data.
> 2. After the Data Flow activity in the pipeline, use a **Stored Procedure Activity** with `@activity('DataFlow').output` metrics to log the run details.
> 3. Use a **Script Activity** in the pipeline after the Data Flow to insert the log row.

---

## Q7. How do you remove duplicate rows in ADF Mapping Data Flows?

### Overview

Deduplication is one of the most common data quality tasks. ADF Mapping Data Flows offers **three main approaches** to remove duplicate rows, each suited to different scenarios:

| Approach | Mechanism | Best For |
|---|---|---|
| **Aggregate transformation** | Group by key columns, collapse duplicates | True deduplication — keep one row per key |
| **Window transformation** | Row number per partition, filter `rowNum == 1` | Dedup with control over which duplicate to keep (e.g., latest) |
| **Exists transformation** | Anti-join against a reference set | Remove rows that already exist in a target table |

---

### Approach 1 — Aggregate Transformation (Simple Dedup)

Use when you want to **collapse all duplicates into one row** and you don't need to control which duplicate is kept (or you aggregate values like MAX, SUM).

#### How It Works

The Aggregate transformation groups rows by the key column(s) and produces one output row per group. Non-key columns are either aggregated (MAX, MIN, FIRST, LAST) or dropped.

#### Configuration

**Group by tab:**
- Add all columns that define uniqueness (e.g., `customer_id`, `order_date`)

**Aggregates tab:**
- For columns you want to keep: `first(column_name)` or `last(column_name)` or `max(column_name)`
- This picks one value from the duplicate group

#### Example

Input:
```
customer_id | name    | email             | created_at
1           | Alice   | alice@test.com    | 2024-01-01
1           | Alice   | alice@test.com    | 2024-01-03   ← duplicate
2           | Bob     | bob@test.com      | 2024-01-02
```

Aggregate: Group by `customer_id`, Aggregates: `name = first(name)`, `email = first(email)`, `created_at = max(created_at)`

Output:
```
customer_id | name  | email           | created_at
1           | Alice | alice@test.com  | 2024-01-03   ← latest kept
2           | Bob   | bob@test.com    | 2024-01-02
```

#### Data Flow

```
[Source]
   │
   ▼
[Aggregate]
  Group by: customer_id
  Aggregates:
    name        = first(name)
    email       = first(email)
    created_at  = max(created_at)
   │
   ▼
[Sink]
```

---

### Approach 2 — Window Transformation + Filter (Recommended — Full Row Control)

Use when you need to **keep a specific duplicate** (e.g., the most recent record, the one with highest version number) while discarding others, and you want to retain the **complete original row** without aggregating columns.

#### How It Works

The Window transformation assigns a **row number** (`rowNumber()`) to each row within a partition (group of duplicates), ordered by your chosen sort column. You then filter to keep only rows where `row_num == 1`.

#### Window Transformation Configuration

| Tab | Setting | Value |
|---|---|---|
| Over | Partition by (group duplicates) | `customer_id` |
| Sort | Order by | `created_at desc` (latest first) |
| Range | Unbounded | (default) |
| Window columns | New column name | `row_num` |
| Window columns | Expression | `rowNumber()` |

#### Filter Transformation (after Window)

```
row_num == 1
```

This keeps only the first-ranked row per partition — which, given `ORDER BY created_at DESC`, is the **most recent** duplicate.

#### Example

Input:
```
customer_id | name  | email           | created_at  | row_num (assigned by Window)
1           | Alice | alice@test.com  | 2024-01-03  | 1  ← KEPT (latest)
1           | Alice | alice@test.com  | 2024-01-01  | 2  ← REMOVED
2           | Bob   | bob@test.com    | 2024-01-02  | 1  ← KEPT
```

After Filter (`row_num == 1`):
```
customer_id | name  | email           | created_at
1           | Alice | alice@test.com  | 2024-01-03
2           | Bob   | bob@test.com    | 2024-01-02
```

#### Full Data Flow

```
[Source]
   │
   ▼
[Window Transformation]
  Partition by: customer_id
  Order by:     created_at DESC
  Column:       row_num = rowNumber()
   │
   ▼
[Filter Transformation]
  Condition: row_num == 1
   │
   ▼
[Select Transformation]
  Drop: row_num  (remove helper column before sink)
   │
   ▼
[Sink]
```

---

### Approach 3 — Exists Transformation (Dedup Against Target)

Use when you want to **filter out rows that already exist in the sink/target** — common in incremental load patterns where you don't want to re-insert existing records.

#### How It Works

The Exists transformation performs a **semi-join** (or anti-join with "Exist type = Doesn't exist") between the incoming source stream and a reference stream (typically the target table). Rows that already exist in the target are filtered out.

#### Configuration

| Setting | Value |
|---|---|
| Exists type | `Doesn't exist` (keep only new rows) |
| Left stream | Incoming source data |
| Right stream | Target table (read via another Source transformation) |
| Conditions | `left.customer_id == right.customer_id` |

#### Data Flow

```
[Source: new data]          [Source: target table]
        │                           │
        └──────────┬────────────────┘
                   ▼
         [Exists Transformation]
           Type: Doesn't exist
           Condition: left.customer_id == right.customer_id
                   │
                   ▼  (only rows NOT in target)
                [Sink → Target Table]
```

---

### Approach Comparison

```
Which approach should you use?

Is the target table involved?
  └── YES → Use Exists transformation (anti-join against target)

Is the target table NOT involved (dedup within source only)?
  └── Do you need to control WHICH duplicate to keep?
        ├── YES → Use Window + Filter
        │         (e.g., keep latest by date, highest by version)
        └── NO  → Use Aggregate
                  (simpler, keeps first/last/max value per key)
```

---

### Full Architecture — Window-Based Dedup (Most Common Interview Answer)

```
┌────────────────────────────────────────────────────────────────────┐
│              Mapping Data Flow: DeduplicateCustomers               │
│                                                                    │
│  [Source: raw_customers CSV]                                       │
│    customer_id, name, email, created_at                            │
│           │                                                        │
│           ▼                                                        │
│  [Window Transformation]                                           │
│    Partition by: customer_id                                       │
│    Order by:     created_at DESC                                   │
│    New column:   row_num = rowNumber()                             │
│           │                                                        │
│           ▼                                                        │
│  [Filter Transformation]                                           │
│    row_num == 1                                                    │
│           │                                                        │
│           ▼                                                        │
│  [Select Transformation]                                           │
│    Drop: row_num                                                   │
│           │                                                        │
│           ▼                                                        │
│  [Sink: clean_customers table in Azure SQL]                        │
└────────────────────────────────────────────────────────────────────┘
```

---

### Dedup on Multiple Columns (Composite Key)

To deduplicate on a **combination of columns** (e.g., `customer_id + order_date`):

**Window Transformation:**
- Partition by: `customer_id`, `order_date` (add both columns in "Over" tab)
- Order by: `updated_timestamp DESC`
- Column: `row_num = rowNumber()`

**Filter:** `row_num == 1`

This gives one row per `(customer_id, order_date)` combination.

---

### Dedup on All Columns (Exact Duplicate Rows)

When you want to remove rows where **every column is identical** (exact duplicates):

**Aggregate Transformation:**
- Group by: ALL columns
- Aggregates: (none needed — grouping alone collapses exact duplicates)

OR

**Window Transformation:**
- Partition by: ALL columns
- Order by: any constant (order doesn't matter for exact dupes)
- Filter: `row_num == 1`

---

### Key Expression Functions for Deduplication

| Function | Description |
|---|---|
| `rowNumber()` | Sequential row number within partition (1, 2, 3, ...) |
| `rank()` | Rank within partition (gaps for ties: 1, 1, 3, ...) |
| `denseRank()` | Dense rank within partition (no gaps: 1, 1, 2, ...) |
| `first(col)` | First value in partition group (Aggregate) |
| `last(col)` | Last value in partition group (Aggregate) |
| `max(col)` | Maximum value in partition group (Aggregate) |

---

### Cross Questions & Answers

**Q: What is the difference between `rowNumber()`, `rank()`, and `denseRank()` in Window transformations?**

> All three number rows within a partition. The difference appears when there are **ties** in the ORDER BY column:
>
> | Value | `rowNumber()` | `rank()` | `denseRank()` |
> |---|---|---|---|
> | 2024-01-01 | 1 | 1 | 1 |
> | 2024-01-01 | 2 | 1 | 1 |
> | 2024-01-03 | 3 | 3 | 2 |
>
> - `rowNumber()` — always unique, arbitrary tiebreak → use for dedup (guarantees exactly one `row_num == 1`)
> - `rank()` — ties share the same rank, gaps in sequence → if two rows tie for #1, both get `rank == 1`, no row gets `rank == 2`
> - `denseRank()` — ties share the same rank, no gaps → same issue as rank for dedup
>
> For deduplication, **always use `rowNumber()`** — it guarantees exactly one row gets rank 1 per partition.

---

**Q: Why is Window + Filter preferred over Aggregate for deduplication?**

> - **Aggregate** requires you to specify an aggregation function for every non-key column (`first()`, `max()`, etc.), and it may mix values from different duplicate rows (e.g., `first(name)` from row 1, `max(email)` from row 2).
> - **Window + Filter** keeps the **entire original row intact** — all columns come from the same source row. This is cleaner and safer for full-row deduplication.
>
> Use Aggregate only when you intentionally want to collapse/aggregate values across duplicates.

---

**Q: Does deduplication using Window transformation cause a shuffle/sort in Spark? Does it affect performance?**

> Yes. The Window transformation internally performs a **Spark `partitionBy` + `orderBy`** which requires a **shuffle** (data redistribution across nodes). This is relatively expensive for large datasets. Performance tips:
> - Partition by a **high-cardinality column** (like `customer_id`) to distribute the data evenly.
> - Avoid partitioning by low-cardinality columns (e.g., `country`) — this concentrates data on few partitions (data skew).
> - Run the dedup Data Flow on an **optimized Spark cluster** (memory-optimized nodes) when the dataset is large.

---

**Q: How do you deduplicate rows when you don't have a reliable timestamp or version column to decide which duplicate to keep?**

> If there is no reliable ordering column, use **`rowNumber()`** with an arbitrary or constant sort order — this picks one row per group non-deterministically (consistent within a single run, but not across runs):
> ```
> Window: Partition by customer_id, Order by customer_id ASC (or any column)
> rowNumber() == 1
> ```
> If determinism matters, add a `sha2(256, concat(col1, col2, ...))` hash column and sort by it — same input always produces the same hash order.

---

**Q: Can you use the Aggregate transformation to count duplicates before removing them (for auditing)?**

> Yes. Add a **separate Aggregate branch** before deduplication to generate a duplicate report:
> ```
> [Source]
>    ├── [Aggregate: Group by customer_id, count = count(1)] → [Sink: duplicate_counts table]
>    └── [Window + Filter: rowNumber == 1] → [Sink: clean data]
> ```
> Use a **New Branch** transformation to fork the stream into both paths simultaneously.

---

**Q: What happens if you write deduplicated data to a sink table that already has the same primary key — will it cause duplicate key errors?**

> Yes, a plain INSERT will fail on duplicate PK. At the Sink level, use one of these strategies:
> - **Upsert (Merge):** Set the sink write method to "Upsert" and define the key columns — ADF generates a SQL `MERGE` statement that updates existing rows and inserts new ones.
> - **Truncate and reload:** Use sink pre-SQL `TRUNCATE TABLE clean_customers` before inserting — only viable for smaller tables.
> - **Allow insert + ON CONFLICT:** Use Script Activity with `INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL) or `MERGE` (SQL Server) as a post-step.

---

**Q: How do you handle deduplication when the source has 100 million rows? Is Mapping Data Flow still the right tool?**

> Yes — Mapping Data Flows run on Spark, which is designed for exactly this scale. For 100M rows:
> 1. Use a **General Purpose** or **Memory Optimized** Spark compute with sufficient cores (16+ cores recommended).
> 2. Ensure the **partition column** has high cardinality to avoid data skew.
> 3. Consider **writing to Parquet or Delta Lake** as the sink — columnar formats handle large dedup outputs efficiently.
> 4. If using Delta Lake as the sink, use **Delta's built-in `MERGE`** operation for upsert dedup instead of doing it in Data Flow — Delta MERGE is optimized for exactly this pattern.

---

## Q8. How do you generate or increment surrogate keys from an existing source using ADF Mapping Data Flows?

### Overview

A **surrogate key** is a system-generated, meaningless integer key assigned to each row — commonly used in data warehouse dimension tables. The challenge in ADF is:

1. **New rows from source** need a new unique key that continues from the **maximum key already in the target** (not just starting from 1 each run).
2. The key must be **monotonically increasing** and have **no gaps or duplicates**.

ADF Mapping Data Flows provides the **Surrogate Key transformation** for generating keys, but by default it always starts from a fixed seed value. To make it continue from the existing max key in the target, you must combine it with a **Lookup** (to fetch the current max) and pass the result as the starting seed via a **Data Flow parameter**.

---

### Approach 1 — Surrogate Key Transformation (Starting from a Fixed Seed)

The simplest use — assign sequential keys starting from a defined number.

#### Surrogate Key Transformation Settings

| Setting | Value |
|---|---|
| Key column name | `surrogate_key` (or `sk_customer_id`) |
| Start value | `1` (fixed) or a **Data Flow parameter** (dynamic) |

ADF assigns: `1, 2, 3, ...` across all rows in the stream (distributed across Spark partitions but guaranteed unique within the run).

> **Important:** The Surrogate Key transformation uses Spark's **monotonically increasing ID** internally — values are unique and increasing but may not be perfectly sequential (gaps can appear between Spark partitions). If strict sequentiality is required, use `rowNumber()` in a Window transformation instead.

---

### Approach 2 — Continue From Existing Max Key (Production Pattern)

This is the real-world pattern for data warehouse loads where the target table already has rows with surrogate keys and new rows must continue the sequence.

#### High-Level Steps

1. **Lookup Activity** in the pipeline → query the target table for `MAX(surrogate_key)`.
2. Pass the result as a **pipeline parameter** to the Data Flow activity.
3. Inside the Data Flow, receive it as a **Data Flow parameter** and use it as the Surrogate Key start seed: `maxKey + 1`.

---

### Step-by-Step Implementation

#### Step 1 — Lookup Max Key (Pipeline Level)

Add a **Lookup Activity** before the Data Flow activity:

```sql
SELECT ISNULL(MAX(surrogate_key), 0) AS max_key
FROM   dim_customer
```

- `ISNULL(MAX(...), 0)` handles the first-ever run when the table is empty — returns `0` so the Data Flow starts from `0 + 1 = 1`.

Lookup output: `@activity('LookupMaxKey').output.firstRow.max_key`

---

#### Step 2 — Pass Max Key as Data Flow Parameter

In the **Data Flow Activity** settings → Parameters tab:

| Parameter Name | Value |
|---|---|
| `p_max_key` | `@activity('LookupMaxKey').output.firstRow.max_key` |

---

#### Step 3 — Define Data Flow Parameter

In the **Mapping Data Flow** canvas → Parameters tab, define:

| Name | Type | Default |
|---|---|---|
| `p_max_key` | `integer` | `0` |

---

#### Step 4 — Surrogate Key Transformation (Dynamic Seed)

In the Surrogate Key transformation:

| Setting | Value |
|---|---|
| Key column name | `surrogate_key` |
| Start value | `$p_max_key + 1` |

> `$p_max_key` references the Data Flow parameter. Adding `+ 1` ensures the first new key is one above the existing maximum.

---

#### Step 5 — Filter Only New Rows (Incremental Load)

Before the Surrogate Key transformation, ensure you are only processing **new rows** (rows not already in the target). Use an **Exists** transformation (anti-join) or a watermark filter to exclude existing records — otherwise old rows will get new surrogate keys.

---

### Full Pipeline + Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  PIPELINE: LoadDimCustomer                                          │
│                                                                     │
│  ① Lookup Activity: GetMaxKey                                      │
│     SQL: SELECT ISNULL(MAX(surrogate_key), 0) AS max_key           │
│           FROM dim_customer                                         │
│     Output: max_key = 1042  (example)                              │
│                        │                                            │
│                        ▼                                            │
│  ② Data Flow Activity: TransformCustomers                          │
│     Parameter: p_max_key = @activity('GetMaxKey')                  │
│                             .output.firstRow.max_key               │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  MAPPING DATA FLOW: TransformCustomers                              │
│  Parameter: p_max_key (integer, default: 0)                        │
│                                                                     │
│  [Source: staging_customers]                                        │
│    new/changed customer rows from staging                           │
│           │                                                         │
│           ▼                                                         │
│  [Source: dim_customer (target)] ───────────────────────┐          │
│                                                         │          │
│  [Exists: Doesn't Exist]  ◄─────────────────────────────┘          │
│    left.customer_id == right.customer_id                            │
│    → Keep only rows NOT in target (truly new customers)             │
│           │                                                         │
│           ▼                                                         │
│  [Surrogate Key Transformation]                                     │
│    Key column: surrogate_key                                        │
│    Start value: $p_max_key + 1                                      │
│    → Assigns: 1043, 1044, 1045 ...                                  │
│           │                                                         │
│           ▼                                                         │
│  [Derived Column]                                                   │
│    effective_date  = currentDate()                                  │
│    expiry_date     = toDate('9999-12-31')                           │
│    is_current      = true()                                         │
│           │                                                         │
│           ▼                                                         │
│  [Sink: dim_customer]                                               │
│    Write method: Insert                                             │
└─────────────────────────────────────────────────────────────────────┘
```

---

### Approach 3 — Window `rowNumber()` as Surrogate Key (Strict Sequential)

When you need **strictly sequential, gapless keys** (no Spark partition gaps):

```
Window Transformation:
  Partition by:  (none — entire dataset as one partition)
  Order by:      any stable column (e.g., customer_id ASC)
  New column:    row_num = rowNumber()

Derived Column:
  surrogate_key = $p_max_key + row_num
```

> **Trade-off:** Setting no partition means Spark collects **all data into one partition** before numbering — this eliminates parallelism and is very slow for large datasets. Use only for small datasets where strict sequentiality is mandatory.

---

### Approach 4 — Using a Sequence Object in Azure SQL (Alternative)

If strict sequential keys without any gaps are business-critical, generate them in the database rather than in ADF:

```sql
-- Create a sequence in Azure SQL
CREATE SEQUENCE dbo.sk_customer_seq
    START WITH 1
    INCREMENT BY 1
    NO CYCLE;

-- Use in a stored procedure called from ADF Script Activity
INSERT INTO dim_customer (surrogate_key, customer_id, name, ...)
SELECT NEXT VALUE FOR dbo.sk_customer_seq, customer_id, name, ...
FROM   staging_customers s
WHERE  NOT EXISTS (
    SELECT 1 FROM dim_customer d WHERE d.customer_id = s.customer_id
);
```

ADF then calls this SP via a **Stored Procedure Activity** — no Surrogate Key transformation needed.

---

### Comparison of Approaches

| Approach | Sequential | Gap-free | Scalable | Parallel-safe |
|---|---|---|---|---|
| Surrogate Key Transformation (fixed seed) | Yes | No (Spark gaps) | Yes | Yes |
| Surrogate Key + Max Lookup (dynamic seed) | Yes | No (Spark gaps) | Yes | Yes |
| Window `rowNumber()` (no partition) | Yes | Yes | No (single partition) | No |
| Azure SQL Sequence (DB-generated) | Yes | Yes | Yes | Yes |

---

### Cross Questions & Answers

**Q: Why does the Surrogate Key transformation produce gaps in key values?**

> The Surrogate Key transformation uses Spark's `monotonically_increasing_id()` function internally, which generates unique IDs per partition. Each Spark partition gets a block of IDs with large gaps between partitions (e.g., partition 0: 0–999999, partition 1: 1000000–1999999). Within a partition the IDs are sequential, but across partitions there are gaps.
>
> If strict sequential keys (1, 2, 3...) are required, use the `rowNumber()` Window approach with no partitioning — but this sacrifices parallelism.

---

**Q: What if two pipeline runs execute simultaneously — will they generate duplicate surrogate keys?**

> Yes — if two runs both read `MAX(surrogate_key) = 1000` before either has committed new rows, both start generating from `1001`, creating duplicates. Solutions:
> 1. **Serialize pipeline runs** — set `concurrency = 1` on the trigger so runs cannot overlap.
> 2. **Database sequence object** — SQL `SEQUENCE` is atomic and thread-safe; concurrent calls always return unique values.
> 3. **Optimistic locking with retry** — check for PK violations after insert and retry (complex, not recommended for ADF).

---

**Q: Is there a difference between a surrogate key and a natural key in data warehousing?**

> Yes:
> - **Natural key:** A key from the source system with business meaning (e.g., `customer_id = 'CUST001'`). Can change, can be reused, may be composite.
> - **Surrogate key:** A system-generated integer with no business meaning (e.g., `sk = 1043`). Immutable, always unique, never reused, always a single integer column.
>
> In a star schema, **dimension tables** use surrogate keys as the primary key and natural keys as business identifiers. **Fact tables** reference dimension surrogate keys as foreign keys — this decouples the warehouse from source system key changes.

---

**Q: How do you handle SCD Type 2 (Slowly Changing Dimension) with surrogate keys in ADF?**

> SCD Type 2 creates a **new row** with a new surrogate key each time a tracked dimension attribute changes, while expiring the old row. In ADF Mapping Data Flows:
> 1. **Alter Row transformation** — tag changed rows as `Insert` (new version) and rows to expire as `Update`.
> 2. **New rows** go through the Surrogate Key transformation to get a new SK (using `$p_max_key + 1`).
> 3. **Expiring rows** get `is_current = false`, `expiry_date = currentDate()` via Derived Column.
> 4. Both streams merge into the sink with `Upsert` write method on the surrogate key.
>
> The Lookup Max Key pattern is essential for SCD2 — every changed record generates a new SK that must be higher than all existing ones.

---

**Q: What does `ISNULL(MAX(surrogate_key), 0)` handle, and why is it important?**

> On the **very first pipeline run**, the target table is empty. `MAX(surrogate_key)` returns `NULL` for an empty table. `NULL + 1` in ADF expressions produces `NULL`, so the Surrogate Key transformation would receive a null start value — causing an error or defaulting unexpectedly.
>
> `ISNULL(MAX(surrogate_key), 0)` converts `NULL → 0`, so the Data Flow starts from `0 + 1 = 1` — the correct seed for a fresh table.

---

**Q: Can you generate surrogate keys fully inside the Data Flow without a pipeline-level Lookup Activity?**

> Yes — read the max key as a second source inside the Data Flow itself:
> ```
> [Source A: staging data]          [Source B: dim_customer]
>        │                                    │
>        │              [Aggregate: max_key = max(surrogate_key)]
>        │                                    │ (single-row result)
>        └──── [Cross Join on true()] ────────┘
>                          │
>              [Derived Column: surrogate_key = max_key + rowNumber()]
> ```
> This is self-contained but adds complexity and requires reading the target table twice in the same Data Flow (once for Exists anti-join, once for max key). The pipeline-level Lookup approach is cleaner and easier to debug.

---

## Q9. How do you calculate a running total using Mapping Data Flows in ADF?

### Overview

A **running total** (also called a cumulative sum) is the progressive accumulation of a value as you move through rows in a defined order. For example:

| order_date | sales_amount | running_total |
|---|---|---|
| 2024-01-01 | 100 | 100 |
| 2024-01-02 | 250 | 350 |
| 2024-01-03 | 180 | 530 |
| 2024-01-04 | 300 | 830 |

In ADF Mapping Data Flows, running totals are calculated using the **Window Transformation** with the `cumSum()` function — the dedicated cumulative sum window function.

---

### Core Concept — Window Transformation with `cumSum()`

The Window transformation in ADF supports **window (analytic) functions** that operate over a set of rows defined by:
- **Partition (Over)** — groups rows (e.g., per customer, per region). The running total resets for each group.
- **Sort** — defines the row order within the partition (e.g., by date ascending).
- **Range/Rows** — bounds the window frame. For a running total, use **unbounded preceding to current row**.

#### Key Expression

```
cumSum(column_to_sum, [sortOrder])
```

Or equivalently using the window frame syntax:

```
sum(sales_amount)
```
…with the Window transformation's sort and range set to "unbounded preceding → current row".

---

### Step-by-Step Implementation

#### Step 1 — Source

Load the data with at least:
- A **value column** to accumulate (e.g., `sales_amount`)
- An **ordering column** (e.g., `order_date`)
- Optionally a **partition column** (e.g., `region`, `customer_id`) — for grouped running totals

---

#### Step 2 — Window Transformation Configuration

| Tab | Setting | Value |
|---|---|---|
| **Over** | Partition by | `region` (or empty for global running total) |
| **Sort** | Order by | `order_date ASC` |
| **Range** | Row range type | `Unbounded` (preceding) to `Current row` |
| **Window columns** | New column name | `running_total` |
| **Window columns** | Expression | `cumSum(sales_amount)` |

> **Partition by empty** = running total across the entire dataset (one global sequence).  
> **Partition by `region`** = running total resets for each region independently.

---

#### Step 3 — Additional Useful Window Columns (Same Transformation)

You can add multiple window columns in the same Window transformation:

| New Column | Expression | Description |
|---|---|---|
| `running_total` | `cumSum(sales_amount)` | Running cumulative sum |
| `running_avg` | `avg(sales_amount)` | Running average |
| `running_max` | `max(sales_amount)` | Running maximum |
| `running_min` | `min(sales_amount)` | Running minimum |
| `running_count` | `count(sales_amount)` | Running row count |
| `row_num` | `rowNumber()` | Row number within partition |

All share the same partition and sort — computed in one pass over the data.

---

### Example Scenarios

#### Scenario A — Global Running Total (No Partition)

**Requirement:** Total sales accumulating over all dates.

```
Window Transformation:
  Over (Partition by): (none)
  Sort:                order_date ASC
  Range:               Unbounded to Current row
  Column:              running_total = cumSum(sales_amount)
```

Input / Output:
```
order_date  | sales_amount | running_total
2024-01-01  |         100  |          100
2024-01-02  |         250  |          350
2024-01-03  |         180  |          530
2024-01-04  |         300  |          830
```

---

#### Scenario B — Running Total Per Region (Partitioned)

**Requirement:** Cumulative sales per region, resetting for each region.

```
Window Transformation:
  Over (Partition by): region
  Sort:                order_date ASC
  Range:               Unbounded to Current row
  Column:              running_total = cumSum(sales_amount)
```

Input / Output:
```
region  | order_date  | sales_amount | running_total
East    | 2024-01-01  |         100  |          100
East    | 2024-01-02  |         200  |          300  ← resets per region
West    | 2024-01-01  |         150  |          150  ← West starts fresh
West    | 2024-01-02  |         250  |          400
```

---

#### Scenario C — Running Total Per Customer Per Month

**Requirement:** Cumulative spend per customer within each calendar month.

```
Window Transformation:
  Over (Partition by): customer_id, year(order_date), month(order_date)
  Sort:                order_date ASC
  Column:              monthly_running_total = cumSum(order_amount)
```

---

### Full Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│            Mapping Data Flow: RunningTotalSales                 │
│                                                                 │
│  [Source: sales_transactions]                                   │
│   region, order_date, sales_amount                              │
│           │                                                     │
│           ▼                                                     │
│  [Derived Column]  ← optional: extract year/month partitions    │
│   sale_year  = year(order_date)                                 │
│   sale_month = month(order_date)                                │
│           │                                                     │
│           ▼                                                     │
│  [Window Transformation]                                        │
│   Partition by:  region                                         │
│   Sort:          order_date ASC                                 │
│   Range:         Unbounded → Current row                        │
│   Columns:                                                      │
│     running_total  = cumSum(sales_amount)                       │
│     running_avg    = avg(sales_amount)                          │
│     row_num        = rowNumber()                                │
│           │                                                     │
│           ▼                                                     │
│  [Select]  ← drop helper columns if not needed in sink          │
│           │                                                     │
│           ▼                                                     │
│  [Sink: sales_with_running_totals table]                        │
└─────────────────────────────────────────────────────────────────┘
```

---

### Running Total with Reset Condition (Moving Window)

Sometimes you need a **moving/rolling window** instead of cumulative (e.g., rolling 7-day sum):

```
Window Transformation:
  Partition by:  region
  Sort:          order_date ASC
  Range:         Rows — preceding 6 rows to current row  (7-row window)
  Column:        rolling_7day_sales = sum(sales_amount)
```

| Window Frame Type | ADF Setting | Effect |
|---|---|---|
| Cumulative (from start) | Unbounded preceding → Current row | Running total from beginning |
| Rolling N rows | Preceding N-1 rows → Current row | Sum of last N rows |
| Entire partition | Unbounded → Unbounded | Total for entire partition (same value for all rows) |

---

### Running Total vs. Aggregate Total — Key Difference

```
Aggregate Transformation:             Window Transformation:
  Groups rows → collapses to 1         Keeps all rows → adds new column
  customer_id | total_sales            customer_id | order_date | running_total
  1           | 630                    1           | 2024-01-01 | 100
  2           | 400                    1           | 2024-01-02 | 350
                                       1           | 2024-01-03 | 630
```

> Use **Aggregate** when you want one summary row per group.  
> Use **Window** when you want to keep all rows and add a cumulative column alongside them.

---

### Cross Questions & Answers

**Q: What is `cumSum()` and how does it differ from `sum()` in a Window transformation?**

> In ADF Mapping Data Flows:
> - `cumSum(col)` is a **dedicated cumulative sum function** that automatically implies the window frame from the start of the partition to the current row — you don't need to specify the range explicitly.
> - `sum(col)` used inside a Window transformation respects whatever **Range** you configure. If you set "Unbounded preceding → Current row", `sum()` behaves identically to `cumSum()`. If you set "Unbounded → Unbounded", `sum()` gives the partition total for every row.
>
> `cumSum()` is the cleaner and more explicit choice for running totals.

---

**Q: Does the Window transformation require sorting the source data before it?**

> No. The **Window transformation handles sorting internally** via its Sort tab. You do not need to add a separate Sort transformation before it. ADF/Spark performs the necessary partition + sort as part of the window function execution.

---

**Q: Can you have multiple running totals on different columns in the same Window transformation?**

> Yes. You can add as many window columns as needed in the same Window transformation — they all share the same partition and sort definition. This is more efficient than chaining multiple Window transformations (which would require multiple Spark shuffles).
>
> Example: Add `running_total = cumSum(sales_amount)` AND `running_units = cumSum(units_sold)` AND `row_num = rowNumber()` in a single Window transformation.

---

**Q: What happens to the running total if there are NULL values in the column being summed?**

> `cumSum()` treats `NULL` as `0` — it skips NULLs and continues accumulating. If you want NULLs to break the running total (reset it), use:
> ```
> iif(isNull(sales_amount), toDouble(null()), cumSum(sales_amount))
> ```
> Or replace NULLs before the Window transformation using a Derived Column:
> ```
> sales_amount = iif(isNull(sales_amount), 0, sales_amount)
> ```

---

**Q: How does the Window transformation handle ties in the sort column (two rows with the same date)?**

> When two rows share the same sort key value (e.g., two transactions on the same date), the **running total for those tied rows will both reflect the cumulative sum including both rows** — ADF does not guarantee a specific row ordering within ties.
>
> To get deterministic results for tied rows, add a secondary sort column (e.g., `order_id ASC`) to break ties:
> ```
> Sort: order_date ASC, order_id ASC
> ```

---

**Q: Is there a performance concern with Window transformations on very large datasets?**

> Yes. Window transformations internally trigger a **Spark shuffle** (`repartitionBy` the partition columns + `sortWithinPartitions`). Key considerations:
> - **Partition by a high-cardinality column** (e.g., `customer_id`) — distributes data evenly across Spark executors, maximizing parallelism.
> - **Avoid partitioning by low-cardinality columns** (e.g., `country` with only 5 values) — all rows for one country land on one node, causing data skew and slow execution.
> - **Global running total (no partition)** — forces all data into a single partition and is processed by a single executor. Avoid for large datasets; partition by a meaningful key instead.

---

**Q: Can you calculate a running total in ADF without using Mapping Data Flows (e.g., in a pipeline)?**

> Not directly — pipelines (Copy Activity, ForEach, etc.) are orchestration tools, not transformation engines. Options outside of Data Flows:
> 1. **Script Activity / Stored Procedure** — run a SQL window function directly in Azure SQL:
>    ```sql
>    SELECT region, order_date, sales_amount,
>           SUM(sales_amount) OVER (PARTITION BY region ORDER BY order_date
>                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
>               AS running_total
>    FROM   sales_transactions;
>    ```
> 2. **Databricks notebook** — use Spark `Window` + `sum().over(windowSpec)` in PySpark.
> 3. **Synapse Analytics** — use T-SQL window functions in a dedicated SQL pool.
>
> For pure file-to-file transformations (no SQL engine available), Mapping Data Flows with the Window transformation is the right tool.

---








