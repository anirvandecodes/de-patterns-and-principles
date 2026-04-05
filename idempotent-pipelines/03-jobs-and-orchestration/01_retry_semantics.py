# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Job Retry Semantics and Idempotency Keys
# MAGIC
# MAGIC Databricks Jobs can automatically retry failed tasks. For retries to be safe,
# MAGIC **every task must be idempotent** — running it twice must produce the same result
# MAGIC as running it once.
# MAGIC
# MAGIC This notebook covers:
# MAGIC 1. Retry configuration in DABs YAML
# MAGIC 2. What makes a task "retry-safe"
# MAGIC 3. Passing a canonical `batch_id` across tasks using Task Values
# MAGIC 4. Using `run_if` to protect destructive downstream tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Retry configuration in DABs YAML
# MAGIC
# MAGIC Configure retries at the task level in your `databricks.yml` or job resource file:
# MAGIC
# MAGIC ```yaml
# MAGIC tasks:
# MAGIC   - task_key: transform_orders
# MAGIC     max_retries: 2                        # retry up to 2 times on failure
# MAGIC     min_retry_interval_millis: 60000      # wait 1 minute between retries
# MAGIC     retry_on_timeout: true                # also retry on cluster timeout
# MAGIC     notebook_task:
# MAGIC       notebook_path: ../src/transform_orders.py
# MAGIC       base_parameters:
# MAGIC         load_date: "{{job.parameters.load_date}}"
# MAGIC ```
# MAGIC
# MAGIC **Rule:** If `max_retries > 0`, the task MUST be idempotent. Databricks will not
# MAGIC check — it will just run the task again.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. What makes a task retry-safe?
# MAGIC
# MAGIC A task is retry-safe if:
# MAGIC - Its **writes** are idempotent (MERGE or partition overwrite — not naked append)
# MAGIC - Its **side effects** (emails, API calls, Slack messages) are guarded by a check
# MAGIC - It does NOT generate a new UUID or timestamp as a partition key on each run
# MAGIC - It scopes its work to a **stable, deterministic identifier** (a date, a batch key)
# MAGIC
# MAGIC ```
# MAGIC RETRY-SAFE                           NOT RETRY-SAFE
# MAGIC ──────────────────────────────────   ──────────────────────────────────
# MAGIC MERGE INTO target ON key = key       df.write.mode("append")
# MAGIC Partition overwrite by load_date     INSERT INTO without dedup
# MAGIC Check pipeline_audit before write    Send email unconditionally
# MAGIC Guard API call with idempotency key  CREATE TABLE without IF NOT EXISTS
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Canonical batch_id via Task Values
# MAGIC
# MAGIC When a job has multiple tasks (extract → transform → load), each task needs to
# MAGIC work on the **same logical batch**. Use `dbutils.jobs.taskValues` to pass a canonical
# MAGIC `batch_id` downstream so every task uses the same identifier — even across retries.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1 (upstream): set the batch_id

# COMMAND ----------

# In your extract/setup task, set a deterministic batch key based on the load_date parameter.
# This is the ONLY place the batch_id is generated — all downstream tasks read it.

load_date = dbutils.widgets.get("load_date") if "load_date" in [w.name for w in dbutils.widgets.getAll()] else "2024-01-15"

# Deterministic: same load_date always produces the same batch_id
batch_id = f"orders_{load_date.replace('-', '')}"
print(f"batch_id: {batch_id}")

# Pass it to all downstream tasks
dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)
dbutils.jobs.taskValues.set(key="load_date", value=load_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2 (downstream): read the batch_id
# MAGIC
# MAGIC ```python
# MAGIC # In your transform or load task:
# MAGIC batch_id  = dbutils.jobs.taskValues.get(taskKey="setup_task", key="batch_id")
# MAGIC load_date = dbutils.jobs.taskValues.get(taskKey="setup_task", key="load_date")
# MAGIC
# MAGIC # Now use batch_id to scope the write idempotently
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Protecting side effects with an idempotency guard
# MAGIC
# MAGIC Side effects (API calls, emails, audit writes) must only happen once per batch,
# MAGIC even if the task retries.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# Create a simple idempotency store
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.idempotency_keys (
        key        STRING,
        created_at TIMESTAMP,
        metadata   STRING
    )
    USING DELTA
""")

# COMMAND ----------

def is_key_used(key: str) -> bool:
    """Check if this batch key has already been processed."""
    count = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {catalog}.{schema}.idempotency_keys
        WHERE key = '{key}'
    """).collect()[0]["cnt"]
    return count > 0

def claim_key(key: str, metadata: str = ""):
    """Mark this key as used — call BEFORE the side effect."""
    spark.sql(f"""
        INSERT INTO {catalog}.{schema}.idempotency_keys
        VALUES ('{key}', current_timestamp(), '{metadata}')
    """)

# COMMAND ----------

def send_daily_report(load_date: str):
    """Side effect: send a report. Protected by idempotency key."""
    key = f"daily_report_{load_date}"

    if is_key_used(key):
        print(f"Report for {load_date} already sent — skipping.")
        return

    # Claim the key BEFORE the side effect
    claim_key(key, metadata=f"daily_report for {load_date}")

    # --- your actual side effect here ---
    print(f"Sending daily report for {load_date}...")
    # send_email(...)  # this would be the real call

# COMMAND ----------

send_daily_report("2024-01-15")   # Sends
send_daily_report("2024-01-15")   # Skipped — already sent
send_daily_report("2024-01-16")   # Sends (different date)

display(spark.table(f"{catalog}.{schema}.idempotency_keys"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `run_if` — protecting destructive downstream tasks
# MAGIC
# MAGIC A common mistake: a cleanup or publish task runs even when an upstream task failed.
# MAGIC Use `run_if: ALL_SUCCESS` on tasks that should only run if all dependencies succeeded.
# MAGIC
# MAGIC ```yaml
# MAGIC tasks:
# MAGIC   - task_key: transform_orders
# MAGIC     ...
# MAGIC
# MAGIC   - task_key: drop_staging_table        # DANGEROUS without run_if
# MAGIC     depends_on:
# MAGIC       - task_key: transform_orders
# MAGIC     run_if: ALL_SUCCESS                  # Only runs if transform succeeded
# MAGIC     notebook_task:
# MAGIC       notebook_path: ../src/drop_staging.py
# MAGIC
# MAGIC   - task_key: notify_downstream
# MAGIC     depends_on:
# MAGIC       - task_key: drop_staging_table
# MAGIC     run_if: ALL_SUCCESS                  # Only notify if everything worked
# MAGIC     notebook_task:
# MAGIC       notebook_path: ../src/notify.py
# MAGIC ```
# MAGIC
# MAGIC **Use `ALL_DONE`** (not `ALL_SUCCESS`) only for tasks that must run regardless,
# MAGIC such as cleanup tasks that should execute even on failure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Practice | Why |
# MAGIC |---|---|
# MAGIC | `max_retries: 2` + idempotent writes | Automatic recovery from transient failures |
# MAGIC | Deterministic `batch_id` from `load_date` | Same logical batch across retries |
# MAGIC | `taskValues` for cross-task batch_id | Every task works on the same identity |
# MAGIC | Idempotency key store for side effects | Emails/API calls fire exactly once |
# MAGIC | `run_if: ALL_SUCCESS` on destructive tasks | Prevent cleanup from running on failure |
