# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Date-Parameterized Jobs: Re-runnable by Design
# MAGIC
# MAGIC The single most important pattern for idempotent batch pipelines:
# MAGIC **every job accepts a `load_date` parameter**.
# MAGIC
# MAGIC This one change makes every notebook re-runnable for any date — historical or current.
# MAGIC It's the foundation that makes backfills trivial.
# MAGIC
# MAGIC ```
# MAGIC Without load_date:  "Process today's data"  ← can only run once per day
# MAGIC With load_date:     "Process data for DATE"  ← can run for any date, any time
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The load_date widget pattern

# COMMAND ----------

# Always default to "today" so the job works unmodified on a schedule.
# When backfilling, override with a specific date.

dbutils.widgets.text("load_date", "")

_raw = dbutils.widgets.get("load_date").strip()
load_date = _raw if _raw else str(spark.sql("SELECT current_date()").collect()[0][0])

print(f"Processing: load_date = {load_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Scope all reads and writes to load_date

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import lit, current_timestamp

def extract_source_data(load_date: str):
    """Read ONLY data for the given date from the source."""
    # In production this would be a real source query:
    # return spark.read.table("raw.orders").filter(f"order_date = '{load_date}'")

    return spark.createDataFrame([
        Row(order_id="O001", customer_id="C1", amount=100.0),
        Row(order_id="O002", customer_id="C2", amount=200.0),
        Row(order_id="O003", customer_id="C3", amount=150.0),
    ]).withColumn("order_date", lit(load_date))

# COMMAND ----------

def transform(df):
    """Apply business logic."""
    return df.withColumn("amount_with_tax", df.amount * 1.1) \
             .withColumn("_processed_at", current_timestamp())

# COMMAND ----------

def load(df, load_date: str):
    """Write to Delta — scoped to the load_date partition."""
    (df.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("order_date")
       .saveAsTable(f"{catalog}.{schema}.orders_processed"))

# COMMAND ----------

# Full ETL for load_date — safe to run multiple times
raw   = extract_source_data(load_date)
clean = transform(raw)
load(clean, load_date)

print(f"Done. Processed {clean.count()} rows for {load_date}.")

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.orders_processed")
         .filter(f"order_date = '{load_date}'")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DABs YAML: job with load_date parameter
# MAGIC
# MAGIC ```yaml
# MAGIC # resources/orders_daily_job.yml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     orders_daily:
# MAGIC       name: "[${bundle.target}] Orders Daily ETL"
# MAGIC
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 6 * * ?"   # 6 AM daily
# MAGIC         timezone_id: "UTC"
# MAGIC         pause_status: UNPAUSED
# MAGIC
# MAGIC       parameters:
# MAGIC         - name: load_date
# MAGIC           default: "{{start_date}}"             # Databricks fills this with today's date
# MAGIC
# MAGIC       tasks:
# MAGIC         - task_key: etl
# MAGIC           max_retries: 2
# MAGIC           min_retry_interval_millis: 60000
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/02_date_parameterized_jobs.py
# MAGIC             base_parameters:
# MAGIC               load_date: "{{job.parameters.load_date}}"
# MAGIC ```
# MAGIC
# MAGIC `{{start_date}}` is a Databricks built-in that resolves to the scheduled run date.
# MAGIC For manual runs, it resolves to today.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Triggering a single date manually via the SDK
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC run = w.jobs.run_now(
# MAGIC     job_id=123456,
# MAGIC     python_named_params={"load_date": "2024-01-10"}
# MAGIC )
# MAGIC print(f"Triggered run: {run.run_id}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Multi-task job with load_date flowing through all tasks
# MAGIC
# MAGIC ```yaml
# MAGIC tasks:
# MAGIC   # Task 1: Extract
# MAGIC   - task_key: extract
# MAGIC     notebook_task:
# MAGIC       notebook_path: ../src/extract.py
# MAGIC       base_parameters:
# MAGIC         load_date: "{{job.parameters.load_date}}"
# MAGIC
# MAGIC   # Task 2: Transform (depends on extract)
# MAGIC   - task_key: transform
# MAGIC     depends_on:
# MAGIC       - task_key: extract
# MAGIC     notebook_task:
# MAGIC       notebook_path: ../src/transform.py
# MAGIC       base_parameters:
# MAGIC         load_date: "{{job.parameters.load_date}}"   # same date, same batch
# MAGIC
# MAGIC   # Task 3: Load (depends on transform)
# MAGIC   - task_key: load
# MAGIC     depends_on:
# MAGIC       - task_key: transform
# MAGIC     run_if: ALL_SUCCESS
# MAGIC     notebook_task:
# MAGIC       notebook_path: ../src/load.py
# MAGIC       base_parameters:
# MAGIC         load_date: "{{job.parameters.load_date}}"
# MAGIC ```
# MAGIC
# MAGIC **Key:** the same `load_date` flows to every task. If the job retries Task 3,
# MAGIC it uses the same date — and the idempotent write logic handles the re-run safely.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The load_date pattern in three lines:
# MAGIC
# MAGIC ```python
# MAGIC # 1. Accept the parameter (default to today)
# MAGIC dbutils.widgets.text("load_date", "")
# MAGIC load_date = dbutils.widgets.get("load_date") or str(spark.sql("SELECT current_date()").collect()[0][0])
# MAGIC
# MAGIC # 2. Scope your reads to load_date
# MAGIC df = spark.read.table("source").filter(f"date = '{load_date}'")
# MAGIC
# MAGIC # 3. Write idempotently scoped to load_date
# MAGIC df.write.format("delta").mode("overwrite").partitionBy("date").saveAsTable("target")
# MAGIC ```
# MAGIC
# MAGIC That's it. Any pipeline following this pattern can be safely re-run for any date.
