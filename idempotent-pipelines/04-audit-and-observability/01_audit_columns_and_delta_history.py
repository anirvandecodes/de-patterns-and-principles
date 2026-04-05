# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Audit Columns and Delta History
# MAGIC
# MAGIC How do you prove your pipeline ran correctly? How do you debug a re-run that produced
# MAGIC unexpected results? How do you recover if a bad write went through?
# MAGIC
# MAGIC This notebook covers:
# MAGIC 1. Standard audit columns — every row knows when and how it arrived
# MAGIC 2. `DESCRIBE HISTORY` — Delta's built-in write ledger
# MAGIC 3. Time travel — query the table at any prior version
# MAGIC 4. Comparing runs: was this re-run truly idempotent?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Standard Audit Columns
# MAGIC
# MAGIC Add these to every table at the ingestion (bronze) layer.
# MAGIC They answer: "when did this row arrive, from where, and via which run?"

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit, col
from delta.tables import DeltaTable
import uuid

def get_raw_orders(load_date: str):
    """Simulate raw orders from a source."""
    return spark.createDataFrame([
        Row(order_id="O001", customer_id="C1", amount=100.0),
        Row(order_id="O002", customer_id="C2", amount=200.0),
        Row(order_id="O003", customer_id="C3", amount=150.0),
    ])

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders_audited (
        order_id       STRING,
        customer_id    STRING,
        amount         DOUBLE,
        order_date     STRING,
        -- audit columns
        _ingested_at   TIMESTAMP,
        _pipeline_run  STRING,
        _load_date     STRING
    )
    USING DELTA
    PARTITIONED BY (order_date)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")

# COMMAND ----------

def ingest_with_audit(load_date: str, run_id: str = None):
    """Load data for a date, stamping each row with audit metadata."""
    run_id = run_id or str(uuid.uuid4())[:8]

    df = (get_raw_orders(load_date)
          .withColumn("order_date",     lit(load_date))
          .withColumn("_ingested_at",   current_timestamp())
          .withColumn("_pipeline_run",  lit(run_id))
          .withColumn("_load_date",     lit(load_date)))

    (df.write
       .format("delta")
       .mode("overwrite")           # dynamic partition overwrite
       .partitionBy("order_date")
       .saveAsTable(f"{catalog}.{schema}.orders_audited"))

    print(f"Ingested {load_date} | run_id={run_id}")

# COMMAND ----------

ingest_with_audit("2024-01-15", run_id="run_001")
display(spark.table(f"{catalog}.{schema}.orders_audited"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now re-run (simulate a retry). Audit columns let you see the most recent run's metadata.

# COMMAND ----------

ingest_with_audit("2024-01-15", run_id="run_002")  # Re-run

display(
    spark.table(f"{catalog}.{schema}.orders_audited")
         .filter("order_date = '2024-01-15'")
         .select("order_id", "_ingested_at", "_pipeline_run")
)
# _pipeline_run will show "run_002" — the latest run replaced the partition

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DESCRIBE HISTORY — Delta's write ledger
# MAGIC
# MAGIC Every write to a Delta table is recorded: version number, timestamp, operation, row counts.
# MAGIC This is your audit trail — it tells you exactly what happened and when.

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {catalog}.{schema}.orders_audited")
display(history.select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading operation metrics
# MAGIC
# MAGIC `operationMetrics` contains row-level counts for each write:
# MAGIC
# MAGIC | Metric | Meaning |
# MAGIC |---|---|
# MAGIC | `numOutputRows` | Rows written in this operation |
# MAGIC | `numAddedFiles` | Parquet files added |
# MAGIC | `numRemovedFiles` | Parquet files removed (replaced) |
# MAGIC | `numTargetRowsInserted` | (MERGE) rows inserted |
# MAGIC | `numTargetRowsUpdated` | (MERGE) rows updated |

# COMMAND ----------

# Extract row counts from the latest operation
latest = spark.sql(f"""
    SELECT version, timestamp, operation,
           operationMetrics['numOutputRows']    AS rows_written,
           operationMetrics['numAddedFiles']    AS files_added,
           operationMetrics['numRemovedFiles']  AS files_removed
    FROM (DESCRIBE HISTORY {catalog}.{schema}.orders_audited)
    ORDER BY version DESC
    LIMIT 5
""")
display(latest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time Travel — query the table at any prior version
# MAGIC
# MAGIC Delta keeps every historical version of the table (up to the retention period, default 30 days).
# MAGIC You can query any prior version to compare, debug, or restore.

# COMMAND ----------

# Query by version number
display(
    spark.sql(f"""
        SELECT * FROM {catalog}.{schema}.orders_audited VERSION AS OF 0
    """)
)

# COMMAND ----------

# Query by timestamp
display(
    spark.sql(f"""
        SELECT * FROM {catalog}.{schema}.orders_audited
        TIMESTAMP AS OF (
            SELECT timestamp FROM (DESCRIBE HISTORY {catalog}.{schema}.orders_audited)
            ORDER BY version ASC LIMIT 1
        )
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparing two versions to verify idempotency

# COMMAND ----------

# Get the two most recent versions
versions = spark.sql(f"""
    SELECT version FROM (DESCRIBE HISTORY {catalog}.{schema}.orders_audited)
    ORDER BY version DESC LIMIT 2
""").collect()

if len(versions) >= 2:
    v_latest = versions[0]["version"]
    v_prev   = versions[1]["version"]

    latest_df = spark.sql(f"SELECT order_id, amount FROM {catalog}.{schema}.orders_audited VERSION AS OF {v_latest}")
    prev_df   = spark.sql(f"SELECT order_id, amount FROM {catalog}.{schema}.orders_audited VERSION AS OF {v_prev}")

    diff = latest_df.exceptAll(prev_df).union(prev_df.exceptAll(latest_df))
    diff_count = diff.count()

    if diff_count == 0:
        print(f"Versions {v_prev} and {v_latest} are IDENTICAL — re-run was idempotent.")
    else:
        print(f"Versions differ by {diff_count} rows — re-run changed data.")
        display(diff)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Restoring a previous version (rollback)
# MAGIC
# MAGIC If a bad write went through, you can restore the table to any prior version.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Restore to a specific version
# MAGIC spark.sql(f"""
# MAGIC     RESTORE TABLE {catalog}.{schema}.orders_audited TO VERSION AS OF 0
# MAGIC """)
# MAGIC
# MAGIC # Or restore to a timestamp
# MAGIC spark.sql(f"""
# MAGIC     RESTORE TABLE {catalog}.{schema}.orders_audited
# MAGIC     TO TIMESTAMP AS OF '2024-01-15 06:00:00'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC After a restore, `DESCRIBE HISTORY` will show the restore operation as a new version —
# MAGIC so you always have a full audit trail even through rollbacks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pipeline run audit table
# MAGIC
# MAGIC For end-to-end observability across multiple pipelines, maintain a centralized audit table.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pipeline_runs (
        pipeline_name  STRING,
        load_date      STRING,
        status         STRING,   -- STARTED, COMPLETED, FAILED
        started_at     TIMESTAMP,
        completed_at   TIMESTAMP,
        rows_written   LONG,
        run_id         STRING,
        error_message  STRING
    )
    USING DELTA
    CLUSTER BY (pipeline_name, load_date)
""")

# COMMAND ----------

from contextlib import contextmanager

@contextmanager
def pipeline_run(pipeline_name: str, load_date: str, run_id: str):
    """Context manager that records start/end of every pipeline run."""
    spark.sql(f"""
        INSERT INTO {catalog}.{schema}.pipeline_runs
        VALUES ('{pipeline_name}', '{load_date}', 'STARTED',
                current_timestamp(), NULL, NULL, '{run_id}', NULL)
    """)
    try:
        yield
        # Update on success
        spark.sql(f"""
            UPDATE {catalog}.{schema}.pipeline_runs
            SET status = 'COMPLETED', completed_at = current_timestamp()
            WHERE pipeline_name = '{pipeline_name}'
              AND load_date = '{load_date}'
              AND run_id = '{run_id}'
        """)
    except Exception as e:
        spark.sql(f"""
            UPDATE {catalog}.{schema}.pipeline_runs
            SET status = 'FAILED', completed_at = current_timestamp(),
                error_message = '{str(e)[:500]}'
            WHERE pipeline_name = '{pipeline_name}'
              AND load_date = '{load_date}'
              AND run_id = '{run_id}'
        """)
        raise

# COMMAND ----------

with pipeline_run("orders_daily", "2024-01-15", "run_abc"):
    ingest_with_audit("2024-01-15", run_id="run_abc")

display(spark.table(f"{catalog}.{schema}.pipeline_runs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Tool | What it answers |
# MAGIC |---|---|
# MAGIC | Audit columns (`_ingested_at`, `_pipeline_run`) | Which run wrote this row? When? |
# MAGIC | `DESCRIBE HISTORY` | What operations happened on this table? |
# MAGIC | Time travel (`VERSION AS OF`) | What did the table look like before this run? |
# MAGIC | Version diff | Was that re-run truly idempotent? |
# MAGIC | `RESTORE TABLE` | Roll back a bad write |
# MAGIC | Pipeline runs table | Which pipelines ran for which dates? |
