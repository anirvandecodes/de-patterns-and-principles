# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Idempotency Fundamentals
# MAGIC
# MAGIC A pipeline is idempotent if running it multiple times produces the **same result** as running it once.
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Demonstrates the duplicate problem (non-idempotent)
# MAGIC 2. Fixes it with partition overwrite
# MAGIC 3. Fixes it with MERGE INTO
# MAGIC 4. Introduces a lightweight job audit pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — configure your catalog and schema

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

print(f"Using: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The Problem: naive append is NOT idempotent
# MAGIC
# MAGIC Every re-run appends the same rows again. This is the most common source of duplicates in data pipelines.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit
from datetime import date

# Simulate a day's worth of orders arriving from a source system
def get_source_orders(load_date: str):
    return spark.createDataFrame([
        Row(order_id="O001", customer_id="C1", amount=100.0, order_date=load_date),
        Row(order_id="O002", customer_id="C2", amount=200.0, order_date=load_date),
        Row(order_id="O003", customer_id="C3", amount=150.0, order_date=load_date),
    ])

# COMMAND ----------

# First run — looks fine
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.orders_bad")

df = get_source_orders("2024-01-15")
df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.orders_bad")

count_after_run1 = spark.table(f"{catalog}.{schema}.orders_bad").count()
print(f"After run 1: {count_after_run1} rows")  # 3

# COMMAND ----------

# Simulate a re-run (retry, backfill, or accidental double trigger)
df = get_source_orders("2024-01-15")
df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.orders_bad")

count_after_run2 = spark.table(f"{catalog}.{schema}.orders_bad").count()
print(f"After run 2: {count_after_run2} rows")  # 6 — duplicates!

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.orders_bad").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fix 1: Dynamic Partition Overwrite
# MAGIC
# MAGIC Instead of appending, **replace the entire partition** for the date being processed.
# MAGIC Re-running produces the same output because we overwrite, not append.
# MAGIC
# MAGIC **Rule:** `spark.sql.sources.partitionOverwriteMode = dynamic` only overwrites partitions
# MAGIC present in the DataFrame — all other partitions are untouched.

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.orders_partition_safe")

# COMMAND ----------

def write_orders_idempotent_partition(load_date: str):
    df = get_source_orders(load_date).withColumn("order_date", lit(load_date))
    (df.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("order_date")
       .saveAsTable(f"{catalog}.{schema}.orders_partition_safe"))

# COMMAND ----------

# Run 1
write_orders_idempotent_partition("2024-01-15")
# Run 2 — simulating a re-run or retry
write_orders_idempotent_partition("2024-01-15")

count = spark.table(f"{catalog}.{schema}.orders_partition_safe").count()
print(f"After 2 runs: {count} rows")  # Still 3 — idempotent!

# COMMAND ----------

# A different date's data is untouched
write_orders_idempotent_partition("2024-01-14")
write_orders_idempotent_partition("2024-01-15")  # re-run Jan 15 only

count = spark.table(f"{catalog}.{schema}.orders_partition_safe").count()
print(f"Two dates, multiple re-runs: {count} rows")  # 6 — Jan 14 + Jan 15, no dupes

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.orders_partition_safe")
         .orderBy("order_date", "order_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fix 2: MERGE INTO (upsert by primary key)
# MAGIC
# MAGIC For tables without a natural date partition — or when you need row-level upsert semantics —
# MAGIC `MERGE INTO` is the right tool. It matches on a key and updates or inserts accordingly.
# MAGIC Running it twice with the same data produces exactly the same result.

# COMMAND ----------

from delta.tables import DeltaTable

# Create target table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders_merge_safe (
        order_id   STRING,
        customer_id STRING,
        amount     DOUBLE,
        order_date STRING,
        updated_at TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (order_id)
    TBLPROPERTIES (
        'delta.enableDeletionVectors'    = 'true',
        'delta.enableRowLevelConcurrency'= 'true'
    )
""")

# COMMAND ----------

def upsert_orders(load_date: str):
    source_df = (get_source_orders(load_date)
                 .withColumn("updated_at", current_timestamp()))

    target = DeltaTable.forName(spark, f"{catalog}.{schema}.orders_merge_safe")

    (target.alias("t")
           .merge(source_df.alias("s"), "t.order_id = s.order_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())

# COMMAND ----------

# Run 1
upsert_orders("2024-01-15")
# Run 2 — identical data, re-run
upsert_orders("2024-01-15")

count = spark.table(f"{catalog}.{schema}.orders_merge_safe").count()
print(f"After 2 runs: {count} rows")  # 3 — idempotent!

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.orders_merge_safe"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Job Audit Pattern: tracking what's already been processed
# MAGIC
# MAGIC For pipelines where checking "has this batch already run?" is important, maintain a lightweight
# MAGIC audit table. Before processing, check if the batch key exists and is marked complete.
# MAGIC This prevents double-processing even if the target table write is idempotent.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pipeline_audit (
        pipeline_name STRING,
        batch_key     STRING,
        status        STRING,
        started_at    TIMESTAMP,
        completed_at  TIMESTAMP,
        rows_written  LONG
    )
    USING DELTA
""")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp as now
from datetime import datetime

def is_already_processed(pipeline: str, batch_key: str) -> bool:
    result = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {catalog}.{schema}.pipeline_audit
        WHERE pipeline_name = '{pipeline}'
          AND batch_key      = '{batch_key}'
          AND status         = 'COMPLETED'
    """).collect()[0]["cnt"]
    return result > 0

def mark_started(pipeline: str, batch_key: str):
    spark.sql(f"""
        INSERT INTO {catalog}.{schema}.pipeline_audit
        VALUES ('{pipeline}', '{batch_key}', 'STARTED',
                current_timestamp(), NULL, NULL)
    """)

def mark_completed(pipeline: str, batch_key: str, rows: int):
    spark.sql(f"""
        UPDATE {catalog}.{schema}.pipeline_audit
        SET status = 'COMPLETED', completed_at = current_timestamp(), rows_written = {rows}
        WHERE pipeline_name = '{pipeline}' AND batch_key = '{batch_key}' AND status = 'STARTED'
    """)

# COMMAND ----------

def run_pipeline(load_date: str):
    pipeline = "orders_daily"
    batch_key = load_date

    if is_already_processed(pipeline, batch_key):
        print(f"Batch {batch_key} already completed — skipping.")
        return

    mark_started(pipeline, batch_key)

    df = get_source_orders(load_date)
    upsert_orders(load_date)
    rows = df.count()

    mark_completed(pipeline, batch_key, rows)
    print(f"Batch {batch_key} completed. Rows written: {rows}")

# COMMAND ----------

run_pipeline("2024-01-16")  # Runs normally
run_pipeline("2024-01-16")  # Skipped — already done

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.pipeline_audit"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Approach | Idempotent? | Best for |
# MAGIC |---|---|---|
# MAGIC | `.mode("append")` | NO | Never for idempotent pipelines |
# MAGIC | Partition overwrite (dynamic) | YES | Date-partitioned tables, bulk replace |
# MAGIC | `MERGE INTO` | YES | Key-based upserts, no natural partition |
# MAGIC | Job audit table | YES | Guarding against double-processing |
# MAGIC
# MAGIC Next: [02-delta-lake-foundations](../02-delta-lake-foundations/README.md) — deep dive on each technique.
