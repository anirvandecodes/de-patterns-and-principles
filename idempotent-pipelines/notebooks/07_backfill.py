# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # orders_daily_pipeline
# MAGIC
# MAGIC Loads daily orders from the source system and upserts into the orders table using MERGE.
# MAGIC Idempotent — re-running the same `load_date` updates existing rows, never duplicates.

# COMMAND ----------

dbutils.widgets.text("load_date",        "")
dbutils.widgets.text("catalog",          "workspace")
dbutils.widgets.text("schema",           "idempotency_demo")
dbutils.widgets.text("simulate_failure", "true")   # set to "false" when backfilling

load_date        = dbutils.widgets.get("load_date")
catalog          = dbutils.widgets.get("catalog")
schema           = dbutils.widgets.get("schema")
simulate_failure = dbutils.widgets.get("simulate_failure").lower() == "true"

assert load_date, "load_date parameter is required"

print(f"load_date:        {load_date}")
print(f"simulate_failure: {simulate_failure}")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import lit, current_timestamp, col
from delta.tables import DeltaTable

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Extract: pull orders from source system

# COMMAND ----------

# Simulates a source system outage.
# Set simulate_failure=true to fail the run, false to succeed (e.g. during backfill).
if simulate_failure:
    import random, hashlib
    EXTRACT_ERRORS = [
        "Payment gateway timeout — no orders received after 3 retries",
        "Source DB connection refused — orders_api replica is unavailable",
        "S3 export incomplete — manifest file missing for partition dt={load_date}",
        "Schema mismatch — column 'unit_price' not found in upstream feed",
        "Row count anomaly — received 0 rows, expected ~500 for {load_date}",
        "SSL certificate error — cannot verify identity of orders-api.internal",
    ]
    seed = int(hashlib.md5(load_date.encode()).hexdigest(), 16)
    error = random.Random(seed).choice(EXTRACT_ERRORS).format(load_date=load_date)
    raise RuntimeError(f"[{load_date}] Extract failed: {error}")

# Simulated order feed for the day
orders_raw = [
    Row(order_id=f"ORD-{load_date}-001", customer_id="C100", product="Laptop Pro 15",       qty=1, unit_price=1299.99, status="SHIPPED"),
    Row(order_id=f"ORD-{load_date}-002", customer_id="C204", product="Wireless Mouse",      qty=3, unit_price=  29.99, status="DELIVERED"),
    Row(order_id=f"ORD-{load_date}-003", customer_id="C087", product="USB-C Hub",           qty=2, unit_price=  49.99, status="PENDING"),
    Row(order_id=f"ORD-{load_date}-004", customer_id="C311", product="Mechanical Keyboard", qty=1, unit_price= 149.99, status="SHIPPED"),
    Row(order_id=f"ORD-{load_date}-005", customer_id="C100", product='Monitor 27"',         qty=1, unit_price= 399.99, status="PENDING"),
]

incoming = (
    spark.createDataFrame(orders_raw)
    .withColumn("order_date", lit(load_date))
    .withColumn("updated_at", current_timestamp())
    .filter(col("order_date") == load_date)   # guard: only process today's records
)

display(incoming)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Load: MERGE into target table
# MAGIC
# MAGIC Match on `order_id` (natural key).
# MAGIC - **Matched** → update `status` and `updated_at` (status can change between runs)
# MAGIC - **Not matched** → insert new order
# MAGIC
# MAGIC Re-running the same `load_date` is safe — existing rows are updated in place.

# COMMAND ----------

table_path = f"{catalog}.{schema}.orders_jobs"

# Create table on first run
incoming.write.format("delta").mode("ignore").saveAsTable(table_path)

(
    DeltaTable.forName(spark, table_path)
    .alias("tgt")
    .merge(incoming.alias("src"), "tgt.order_id = src.order_id")
    .whenMatchedUpdate(set={
        "status":     "src.status",
        "updated_at": "src.updated_at",
    })
    .whenNotMatchedInsertAll()
    .execute()
)

row_count = spark.table(table_path).filter(col("order_date") == load_date).count()
print(f"✓ Merged {load_date} → {table_path}")
print(f"  Rows for {load_date}: {row_count}  ← re-run this cell, count stays the same")
