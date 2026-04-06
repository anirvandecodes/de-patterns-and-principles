# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # The Problem: What Happens When You Re-run
# MAGIC
# MAGIC **Run this job multiple times and watch the row count grow.**
# MAGIC
# MAGIC This simulates a real daily pipeline that processes orders for Jan 15.
# MAGIC Every time the job runs — intentionally or not — the same rows get appended again.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

# This is the source — 3 orders for Jan 15
# Think of this as reading from your source system
source_df = spark.createDataFrame([
    Row(order_id="O001", customer_id="C1", amount=100.0, order_date="2024-01-15"),
    Row(order_id="O002", customer_id="C2", amount=200.0, order_date="2024-01-15"),
    Row(order_id="O003", customer_id="C3", amount=150.0, order_date="2024-01-15"),
]).withColumn("_loaded_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## The pipeline — written the naive way

# COMMAND ----------

# ⚠️ No DROP TABLE here — this table lives between job runs
# That is exactly how a real pipeline works
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders
    (order_id STRING, customer_id STRING, amount DOUBLE,
     order_date STRING, _loaded_at TIMESTAMP)
    USING DELTA
""")

# The classic mistake: .mode("append") with no deduplication
source_df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What does the table look like now?

# COMMAND ----------

total       = spark.table(f"{catalog}.{schema}.orders").count()
total_revenue = spark.sql(f"SELECT SUM(amount) AS revenue FROM {catalog}.{schema}.orders").collect()[0]["revenue"]

print(f"Row count  : {total}")
print(f"Revenue    : ${total_revenue:,.2f}")
print()

expected_rows    = 3
expected_revenue = 450.0

if total == expected_rows:
    print("✓ Run 1 — looks correct. 3 rows, $450 revenue.")
else:
    multiplier = total // expected_rows
    print(f"✗ Run {multiplier} — {total} rows. Every order appears {multiplier}x.")
    print(f"✗ Revenue shows ${total_revenue:,.2f} — should be ${expected_revenue:,.2f}.")
    print()
    print("  No error was raised. The job shows SUCCESS in the UI.")
    print("  The data is silently wrong.")

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.orders").orderBy("order_id", "_loaded_at"))
