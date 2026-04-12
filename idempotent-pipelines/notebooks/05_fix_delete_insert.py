# Databricks notebook source
# MAGIC %md
# MAGIC # Fix 3: DELETE + INSERT
# MAGIC
# MAGIC A simpler alternative to MERGE when you don't need to handle updates at the row level.
# MAGIC
# MAGIC **The pattern:**
# MAGIC 1. Delete all rows for the batch key (e.g. `order_date = '2024-01-15'`)
# MAGIC 2. Insert the fresh data
# MAGIC
# MAGIC Both steps together are atomic in Delta Lake — if the job fails mid-way, the transaction rolls back.
# MAGIC Re-running starts from a clean slate for that batch key.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema",  "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

def get_source(load_date: str):
    return spark.createDataFrame([
        Row(order_id="O001", customer_id="C1", amount=100.0, order_date=load_date),
        Row(order_id="O002", customer_id="C2", amount=200.0, order_date=load_date),
        Row(order_id="O003", customer_id="C3", amount=150.0, order_date=load_date),
    ]).withColumn("_loaded_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the target table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders_delete_insert (
        order_id    STRING,
        customer_id STRING,
        amount      DOUBLE,
        order_date  STRING,
        _loaded_at  TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The idempotent pipeline — DELETE then INSERT

# COMMAND ----------

def load(load_date: str):
    # Step 1: delete all rows for this batch key
    spark.sql(f"""
        DELETE FROM {catalog}.{schema}.orders_delete_insert
        WHERE order_date = '{load_date}'
    """)

    # Step 2: insert fresh data
    get_source(load_date).write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"{catalog}.{schema}.orders_delete_insert")

# COMMAND ----------

# Run it multiple times — count stays the same
load("2024-01-15")
load("2024-01-15")
load("2024-01-15")

total         = spark.table(f"{catalog}.{schema}.orders_delete_insert").count()
total_revenue = spark.sql(f"SELECT SUM(amount) AS r FROM {catalog}.{schema}.orders_delete_insert").collect()[0]["r"]

print(f"Row count : {total}   ← always 3")
print(f"Revenue   : ${total_revenue:,.2f}  ← always $450.00")
print()
print("✓ Idempotent. DELETE removes stale rows. INSERT brings fresh data.")

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.orders_delete_insert").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to use DELETE + INSERT vs MERGE
# MAGIC
# MAGIC | | DELETE + INSERT | MERGE INTO |
# MAGIC |---|---|---|
# MAGIC | **Syntax** | Simpler | More expressive |
# MAGIC | **Handles deletes from source** | No | Yes |
# MAGIC | **Row-level update logic** | No | Yes (`WHEN MATCHED AND condition`) |
# MAGIC | **Best for** | Replace all rows for a batch key | Row-level CDC, soft deletes |
# MAGIC
# MAGIC If your logic is "replace everything for this date" — DELETE + INSERT is cleaner.
# MAGIC If you need to handle individual row updates or deletes — use MERGE.
