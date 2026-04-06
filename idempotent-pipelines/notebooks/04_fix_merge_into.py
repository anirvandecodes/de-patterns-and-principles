# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Fix 2: MERGE INTO
# MAGIC
# MAGIC **Run this job as many times as you want. Row count stays 3. Always.**
# MAGIC
# MAGIC When your table has no natural date partition — or you need to update individual rows —
# MAGIC `MERGE INTO` is the answer.
# MAGIC Match on a key. Update if found. Insert if new. Re-running the same data is a no-op.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

source_df = spark.createDataFrame([
    Row(order_id="O001", customer_id="C1", amount=100.0, order_date="2024-01-15"),
    Row(order_id="O002", customer_id="C2", amount=200.0, order_date="2024-01-15"),
    Row(order_id="O003", customer_id="C3", amount=150.0, order_date="2024-01-15"),
]).withColumn("_loaded_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the target table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders_fixed_merge (
        order_id    STRING,
        customer_id STRING,
        amount      DOUBLE,
        order_date  STRING,
        _loaded_at  TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (order_id)
    TBLPROPERTIES (
        'delta.enableDeletionVectors'     = 'true',
        'delta.enableRowLevelConcurrency' = 'true'
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The fixed pipeline

# COMMAND ----------

target = DeltaTable.forName(spark, f"{catalog}.{schema}.orders_fixed_merge")

(target.alias("t")
       .merge(source_df.alias("s"), "t.order_id = s.order_id")
       .whenMatchedUpdateAll()    # row exists → update it
       .whenNotMatchedInsertAll() # row is new  → insert it
       .execute())

# COMMAND ----------

total         = spark.table(f"{catalog}.{schema}.orders_fixed_merge").count()
total_revenue = spark.sql(f"SELECT SUM(amount) AS revenue FROM {catalog}.{schema}.orders_fixed_merge").collect()[0]["revenue"]

print(f"Row count : {total}  ← always 3, no matter how many times you run this")
print(f"Revenue   : ${total_revenue:,.2f}  ← always $450.00")
print()
print("✓ Idempotent. Safe to retry. Safe to backfill.")

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.orders_fixed_merge").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to use MERGE vs Partition Overwrite
# MAGIC
# MAGIC | Situation | Use |
# MAGIC |---|---|
# MAGIC | Table is partitioned by date, you process one date at a time | **Partition Overwrite** |
# MAGIC | Table has a primary key, you need row-level upsert | **MERGE INTO** |
# MAGIC | You need to handle deletes from the source | **MERGE INTO** |
# MAGIC | Bulk replace an entire day's data | **Partition Overwrite** |
