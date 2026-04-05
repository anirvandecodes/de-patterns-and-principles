# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Dynamic Partition Overwrite
# MAGIC
# MAGIC The simplest idempotency technique for date-partitioned batch pipelines.
# MAGIC Instead of appending, you **replace** the partition you're writing to.
# MAGIC Re-running the same date produces the same result.
# MAGIC
# MAGIC **Key insight:** With `partitionOverwriteMode = dynamic`, Spark only replaces
# MAGIC partitions that exist in the incoming DataFrame — all other partitions are untouched.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static vs Dynamic: understand the difference
# MAGIC
# MAGIC ```
# MAGIC Table has partitions: 2024-01-13, 2024-01-14, 2024-01-15
# MAGIC You write a DataFrame for 2024-01-15 only.
# MAGIC
# MAGIC STATIC overwrite:  wipes ALL partitions → only 2024-01-15 remains  ← DANGEROUS
# MAGIC DYNAMIC overwrite: replaces ONLY 2024-01-15 → all three dates intact  ← CORRECT
# MAGIC ```

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import lit

def make_sales(date_str: str, multiplier: int = 1):
    """Generate sample sales rows for a given date."""
    return spark.createDataFrame([
        Row(sale_id=f"S{i:03d}", region="US", revenue=float(i * 100 * multiplier),
            sale_date=date_str)
        for i in range(1, 4)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Static overwrite (dangerous — avoid)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.sales_static")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

# Write three dates
for d in ["2024-01-13", "2024-01-14", "2024-01-15"]:
    make_sales(d).write.format("delta").mode("overwrite").partitionBy("sale_date") \
        .saveAsTable(f"{catalog}.{schema}.sales_static")

print("Before re-run:")
display(spark.table(f"{catalog}.{schema}.sales_static").orderBy("sale_date", "sale_id"))

# COMMAND ----------

# Now re-run only Jan 15 — static mode nukes everything else
make_sales("2024-01-15").write.format("delta").mode("overwrite").partitionBy("sale_date") \
    .saveAsTable(f"{catalog}.{schema}.sales_static")

count = spark.table(f"{catalog}.{schema}.sales_static").count()
print(f"After static re-run for Jan 15 only: {count} rows")  # Only 3 — Jan 13 + Jan 14 are GONE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic overwrite (correct — always use this)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.sales_dynamic")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Write three dates
for d in ["2024-01-13", "2024-01-14", "2024-01-15"]:
    make_sales(d).write.format("delta").mode("overwrite").partitionBy("sale_date") \
        .saveAsTable(f"{catalog}.{schema}.sales_dynamic")

print("Before re-run:")
print(f"  Rows: {spark.table(f'{catalog}.{schema}.sales_dynamic').count()}")

# COMMAND ----------

# Re-run only Jan 15 — dynamic mode only touches that partition
make_sales("2024-01-15", multiplier=99).write \
    .format("delta").mode("overwrite").partitionBy("sale_date") \
    .saveAsTable(f"{catalog}.{schema}.sales_dynamic")

print("After dynamic re-run for Jan 15 only:")
display(
    spark.table(f"{catalog}.{schema}.sales_dynamic").orderBy("sale_date", "sale_id")
)
# Jan 13 and Jan 14 are still there. Jan 15 is updated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency proof: run the same date twice, same result

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.sales_idempotent")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def load_date(date_str: str):
    """The canonical idempotent batch load function."""
    df = make_sales(date_str)
    (df.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("sale_date")
       .saveAsTable(f"{catalog}.{schema}.sales_idempotent"))
    return df.count()

# COMMAND ----------

load_date("2024-01-15")
count_run1 = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {catalog}.{schema}.sales_idempotent "
    f"WHERE sale_date = '2024-01-15'"
).collect()[0]["cnt"]

load_date("2024-01-15")  # Re-run
count_run2 = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {catalog}.{schema}.sales_idempotent "
    f"WHERE sale_date = '2024-01-15'"
).collect()[0]["cnt"]

print(f"After run 1: {count_run1} rows for Jan 15")
print(f"After run 2: {count_run2} rows for Jan 15")
assert count_run1 == count_run2, "Idempotency violated!"
print("Idempotency confirmed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to use partition overwrite vs MERGE INTO
# MAGIC
# MAGIC | Situation | Use |
# MAGIC |---|---|
# MAGIC | Table is partitioned by date and you process one date at a time | **Partition overwrite** |
# MAGIC | You're replacing an entire day's snapshot | **Partition overwrite** |
# MAGIC | You need row-level upsert (update individual records) | **MERGE INTO** |
# MAGIC | Table has no natural date partition | **MERGE INTO** |
# MAGIC | You have a primary key and need deduplication | **MERGE INTO** |
# MAGIC
# MAGIC ## Gotchas
# MAGIC
# MAGIC 1. **Always set `dynamic` mode explicitly** — the default (`static`) will silently destroy partitions.
# MAGIC 2. **The DataFrame must contain at least one row for the partition you want to overwrite.** An empty DataFrame won't touch anything — the partition will remain as-is.
# MAGIC 3. **Multiple columns can be partition keys** — all combinations present in the DataFrame will be overwritten.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting dynamic mode globally in a job
# MAGIC
# MAGIC Add this to your cluster's Spark config to avoid setting it per notebook:
# MAGIC
# MAGIC ```
# MAGIC spark.sql.sources.partitionOverwriteMode dynamic
# MAGIC ```
# MAGIC
# MAGIC Or set it once at the top of every pipeline notebook:
# MAGIC
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# MAGIC ```
