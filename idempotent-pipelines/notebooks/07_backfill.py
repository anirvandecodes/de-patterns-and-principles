# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Backfilling
# MAGIC
# MAGIC A backfill = running your pipeline for historical dates.
# MAGIC
# MAGIC **Backfilling is only safe if your writes are idempotent.**
# MAGIC If you're using `.mode("append")` — backfilling will corrupt your data.
# MAGIC Fix the write first. Then backfilling becomes trivial.
# MAGIC
# MAGIC Three strategies depending on your situation.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

from pyspark.sql import Row
from pyspark.sql.functions import lit, col
from datetime import date, timedelta

def get_source(load_date: str):
    """Simulates reading from a source system for a given date."""
    return spark.createDataFrame([
        Row(order_id=f"{load_date}_1", amount=100.0, region="US"),
        Row(order_id=f"{load_date}_2", amount=200.0, region="EU"),
    ]).withColumn("order_date", lit(load_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 1: Full Recompute
# MAGIC
# MAGIC Drop everything and rebuild from scratch.
# MAGIC
# MAGIC **When:** small table, schema change, or logic was fundamentally wrong.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.orders_backfill")

all_dates = [str(date(2024, 1, 10) + timedelta(days=i)) for i in range(7)]

for d in all_dates:
    get_source(d).write.format("delta").mode("append") \
        .partitionBy("order_date") \
        .saveAsTable(f"{catalog}.{schema}.orders_backfill")

print(f"✓ Full recompute done — {spark.table(f'{catalog}.{schema}.orders_backfill').count()} rows across {len(all_dates)} dates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 2: Date-Loop Backfill
# MAGIC
# MAGIC Process one date at a time. Safe to resume — re-running the same date is a no-op.
# MAGIC
# MAGIC **When:** large table, only specific dates need rebuilding.

# COMMAND ----------

def backfill_date(load_date: str):
    df = get_source(load_date).withColumn("amount_corrected", col("amount") * 1.1)
    df.write.format("delta").mode("overwrite") \
        .partitionBy("order_date") \
        .saveAsTable(f"{catalog}.{schema}.orders_backfill")
    print(f"  ✓ {load_date}")

# Only Jan 12–14 had bad data — reprocess just those dates
dates_to_fix = ["2024-01-12", "2024-01-13", "2024-01-14"]

print("Backfilling affected dates:")
for d in dates_to_fix:
    backfill_date(d)

# Re-run Jan 13 — completely safe
backfill_date("2024-01-13")
print()
print(f"Total rows: {spark.table(f'{catalog}.{schema}.orders_backfill').count()} — no duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 3: Partition-Based Overwrite
# MAGIC
# MAGIC You know exactly which partition has bad data. Replace just that one.
# MAGIC Fastest — everything else is untouched.
# MAGIC
# MAGIC **When:** a specific partition has bad data and the rest of the table is correct.

# COMMAND ----------

# Jan 16 had wrong amounts — fix just that partition
get_source("2024-01-16").withColumn("amount", col("amount") * 1.15) \
    .write.format("delta").mode("overwrite") \
    .partitionBy("order_date") \
    .saveAsTable(f"{catalog}.{schema}.orders_backfill")

print(f"✓ Rebuilt Jan 16 partition only")
print(f"Total rows: {spark.table(f'{catalog}.{schema}.orders_backfill').count()} — all other dates untouched")

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.orders_backfill").orderBy("order_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backfill decision guide
# MAGIC
# MAGIC ```
# MAGIC Is the table small or logic fundamentally broken?
# MAGIC   → Strategy 1: Full Recompute
# MAGIC
# MAGIC Large table, multiple dates affected?
# MAGIC   → Strategy 2: Date-Loop (one date at a time, resumable)
# MAGIC
# MAGIC Only one or two specific partitions are wrong?
# MAGIC   → Strategy 3: Partition Overwrite (surgical, fastest)
# MAGIC ```
# MAGIC
# MAGIC **All three are safe because the underlying write is idempotent.**
