# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Anti-Patterns: What Silently Breaks Idempotency
# MAGIC
# MAGIC Each cell shows a real mistake and its fix.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit, col, sha2, concat_ws
from delta.tables import DeltaTable

def get_orders():
    return spark.createDataFrame([
        Row(order_id="O001", amount=100.0, order_date="2024-01-15"),
        Row(order_id="O002", amount=200.0, order_date="2024-01-15"),
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 1: Naked Append
# MAGIC **Row count grows every run. No error raised.**

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap1_bad")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap1_good")

# BAD
for run in range(1, 4):
    get_orders().write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.ap1_bad")

# GOOD
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
for run in range(1, 4):
    get_orders().write.format("delta").mode("overwrite").partitionBy("order_date") \
        .saveAsTable(f"{catalog}.{schema}.ap1_good")

bad  = spark.table(f"{catalog}.{schema}.ap1_bad").count()
good = spark.table(f"{catalog}.{schema}.ap1_good").count()
print(f"BAD  (append):            {bad} rows after 3 runs  ✗")
print(f"GOOD (partition overwrite): {good} rows after 3 runs  ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 2: Static Partition Overwrite (the default)
# MAGIC **Silently deletes all other partitions when you re-run for one date.**

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap2")
for d in ["2024-01-13", "2024-01-14", "2024-01-15"]:
    spark.createDataFrame([Row(order_id="X", amount=1.0, order_date=d)]) \
        .write.format("delta").mode("append").partitionBy("order_date") \
        .saveAsTable(f"{catalog}.{schema}.ap2")

before = spark.table(f"{catalog}.{schema}.ap2").count()

# Re-run only Jan 15 — static mode nukes Jan 13 and Jan 14!
get_orders().write.format("delta").mode("overwrite").partitionBy("order_date") \
    .saveAsTable(f"{catalog}.{schema}.ap2")

after = spark.table(f"{catalog}.{schema}.ap2").count()
print(f"Before: {before} rows (3 dates)")
print(f"After:  {after} row  (Jan 13 + Jan 14 silently deleted)  ✗")
print()
print("Fix: spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')")

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 3: Partitioning by Ingestion Timestamp
# MAGIC **Every run creates a new partition. Table grows forever.**

# COMMAND ----------

from pyspark.sql.functions import date_format

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap3_bad")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap3_good")

# BAD — partition by when the job ran
for _ in range(3):
    get_orders() \
        .withColumn("ingestion_ts", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
        .write.format("delta").mode("append").partitionBy("ingestion_ts") \
        .saveAsTable(f"{catalog}.{schema}.ap3_bad")

# GOOD — partition by business date
for _ in range(3):
    get_orders() \
        .write.format("delta").mode("overwrite").partitionBy("order_date") \
        .saveAsTable(f"{catalog}.{schema}.ap3_good")

bad_parts  = spark.sql(f"SHOW PARTITIONS {catalog}.{schema}.ap3_bad").count()
good_parts = spark.sql(f"SHOW PARTITIONS {catalog}.{schema}.ap3_good").count()
print(f"BAD  (partition by ingestion time): {bad_parts} partitions after 3 runs  ✗")
print(f"GOOD (partition by business date):  {good_parts} partition after 3 runs   ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 4: Generating UUID as a Key
# MAGIC **Different key every run — MERGE and deduplication become impossible.**

# COMMAND ----------

from pyspark.sql.functions import expr

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap4_bad")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap4_good")

# BAD — new UUID every run
for _ in range(2):
    get_orders().withColumn("id", expr("uuid()")) \
        .write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.ap4_bad")

# GOOD — deterministic key from business attributes
for _ in range(2):
    get_orders() \
        .withColumn("id", sha2(concat_ws("|", col("order_id"), col("order_date")), 256)) \
        .write.format("delta").mode("overwrite").partitionBy("order_date") \
        .saveAsTable(f"{catalog}.{schema}.ap4_good")

bad  = spark.table(f"{catalog}.{schema}.ap4_bad").count()
good = spark.table(f"{catalog}.{schema}.ap4_good").count()
print(f"BAD  (uuid key):             {bad} rows after 2 runs  ✗")
print(f"GOOD (deterministic key):    {good} rows after 2 runs  ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 5: DROP TABLE Without IF NOT EXISTS
# MAGIC **Crashes on re-run because the table was already dropped.**

# COMMAND ----------

# BAD — fails on second run
# spark.sql("DROP TABLE my_table")   ← AnalysisException if already dropped

# GOOD — always safe
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap5_example")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap5_example")  # safe to call twice
print("✓ DROP TABLE IF NOT EXISTS — never fails on re-run")

spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ap5_example (id STRING) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ap5_example (id STRING) USING DELTA")  # safe
print("✓ CREATE TABLE IF NOT EXISTS — never fails on re-run")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Anti-Pattern | Effect | Fix |
# MAGIC |---|---|---|
# MAGIC | Naked append | Duplicates every run | Partition overwrite or MERGE |
# MAGIC | Static partition overwrite | Deletes all other partitions | Set `partitionOverwriteMode=dynamic` |
# MAGIC | Partition by ingestion time | New partition every run | Partition by business date |
# MAGIC | UUID as key | Can't deduplicate or MERGE | Hash of business attributes |
# MAGIC | DROP TABLE without IF NOT EXISTS | Crashes on re-run | Always use IF NOT EXISTS |
