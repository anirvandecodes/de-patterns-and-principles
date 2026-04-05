# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Anti-Patterns: What Breaks Idempotency
# MAGIC
# MAGIC Each section below shows a real mistake, explains why it breaks idempotency,
# MAGIC and gives the correct replacement.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit, col
from delta.tables import DeltaTable

def make_df():
    return spark.createDataFrame([
        Row(id="A", value=1.0, date="2024-01-15"),
        Row(id="B", value=2.0, date="2024-01-15"),
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 1: Naked Append
# MAGIC
# MAGIC **The mistake:** using `.mode("append")` for a pipeline that may be re-run.

# COMMAND ----------

# BAD: every re-run adds more rows
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap1_bad")
make_df().write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.ap1_bad")
make_df().write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.ap1_bad")
print(f"BAD — row count after 2 runs: {spark.table(f'{catalog}.{schema}.ap1_bad').count()}")  # 4

# COMMAND ----------

# GOOD: partition overwrite — re-run is a no-op
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap1_good")
for _ in range(2):
    make_df().write.format("delta").mode("overwrite").partitionBy("date") \
        .saveAsTable(f"{catalog}.{schema}.ap1_good")
print(f"GOOD — row count after 2 runs: {spark.table(f'{catalog}.{schema}.ap1_good').count()}")  # 2

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 2: Static Partition Overwrite (the default)
# MAGIC
# MAGIC **The mistake:** relying on the default `partitionOverwriteMode = static` when writing
# MAGIC to a partitioned table. This silently drops ALL other partitions.

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")  # the dangerous default

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap2_bad")

# Seed with 2 dates
for d in ["2024-01-14", "2024-01-15"]:
    spark.createDataFrame([Row(id="X", value=1.0, date=d)]) \
         .write.format("delta").mode("append").partitionBy("date") \
         .saveAsTable(f"{catalog}.{schema}.ap2_bad")

print(f"Before: {spark.table(f'{catalog}.{schema}.ap2_bad').count()} rows (2 dates)")

# Re-run for Jan 15 only — static mode nukes Jan 14!
make_df().write.format("delta").mode("overwrite").partitionBy("date") \
    .saveAsTable(f"{catalog}.{schema}.ap2_bad")

print(f"BAD (static) — after: {spark.table(f'{catalog}.{schema}.ap2_bad').count()} rows (Jan 14 gone!)")

# COMMAND ----------

# GOOD: dynamic overwrite — only the matching partition is replaced
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")  # always set this

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap2_good")
for d in ["2024-01-14", "2024-01-15"]:
    spark.createDataFrame([Row(id="X", value=1.0, date=d)]) \
         .write.format("delta").mode("append").partitionBy("date") \
         .saveAsTable(f"{catalog}.{schema}.ap2_good")

make_df().write.format("delta").mode("overwrite").partitionBy("date") \
    .saveAsTable(f"{catalog}.{schema}.ap2_good")

print(f"GOOD (dynamic) — after: {spark.table(f'{catalog}.{schema}.ap2_good').count()} rows (Jan 14 intact)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 3: Using `current_timestamp()` as a partition key
# MAGIC
# MAGIC **The mistake:** partitioning by an ingestion timestamp that changes on every run.
# MAGIC Each re-run creates a new partition, so the table grows without bound.

# COMMAND ----------

from pyspark.sql.functions import date_format

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap3_bad")

# BAD: partition by ingestion time — new partition every run
for _ in range(3):
    (make_df()
     .withColumn("ingestion_date", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
     .write.format("delta").mode("append").partitionBy("ingestion_date")
     .saveAsTable(f"{catalog}.{schema}.ap3_bad"))

partitions_bad = spark.sql(f"SHOW PARTITIONS {catalog}.{schema}.ap3_bad").count()
print(f"BAD — {partitions_bad} partitions after 3 runs (grows every run)")

# COMMAND ----------

# GOOD: partition by the business date (load_date), not the ingestion timestamp
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap3_good")

for _ in range(3):
    (make_df()
     .withColumn("_ingested_at", current_timestamp())  # keep as an audit column
     .write.format("delta").mode("overwrite").partitionBy("date")   # partition by business date
     .saveAsTable(f"{catalog}.{schema}.ap3_good"))

partitions_good = spark.sql(f"SHOW PARTITIONS {catalog}.{schema}.ap3_good").count()
print(f"GOOD — {partitions_good} partition(s) after 3 runs (stable)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 4: DROP TABLE without IF NOT EXISTS guard on re-run
# MAGIC
# MAGIC **The mistake:** a notebook that drops and recreates a table at the top level
# MAGIC (not inside a function with a guard). On retry, the table is dropped even if
# MAGIC a later task already wrote good data.

# COMMAND ----------

# BAD: side-effectful code at notebook top level
# spark.sql("DROP TABLE my_staging")   ← runs on every retry, including retries after partial success

# COMMAND ----------

# GOOD 1: use CREATE OR REPLACE only when you truly mean to reset
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.staging_safe
    (id STRING, value DOUBLE)
    USING DELTA
""")
# This is safe because CREATE OR REPLACE is an atomic swap, not a separate drop+create

# COMMAND ----------

# GOOD 2: guard destructive operations with a run_if: ALL_SUCCESS in your job YAML
# so cleanup only runs after the pipeline succeeded.
#
# tasks:
#   - task_key: drop_staging
#     depends_on: [transform]
#     run_if: ALL_SUCCESS          <- staging is dropped ONLY if transform succeeded
#     notebook_task:
#       notebook_path: ../src/drop_staging.py

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 5: Generating a new UUID as a key on each run
# MAGIC
# MAGIC **The mistake:** using `uuid()` or `monotonically_increasing_id()` to generate
# MAGIC surrogate keys. Each re-run creates different keys for the same logical rows.

# COMMAND ----------

from pyspark.sql.functions import expr, monotonically_increasing_id

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap5_bad")

# BAD: different UUID on every run — can't MERGE or deduplicate
for _ in range(2):
    (make_df()
     .withColumn("surrogate_key", expr("uuid()"))
     .write.format("delta").mode("append")
     .saveAsTable(f"{catalog}.{schema}.ap5_bad"))

print(f"BAD — {spark.table(f'{catalog}.{schema}.ap5_bad').count()} rows after 2 runs")
display(spark.table(f"{catalog}.{schema}.ap5_bad"))

# COMMAND ----------

# GOOD: derive the key deterministically from business attributes
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.ap5_good")

from pyspark.sql.functions import sha2, concat_ws

for _ in range(2):
    (make_df()
     .withColumn("surrogate_key", sha2(concat_ws("|", col("id"), col("date")), 256))
     .write.format("delta").mode("overwrite").partitionBy("date")
     .saveAsTable(f"{catalog}.{schema}.ap5_good"))

print(f"GOOD — {spark.table(f'{catalog}.{schema}.ap5_good').count()} rows after 2 runs")
display(spark.table(f"{catalog}.{schema}.ap5_good"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 6: Overwriting the whole table to update one partition
# MAGIC
# MAGIC **The mistake:** reading the full target table, applying an update, and writing the whole
# MAGIC thing back. This is slow, expensive, and fails under concurrency.

# COMMAND ----------

# BAD: read all + overwrite all just to update one day
# df_all = spark.table("target")
# df_updated = df_all.filter("date != '2024-01-15'").union(new_jan15_data)
# df_updated.write.format("delta").mode("overwrite").saveAsTable("target")  # replaces everything

# COMMAND ----------

# GOOD: update only the affected partition using dynamic overwrite
# new_jan15_data.write.format("delta").mode("overwrite").partitionBy("date") \
#     .saveAsTable("target")
# Only the 2024-01-15 partition is replaced. All others are untouched.

print("Anti-pattern 6: conceptual — see comments above for the pattern")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 7: No `CREATE SCHEMA IF NOT EXISTS` guard
# MAGIC
# MAGIC **The mistake:** `CREATE SCHEMA my_schema` (without `IF NOT EXISTS`) fails on re-run
# MAGIC because the schema already exists. This crashes the notebook before any data is written.

# COMMAND ----------

# BAD: fails on re-run
# spark.sql("CREATE SCHEMA my_schema")  ← AnalysisException: schema already exists

# GOOD: always idempotent
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print("Schema created (or already existed) — no error")

# Same principle applies to tables:
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.setup_example
    (id STRING) USING DELTA
""")
print("Table created (or already existed) — no error")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Anti-Pattern 8: Missing `run_if` on a publish/notify task
# MAGIC
# MAGIC **The mistake:** a notification or publish task runs even when an upstream task failed,
# MAGIC because `run_if` was not set. Downstream teams get a "success" notification
# MAGIC for a pipeline that actually failed.
# MAGIC
# MAGIC ```yaml
# MAGIC # BAD: notify_success runs even if transform failed
# MAGIC tasks:
# MAGIC   - task_key: transform
# MAGIC     ...
# MAGIC   - task_key: notify_success
# MAGIC     depends_on: [transform]
# MAGIC     # no run_if — runs regardless of transform outcome
# MAGIC     notebook_task: ...
# MAGIC
# MAGIC # GOOD: notify_success only runs if transform succeeded
# MAGIC tasks:
# MAGIC   - task_key: transform
# MAGIC     ...
# MAGIC   - task_key: notify_success
# MAGIC     depends_on: [transform]
# MAGIC     run_if: ALL_SUCCESS        ← gate on upstream success
# MAGIC     notebook_task: ...
# MAGIC
# MAGIC   - task_key: cleanup_on_failure
# MAGIC     depends_on: [transform]
# MAGIC     run_if: AT_LEAST_ONE_FAILED  ← runs only when something failed
# MAGIC     notebook_task: ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Quick Reference
# MAGIC
# MAGIC | Anti-pattern | Why it breaks | Fix |
# MAGIC |---|---|---|
# MAGIC | Naked append | Row count grows on re-run | Partition overwrite or MERGE INTO |
# MAGIC | Static partition overwrite | Drops all other partitions | `partitionOverwriteMode = dynamic` |
# MAGIC | Partition by ingestion time | New partition every run | Partition by business date |
# MAGIC | DROP TABLE at top level | Drops data on retry | Guard with `run_if: ALL_SUCCESS` |
# MAGIC | UUID surrogate key | Different key each run | Hash of business attributes |
# MAGIC | Full table overwrite for one partition | Slow, concurrent-unsafe | Dynamic partition overwrite |
# MAGIC | `CREATE SCHEMA` without `IF NOT EXISTS` | Fails on re-run | Always use `IF NOT EXISTS` |
# MAGIC | Missing `run_if` on publish task | Notifies on failure | `run_if: ALL_SUCCESS` |
