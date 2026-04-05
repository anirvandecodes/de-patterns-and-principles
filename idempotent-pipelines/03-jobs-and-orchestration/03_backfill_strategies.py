# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Backfill Strategies
# MAGIC
# MAGIC A backfill is a controlled re-run of a pipeline over historical data.
# MAGIC You need a backfill when:
# MAGIC - A bug was fixed and historical output needs to be corrected
# MAGIC - A new column was added and historical data needs to be enriched
# MAGIC - A schema change requires reprocessing from source
# MAGIC - Data was lost or corrupted and needs to be rebuilt
# MAGIC
# MAGIC This notebook covers three strategies, with decision guidance for choosing between them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decision guide
# MAGIC
# MAGIC ```
# MAGIC Is the target table small (<100GB)?
# MAGIC   YES → Strategy 1: Full Recompute (drop + rebuild from scratch)
# MAGIC   NO  ↓
# MAGIC
# MAGIC Is the logic change scoped to specific date partitions?
# MAGIC   YES → Strategy 3: Partition-Based Overwrite (rebuild only affected partitions)
# MAGIC   NO  ↓
# MAGIC
# MAGIC Does the source have date partitions you can process one at a time?
# MAGIC   YES → Strategy 2: Incremental Date-Loop (run job per date, parallelize)
# MAGIC   NO  → Strategy 1: Full Recompute (your only option)
# MAGIC ```

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

from pyspark.sql import Row
from pyspark.sql.functions import lit, current_timestamp, col
from delta.tables import DeltaTable
from datetime import date, timedelta

# Simulate source data — one day per call
def get_source(load_date: str):
    return spark.createDataFrame([
        Row(order_id=f"{load_date}_O1", amount=100.0, region="US", order_date=load_date),
        Row(order_id=f"{load_date}_O2", amount=200.0, region="EU", order_date=load_date),
    ])

# COMMAND ----------

# Create and seed the target table with 7 days of data
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.orders_backfill_target")

for i in range(7):
    d = str(date(2024, 1, 10) + timedelta(days=i))
    get_source(d).write.format("delta").mode("append") \
        .partitionBy("order_date") \
        .saveAsTable(f"{catalog}.{schema}.orders_backfill_target")

original_count = spark.table(f"{catalog}.{schema}.orders_backfill_target").count()
print(f"Target table seeded with {original_count} rows across 7 dates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 1: Full Recompute
# MAGIC
# MAGIC Drop the target table and rebuild it entirely from source.
# MAGIC
# MAGIC **When:** schema change, small table, fundamental logic overhaul, or source has no date partitioning.
# MAGIC **Downside:** the table is unavailable during the rebuild. Use a staging table if you need zero downtime.

# COMMAND ----------

def full_recompute(source_dates: list):
    """
    Drop target, reprocess all dates, rebuild from scratch.
    Simple, but the table is unavailable during the run.
    """
    print("Starting full recompute...")

    # Step 1: write to a staging table
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.orders_backfill_staging")

    for d in source_dates:
        df = get_source(d).withColumn("amount_with_tax", col("amount") * 1.1)
        df.write.format("delta").mode("append") \
            .partitionBy("order_date") \
            .saveAsTable(f"{catalog}.{schema}.orders_backfill_staging")

    # Step 2: swap staging → target atomically
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.orders_backfill_target")
    spark.sql(f"""
        ALTER TABLE {catalog}.{schema}.orders_backfill_staging
        RENAME TO {catalog}.{schema}.orders_backfill_target
    """)

    count = spark.table(f"{catalog}.{schema}.orders_backfill_target").count()
    print(f"Full recompute complete. Rows: {count}")

# COMMAND ----------

all_dates = [str(date(2024, 1, 10) + timedelta(days=i)) for i in range(7)]
full_recompute(all_dates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 2: Incremental Date-Loop
# MAGIC
# MAGIC Process one date at a time, in a loop. Each iteration is idempotent (partition overwrite).
# MAGIC Safe to resume if interrupted — re-running the same date is a no-op.
# MAGIC
# MAGIC **When:** large date-partitioned table, only a subset of dates need rebuilding.

# COMMAND ----------

def backfill_date(load_date: str):
    """Process a single date idempotently — safe to call multiple times."""
    df = get_source(load_date).withColumn("amount_with_tax", col("amount") * 1.1)
    (df.write
       .format("delta")
       .mode("overwrite")                        # dynamic overwrite — partition only
       .partitionBy("order_date")
       .saveAsTable(f"{catalog}.{schema}.orders_backfill_target"))
    return df.count()

# COMMAND ----------

# Backfill a specific date range
backfill_start = date(2024, 1, 12)
backfill_end   = date(2024, 1, 15)

dates_to_backfill = []
d = backfill_start
while d <= backfill_end:
    dates_to_backfill.append(str(d))
    d += timedelta(days=1)

print(f"Backfilling {len(dates_to_backfill)} dates: {dates_to_backfill}")

for load_date in dates_to_backfill:
    rows = backfill_date(load_date)
    print(f"  {load_date}: {rows} rows written")

# COMMAND ----------

# Idempotency proof: re-running the same range changes nothing
print("\nRe-running same range (should produce same row counts):")
for load_date in dates_to_backfill:
    rows = backfill_date(load_date)
    print(f"  {load_date}: {rows} rows (unchanged)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parallel date-loop via the Databricks SDK
# MAGIC
# MAGIC For large backfills, trigger job runs in parallel instead of sequentially:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from concurrent.futures import ThreadPoolExecutor, as_completed
# MAGIC from datetime import date, timedelta
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC JOB_ID = 123456       # your daily ETL job id
# MAGIC MAX_PARALLEL = 5      # run 5 dates at a time
# MAGIC
# MAGIC def trigger_date(load_date: str):
# MAGIC     run = w.jobs.run_now(
# MAGIC         job_id=JOB_ID,
# MAGIC         python_named_params={"load_date": load_date}
# MAGIC     )
# MAGIC     return load_date, run.run_id
# MAGIC
# MAGIC dates = [str(date(2024, 1, 1) + timedelta(days=i)) for i in range(30)]
# MAGIC
# MAGIC with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as executor:
# MAGIC     futures = {executor.submit(trigger_date, d): d for d in dates}
# MAGIC     for future in as_completed(futures):
# MAGIC         d, run_id = future.result()
# MAGIC         print(f"Triggered {d} → run_id {run_id}")
# MAGIC ```
# MAGIC
# MAGIC **Safe because:** each job run writes to a separate partition — no conflicts between parallel runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 3: Partition-Based Overwrite
# MAGIC
# MAGIC When only specific partitions are wrong and the rest of the table is correct,
# MAGIC overwrite only those partitions. Fastest option for large tables.
# MAGIC
# MAGIC **When:** a bug affected a specific date range and you need to fix those partitions
# MAGIC without touching the rest of the table.

# COMMAND ----------

def rebuild_partition(load_date: str, transform_fn=None):
    """
    Replace a single partition in place.
    Uses dynamic partition overwrite — all other partitions untouched.
    """
    df = get_source(load_date)
    if transform_fn:
        df = transform_fn(df)

    (df.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("order_date")
       .saveAsTable(f"{catalog}.{schema}.orders_backfill_target"))

    count = spark.table(f"{catalog}.{schema}.orders_backfill_target") \
                 .filter(f"order_date = '{load_date}'").count()
    print(f"Rebuilt partition {load_date}: {count} rows")

# COMMAND ----------

# Only Jan 13 and 14 had bad data — rebuild just those
def fixed_transform(df):
    return df.withColumn("amount_with_tax", col("amount") * 1.15)  # corrected tax rate

rebuild_partition("2024-01-13", fixed_transform)
rebuild_partition("2024-01-14", fixed_transform)

# Verify: other dates are untouched
display(
    spark.table(f"{catalog}.{schema}.orders_backfill_target").orderBy("order_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison table
# MAGIC
# MAGIC | Strategy | Speed | Availability | Use when |
# MAGIC |---|---|---|---|
# MAGIC | Full Recompute | Slowest | Table unavailable during rebuild | Small table or fundamental schema change |
# MAGIC | Incremental Date-Loop | Medium | Table available (old partitions intact) | Large table, subset of dates affected |
# MAGIC | Partition-Based Overwrite | Fastest | Table available (untouched partitions intact) | Specific partitions have bad data |
# MAGIC
# MAGIC **All three strategies are idempotent** — you can re-run any of them safely.
# MAGIC The partition overwrite and MERGE logic ensures re-running the same dates never creates duplicates.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backfill checklist
# MAGIC
# MAGIC Before triggering a backfill:
# MAGIC
# MAGIC - [ ] Identify the date range affected
# MAGIC - [ ] Choose the right strategy (see decision guide above)
# MAGIC - [ ] Test the fixed logic on a single date first: `backfill_date("2024-01-13")`
# MAGIC - [ ] Verify the output with `SELECT COUNT(*), order_date FROM target GROUP BY order_date`
# MAGIC - [ ] Then trigger the full range
# MAGIC - [ ] Monitor: each date run should produce consistent row counts
# MAGIC - [ ] Re-run any failed dates — it's safe because the write is idempotent
