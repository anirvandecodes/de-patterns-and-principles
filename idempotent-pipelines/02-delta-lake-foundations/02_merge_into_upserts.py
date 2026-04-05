# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # MERGE INTO for Idempotent Upserts
# MAGIC
# MAGIC `MERGE INTO` is the Swiss Army knife of idempotent writes. It matches source rows
# MAGIC against a target table on a key, then updates matched rows and inserts new ones.
# MAGIC Running the same MERGE twice with identical data is a no-op — the result is identical.
# MAGIC
# MAGIC This notebook covers:
# MAGIC 1. Basic upsert (SCD Type 1 — current state only)
# MAGIC 2. Soft delete pattern
# MAGIC 3. Partition pruning for performance
# MAGIC 4. The Python Delta API vs SQL syntax

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit, col
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Basic Upsert — SCD Type 1 (current state only)
# MAGIC
# MAGIC **When to use:** you only care about the current value. A customer changes their email?
# MAGIC Update the row. Don't keep the old one.

# COMMAND ----------

# Create target table with Deletion Vectors + Row-Level Concurrency enabled
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customers (
        customer_id  STRING,
        name         STRING,
        email        STRING,
        tier         STRING,
        updated_at   TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (customer_id)
    TBLPROPERTIES (
        'delta.enableDeletionVectors'     = 'true',
        'delta.enableRowLevelConcurrency' = 'true'
    )
""")

# COMMAND ----------

def get_customer_updates(batch: int):
    """Simulate customer records arriving from a CDC source."""
    if batch == 1:
        return spark.createDataFrame([
            Row(customer_id="C001", name="Alice",   email="alice@example.com",  tier="gold"),
            Row(customer_id="C002", name="Bob",     email="bob@example.com",    tier="silver"),
            Row(customer_id="C003", name="Charlie", email="charlie@example.com",tier="bronze"),
        ])
    elif batch == 2:
        # C001 changed email, C004 is new
        return spark.createDataFrame([
            Row(customer_id="C001", name="Alice",  email="alice@new.com",       tier="gold"),
            Row(customer_id="C004", name="Diana",  email="diana@example.com",   tier="platinum"),
        ])

# COMMAND ----------

def upsert_customers(source_df):
    source_with_ts = source_df.withColumn("updated_at", current_timestamp())

    target = DeltaTable.forName(spark, f"{catalog}.{schema}.customers")

    (target.alias("t")
           .merge(source_with_ts.alias("s"), "t.customer_id = s.customer_id")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())

# COMMAND ----------

# Initial load
upsert_customers(get_customer_updates(1))
print(f"After batch 1: {spark.table(f'{catalog}.{schema}.customers').count()} customers")

# COMMAND ----------

# Re-run batch 1 (retry scenario) — result must be identical
upsert_customers(get_customer_updates(1))
count_after_retry = spark.table(f"{catalog}.{schema}.customers").count()
print(f"After batch 1 re-run: {count_after_retry} customers")  # Still 3 — idempotent
assert count_after_retry == 3

# COMMAND ----------

# Batch 2: email update + new customer
upsert_customers(get_customer_updates(2))
display(spark.table(f"{catalog}.{schema}.customers").orderBy("customer_id"))
# C001 email is updated; C004 is inserted; C002, C003 unchanged

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SQL MERGE syntax
# MAGIC
# MAGIC The Python Delta API and SQL MERGE are equivalent. SQL is often more readable for complex logic.

# COMMAND ----------

# Equivalent SQL MERGE
source_df = get_customer_updates(2).withColumn("updated_at", current_timestamp())
source_df.createOrReplaceTempView("customer_updates")

spark.sql(f"""
    MERGE INTO {catalog}.{schema}.customers AS t
    USING customer_updates AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            t.name       = s.name,
            t.email      = s.email,
            t.tier       = s.tier,
            t.updated_at = s.updated_at
    WHEN NOT MATCHED THEN
        INSERT (customer_id, name, email, tier, updated_at)
        VALUES (s.customer_id, s.name, s.email, s.tier, s.updated_at)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Soft Delete Pattern
# MAGIC
# MAGIC Instead of physically deleting rows (which complicates backfills and auditing),
# MAGIC mark them as deleted with a flag. The MERGE handles the delete signal from the source.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customers_with_deletes (
        customer_id  STRING,
        name         STRING,
        email        STRING,
        is_deleted   BOOLEAN,
        updated_at   TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (customer_id)
    TBLPROPERTIES (
        'delta.enableDeletionVectors'     = 'true',
        'delta.enableRowLevelConcurrency' = 'true'
    )
""")

# COMMAND ----------

def upsert_with_soft_delete(source_df):
    """Source has an 'op' column: 'UPSERT' or 'DELETE'."""
    source_with_ts = source_df.withColumn("updated_at", current_timestamp())
    source_with_ts.createOrReplaceTempView("cdc_source")

    spark.sql(f"""
        MERGE INTO {catalog}.{schema}.customers_with_deletes AS t
        USING cdc_source AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.op = 'DELETE' THEN
            UPDATE SET t.is_deleted = true, t.updated_at = s.updated_at
        WHEN MATCHED AND s.op = 'UPSERT' THEN
            UPDATE SET
                t.name       = s.name,
                t.email      = s.email,
                t.is_deleted = false,
                t.updated_at = s.updated_at
        WHEN NOT MATCHED AND s.op = 'UPSERT' THEN
            INSERT (customer_id, name, email, is_deleted, updated_at)
            VALUES (s.customer_id, s.name, s.email, false, s.updated_at)
    """)

# COMMAND ----------

cdc_data = spark.createDataFrame([
    Row(customer_id="C001", name="Alice", email="alice@example.com", op="UPSERT"),
    Row(customer_id="C002", name="Bob",   email="bob@example.com",   op="UPSERT"),
    Row(customer_id="C003", name="Charlie",email="charlie@example.com", op="UPSERT"),
])
upsert_with_soft_delete(cdc_data)

# Now delete C002
delete_signal = spark.createDataFrame([
    Row(customer_id="C002", name=None, email=None, op="DELETE"),
])
upsert_with_soft_delete(delete_signal)

# Re-run the delete (idempotent — result is the same)
upsert_with_soft_delete(delete_signal)

display(spark.table(f"{catalog}.{schema}.customers_with_deletes").orderBy("customer_id"))
# C002 is marked is_deleted=true, not physically removed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Partition Pruning — making MERGE fast on large tables
# MAGIC
# MAGIC When your target table is partitioned, include the partition column in the MERGE condition.
# MAGIC This tells Spark to scan only the relevant partition(s), not the entire table.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.events_partitioned (
        event_id   STRING,
        user_id    STRING,
        event_type STRING,
        event_date STRING,
        processed_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (event_date)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")

# COMMAND ----------

def upsert_events(source_df, load_date: str):
    """MERGE with partition column in the condition — skips irrelevant partitions."""
    source_df.createOrReplaceTempView("event_updates")

    spark.sql(f"""
        MERGE INTO {catalog}.{schema}.events_partitioned AS t
        USING event_updates AS s
        ON t.event_id = s.event_id
        AND t.event_date = '{load_date}'          -- partition pruning: scan only this date
        WHEN MATCHED THEN
            UPDATE SET t.event_type = s.event_type, t.processed_at = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT *
    """)

# COMMAND ----------

events = spark.createDataFrame([
    Row(event_id="E001", user_id="U1", event_type="click",    event_date="2024-01-15", processed_at=None),
    Row(event_id="E002", user_id="U2", event_type="purchase", event_date="2024-01-15", processed_at=None),
])

upsert_events(events, "2024-01-15")
upsert_events(events, "2024-01-15")  # Re-run — still idempotent

display(spark.table(f"{catalog}.{schema}.events_partitioned"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Pattern | MERGE clause | Use case |
# MAGIC |---|---|---|
# MAGIC | Basic upsert | `WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT` | Current-state sync |
# MAGIC | Soft delete | Add `WHEN MATCHED AND op='DELETE' UPDATE SET is_deleted=true` | CDC with deletes |
# MAGIC | Partition pruning | Add partition col to `ON` condition | Large partitioned tables |
# MAGIC
# MAGIC **Always enable on target tables:**
# MAGIC - `delta.enableDeletionVectors = true` — soft deletes without file rewrite
# MAGIC - `delta.enableRowLevelConcurrency = true` — concurrent MERGEs to different rows
# MAGIC - `CLUSTER BY (merge_key)` — automatic data layout optimization (no manual OPTIMIZE needed)
