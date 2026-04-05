# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Table Properties for Idempotent Pipelines
# MAGIC
# MAGIC Three Delta features dramatically improve the performance and correctness of idempotent writes:
# MAGIC
# MAGIC | Feature | What it does | Why it matters for idempotency |
# MAGIC |---|---|---|
# MAGIC | **Deletion Vectors** | Marks deleted rows without rewriting files | Faster MERGE, lower latency on re-runs |
# MAGIC | **Row-Level Concurrency** | Allows concurrent writes to different rows | Safe parallel MERGE from multiple jobs |
# MAGIC | **Liquid Clustering** | Auto-organizes data layout on write | No manual OPTIMIZE/ZORDER needed |

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
# MAGIC ## 1. Deletion Vectors
# MAGIC
# MAGIC Without Deletion Vectors, a DELETE or MERGE that removes rows rewrites the entire Parquet file.
# MAGIC With Deletion Vectors, the deleted rows are marked in a small sidecar file — no file rewrite.
# MAGIC
# MAGIC **Result:** MERGE and DELETE operations are significantly faster, especially on large tables.

# COMMAND ----------

# Create table WITH Deletion Vectors
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.orders_dv (
        order_id   STRING,
        customer_id STRING,
        status     STRING,
        amount     DOUBLE
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")

# COMMAND ----------

from pyspark.sql import Row

# Insert initial data
spark.createDataFrame([
    Row(order_id="O001", customer_id="C1", status="pending",   amount=100.0),
    Row(order_id="O002", customer_id="C2", status="pending",   amount=200.0),
    Row(order_id="O003", customer_id="C3", status="shipped",   amount=150.0),
    Row(order_id="O004", customer_id="C4", status="cancelled", amount=50.0),
]).write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.orders_dv")

# COMMAND ----------

# DELETE using deletion vector (no file rewrite)
spark.sql(f"""
    DELETE FROM {catalog}.{schema}.orders_dv
    WHERE status = 'cancelled'
""")

display(spark.table(f"{catalog}.{schema}.orders_dv"))

# COMMAND ----------

# Inspect Delta history — see the DELETE operation
spark.sql(f"DESCRIBE HISTORY {catalog}.{schema}.orders_dv").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Row-Level Concurrency
# MAGIC
# MAGIC Standard Delta uses optimistic concurrency at the **file level** — two concurrent writes
# MAGIC that touch the same file conflict. Row-Level Concurrency (RLC) uses **row-level** conflict
# MAGIC detection — two concurrent MERGEs only conflict if they touch the **same rows**.
# MAGIC
# MAGIC This is critical for:
# MAGIC - Parallel backfill jobs running MERGEs on different date ranges
# MAGIC - Multiple pipeline tasks writing to the same table concurrently

# COMMAND ----------

# Enable both DV and RLC
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.orders_rlc (
        order_id    STRING,
        customer_id STRING,
        region      STRING,
        status      STRING,
        amount      DOUBLE
    )
    USING DELTA
    CLUSTER BY (order_id)
    TBLPROPERTIES (
        'delta.enableDeletionVectors'     = 'true',
        'delta.enableRowLevelConcurrency' = 'true'
    )
""")

# COMMAND ----------

# Verify properties are set
spark.sql(f"SHOW TBLPROPERTIES {catalog}.{schema}.orders_rlc").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Liquid Clustering (replaces PARTITION BY + ZORDER)
# MAGIC
# MAGIC Traditional optimization required:
# MAGIC 1. `PARTITION BY (date)` at table creation
# MAGIC 2. Running `OPTIMIZE ... ZORDER BY (key)` after each write
# MAGIC
# MAGIC Liquid Clustering (`CLUSTER BY`) does this automatically:
# MAGIC - Data is incrementally clustered as you write
# MAGIC - No manual OPTIMIZE needed (though you can still run it)
# MAGIC - Clustering columns can be changed without a full table rewrite
# MAGIC - Works with any number of clustering columns

# COMMAND ----------

# Old way — avoid
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.orders_legacy (
        order_id   STRING,
        order_date STRING,
        region     STRING,
        amount     DOUBLE
    )
    USING DELTA
    PARTITIONED BY (order_date)
    -- requires: OPTIMIZE orders_legacy ZORDER BY (region) after each write
""")

# COMMAND ----------

# New way — Liquid Clustering
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.orders_clustered (
        order_id   STRING,
        order_date STRING,
        region     STRING,
        amount     DOUBLE
    )
    USING DELTA
    CLUSTER BY (order_date, region)
    TBLPROPERTIES (
        'delta.enableDeletionVectors'     = 'true',
        'delta.enableRowLevelConcurrency' = 'true'
    )
    -- No manual OPTIMIZE needed. Data is auto-clustered on write.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Changing clustering columns without a full rewrite

# COMMAND ----------

# Add a new clustering column — no data migration needed
spark.sql(f"""
    ALTER TABLE {catalog}.{schema}.orders_clustered
    CLUSTER BY (order_date, region, order_id)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ALTER TABLE: enabling features on existing tables

# COMMAND ----------

# If you have an existing table and want to enable these features:
spark.sql(f"""
    ALTER TABLE {catalog}.{schema}.orders_legacy
    SET TBLPROPERTIES (
        'delta.enableDeletionVectors'     = 'true',
        'delta.enableRowLevelConcurrency' = 'true'
    )
""")

spark.sql(f"""
    ALTER TABLE {catalog}.{schema}.orders_legacy
    CLUSTER BY (order_date)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The recommended table template for idempotent pipelines

# COMMAND ----------

# This is the table definition you should use as a starting point
# for any table that will receive MERGE writes:

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.recommended_template (
        -- your columns here
        id         STRING    NOT NULL,
        name       STRING,
        value      DOUBLE,
        event_date STRING,
        updated_at TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (id)                          -- Liquid Clustering on merge key
    TBLPROPERTIES (
        'delta.enableDeletionVectors'     = 'true',   -- faster deletes + MERGE
        'delta.enableRowLevelConcurrency' = 'true'    -- safe concurrent MERGEs
    )
    COMMENT 'Template: idempotent MERGE target with Liquid Clustering + DV + RLC'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Always enable these three things on tables that receive MERGE or DELETE operations:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE my_table (...)
# MAGIC USING DELTA
# MAGIC CLUSTER BY (merge_key)                            -- replaces PARTITION BY + ZORDER
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableDeletionVectors'     = 'true',   -- faster deletes
# MAGIC     'delta.enableRowLevelConcurrency' = 'true'    -- safe concurrency
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Or for existing tables:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE my_table
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.enableDeletionVectors'     = 'true',
# MAGIC     'delta.enableRowLevelConcurrency' = 'true'
# MAGIC );
# MAGIC ALTER TABLE my_table CLUSTER BY (merge_key);
# MAGIC ```
