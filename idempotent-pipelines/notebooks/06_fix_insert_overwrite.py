# Databricks notebook source
# MAGIC %md
# MAGIC # Fix 4: INSERT OVERWRITE
# MAGIC
# MAGIC A SQL-first alternative to the Python partition overwrite pattern.
# MAGIC `INSERT OVERWRITE` replaces the target partition in a single atomic SQL statement.
# MAGIC
# MAGIC Same guarantee as dynamic partition overwrite — only the matching partition is replaced.
# MAGIC All other partitions are untouched.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema",  "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the target table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders_insert_overwrite (
        order_id    STRING,
        customer_id STRING,
        amount      DOUBLE,
        order_date  STRING
    )
    USING DELTA
    PARTITIONED BY (order_date)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed with multiple dates so we can prove only the target partition changes

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {catalog}.{schema}.orders_insert_overwrite VALUES
    ('O901', 'C9', 90.0,  '2024-01-14'),
    ('O902', 'C9', 95.0,  '2024-01-14')
""")

before = spark.table(f"{catalog}.{schema}.orders_insert_overwrite").count()
print(f"Seeded Jan 14: {before} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The idempotent pipeline — INSERT OVERWRITE

# COMMAND ----------

def load(load_date: str):
    # Replaces only the partition for load_date — Jan 14 is untouched
    spark.sql(f"""
        INSERT OVERWRITE {catalog}.{schema}.orders_insert_overwrite
        PARTITION (order_date = '{load_date}')
        SELECT 'O001', 'C1', 100.0
        UNION ALL
        SELECT 'O002', 'C2', 200.0
        UNION ALL
        SELECT 'O003', 'C3', 150.0
    """)

# COMMAND ----------

# Run it multiple times — Jan 15 count stays 3, Jan 14 stays untouched
load("2024-01-15")
load("2024-01-15")
load("2024-01-15")

jan14 = spark.table(f"{catalog}.{schema}.orders_insert_overwrite").filter("order_date = '2024-01-14'").count()
jan15 = spark.table(f"{catalog}.{schema}.orders_insert_overwrite").filter("order_date = '2024-01-15'").count()

print(f"Jan 14 : {jan14} rows  ← untouched")
print(f"Jan 15 : {jan15} rows  ← always 3, no matter how many times you run")
print()
print("✓ Idempotent. Only the target partition is replaced.")

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.orders_insert_overwrite").orderBy("order_date", "order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## INSERT OVERWRITE vs Python partition overwrite
# MAGIC
# MAGIC Both achieve the same result — choose based on your preference:
# MAGIC
# MAGIC | | INSERT OVERWRITE (SQL) | Partition Overwrite (Python) |
# MAGIC |---|---|---|
# MAGIC | **Style** | SQL-first | PySpark |
# MAGIC | **Source** | Inline SQL or subquery | DataFrame |
# MAGIC | **Config needed** | None | `partitionOverwriteMode = dynamic` |
# MAGIC | **Best for** | SQL notebooks, simple transformations | Complex Python pipelines |
