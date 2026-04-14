# Databricks notebook source
# MAGIC %md
# MAGIC # Fix 1: Dynamic Partition Overwrite
# MAGIC
# MAGIC **Run this job as many times as you want. Row count stays 3. Always.**
# MAGIC
# MAGIC Instead of appending, we **replace** the partition for the date we're processing.
# MAGIC One config line makes it safe:
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "idempotency_demo")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# THE key config — always set this for batch pipelines
#spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists workspace.idempotency_demo.orders_fixed_partition;

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

source_df = spark.createDataFrame([
    Row(order_id="O001", customer_id="C1", amount=100.0, order_date="2024-01-14"),
    Row(order_id="O002", customer_id="C2", amount=200.0, order_date="2024-01-15"),
    Row(order_id="O003", customer_id="C3", amount=150.0, order_date="2024-01-16"),
    Row(order_id="O004", customer_id="C4", amount=120.0, order_date="2024-01-14"),
    Row(order_id="O005", customer_id="C5", amount=180.0, order_date="2024-01-15"),
    Row(order_id="O006", customer_id="C6", amount=170.0, order_date="2024-01-16"),
]).withColumn("_loaded_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## The fixed pipeline

# COMMAND ----------

# mode("overwrite") + partitionBy = replace only the target partition
# All other partitions are untouched
source_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .saveAsTable(f"{catalog}.{schema}.orders_fixed_partition")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.idempotency_demo.orders_fixed_partition order by order_date;

# COMMAND ----------

new_data_df = spark.createDataFrame([
    Row(order_id="O003", customer_id="C3", amount=150.0, order_date="2024-01-16"),
    Row(order_id="O006", customer_id="C6", amount=170.0, order_date="2024-01-16")
]).withColumn("_loaded_at", current_timestamp())

# COMMAND ----------

# mode("overwrite") + partitionBy = replace only the target partition
# All other partitions are untouched
new_data_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .saveAsTable(f"{catalog}.{schema}.orders_fixed_partition")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.idempotency_demo.orders_fixed_partition order by order_date;

# COMMAND ----------

# DBTITLE 1,Set the configuration
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

source_df = spark.createDataFrame([
    Row(order_id="O001", customer_id="C1", amount=100.0, order_date="2024-01-14"),
    Row(order_id="O002", customer_id="C2", amount=200.0, order_date="2024-01-15"),
    Row(order_id="O003", customer_id="C3", amount=150.0, order_date="2024-01-16"),
    Row(order_id="O004", customer_id="C4", amount=120.0, order_date="2024-01-14"),
    Row(order_id="O005", customer_id="C5", amount=180.0, order_date="2024-01-15"),
    Row(order_id="O006", customer_id="C6", amount=170.0, order_date="2024-01-16"),
]).withColumn("_loaded_at", current_timestamp())
source_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .saveAsTable(f"{catalog}.{schema}.orders_fixed_partition")
display(spark.table(f"{catalog}.{schema}.orders_fixed_partition").orderBy("order_id"))


# COMMAND ----------

new_data_df = spark.createDataFrame([
    Row(order_id="O003", customer_id="C3", amount=150.0, order_date="2024-01-16"),
    Row(order_id="O006", customer_id="C6", amount=170.0, order_date="2024-01-16")
]).withColumn("_loaded_at", current_timestamp())


new_data_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .saveAsTable(f"{catalog}.{schema}.orders_fixed_partition")
display(spark.table(f"{catalog}.{schema}.orders_fixed_partition").orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why does this work?
# MAGIC
# MAGIC ```
# MAGIC Table partitions:  [2024-01-14] [2024-01-15] [2024-01-16]
# MAGIC
# MAGIC You write data for 2024-01-16 again.
# MAGIC
# MAGIC STATIC (default):  wipes ALL partitions → only Jan 16 remains  ← DANGEROUS
# MAGIC DYNAMIC (correct): replaces ONLY Jan 16  → Jan 13, Jan 14 intact ← SAFE
# MAGIC ```
# MAGIC
# MAGIC **Rule:** Always set `partitionOverwriteMode = dynamic`. Never rely on the default.
