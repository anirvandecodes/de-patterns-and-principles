# Databricks notebook source
# MAGIC %md
# MAGIC # Idempotency & Why It Matters
# MAGIC
# MAGIC ## What is idempotency?
# MAGIC
# MAGIC A pipeline is **idempotent** if running it multiple times produces the **same result** as running it once.
# MAGIC
# MAGIC In plain English:
# MAGIC > Re-running your pipeline on data it already processed should not change the output.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why does this matter?
# MAGIC
# MAGIC Most engineers think their pipeline will run once and be fine.
# MAGIC Production is different. **Pipelines get re-run all the time.**
# MAGIC
# MAGIC If your pipeline is idempotent → re-running is completely safe. Same result every time.
# MAGIC
# MAGIC If your pipeline is NOT idempotent → every re-run silently corrupts your data.
# MAGIC No error. No alert. Just wrong numbers.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why would a pipeline re-run?
# MAGIC
# MAGIC | # | Reason | What happens |
# MAGIC |---|---|---|
# MAGIC | 1 | **Cluster failure** | Databricks evicts the cluster mid-run and automatically retries the task |
# MAGIC | 2 | **Transient error** | A network timeout at 3 AM. Job fails on task 3 of 5. You re-trigger — tasks 1 and 2 run again |
# MAGIC | 3 | **Bad data in source** | Source sent wrong values for Jan 15. They fix it. You reprocess that date |
# MAGIC | 4 | **Bug in transform logic** | A calculation was wrong for 3 months. You fix it and backfill 90 days |
# MAGIC | 5 | **Accidental double trigger** | Two people. One job. Both hit "Run Now" |

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## The question is never IF your pipeline will re-run.
# MAGIC ## The question is: **what happens when it does?**
