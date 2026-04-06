# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Idempotency & Why It Matters
# MAGIC
# MAGIC ## What is idempotency?
# MAGIC
# MAGIC A pipeline is **idempotent** if running it multiple times produces the **same result** as running it once.
# MAGIC
# MAGIC Mathematically: `f(f(x)) = f(x)`
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
# MAGIC ## Why would a pipeline re-run?
# MAGIC
# MAGIC ```
# MAGIC 1. CLUSTER FAILURE
# MAGIC    Your job is running. The cluster gets evicted.
# MAGIC    Databricks automatically retries the task.
# MAGIC    Same data. Same pipeline. Runs again.
# MAGIC
# MAGIC 2. TRANSIENT ERROR
# MAGIC    A network timeout at 3 AM.
# MAGIC    The job fails on task 3 of 5.
# MAGIC    You fix nothing — just re-trigger.
# MAGIC    Tasks 1 and 2 already ran. Now they run again.
# MAGIC
# MAGIC 3. BAD DATA IN SOURCE
# MAGIC    A bug in the source system sent wrong values on Jan 15.
# MAGIC    Source fixes it. You reprocess Jan 15.
# MAGIC    Pipeline runs again for that date.
# MAGIC
# MAGIC 4. BUG IN YOUR TRANSFORM LOGIC
# MAGIC    You discover a calculation was wrong for the last 3 months.
# MAGIC    You fix the logic and backfill 90 days.
# MAGIC    Pipeline runs 90 times for historical dates.
# MAGIC
# MAGIC 5. ACCIDENTAL DOUBLE TRIGGER
# MAGIC    Two people. One job. Both hit "Run Now".
# MAGIC    It happens more than you think.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## The question is never IF your pipeline will re-run.
# MAGIC ## The question is: **what happens when it does?**
# MAGIC
# MAGIC → Open `02_the_problem.py` to see exactly what goes wrong.
