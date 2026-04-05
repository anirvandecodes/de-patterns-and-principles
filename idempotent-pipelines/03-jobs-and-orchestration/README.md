# Jobs and Orchestration for Idempotent Pipelines

Idempotency at the write layer (MERGE, partition overwrite) is necessary but not sufficient.
Your orchestration layer also needs to be designed for safe re-runs.

## The core principle

**Every task in a job must be safe to retry independently.**

If Task 3 fails and Databricks retries it, it should produce the same result as if it had succeeded on the first try. This means each task needs idempotent write logic — but it also means you need to think about:
- What happens if a task runs twice with the same parameters?
- How do you scope a task to exactly one logical unit of work (e.g., one date)?
- How do you trigger a controlled backfill over a historical date range?

## Notebooks in this section

| Notebook | What it teaches |
|---|---|
| `01_retry_semantics.py` | Job retry config, idempotency keys via task values |
| `02_date_parameterized_jobs.py` | `load_date` parameter pattern — re-runnable by design |
| `03_backfill_strategies.py` | Three backfill strategies with code |
