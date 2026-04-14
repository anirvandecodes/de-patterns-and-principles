# DE Patterns & Principles

A personal field guide for building production-grade data pipelines. Each module covers one core concept — with notebooks you can run, break, and learn from.

---

## Modules


| #   | Module                                        | What it covers                                                                                |
| --- | --------------------------------------------- | --------------------------------------------------------------------------------------------- |
| 1   | [Idempotent Pipelines](idempotent-pipelines/) | Why pipelines re-run, how duplicates happen, and 4 patterns to fix them + backfill strategies |


---

## How to use this repo

Each module lives in its own folder with:

- A `README.md` describing the notebooks and the order to run them
- Databricks notebooks (`notebooks/`) you can import and run directly

Run the notebooks in order. They are designed to build on each other — some intentionally produce broken output so you can see the problem before applying the fix.

---

## Modules in detail

### 1. Idempotent Pipelines

> **Path:** `idempotent-pipelines/`

A pipeline is **idempotent** if running it multiple times produces the same result as running it once. Most pipelines are not — and that causes silent data corruption.


| Notebook                        | What it covers                                                        |
| ------------------------------- | --------------------------------------------------------------------- |
| `01_why_rerun.py`               | What idempotency is and why pipelines always get re-run in production |
| `02_the_problem.py`             | Run this 3 times and watch duplicates accumulate                      |
| `03_fix_partition_overwrite.py` | Fix 1 — dynamic partition overwrite                                   |
| `04_fix_insert_overwrite.py`    | Fix 2 — INSERT OVERWRITE                                              |
| `05_fix_merge_into.py`          | Fix 3 — MERGE INTO                                                    |
| `06_fix_delete_insert.py`       | Fix 4 — DELETE + INSERT                                               |
| `07_backfill.py`                | 3 backfill strategies for reprocessing historical data                |


