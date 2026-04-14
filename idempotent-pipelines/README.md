# Idempotent Pipelines & Backfilling

Run the notebooks in order. Each one builds on the previous.

| # | Notebook | What it covers |
|---|---|---|
| 01 | `01_why_rerun.py` | What is idempotency + why pipelines get re-run |
| 02 | `02_the_problem.py` | **Run this multiple times** — watch duplicates accumulate |
| 03 | `03_fix_partition_overwrite.py` | Fix 1 — dynamic partition overwrite |
| 04 | `04_fix_insert_overwrite.py` | Fix 2 — INSERT OVERWRITE |
| 05 | `05_fix_merge_into.py` | Fix 3 — MERGE INTO |
| 06 | `06_backfill.py` | 3 backfill strategies |

> **Tip for the demo:** run notebook 02 three times before opening it.
> The output will show run 1 → 3 rows, run 2 → 6 rows, run 3 → 9 rows.
