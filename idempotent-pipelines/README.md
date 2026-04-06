# Idempotent Pipelines & Backfilling

Run the notebooks in order. Each one builds on the previous.

| # | Notebook | What it covers |
|---|---|---|
| 01 | `01_why_rerun.py` | Why pipelines get re-run — sets the context |
| 02 | `02_the_problem.py` | **Run this multiple times** — watch duplicates accumulate |
| 03 | `03_fix_partition_overwrite.py` | Fix 1 — dynamic partition overwrite |
| 04 | `04_fix_merge_into.py` | Fix 2 — MERGE INTO |
| 05 | `05_backfill.py` | 3 backfill strategies |
| 06 | `06_anti_patterns.py` | 5 anti-patterns with fixes |

> **Tip for the demo:** run notebook 02 three times before opening it.
> The output will show run 1 → 3 rows, run 2 → 6 rows, run 3 → 9 rows.
