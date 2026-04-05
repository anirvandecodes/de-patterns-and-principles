# Idempotent Pipelines & Backfilling

A field guide to building batch pipelines that are safe to re-run, retry, and backfill — using PySpark and Delta Lake on Databricks.

## Why this matters

A non-idempotent pipeline doubles your row count every time it re-runs. That 3 AM retry after a transient failure? Duplicates. That backfill job you kicked off for a date range? Duplicates. Getting idempotency right means you can re-run anything, anytime, without fear.

## Reading order

```
01-what-is-idempotency/       ← Start here. Concept + the duplicate problem demo.
02-delta-lake-foundations/    ← How Delta makes idempotent writes possible.
03-jobs-and-orchestration/    ← Retry semantics, date params, backfill strategies.
04-audit-and-observability/   ← Proving idempotency with audit columns + Delta history.
05-anti-patterns/             ← What breaks idempotency — and the fixes.
```

## Databricks feature map

| Feature | Section |
|---------|---------|
| `partitionOverwriteMode = dynamic` | 02 |
| `MERGE INTO` upserts | 02 |
| Deletion Vectors + Row-Level Concurrency | 02 |
| Liquid Clustering (`CLUSTER BY`) | 02 |
| Job retry semantics + idempotency keys | 03 |
| `load_date` parameterized jobs | 03 |
| Backfill strategies (full / incremental / partition-based) | 03 |
| `_ingested_at` audit columns | 04 |
| `DESCRIBE HISTORY` + time travel | 04 |
