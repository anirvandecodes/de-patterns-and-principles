# Audit and Observability

Idempotency is only useful if you can verify it. These techniques let you inspect what ran,
when it ran, what it wrote, and how to roll back if something went wrong.

## What's in this section

`01_audit_columns_and_delta_history.py` covers:

- **Audit columns** — `_ingested_at`, `_source_file`, `_pipeline_run_id` — baked into every row
- **`DESCRIBE HISTORY`** — every write to a Delta table is logged with timestamp, operation, and metrics
- **Delta time travel** — query the table as it was at any prior version
- **A pipeline audit table** — lightweight tracking of every pipeline run
