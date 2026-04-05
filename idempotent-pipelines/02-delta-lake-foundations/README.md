# Delta Lake Foundations for Idempotency

Delta Lake is the reason idempotent batch pipelines are tractable on Databricks. Three features do most of the work:

## 1. ACID transactions
Every Delta write is atomic. There are no partial writes. If your job crashes mid-write, the transaction is rolled back — you'll never get a half-written table. This eliminates Failure Mode 2 (partial write corruption) for free.

## 2. Dynamic partition overwrite
`spark.sql.sources.partitionOverwriteMode = dynamic` replaces only the partitions present in your DataFrame. All other partitions are untouched. This makes date-partitioned batch jobs re-runnable at zero extra cost.

## 3. MERGE INTO
Row-level upsert: match on a key, update if found, insert if new. Running the same MERGE twice with the same data is a no-op — the result is identical. Paired with Deletion Vectors and Row-Level Concurrency, this scales to concurrent workloads.

## Notebooks in this section

| Notebook | What it teaches |
|---|---|
| `01_partition_overwrite.py` | Dynamic vs static overwrite; when to use each |
| `02_merge_into_upserts.py` | MERGE patterns: SCD Type 1, soft deletes, partition pruning |
| `03_delta_table_properties.py` | Deletion Vectors, Row-Level Concurrency, Liquid Clustering |
