# What is Idempotency?

## The one-sentence definition

**A pipeline is idempotent if running it multiple times produces the same result as running it once.**

Mathematically: `f(f(x)) = f(x)`

In plain English: re-running your pipeline on data it already processed should not change the output.

---

## Why pipelines fail and get re-run

Production pipelines fail. Always. The question is not *if* they'll be re-run, but *whether it's safe to do so*.

Common re-run triggers:
- Transient network error → job retried automatically
- Bad data in source → you fix it and re-process the same date
- Bug in transform logic → you backfill a date range
- Cluster eviction mid-run → Databricks retries the task
- Someone manually kicks off a job that already ran

---

## The three failure modes

```
┌─────────────────────────────────────────────────────────────────┐
│  FAILURE MODE 1: Duplicate rows                                  │
│                                                                  │
│  Run 1: append 1000 rows → table has 1000 rows                  │
│  Run 2: append 1000 rows → table has 2000 rows  ← WRONG         │
│                                                                  │
│  Fix: MERGE INTO or partition overwrite                          │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  FAILURE MODE 2: Partial write corruption                        │
│                                                                  │
│  Run 1: writes 500 of 1000 rows → crashes                       │
│  Run 2: appends 1000 rows → table has 1500 rows  ← WRONG        │
│                                                                  │
│  Fix: Delta's ACID transactions — writes are all-or-nothing      │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  FAILURE MODE 3: Side effects on re-run                          │
│                                                                  │
│  Run 1: sends 1000 emails, writes audit log                      │
│  Run 2: sends 1000 emails again  ← WRONG                        │
│                                                                  │
│  Fix: guard side effects with idempotency check tables           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Choosing the right tool

```
Is the target table partitioned by date?
  YES → dynamic partition overwrite (replace only the target partition)
  NO  → MERGE INTO (upsert by primary key)

Does the source have a primary key?
  YES → MERGE INTO on that key
  NO  → deduplication + partition overwrite

Is the pipeline batch or streaming?
  BATCH  → covered in this section (01–05)
  STREAM → checkpoints + foreachBatch + MERGE (separate topic)
```

---

## What's in the notebook

`01_idempotency_fundamentals.py` demonstrates:
1. The duplicate problem — a naive append run twice
2. Fix 1: partition overwrite (replace-not-append)
3. Fix 2: MERGE INTO (upsert by key)
4. A lightweight "job audit" table pattern for tracking what's already been processed
