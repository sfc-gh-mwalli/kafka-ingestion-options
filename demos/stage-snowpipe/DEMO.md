# Stage + Snowpipe Demo (Batch File Ingestion)

Demonstrates loading batch files from cloud storage into Snowflake using
**Snowpipe with `AUTO_INGEST = TRUE`**. Files landing in an external stage
trigger a cloud storage event notification, which Snowpipe consumes to load
the files automatically — no warehouse polling, no manual `COPY INTO`.

```
sample .jsonl.gz files  →  external stage (S3 / ADLS / GCS)
                              │
                  cloud storage event notification
                              │
                  Snowpipe AUTO_INGEST (serverless)
                              │
                  KAFKA_DEMO.BATCH.CLICKSTREAM table
```

This demo focuses on the **stage → Snowpipe → table** path. The files
represent data that would have been produced upstream (e.g. by a Kafka
producer) and landed in cloud storage by your existing tooling. Producing
the files and landing them in storage is intentionally decoupled and
environment-specific.

---

## Prerequisite

An **external stage** named `KAFKA_DEMO.BATCH.CLICKSTREAM_STAGE`, backed by a
storage integration for your cloud provider (AWS S3, Azure ADLS/Blob, or GCS),
plus an `AUTO_INGEST = TRUE` pipe, must already exist.

Creating the storage integration, cloud IAM/role, and the storage-event
notification is **environment-specific and out of scope** for this demo. See
your cloud provider's Snowflake documentation:
- [Automating Snowpipe for Amazon S3](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3)
- [Automating Snowpipe for Microsoft Azure Blob Storage](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-azure)
- [Automating Snowpipe for Google Cloud Storage](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-gcs)

Once the stage exists, run [`setup/snowflake_setup.sql`](setup/snowflake_setup.sql)
to create the target table and the pipe.

---

## Demo Steps

### 1. Create the Snowflake objects (one-time)

Run [`setup/snowflake_setup.sql`](setup/snowflake_setup.sql). It creates:
- `KAFKA_DEMO.BATCH.CLICKSTREAM` — target table
- `KAFKA_DEMO.BATCH.CLICKSTREAM_PIPE` — Snowpipe with `AUTO_INGEST = TRUE`

After creating the pipe, configure your cloud storage event notification to
target the pipe's `notification_channel` (from `SHOW PIPES`). This is part of
the prerequisite cloud setup.

### 2. Prep — start clean (before each run)

Reset to a clean slate so the row count visibly climbs from zero:

```sql
TRUNCATE TABLE KAFKA_DEMO.BATCH.CLICKSTREAM;
SELECT SYSTEM$PIPE_STATUS('KAFKA_DEMO.BATCH.CLICKSTREAM_PIPE');  -- expect "RUNNING"
LIST @KAFKA_DEMO.BATCH.CLICKSTREAM_STAGE;                        -- check for leftover files
```

> **Dedup note:** `TRUNCATE` clears the table but does *not* reset Snowpipe's
> file load history (that lives in the pipe). The generator gives every file a
> unique name each run, so this is never a problem — just always generate fresh
> files rather than re-uploading old ones.

### 3. Generate sample files

```bash
pip install -r requirements.txt
python generate_events.py --files 5 --rows 1000
# writes uniquely-named .jsonl.gz files to ./output/
```

Filenames include a UTC timestamp and a unique id (e.g.
`clickstream_20260608T213724Z_5b316fc1.jsonl.gz`), so every run produces new
names that Snowpipe will always load. Files are **gzip-compressed JSON Lines** —
Snowflake auto-detects gzip, no configuration needed. Compression is a best
practice: smaller files mean lower storage and transfer cost.

### 4. Land the files in cloud storage

Upload the `output/` files to your external stage location. **How you do this is
environment-specific** — use whatever your environment uses. Examples:

```bash
# AWS S3
aws s3 cp output/ s3://<your-bucket>/kafka-demo/ --recursive

# Azure ADLS / Blob
az storage blob upload-batch -d <container>/kafka-demo -s output/

# Google Cloud Storage
gsutil cp output/*.jsonl.gz gs://<your-bucket>/kafka-demo/
```

Or simply drag-and-drop the files into the storage location via your cloud
provider's web console.

### 5. Watch Snowpipe load them — what to show

The moment files land, the storage event notification fires and Snowpipe
loads them (typically within seconds to ~1 minute). Run the queries in
[`status.sql`](status.sql) to watch progress and narrate the flow:

- Start with an **empty table** (row count 0) and the pipe `RUNNING`.
- After uploading, re-run `SYSTEM$PIPE_STATUS` — point out
  `lastReceivedMessageTimestamp` and `pendingFileCount` changing as the event arrives.
- Re-run the **row count** — it climbs within seconds, no warehouse, no manual COPY.
- Show **rows per file** (lineage via `METADATA$FILENAME`) — each file loaded once.
- Emphasize: **serverless & event-driven**, **file-level dedup**, **seconds latency**.

| Query | Shows |
|-------|-------|
| #1 | Files currently in the stage |
| #2 | Pipe status — event activity, pending files |
| #3 | Live row count (run repeatedly) |
| #4 | Rows per source file (file-level lineage) |
| #7 | Snowpipe load history (per-file COPY results) |

### 6. Reset for next run

```sql
TRUNCATE TABLE KAFKA_DEMO.BATCH.CLICKSTREAM;
```

Then delete the uploaded files **via the cloud console** (S3 / ADLS / GCS).
Snowflake `REMOVE @stage` is blocked here — the storage integration is
read-only (no `s3:DeleteObject`). The next run generates new uniquely-named
files automatically, so there is no dedup cleanup to worry about.

---

## Files

| File | Purpose |
|------|---------|
| `setup/snowflake_setup.sql` | Target table + AUTO_INGEST pipe (stage = prereq) |
| `generate_events.py` | Generate uniquely-named gzipped JSONL batch files |
| `requirements.txt` | `faker` (only needed for generate_events.py) |
| `status.sql` | Monitoring / verification queries |

---

## How This Differs from the Snowpipe Streaming SDK Demo

| | Stage + Snowpipe (this demo) | Snowpipe Streaming SDK |
|---|---|---|
| Latency | Seconds to ~1 min (file-based) | Sub-second (row-based) |
| Unit of work | Files in cloud storage | Rows over HTTPS |
| Compute | Snowpipe serverless | Snowpipe Streaming (serverless) |
| Code to maintain | None (files land via existing tooling) | A streaming client/consumer |
| Dedup | File-level (load history) | Row-level (offset tokens) |
| Best for | Existing file-drop / batch pipelines | New low-latency streaming pipelines |
