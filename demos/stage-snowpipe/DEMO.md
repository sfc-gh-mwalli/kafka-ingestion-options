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

### 2. Get sample files

Two options:

**Option A — use the committed samples (zero setup):**
The [`samples/`](samples/) folder contains ready-made gzipped JSONL files:
```
samples/clickstream_batch_001.jsonl.gz
samples/clickstream_batch_002.jsonl.gz
samples/clickstream_batch_003.jsonl.gz
```

**Option B — generate fresh files:**
```bash
pip install -r requirements.txt
python generate_events.py --files 5 --rows 1000
# writes gzipped .jsonl.gz files to ./output/
```

Files are **gzip-compressed JSON Lines** (`.jsonl.gz`). Snowflake's JSON file
format auto-detects gzip — no configuration needed. Compression is a best
practice: smaller files mean lower storage and transfer cost.

### 3. Land the files in cloud storage

Upload the files to your external stage location. **How you do this is
environment-specific** — use whatever your environment uses. Examples:

```bash
# AWS S3
aws s3 cp samples/ s3://<your-bucket>/kafka-demo/ --recursive

# Azure ADLS / Blob
az storage blob upload-batch -d <container>/kafka-demo -s samples/

# Google Cloud Storage
gsutil cp samples/*.jsonl.gz gs://<your-bucket>/kafka-demo/
```

Or simply drag-and-drop the files into the storage location via your cloud
provider's web console.

### 4. Watch Snowpipe load them

The moment files land, the storage event notification fires and Snowpipe
loads them (typically within seconds to ~1 minute). Run the queries in
[`status.sql`](status.sql) to watch progress:

| Query | Shows |
|-------|-------|
| #1 | Files currently in the stage |
| #2 | Pipe status — event activity, pending files |
| #3 | Live row count (run repeatedly) |
| #4 | Rows per source file (file-level lineage) |
| #7 | Snowpipe load history (per-file COPY results) |

---

## Files

| File | Purpose |
|------|---------|
| `setup/snowflake_setup.sql` | Target table + AUTO_INGEST pipe (stage = prereq) |
| `generate_events.py` | Generate gzipped JSONL batch files |
| `requirements.txt` | `faker` (only needed for generate_events.py) |
| `samples/*.jsonl.gz` | Ready-made sample files for zero-setup demos |
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
