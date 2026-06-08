-- ──────────────────────────────────────────────────────────────────
-- Snowpipe Streaming Demo — Status & Progress Queries
-- Database: KAFKA_DEMO  |  Schema: STREAMING  |  Table: CLICKSTREAM
-- ──────────────────────────────────────────────────────────────────

USE ROLE SYSADMIN;
USE DATABASE KAFKA_DEMO;
USE SCHEMA STREAMING;


-- ── 1. Live row count ─────────────────────────────────────────────
-- Quick sanity check — run repeatedly to watch the number climb.
SELECT COUNT(*) AS total_rows FROM CLICKSTREAM;


-- ── 2. Rows per minute (last 10 minutes) ─────────────────────────
-- Shows ingestion rate over time; each row = one minute bucket.
SELECT
    DATE_TRUNC('minute', event_ts)  AS minute_bucket,
    COUNT(*)                        AS rows_ingested
FROM CLICKSTREAM
WHERE event_ts >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;


-- ── 3. Rows per Kafka partition ───────────────────────────────────
-- Verifies all partitions are contributing; uneven counts are normal.
SELECT
    kafka_partition,
    COUNT(*)                        AS row_count,
    MIN(kafka_offset)               AS min_offset,
    MAX(kafka_offset)               AS max_offset,
    MAX(event_ts)                   AS latest_event_ts
FROM CLICKSTREAM
GROUP BY kafka_partition
ORDER BY kafka_partition;


-- ── 4. Latest 10 events ───────────────────────────────────────────
-- Live preview of the most recently ingested rows.
SELECT
    event_id,
    event_type,
    user_id,
    event_ts,
    kafka_partition,
    kafka_offset,
    payload
FROM CLICKSTREAM
ORDER BY event_ts DESC
LIMIT 10;


-- ── 5. Event type breakdown ───────────────────────────────────────
-- Confirms the producer's distribution (page_view / click / purchase / signup).
SELECT
    event_type,
    COUNT(*)                        AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM CLICKSTREAM
GROUP BY event_type
ORDER BY row_count DESC;


-- ── 6. Pipeline staleness — how old is the newest event? ──────────
-- Answers: "Is the pipeline keeping up right now?"
-- Near-zero = healthy.  Large = producer stopped or consumer is behind.
-- NOTE: This is NOT row-level lag — it measures the age of the most
--       recently ingested event.  For per-row ingest latency you would
--       need a separate ingest_ts column (DEFAULT CURRENT_TIMESTAMP()).
SELECT
    MAX(event_ts)                                               AS newest_event_ts,
    CURRENT_TIMESTAMP()                                         AS query_time,
    DATEDIFF('second', MAX(event_ts), CURRENT_TIMESTAMP())      AS pipeline_staleness_secs
FROM CLICKSTREAM;


-- ── 7. Pipe status (Snowpipe Streaming auto-created pipe) ─────────
-- Shows the PIPE object the SDK created on first channel open.
SHOW PIPES LIKE '%CLICKSTREAM%';

-- Pipe health (JSON) — valid for Snowpipe Streaming pipes:
SELECT SYSTEM$PIPE_STATUS('KAFKA_DEMO.STREAMING.CLICKSTREAM-STREAMING') AS pipe_status_json;

-- Channel-level status — one row per open channel (partition_0, partition_1, ...):
SHOW CHANNELS IN PIPE KAFKA_DEMO.STREAMING."CLICKSTREAM-STREAMING";


-- ── 8. Snowpipe Streaming ingestion history (last hour) ───────────
-- Uses ACCOUNT_USAGE — reflects rows committed via streaming channels.
-- NOTE: PIPE_USAGE_HISTORY (INFORMATION_SCHEMA) covers classic Snowpipe
--       only and returns no rows for Snowpipe Streaming pipes.
SELECT
    start_time,
    end_time,
    credits_used,
    num_bytes_migrated  AS bytes_ingested,
    num_rows_migrated   AS rows_ingested
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY
WHERE table_name  = 'CLICKSTREAM'
  AND schema_name = 'STREAMING'
  AND database_name = 'KAFKA_DEMO'
  AND start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;


-- ── 9. Throughput summary (last 30 minutes) ───────────────────────
SELECT
    COUNT(*)                                                    AS total_rows,
    COUNT(DISTINCT user_id)                                     AS unique_users,
    COUNT(DISTINCT kafka_partition)                             AS active_partitions,
    MIN(event_ts)                                               AS window_start,
    MAX(event_ts)                                               AS window_end,
    DATEDIFF('second', MIN(event_ts), MAX(event_ts))            AS window_secs,
    ROUND(COUNT(*) / NULLIF(DATEDIFF('second', MIN(event_ts), MAX(event_ts)), 0), 1) AS rows_per_sec
FROM CLICKSTREAM
WHERE event_ts >= DATEADD('minute', -30, CURRENT_TIMESTAMP());
