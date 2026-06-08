-- ──────────────────────────────────────────────────────────────────
-- Stage + Snowpipe Demo — Status & Progress Queries
-- Database: KAFKA_DEMO  |  Schema: BATCH  |  Table: CLICKSTREAM
-- ──────────────────────────────────────────────────────────────────

USE ROLE SYSADMIN;
USE DATABASE KAFKA_DEMO;
USE SCHEMA BATCH;


-- ── 0. Prep / Reset between runs ──────────────────────────────────
-- Run before a demo (clean slate) and after (reset). The row count then
-- visibly climbs from zero as files load.
--   TRUNCATE clears the table but does NOT reset Snowpipe's file load history
--   (that lives in the pipe). generate_events.py emits uniquely-named files
--   each run, so re-loads always happen -- just generate fresh files.
--   Staged files must be deleted via the cloud console: REMOVE @stage is
--   blocked because the storage integration is read-only (no s3:DeleteObject).
TRUNCATE TABLE CLICKSTREAM;


-- ── 1. Files currently in the stage ───────────────────────────────
-- Shows files that have landed in the external stage location.
LIST @CLICKSTREAM_STAGE;


-- ── 2. Pipe status ─────────────────────────────────────────────────
-- executionState should be RUNNING. Watch:
--   numOutstandingMessagesOnChannel  - events received from cloud storage
--   lastReceivedMessageTimestamp     - last storage event notification
--   lastForwardedFilePath            - most recent file queued for load
--   pendingFileCount                 - files queued, not yet loaded
SELECT SYSTEM$PIPE_STATUS('KAFKA_DEMO.BATCH.CLICKSTREAM_PIPE') AS pipe_status;


-- ── 3. Live row count ─────────────────────────────────────────────
-- Run repeatedly after uploading files to watch rows arrive.
SELECT COUNT(*) AS total_rows FROM CLICKSTREAM;


-- ── 4. Rows per source file ───────────────────────────────────────
-- Confirms which staged file delivered which rows (file-level lineage).
SELECT
    ingest_file,
    COUNT(*)            AS row_count,
    MIN(kafka_offset)   AS min_offset,
    MAX(kafka_offset)   AS max_offset
FROM CLICKSTREAM
GROUP BY ingest_file
ORDER BY ingest_file;


-- ── 5. Latest 10 rows ─────────────────────────────────────────────
SELECT
    event_id,
    event_type,
    user_id,
    payload:country::VARCHAR AS country,
    payload:amount::FLOAT    AS amount,
    event_ts,
    ingest_file
FROM CLICKSTREAM
ORDER BY event_ts DESC
LIMIT 10;


-- ── 6. Event type breakdown ───────────────────────────────────────
SELECT
    event_type,
    COUNT(*)                                           AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM CLICKSTREAM
GROUP BY event_type
ORDER BY row_count DESC;


-- ── 7. Snowpipe load history (last hour) ──────────────────────────
-- Shows per-file COPY activity: rows loaded, errors, load time.
SELECT
    file_name,
    status,
    row_count,
    row_parsed,
    first_error_message,
    last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME  => 'KAFKA_DEMO.BATCH.CLICKSTREAM',
    START_TIME  => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;


-- ── 8. Snowpipe credit usage (account-level, ~latency in ACCOUNT_USAGE) ──
SELECT
    start_time,
    end_time,
    pipe_name,
    credits_used,
    bytes_inserted,
    files_inserted
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE pipe_name = 'CLICKSTREAM_PIPE'
  AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
