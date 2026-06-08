-- ──────────────────────────────────────────────────────────────────
-- Stage + Snowpipe Demo — Snowflake objects (cloud-agnostic)
-- Database: KAFKA_DEMO  |  Schema: BATCH  |  Table: CLICKSTREAM
-- ──────────────────────────────────────────────────────────────────
--
-- PREREQUISITE (NOT created here):
--   An EXTERNAL STAGE named KAFKA_DEMO.BATCH.CLICKSTREAM_STAGE must already
--   exist, backed by a storage integration for your cloud provider
--   (AWS S3, Azure ADLS/Blob, or GCS). Creating the storage integration,
--   cloud IAM/role, and the storage-event notification is environment-specific
--   and intentionally out of scope for this demo.
--
--   The stage must point at the location your file-landing process writes to,
--   e.g.:
--     CREATE STAGE KAFKA_DEMO.BATCH.CLICKSTREAM_STAGE
--       URL = '<s3://... | azure://... | gcs://...>'
--       STORAGE_INTEGRATION = <your_integration>
--       FILE_FORMAT = (TYPE='JSON' STRIP_OUTER_ARRAY=FALSE);
--
--   See your cloud provider's Snowflake docs for the storage integration +
--   event-notification setup.
-- ──────────────────────────────────────────────────────────────────

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS KAFKA_DEMO;
CREATE SCHEMA IF NOT EXISTS KAFKA_DEMO.BATCH;

-- ── Target table ──────────────────────────────────────────────────
CREATE OR REPLACE TABLE KAFKA_DEMO.BATCH.CLICKSTREAM (
    event_id        VARCHAR,
    event_type      VARCHAR,
    user_id         VARCHAR,
    payload         VARIANT,
    event_ts        TIMESTAMP_NTZ,
    kafka_offset    NUMBER,
    kafka_partition NUMBER,
    ingest_file     VARCHAR   -- METADATA$FILENAME: source file each row came from
);

-- ── Snowpipe with auto-ingest ─────────────────────────────────────
-- Files landing in the stage location trigger a cloud storage event
-- notification, which Snowpipe consumes to load files automatically.
-- After creating the pipe, run SHOW PIPES and configure your cloud
-- storage event notification to target the pipe's notification_channel.
CREATE OR REPLACE PIPE KAFKA_DEMO.BATCH.CLICKSTREAM_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO KAFKA_DEMO.BATCH.CLICKSTREAM (
    event_id, event_type, user_id, payload, event_ts,
    kafka_offset, kafka_partition, ingest_file
)
FROM (
    SELECT
        $1:event_id::VARCHAR,
        $1:event_type::VARCHAR,
        $1:user_id::VARCHAR,
        $1:payload,
        $1:event_ts::TIMESTAMP_NTZ,
        $1:kafka_offset::NUMBER,
        $1:kafka_partition::NUMBER,
        METADATA$FILENAME
    FROM @KAFKA_DEMO.BATCH.CLICKSTREAM_STAGE
)
FILE_FORMAT = (TYPE='JSON');

-- ── Post-create: get the notification channel for your cloud event setup ──
-- SHOW PIPES LIKE 'CLICKSTREAM_PIPE' IN SCHEMA KAFKA_DEMO.BATCH;
-- Copy the notification_channel value and configure your storage event
-- notification (S3 event / Azure Event Grid / GCS Pub/Sub) to target it.

-- ── Verify ────────────────────────────────────────────────────────
-- SELECT SYSTEM$PIPE_STATUS('KAFKA_DEMO.BATCH.CLICKSTREAM_PIPE');
-- LIST @KAFKA_DEMO.BATCH.CLICKSTREAM_STAGE;
