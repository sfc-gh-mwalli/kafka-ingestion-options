# Kafka → Snowflake: Ingestion Options

A reference site and hands-on demos comparing the ways to get data from Apache
Kafka (and file-based pipelines) into Snowflake.

**Live site:** https://sfc-gh-mwalli.github.io/kafka-ingestion-options/

The site (`index.html`) compares four ingestion approaches and provides working,
self-contained demos for each as they are built out.

## Ingestion Options

| Option | Latency | Managed by Snowflake | Demo |
|--------|---------|----------------------|------|
| **Snowflake Connector for Kafka (v4)** | Sub-second | No (you run Kafka Connect) | planned |
| **Openflow (Snowflake-managed)** | Sub-second | Yes | planned |
| **Snowpipe Streaming SDK** | Sub-second | No (ingestion layer only) | [`demos/snowpipe-streaming-sdk/`](demos/snowpipe-streaming-sdk/) |
| **Stage + Snowpipe (Batch)** | Seconds–minutes | Partially (Snowpipe side) | [`demos/stage-snowpipe/`](demos/stage-snowpipe/) |

## Demos

### Snowpipe Streaming SDK
Local Kafka (Docker) → Python producer → Snowpipe Streaming SDK consumer → Snowflake.
Sub-second, row-level ingestion with exactly-once delivery via offset tokens.
See [`demos/snowpipe-streaming-sdk/DEMO.md`](demos/snowpipe-streaming-sdk/DEMO.md).

### Stage + Snowpipe (Batch)
Gzipped JSONL files → external stage (S3 / ADLS / GCS) → Snowpipe `AUTO_INGEST` → Snowflake.
Event-driven, file-based batch ingestion. Cloud-agnostic; assumes an external stage
with a storage integration is already in place.
See [`demos/stage-snowpipe/DEMO.md`](demos/stage-snowpipe/DEMO.md).

## Repository Layout

```
.
├── index.html                      # comparison site + demo guides (GitHub Pages)
└── demos/
    ├── snowpipe-streaming-sdk/      # Kafka → SDK consumer → Snowflake
    └── stage-snowpipe/              # files → external stage → Snowpipe AUTO_INGEST
```

## Notes

- The site is served by GitHub Pages from `index.html` on the `main` branch.
- Demos use the `KAFKA_DEMO` database in Snowflake.
- Cloud storage setup (storage integration, IAM/role, event notification) is
  environment-specific and treated as a prerequisite in the Stage + Snowpipe demo.
