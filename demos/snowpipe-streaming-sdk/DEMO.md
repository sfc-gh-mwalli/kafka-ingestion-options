# Snowpipe Streaming SDK Demo — Startup & Shutdown

Local Kafka (Docker) → Python producer → Snowpipe Streaming SDK consumer → Snowflake

---

## Prerequisites

- Docker Desktop running
- Python 3.9+
- Dependencies installed: `pip install -r requirements.txt`
- PAT stored as `password` in `~/.snowflake/connections.toml` for account `SFSENORTHAMERICA-MWALLI_AWSUSEAST2`
- Snowflake table created (one-time): run `setup/snowflake_setup.sql`

---

## Startup

Open **three terminal windows**.

### Terminal 1 — Kafka broker
```bash
docker compose up -d
```
Starts a single-node Kafka broker in KRaft mode on `localhost:9092`.  
Verify it's up:
```bash
docker ps
```
You should see `kafka-demo` with status `Up`.

Create the `events` topic with 3 partitions (one-time after first `docker compose up`):
```bash
docker exec kafka-demo /opt/kafka/bin/kafka-topics.sh \
  --create --topic events --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### Terminal 2 — Consumer (Snowpipe Streaming)
```bash
python consumer.py
```
Opens one Snowpipe Streaming channel per Kafka partition and begins polling.  
You'll see output like:
```
Consumer started. Waiting for messages...
[14:32:05] Flushed 100 rows to Snowflake | total rows: 100 | partitions: [0, 1, 2]
[14:32:10] Flushed 97 rows to Snowflake  | total rows: 197 | partitions: [0, 1, 2]
```
Each flush line confirms rows are durably written to Snowflake.

### Terminal 3 — Producer
```bash
python producer.py
```
Generates synthetic clickstream events (page_view / click / purchase / signup) at ~10/sec and publishes them to the `events` Kafka topic.

---

## Verify data in Snowflake

Run the queries in `status.sql` against `KAFKA_DEMO.STREAMING.CLICKSTREAM`.  
Key queries:

| Query | What it shows |
|---|---|
| #1 | Live row count — watch it climb |
| #2 | Rows per minute — ingestion rate |
| #4 | Latest 10 events with payload |
| #6 | Pipeline staleness — should be ~5–10 seconds |

---

## Shutdown

### Stop the producer
In Terminal 3: `Ctrl+C`

### Stop the consumer
In Terminal 2: `Ctrl+C`  
The consumer drains and flushes any buffered rows before exiting.

### Stop Kafka
```bash
docker compose down
```
This stops and removes the container. Kafka's topic data is not persisted — it will be empty on next startup.

---

## Restart after a previous run

Kafka topic data is lost when the container stops, but **Snowflake data is permanent**.  
The consumer will reopen the same named channels (`partition_0`, `partition_1`, `partition_2`) and continue appending rows.

```bash
docker compose up -d   # restart Kafka
python consumer.py     # restart consumer
python producer.py     # restart producer
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Consumer exits immediately | PAT not found in connections.toml | Check account/user match in config.py |
| No flush messages after 10s | Producer not running | Start producer.py |
| `localhost:9092 unreachable` | Kafka container not running | `docker compose up -d` |
| Row count not increasing | Consumer stopped | Restart consumer.py |
| High pipeline staleness (>30s) | Consumer or producer stopped | Check both terminals |
