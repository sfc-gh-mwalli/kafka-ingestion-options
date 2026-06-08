import json
import os
import signal
import tempfile
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition

import config

# Controls the main poll loop — set to False by signal handlers to trigger graceful shutdown
running = True


def handle_signal(sig, frame):
    # Catches SIGINT (Ctrl+C) and SIGTERM (docker stop / kill).
    # Sets the flag so the main loop exits cleanly after the current poll cycle.
    global running
    running = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# Flush to Snowflake after this many rows are buffered across all partitions
BATCH_SIZE = 100

# Also flush if this many seconds have passed with any buffered rows (low-volume safety net)
FLUSH_INTERVAL_SECS = 5.0


def build_profile_file():
    # The Snowpipe Streaming SDK reads auth config from a JSON profile file.
    # We write it to a temp file so the PAT is never stored on disk permanently.
    # The file is deleted on shutdown in the finally block.
    profile = {
        "authorization_type": "PAT",
        "url": f"https://{config.SNOWFLAKE_ACCOUNT}.snowflakecomputing.com:443",
        "account": config.SNOWFLAKE_ACCOUNT,
        "role": config.SNOWFLAKE_ROLE,
        "personal_access_token": config.SNOWFLAKE_PAT,
    }
    path = os.path.join(tempfile.gettempdir(), "sf_snowpipe_profile.json")
    with open(path, "w") as f:
        json.dump(profile, f)
    return path


def main():
    from snowflake.ingest.streaming import StreamingIngestClient

    profile_path = build_profile_file()

    # The pipe name follows Snowpipe Streaming's default convention: <TABLE>-STREAMING.
    # The SDK auto-creates this pipe in Snowflake on the first open_channel() call.
    pipe_name = f"{config.SNOWFLAKE_TABLE}-STREAMING"

    print(f"Connecting to Snowflake account {config.SNOWFLAKE_ACCOUNT}")
    print(f"Target: {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.{config.SNOWFLAKE_TABLE}")
    print(f"Pipe: {pipe_name}")

    # StreamingIngestClient manages the connection to Snowflake and owns the
    # background threads that compress and upload row batches to S3/blob storage.
    sf_client = StreamingIngestClient(
        client_name="kafka-demo-consumer",
        db_name=config.SNOWFLAKE_DATABASE,
        schema_name=config.SNOWFLAKE_SCHEMA,
        pipe_name=pipe_name,
        profile_json=profile_path,
    )

    # One channel per Kafka partition. Channels are named "partition_0", "partition_1", etc.
    # Named channels are resumable — if this consumer restarts and picks up the same
    # partition, it reopens the same channel and continues from the last committed offset.
    channels = {}

    def get_channel(partition_id):
        if partition_id not in channels:
            ch, _ = sf_client.open_channel(channel_name=f"partition_{partition_id}")
            channels[partition_id] = ch
            print(f"Opened channel for partition {partition_id}")
        return channels[partition_id]

    # Kafka consumer config:
    # - auto.offset.reset=earliest: start from the beginning if no committed offset exists
    # - enable.auto.commit=False: we commit offsets manually, AFTER Snowflake confirms flush.
    #   This ensures Kafka offsets only advance when data is durably in Snowflake.
    kafka = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    kafka.subscribe([config.KAFKA_TOPIC])
    print(f"Subscribed to Kafka topic '{config.KAFKA_TOPIC}' on {config.KAFKA_BOOTSTRAP_SERVERS}")
    print("Press Ctrl+C to stop.\n")

    # batch: {partition_id: [(row_dict, offset_token), ...]}
    # Rows are grouped by partition so they go to the correct Snowpipe channel.
    batch = {}

    # offsets_to_commit: {partition_id: next_offset_to_commit}
    # Tracked separately so we only commit after a successful Snowflake flush.
    offsets_to_commit = {}

    last_flush = time.time()
    total_flushed = 0

    try:
        while running:
            # Poll Kafka for up to 1 second. Returns None on timeout.
            msg = kafka.poll(1.0)

            if msg is None:
                pass
            elif msg.error():
                print(f"Consumer error: {msg.error()}")
            else:
                p = msg.partition()
                row = json.loads(msg.value())

                # Annotate each row with its Kafka metadata so we can trace
                # any row in Snowflake back to its exact Kafka position.
                row["kafka_offset"] = msg.offset()
                row["kafka_partition"] = p

                # Buffer the row alongside its offset token (used by the SDK
                # to track the highest successfully written position per channel).
                batch.setdefault(p, []).append((row, str(msg.offset())))

                # Track the next offset to commit for this partition.
                # +1 because Kafka commit means "I've processed up to here".
                offsets_to_commit[p] = msg.offset() + 1

            total_buffered = sum(len(v) for v in batch.values())
            elapsed = time.time() - last_flush

            # Flush condition: enough rows buffered OR enough time has passed.
            # The time-based flush handles low-throughput periods where BATCH_SIZE
            # might never be reached.
            if total_buffered >= BATCH_SIZE or (elapsed >= FLUSH_INTERVAL_SECS and total_buffered > 0):

                # Append all buffered rows to their respective Snowpipe channels.
                # append_row() is non-blocking — it queues rows in the SDK's internal buffer.
                for p, rows in batch.items():
                    ch = get_channel(p)
                    for row, offset_token in rows:
                        ch.append_row(row, offset_token=offset_token)

                # Block until Snowflake confirms all queued rows are durably written.
                # Only after this do we commit Kafka offsets — guaranteeing at-least-once delivery.
                sf_client.wait_for_flush(timeout_seconds=30)

                # Commit Kafka offsets now that Snowflake has confirmed the data.
                # If the consumer crashes before this point, Kafka will re-deliver
                # the same messages and the SDK will deduplicate via offset tokens.
                for p, offset in offsets_to_commit.items():
                    kafka.commit(offsets=[TopicPartition(config.KAFKA_TOPIC, p, offset)])

                total_flushed += total_buffered
                ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"[{ts}] Flushed {total_buffered} rows to Snowflake | total rows: {total_flushed} | partitions: {list(batch.keys())}")
                batch.clear()
                offsets_to_commit.clear()
                last_flush = time.time()

    finally:
        # Graceful shutdown: close all Snowpipe channels, the SDK client,
        # the Kafka consumer, and clean up the temp profile file.
        for ch in channels.values():
            try:
                ch.close()
            except Exception:
                pass
        sf_client.close()
        kafka.close()
        if os.path.exists(profile_path):
            os.remove(profile_path)
        print(f"\nShutdown complete. Total rows flushed: {total_flushed}")


if __name__ == "__main__":
    main()
