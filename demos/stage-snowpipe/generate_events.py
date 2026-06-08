#!/usr/bin/env python3
"""
Generate synthetic clickstream event files for the Stage + Snowpipe demo.

Writes gzip-compressed JSON Lines (.jsonl.gz) batch files to an output
directory. Each line is one event matching the KAFKA_DEMO.BATCH.CLICKSTREAM
table schema. These files represent data that would have been produced
upstream (e.g. by a Kafka producer) and landed in cloud storage.

Upload the generated files to your external stage location (S3 / ADLS / GCS)
by whatever means your environment uses; Snowpipe AUTO_INGEST loads them
automatically.

Usage:
    python generate_events.py                 # 3 files, 500 events each
    python generate_events.py --files 5 --rows 1000
    python generate_events.py --output-dir output --uncompressed
"""
import argparse
import gzip
import json
import os
import uuid
from datetime import datetime, timezone

from faker import Faker

fake = Faker()

EVENT_TYPES = ["page_view", "click", "purchase", "signup"]


def make_event(offset: int, partition: int) -> dict:
    # Mirrors the upstream producer's event shape. kafka_offset / kafka_partition
    # are simulated lineage fields representing the source topic position.
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": fake.random_element(EVENT_TYPES),
        "user_id": str(uuid.uuid4()),
        "payload": {
            "ip": fake.ipv4(),
            "country": fake.country_code(),
            "amount": round(fake.pyfloat(min_value=1, max_value=500), 2),
            "browser": fake.user_agent(),
        },
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "kafka_offset": offset,
        "kafka_partition": partition,
    }


def write_batch(path: str, rows: int, start_offset: int, compress: bool) -> None:
    lines = []
    for i in range(rows):
        offset = start_offset + i
        partition = offset % 3  # simulate 3 partitions
        lines.append(json.dumps(make_event(offset, partition)))
    data = ("\n".join(lines) + "\n").encode("utf-8")

    if compress:
        with gzip.open(path, "wb") as f:
            f.write(data)
    else:
        with open(path, "wb") as f:
            f.write(data)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate clickstream batch files.")
    parser.add_argument("--files", type=int, default=3, help="Number of batch files (default 3)")
    parser.add_argument("--rows", type=int, default=500, help="Events per file (default 500)")
    parser.add_argument("--output-dir", default="output", help="Output directory (default ./output)")
    parser.add_argument("--uncompressed", action="store_true", help="Write plain .jsonl instead of .jsonl.gz")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    compress = not args.uncompressed
    ext = "jsonl.gz" if compress else "jsonl"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    offset = 0
    for n in range(1, args.files + 1):
        # Each filename includes a UTC timestamp and a unique uuid fragment, so
        # filenames never collide within or across runs. This matters because
        # Snowpipe deduplicates on filename (load history lives in the pipe and
        # is NOT reset by TRUNCATE TABLE) -- unique names guarantee every file
        # is always loaded, with no dedup surprises on repeat demo runs.
        unique = uuid.uuid4().hex[:8]
        fname = f"clickstream_{ts}_{unique}.{ext}"
        path = os.path.join(args.output_dir, fname)
        write_batch(path, args.rows, offset, compress)
        offset += args.rows
        size = os.path.getsize(path)
        print(f"Wrote {args.rows} events -> {path} ({size:,} bytes)")

    print(f"\nDone. {args.files} file(s) in '{args.output_dir}/'.")
    print("Filenames are unique each run, so Snowpipe AUTO_INGEST always loads them.")
    print("Upload them to your external stage location to trigger ingestion.")


if __name__ == "__main__":
    main()
