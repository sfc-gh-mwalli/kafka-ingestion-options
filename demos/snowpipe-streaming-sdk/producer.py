import json
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

import config

# Faker instance for generating realistic synthetic data
fake = Faker()


def delivery_report(err, msg):
    # Called asynchronously by confluent-kafka after each produce() attempt.
    # Only prints on failure — successful deliveries are silent.
    if err:
        print(f"Delivery failed: {err}")


def main():
    # Connect to the local Kafka broker defined in config.py
    p = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    count = 0
    print(f"Producing events to topic '{config.KAFKA_TOPIC}' on {config.KAFKA_BOOTSTRAP_SERVERS}")
    print("Press Ctrl+C to stop.\n")
    try:
        while True:
            # Build a synthetic user event with a nested payload dict.
            # The payload is a native Python dict — this ensures it lands as
            # a VARIANT OBJECT in Snowflake, not a VARCHAR string.
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": fake.random_element(["page_view", "click", "purchase", "signup"]),
                "user_id": str(uuid.uuid4()),
                "payload": {
                    "ip": fake.ipv4(),
                    "country": fake.country_code(),
                    "amount": round(fake.pyfloat(min_value=1, max_value=500), 2),
                    "browser": fake.user_agent(),
                },
                "event_ts": datetime.now(timezone.utc).isoformat(),
            }

            # Produce the event as a JSON string.
            # user_id is used as the Kafka message key, which determines
            # which partition the message lands in (consistent hashing).
            p.produce(
                config.KAFKA_TOPIC,
                key=event["user_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            count += 1

            # flush() forces all buffered messages to be sent to the broker.
            # Called every 100 messages to prevent unbounded buffering.
            if count % 100 == 0:
                p.flush()
                print(f"Produced {count} events")

            # ~10 events/second
            time.sleep(0.1)

    except KeyboardInterrupt:
        # Drain any remaining buffered messages before exiting
        p.flush()
        print(f"\nStopped. Total produced: {count}")


if __name__ == "__main__":
    main()
