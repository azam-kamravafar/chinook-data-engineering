"""One-time Chinook Kafka -> PostgreSQL staging lab consumer.

Run in Ubuntu/WSL. Reads at most one *real* customer change. The known
synthetic test message is skipped. Commit Kafka offsets only after the
PostgreSQL transaction has committed.
"""

import json
import time
import os
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb
from confluent_kafka import Consumer, KafkaException

TOPIC = "chinook.customer.changes"
GROUP_ID = "chinook-postgres-staging-v1"
ENV_FILE = Path.home() / "chinook-data-engineering" / "postgres" / ".env"


def postgres_password():
    password_from_env = os.environ.get("POSTGRES_PASSWORD")
    if password_from_env:
        return password_from_env

    if not ENV_FILE.is_file():
        raise FileNotFoundError(f"PostgreSQL .env not found: {ENV_FILE}")
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("POSTGRES_PASSWORD="):
            value = line.split("=", 1)[1].strip().strip('"').strip("'")
            if value:
                return value
    raise ValueError("POSTGRES_PASSWORD not found in PostgreSQL .env")


def validate_event(event):
    event_id = event.get("EventId")
    if type(event_id) is not int or event_id <= 0:
        raise ValueError(f"Invalid real EventId: {event_id!r}")
    if event.get("EntityName") != "Customer":
        raise ValueError("Unexpected entity in customer topic")
    entity_id = event.get("EntityId")
    if type(entity_id) is not int or entity_id <= 0:
        raise ValueError("Invalid EntityId")
    operation = event.get("OperationType")
    if operation not in ("INSERT", "UPDATE", "DELETE"):
        raise ValueError("Unexpected operation type")
    created_at = event.get("CreatedAtUTC")
    if not isinstance(created_at, str):
        raise ValueError("Missing source timestamp")
    parsed_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    if parsed_at.tzinfo is None:
        raise ValueError("Source timestamp must contain UTC timezone")
    before = event.get("BeforeData")
    after = event.get("AfterData")
    if before is not None and not isinstance(before, dict):
        raise ValueError("BeforeData must be an object or null")
    if after is not None and not isinstance(after, dict):
        raise ValueError("AfterData must be an object or null")
    return event_id, entity_id, operation, parsed_at, before, after


def commit_offset(consumer, message):
    result = consumer.commit(message=message, asynchronous=False)
    if not result or result[0].error is not None:
        raise RuntimeError(f"Kafka offset commit failed: {result}")
    print(f"KAFKA_COMMITTED next_offset={message.offset() + 1}")


def main():
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092"),
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "allow.auto.create.topics": False,
    })

    try:
        with psycopg.connect(
            host=os.environ.get("PG_HOST", "127.0.0.1"), port=int(os.environ.get("PG_PORT", "15432")),
            dbname="chinook_staging", user="chinook_user",
            password=postgres_password(), connect_timeout=10,
            autocommit=True,
        ) as conn:
            consumer.subscribe([TOPIC])
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                message = consumer.poll(2.0)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())

                event = json.loads(message.value().decode("utf-8"))
                if not isinstance(event, dict):
                    raise ValueError("Kafka message must be a JSON object")

                if event.get("EventId") == "test-001":
                    print(f"SKIPPED_KNOWN_TEST offset={message.offset()}")
                    commit_offset(consumer, message)
                    continue

                event_id, entity_id, operation, created_at, before, after = validate_event(event)

                # The database transaction must complete BEFORE Kafka offset commit.
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO staging.customer_change_events (
                                event_id, entity_name, entity_id, operation_type,
                                before_data, after_data, source_created_at_utc,
                                kafka_topic, kafka_partition, kafka_offset
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (event_id) DO NOTHING
                            RETURNING event_id
                        """, (
                            event_id, "Customer", entity_id, operation,
                            Jsonb(before) if before is not None else None,
                            Jsonb(after) if after is not None else None,
                            created_at, message.topic(), message.partition(), message.offset(),
                        ))
                        inserted = cur.fetchone() is not None

                print(f"POSTGRES_{'INSERTED' if inserted else 'ALREADY_EXISTS'} EventId={event_id}")
                commit_offset(consumer, message)
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM staging.customer_change_events")
                    print(f"STAGING_TOTAL_EVENTS={cur.fetchone()[0]}")
                print("KAFKA_TO_POSTGRES_LAB_COMPLETED")
                return
            print("NO_REAL_MESSAGE_RECEIVED_WITHIN_60_SECONDS")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
