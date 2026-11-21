"""Publish one unpublished Chinook CDC outbox event to local Kafka.

Lab mode: single producer instance, one event per invocation.
Delivery semantics across SQL Server and Kafka: at least once.
"""

import json

import pyodbc
from confluent_kafka import Producer

SQL_CONNECTION = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Chinook_CDC_Lab;"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"  # Local lab only.
)
TOPIC = "chinook.customer.changes"


def main():
    connection = pyodbc.connect(SQL_CONNECTION, timeout=5)
    try:
        cursor = connection.cursor()
        row = cursor.execute(
            """
            SELECT TOP (1)
                EventId, EntityName, EntityId, OperationType,
                BeforeData, AfterData, CreatedAtUTC
            FROM dbo.ChangeOutbox
            WHERE PublishedAtUTC IS NULL
              AND EntityName = 'Customer'
            ORDER BY EventId ASC;
            """
        ).fetchone()

        if row is None:
            print("NO_PENDING_EVENTS: no unpublished customer changes")
            return

        event = {
            "EventId": int(row.EventId),
            "EntityName": row.EntityName,
            "EntityId": row.EntityId,
            "OperationType": row.OperationType,
            "BeforeData": json.loads(row.BeforeData) if row.BeforeData else None,
            "AfterData": json.loads(row.AfterData) if row.AfterData else None,
            "CreatedAtUTC": row.CreatedAtUTC.isoformat(timespec="microseconds") + "Z",
        }
        print(f"PENDING EventId={event['EventId']} Operation={event['OperationType']}")

        producer = Producer(
            {
                "bootstrap.servers": "127.0.0.1:29092",
                "acks": "all",
                "enable.idempotence": True,
                "message.timeout.ms": 15000,
            }
        )

        delivery = {"success": False}

        def on_delivery(error, message):
            if error is not None:
                print(f"KAFKA_DELIVERY_FAILED: {error}")
            else:
                delivery["success"] = True
                print(
                    f"KAFKA_ACK topic={message.topic()} "
                    f"partition={message.partition()} offset={message.offset()}"
                )

        producer.produce(
            TOPIC,
            key=str(row.EntityId).encode("utf-8"),
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            callback=on_delivery,
        )
        remaining = producer.flush(20)

        if remaining != 0 or not delivery["success"]:
            raise RuntimeError("Kafka delivery was not confirmed; SQL event NOT marked published")

        cursor.execute(
            """
            UPDATE dbo.ChangeOutbox
            SET PublishedAtUTC = SYSUTCDATETIME()
            WHERE EventId = ? AND PublishedAtUTC IS NULL;
            """,
            event["EventId"],
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Outbox publish marker was not updated exactly once")
        connection.commit()
        print(f"SQL_MARKED_PUBLISHED EventId={event['EventId']}")
        print("CDC_PRODUCER_COMPLETED")
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
