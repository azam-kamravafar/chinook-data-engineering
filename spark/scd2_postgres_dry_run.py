
import os
import json
import base64
import urllib.request
import urllib.error

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ==================================================
# 1. CONFIGURATION
# ==================================================

PG_HOST = "chinook-postgres"
PG_PORT = "5432"
PG_DATABASE = "chinook_staging"
PG_USER = "chinook_user"
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

CH_URL = "http://chinook-clickhouse:8123/"
CH_USER = os.environ.get("CH_USER")
CH_PASSWORD = os.environ.get("CH_PASSWORD")

if not all([PG_PASSWORD, CH_USER, CH_PASSWORD]):
    raise RuntimeError("Required database credentials are missing.")


JDBC_URL = (
    f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)


# Attributes shared by SQL Server and ClickHouse DimCustomer.
DIM_FIELDS = [
    "FirstName",
    "LastName",
    "Company",
    "Address",
    "City",
    "State",
    "Country",
    "PostalCode",
    "Email",
    "SupportRepId",
]


# ==================================================
# 2. CLICKHOUSE HTTP CLIENT
# ==================================================

def clickhouse(sql, rows=False):

    token = base64.b64encode(
        f"{CH_USER}:{CH_PASSWORD}".encode()
    ).decode()

    statement = sql.strip().rstrip(";")

    if rows:
        statement += "\nFORMAT JSONEachRow"

    request = urllib.request.Request(
        CH_URL,
        data=statement.encode("utf-8"),
        headers={
            "Authorization": f"Basic {token}"
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(
            request, timeout=90
        ) as response:

            body = response.read().decode("utf-8")

    except urllib.error.HTTPError as exc:

        detail = exc.read().decode(
            "utf-8", errors="replace"
        )

        raise RuntimeError(
            f"ClickHouse query failed: {detail}"
        ) from exc

    if rows:
        return [
            json.loads(line)
            for line in body.splitlines()
            if line.strip()
        ]

    return None


# ==================================================
# 3. DATA COMPARISON HELPERS
# ==================================================

def same_value(a, b):

    if a is None or b is None:
        return a is None and b is None

    if isinstance(a, (int, float)) or isinstance(
        b, (int, float)
    ):
        try:
            return int(a) == int(b)
        except (ValueError, TypeError):
            pass

    return str(a) == str(b)


def parse_datetime(value):

    value = str(value)

    if "." in value:
        return datetime.strptime(
            value[:23],
            "%Y-%m-%d %H:%M:%S.%f"
        )

    return datetime.strptime(
        value[:19],
        "%Y-%m-%d %H:%M:%S"
    )


def parse_json(value):

    if isinstance(value, dict):
        return value

    if isinstance(value, str):
        return json.loads(value)

    raise RuntimeError("Invalid JSON event payload.")


# ==================================================
# 4. SPARK SESSION
# ==================================================

spark = (
    SparkSession.builder
    .appName("Chinook-SCD2-PostgreSQL-Dry-Run")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

spark.conf.set(
    "spark.sql.session.timeZone", "UTC"
)


try:

    # ==================================================
    # 5. READ POSTGRESQL STAGING
    # ==================================================

    print("\n=== READING POSTGRESQL EVENTS ===")

    events_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option(
            "dbtable",
            """
            (
                SELECT
                    event_id,
                    entity_name,
                    entity_id,
                    operation_type,
                    before_data::text AS before_data,
                    after_data::text AS after_data,
                    source_created_at_utc,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset
                FROM staging.customer_change_events
                WHERE entity_name = 'Customer'
            ) AS events
            """
        )
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    events_df = (
        events_df
        .withColumn(
            "changed_at_utc",
            F.date_format(
                "source_created_at_utc",
                "yyyy-MM-dd HH:mm:ss.SSS"
            )
        )
        .orderBy(
            "entity_id",
            "changed_at_utc",
            "event_id"
        )
    )

    event_rows = [
        row.asDict(recursive=True)
        for row in events_df.collect()
    ]

    print("Incoming Customer Events:", len(event_rows))

    if not event_rows:
        print("No Customer events found.")
        raise SystemExit(0)


    # ==================================================
    # 6. READ CURRENT CLICKHOUSE DIMENSION
    # ==================================================

    print("\n=== READING CLICKHOUSE DIMCUSTOMER ===")

    current_rows = clickhouse("""
        SELECT
            CustomerKey,
            CustomerId,
            FirstName,
            LastName,
            Company,
            Address,
            City,
            State,
            Country,
            PostalCode,
            Email,
            SupportRepId,
            ValidFrom,
            IsCurrent
        FROM ChinookDW.DimCustomer
        WHERE IsCurrent = 1
    """, rows=True)

    current_by_customer = {}

    for row in current_rows:

        customer_id = int(row["CustomerId"])

        if customer_id in current_by_customer:
            raise RuntimeError(
                f"Multiple current versions for CustomerId={customer_id}"
            )

        current_by_customer[customer_id] = row

    print("Current Customers:", len(current_by_customer))


    # ==================================================
    # 7. READ NEXT AVAILABLE CUSTOMER KEY
    # ==================================================

    max_key_rows = clickhouse("""
        SELECT max(CustomerKey) AS MaxKey
        FROM ChinookDW.DimCustomer
    """, rows=True)

    next_key = int(max_key_rows[0]["MaxKey"]) + 1

    print("Next Available CustomerKey:", next_key)


    # ==================================================
    # 8. VALIDATE AND SIMULATE EVENTS
    # ==================================================

    print("\n=== SCD2 POSTGRES PLAN ===")

    plan_rows = []

    seen_event_ids = set()
    seen_timestamps = set()

    for event in event_rows:

        event_id = int(event["event_id"])
        customer_id = int(event["entity_id"])

        operation = event["operation_type"].upper()

        changed_at = event["changed_at_utc"]

        if not changed_at:
            raise RuntimeError(
                f"Missing event timestamp: EventId={event_id}"
            )

        event_time = parse_datetime(changed_at)

        if event_id in seen_event_ids:
            raise RuntimeError(
                f"Duplicate EventId={event_id}"
            )

        seen_event_ids.add(event_id)

        timestamp_key = (customer_id, changed_at)

        if timestamp_key in seen_timestamps:
            raise RuntimeError(
                f"Ambiguous event timestamp: CustomerId={customer_id}"
            )

        seen_timestamps.add(timestamp_key)

        state = current_by_customer.get(customer_id)

        action = None
        mismatch_columns = []
        changed_columns = []
        old_key = None
        old_country = None
        new_country = None
        proposed_key = None

        if operation != "UPDATE":

            action = "UNSUPPORTED_OPERATION"

        elif state is None:

            action = "NO_CURRENT_VERSION"

        else:

            old_key = int(state["CustomerKey"])
            old_country = state["Country"]

            before = parse_json(event["before_data"])
            after = parse_json(event["after_data"])

            missing_fields = [
                field
                for field in DIM_FIELDS
                if field not in before or field not in after
            ]

            if missing_fields:

                action = "MISSING_IMAGE_FIELDS"

                mismatch_columns = missing_fields

            else:

                new_country = after["Country"]

                changed_columns = [
                    field
                    for field in DIM_FIELDS
                    if not same_value(
                        before[field],
                        after[field]
                    )
                ]

                current_valid_from = parse_datetime(
                    state["ValidFrom"]
                )

                after_matches = all(
                    same_value(
                        after[field],
                        state[field]
                    )
                    for field in DIM_FIELDS
                )

                if (
                    event_time == current_valid_from
                    and after_matches
                ):

                    action = "ALREADY_APPLIED"

                elif event_time <= current_valid_from:

                    action = "STALE_EVENT"

                else:

                    mismatch_columns = [
                        field
                        for field in DIM_FIELDS
                        if not same_value(
                            before[field],
                            state[field]
                        )
                    ]

                    if mismatch_columns:

                        action = "BEFORE_IMAGE_MISMATCH"

                    elif not changed_columns:

                        action = "NO_CHANGE"

                    else:

                        action = "UPDATE_REQUIRED"

                        proposed_key = next_key
                        next_key += 1

                        # Simulate the new current version.
                        # No ClickHouse data is modified.

                        simulated_state = dict(state)

                        for field in DIM_FIELDS:
                            simulated_state[field] = after[field]

                        simulated_state["CustomerKey"] = proposed_key
                        simulated_state["ValidFrom"] = changed_at

                        current_by_customer[customer_id] = simulated_state

        plan_rows.append({
            "EventId": event_id,
            "CustomerId": customer_id,
            "OldKey": old_key,
            "OldCountry": old_country,
            "NewCountry": new_country,
            "ChangedAtUTC": changed_at,
            "ChangedColumns": ", ".join(changed_columns),
            "ProposedKey": proposed_key,
            "Action": action,
            "MismatchColumns": ", ".join(mismatch_columns),
        })


    # ==================================================
    # 9. DISPLAY EXECUTION PLAN
    # ==================================================

    plan_schema = """
        EventId LONG,
        CustomerId INT,
        OldKey LONG,
        OldCountry STRING,
        NewCountry STRING,
        ChangedAtUTC STRING,
        ChangedColumns STRING,
        ProposedKey LONG,
        Action STRING,
        MismatchColumns STRING
    """

    plan_df = spark.createDataFrame(
        plan_rows,
        schema=plan_schema
    )

    plan_df.orderBy(
        "CustomerId",
        "ChangedAtUTC",
        "EventId"
    ).show(truncate=False)

    update_count = sum(
        1 for row in plan_rows
        if row["Action"] == "UPDATE_REQUIRED"
    )

    print("Total Events:", len(plan_rows))
    print("Updates Required:", update_count)

    print("\n=== DRY RUN SUMMARY ===")

    for row in plan_rows:
        print(
            f"EventId={row['EventId']} "
            f"CustomerId={row['CustomerId']} "
            f"Action={row['Action']}"
        )

    print("\nSCD2 POSTGRES DRY RUN COMPLETED")
    print("NO CLICKHOUSE DATA WAS MODIFIED")

finally:

    spark.stop()
