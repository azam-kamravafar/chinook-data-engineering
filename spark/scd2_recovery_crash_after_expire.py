
import os
import json
import base64
import urllib.request
import urllib.error
import urllib.parse

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. CONFIGURATION
# ============================================================

PG_HOST = "chinook-postgres"
PG_PORT = 5432
PG_DATABASE = "chinook_recovery_lab"
PG_USER = "chinook_user"
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

CH_URL = "http://chinook-clickhouse:8123/"
CH_USER = os.environ.get("CH_USER")
CH_PASSWORD = os.environ.get("CH_PASSWORD")

if not all([PG_PASSWORD, CH_USER, CH_PASSWORD]):
    raise RuntimeError("Database credentials are missing.")

JDBC_URL = (
    f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)

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

CH_TOKEN = base64.b64encode(
    f"{CH_USER}:{CH_PASSWORD}".encode()
).decode()


# ============================================================
# 2. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Chinook-SCD2-PostgreSQL-Apply")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")


# ============================================================
# 3. CLICKHOUSE HTTP CLIENT
# ============================================================

def clickhouse(sql, rows=False):

    statement = sql.strip().rstrip(";")

    if rows:
        statement += "\nFORMAT JSONEachRow"

    request = urllib.request.Request(
        CH_URL,
        data=statement.encode("utf-8"),
        headers={
            "Authorization": f"Basic {CH_TOKEN}"
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


def clickhouse_insert(row):

    columns = """
        CustomerKey, CustomerId,
        FirstName, LastName,
        Company, Address, City, State,
        Country, PostalCode, Email,
        SupportRepId,
        ValidFrom, ValidTo,
        IsCurrent, RowHash, SourceEventId
    """

    query = (
        "INSERT INTO ChinookRecoveryLab.DimCustomer "
        f"({columns}) FORMAT JSONEachRow"
    )

    url = CH_URL + "?" + urllib.parse.urlencode(
        {"query": query}
    )

    payload = (
        json.dumps(row, ensure_ascii=False) + "\n"
    ).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Basic {CH_TOKEN}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(
            request, timeout=90
        ) as response:

            response.read()

    except urllib.error.HTTPError as exc:

        detail = exc.read().decode(
            "utf-8", errors="replace"
        )

        raise RuntimeError(
            f"ClickHouse INSERT failed: {detail}"
        ) from exc


# ============================================================
# 4. POSTGRESQL LEDGER CONNECTION
# ============================================================

def pg_connection():

    spark._jvm.java.lang.Class.forName(
        "org.postgresql.Driver"
    )

    return spark._jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        PG_USER,
        PG_PASSWORD,
    )


def get_ledger(event_id):

    conn = pg_connection()

    try:
        stmt = conn.prepareStatement("""
            SELECT
                status,
                old_customer_key,
                new_customer_key,
                attempt_count
            FROM staging.event_processing
            WHERE event_id = ?
        """)

        stmt.setLong(1, event_id)
        rs = stmt.executeQuery()

        if not rs.next():
            rs.close()
            stmt.close()
            return None

        status = rs.getString(1)

        old_key = rs.getLong(2)
        if rs.wasNull():
            old_key = None

        new_key = rs.getLong(3)
        if rs.wasNull():
            new_key = None

        attempt_count = rs.getInt(4)

        rs.close()
        stmt.close()

        return {
            "status": status,
            "old_key": old_key,
            "new_key": new_key,
            "attempt_count": attempt_count,
        }

    finally:
        conn.close()


def reserve_processing(
    event_id,
    customer_id,
    old_key,
    new_key,
):

    conn = pg_connection()

    try:
        stmt = conn.prepareStatement("""
            INSERT INTO staging.event_processing
            (
                event_id,
                entity_name,
                entity_id,
                status,
                old_customer_key,
                new_customer_key
            )
            VALUES (?, 'Customer', ?, 'PROCESSING', ?, ?)

            ON CONFLICT (event_id)
            DO UPDATE SET

                status = 'PROCESSING',

                old_customer_key = COALESCE(
                    staging.event_processing.old_customer_key,
                    EXCLUDED.old_customer_key
                ),

                new_customer_key = COALESCE(
                    staging.event_processing.new_customer_key,
                    EXCLUDED.new_customer_key
                ),

                attempt_count =
                    staging.event_processing.attempt_count + 1,

                updated_at_utc = CURRENT_TIMESTAMP,
                completed_at_utc = NULL,
                error_message = NULL

            WHERE staging.event_processing.status <> 'APPLIED'

              AND (
                    staging.event_processing.old_customer_key IS NULL
                    OR staging.event_processing.old_customer_key =
                       EXCLUDED.old_customer_key
              )

              AND (
                    staging.event_processing.new_customer_key IS NULL
                    OR staging.event_processing.new_customer_key =
                       EXCLUDED.new_customer_key
              )
        """)

        stmt.setLong(1, event_id)
        stmt.setInt(2, customer_id)
        stmt.setLong(3, old_key)
        stmt.setLong(4, new_key)

        updated = stmt.executeUpdate()
        stmt.close()

        if updated != 1:
            raise RuntimeError(
                "Could not reserve event: ledger is APPLIED "
                "or reserved keys do not match."
            )

    finally:
        conn.close()


def mark_applied(event_id, old_key, new_key):

    conn = pg_connection()

    try:
        stmt = conn.prepareStatement("""
            UPDATE staging.event_processing

            SET
                status = 'APPLIED',
                old_customer_key = ?,
                new_customer_key = ?,
                updated_at_utc = CURRENT_TIMESTAMP,
                completed_at_utc = CURRENT_TIMESTAMP,
                error_message = NULL

            WHERE event_id = ?
        """)

        stmt.setLong(1, old_key)
        stmt.setLong(2, new_key)
        stmt.setLong(3, event_id)

        updated = stmt.executeUpdate()
        stmt.close()

        if updated != 1:
            raise RuntimeError(
                "Could not mark event as APPLIED."
            )

    finally:
        conn.close()


def mark_failed(event_id, error):

    conn = pg_connection()

    try:
        stmt = conn.prepareStatement("""
            UPDATE staging.event_processing

            SET
                status = 'FAILED',
                updated_at_utc = CURRENT_TIMESTAMP,
                completed_at_utc = NULL,
                error_message = ?

            WHERE event_id = ?
              AND status <> 'APPLIED'
        """)

        stmt.setString(1, str(error)[:2000])
        stmt.setLong(2, event_id)

        stmt.executeUpdate()
        stmt.close()

    finally:
        conn.close()


# ============================================================
# 5. VALIDATION HELPERS
# ============================================================

def parse_json(value):

    if isinstance(value, dict):
        return value

    return json.loads(value)


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


def validate_image(image, label):

    missing = [
        field
        for field in DIM_FIELDS
        if field not in image
    ]

    if missing:
        raise RuntimeError(
            f"{label} is missing fields: {missing}"
        )


def validate_before_image(before, dimension):

    mismatches = [
        field
        for field in DIM_FIELDS
        if not same_value(
            before[field],
            dimension[field]
        )
    ]

    if mismatches:
        raise RuntimeError(
            f"Before image mismatch: {mismatches}"
        )


def validate_after_image(after, dimension):

    mismatches = [
        field
        for field in DIM_FIELDS
        if not same_value(
            after[field],
            dimension[field]
        )
    ]

    if mismatches:
        raise RuntimeError(
            f"After image mismatch: {mismatches}"
        )


# ============================================================
# 6. CLICKHOUSE READ HELPERS
# ============================================================

DIMENSION_SELECT = """
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
    ValidTo,
    IsCurrent,
    RowHash,
    SourceEventId
"""


def read_current(customer_id):

    return clickhouse(f"""
        SELECT {DIMENSION_SELECT}
        FROM ChinookRecoveryLab.DimCustomer
        WHERE CustomerId = {customer_id}
          AND IsCurrent = 1
    """, rows=True)


def read_by_key(customer_id, customer_key):

    return clickhouse(f"""
        SELECT {DIMENSION_SELECT}
        FROM ChinookRecoveryLab.DimCustomer
        WHERE CustomerId = {customer_id}
          AND CustomerKey = {customer_key}
    """, rows=True)


def read_event_version(event_id):

    return clickhouse(f"""
        SELECT {DIMENSION_SELECT}
        FROM ChinookRecoveryLab.DimCustomer
        WHERE SourceEventId = {event_id}
    """, rows=True)


def read_expired_version_by_time(customer_id, changed_at):

    return clickhouse(f"""
        SELECT {DIMENSION_SELECT}
        FROM ChinookRecoveryLab.DimCustomer
        WHERE CustomerId = {customer_id}
          AND IsCurrent = 0
          AND ValidTo = toDateTime64(
              '{changed_at}', 3, 'UTC'
          )
    """, rows=True)


# ============================================================
# 7. ROW HASH
# ============================================================

def calculate_row_hash(after):

    encoded = base64.b64encode(
        json.dumps(
            after,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")
    ).decode()

    result = clickhouse(f"""
        WITH base64Decode('{encoded}') AS j

        SELECT lower(
            hex(
                SHA256(
                    toJSONString(
                        tuple(
                            JSONExtractString(j, 'FirstName'),
                            JSONExtractString(j, 'LastName'),
                            JSONExtract(j, 'Company', 'Nullable(String)'),
                            JSONExtract(j, 'Address', 'Nullable(String)'),
                            JSONExtract(j, 'City', 'Nullable(String)'),
                            JSONExtract(j, 'State', 'Nullable(String)'),
                            JSONExtract(j, 'Country', 'Nullable(String)'),
                            JSONExtract(j, 'PostalCode', 'Nullable(String)'),
                            JSONExtractString(j, 'Email'),
                            JSONExtract(j, 'SupportRepId', 'Nullable(UInt32)')
                        )
                    )
                )
            )
        ) AS RowHash
    """, rows=True)

    if len(result) != 1:
        raise RuntimeError("Could not calculate RowHash.")

    return result[0]["RowHash"]


# ============================================================
# 8. EVENT MARKER VALIDATION AND RECOVERY
# ============================================================

def validate_event_marker(
    marker,
    event_id,
    customer_id,
    event_time,
    changed_at,
    before,
    after,
    expected_hash,
    ledger,
    require_current,
):

    marker_event_id = marker["SourceEventId"]

    if marker_event_id is None or int(marker_event_id) != event_id:
        raise RuntimeError(
            "SourceEventId does not match the incoming event."
        )

    if int(marker["CustomerId"]) != customer_id:
        raise RuntimeError(
            "SourceEventId belongs to another customer."
        )

    new_key = int(marker["CustomerKey"])

    if ledger and ledger["new_key"] is not None:
        if int(ledger["new_key"]) != new_key:
            raise RuntimeError(
                "Reserved new CustomerKey does not match ClickHouse."
            )

    validate_after_image(after, marker)

    if parse_datetime(marker["ValidFrom"]) != event_time:
        raise RuntimeError(
            "Inserted version ValidFrom does not match event timestamp."
        )

    if not same_value(marker["RowHash"], expected_hash):
        raise RuntimeError(
            "Inserted version RowHash does not match AfterData."
        )

    marker_valid_to = marker["ValidTo"]
    marker_is_current = int(marker["IsCurrent"])

    current_rows = read_current(customer_id)

    if len(current_rows) != 1:
        raise RuntimeError(
            "Expected exactly one current customer version."
        )

    current_key = int(current_rows[0]["CustomerKey"])
    current_valid_from = parse_datetime(
        current_rows[0]["ValidFrom"]
    )

    if require_current:

        if marker_valid_to is not None or marker_is_current != 1:
            raise RuntimeError(
                "Recovered event marker is not the current version."
            )

        if current_key != new_key:
            raise RuntimeError(
                "Current CustomerKey does not match event marker."
            )

    else:

        # An already-applied event may have become historical
        # after a later event. Its ValidTo can be non-NULL.

        if marker_valid_to is None:

            if marker_is_current != 1:
                raise RuntimeError(
                    "Current marker has an inconsistent IsCurrent flag."
                )

            if current_key != new_key:
                raise RuntimeError(
                    "Current dimension key does not match event marker."
                )

        else:

            if marker_is_current != 0:
                raise RuntimeError(
                    "Historical marker has an inconsistent IsCurrent flag."
                )

            marker_valid_to_time = parse_datetime(marker_valid_to)

            if marker_valid_to_time <= event_time:
                raise RuntimeError(
                    "Historical marker has an invalid ValidTo timestamp."
                )

            if (
                current_key == new_key
                or current_valid_from < marker_valid_to_time
            ):
                raise RuntimeError(
                    "Current version does not follow the historical event marker."
                )

    old_key = (
        ledger["old_key"]
        if ledger and ledger["old_key"] is not None
        else None
    )

    if old_key is not None:

        old_rows = read_by_key(
            customer_id,
            int(old_key)
        )

    else:

        old_rows = read_expired_version_by_time(
            customer_id,
            changed_at
        )

    if len(old_rows) != 1:
        raise RuntimeError(
            "Could not identify exactly one expired old version."
        )

    old = old_rows[0]
    discovered_old_key = int(old["CustomerKey"])

    if old_key is not None and discovered_old_key != int(old_key):
        raise RuntimeError(
            "Old CustomerKey does not match the processing ledger."
        )

    if discovered_old_key == new_key:
        raise RuntimeError(
            "Old and new CustomerKeys cannot be identical."
        )

    if int(old["IsCurrent"]) != 0:
        raise RuntimeError(
            "Old version is not expired."
        )

    if parse_datetime(old["ValidTo"]) != event_time:
        raise RuntimeError(
            "Old version ValidTo does not match event timestamp."
        )

    if event_time <= parse_datetime(old["ValidFrom"]):
        raise RuntimeError(
            "Event timestamp does not follow old version ValidFrom."
        )

    validate_before_image(before, old)

    return discovered_old_key, new_key


# ============================================================
# 9. PROCESS ONE EVENT
# ============================================================

def process_event(event):

    event_id = int(event["event_id"])
    customer_id = int(event["entity_id"])

    changed_at = event["changed_at"]
    event_time = parse_datetime(changed_at)

    operation = event["operation_type"].upper()

    before = parse_json(event["before_data"])
    after = parse_json(event["after_data"])

    validate_image(before, "BeforeData")
    validate_image(after, "AfterData")

    ledger_started = False

    try:

        print(
            f"\nPROCESSING EventId={event_id} "
            f"CustomerId={customer_id}"
        )

        ledger = get_ledger(event_id)
        markers = read_event_version(event_id)

        if len(markers) > 1:
            raise RuntimeError(
                "Duplicate SourceEventId detected in ClickHouse."
            )

        # ----------------------------------------------------
        # CASE 1: Event already marked APPLIED.
        # Its marker may be current or historical.
        # ----------------------------------------------------

        if ledger and ledger["status"] == "APPLIED":

            if len(markers) != 1:
                raise RuntimeError(
                    "Ledger says APPLIED but ClickHouse marker is missing."
                )

            expected_hash = calculate_row_hash(after)

            old_key, new_key = validate_event_marker(
                marker=markers[0],
                event_id=event_id,
                customer_id=customer_id,
                event_time=event_time,
                changed_at=changed_at,
                before=before,
                after=after,
                expected_hash=expected_hash,
                ledger=ledger,
                require_current=False,
            )

            if ledger["old_key"] is not None:
                if int(ledger["old_key"]) != old_key:
                    raise RuntimeError(
                        "Ledger old CustomerKey does not match ClickHouse."
                    )

            print(f"ALREADY_APPLIED EventId={event_id}")
            return

        # ----------------------------------------------------
        # CASE 2: ClickHouse insert succeeded but PostgreSQL
        # ledger was not marked APPLIED.
        # ----------------------------------------------------

        if len(markers) == 1:

            expected_hash = calculate_row_hash(after)

            old_key, new_key = validate_event_marker(
                marker=markers[0],
                event_id=event_id,
                customer_id=customer_id,
                event_time=event_time,
                changed_at=changed_at,
                before=before,
                after=after,
                expected_hash=expected_hash,
                ledger=ledger,
                require_current=True,
            )

            reserve_processing(
                event_id,
                customer_id,
                old_key,
                new_key
            )

            ledger_started = True

            mark_applied(
                event_id,
                old_key,
                new_key
            )

            print(
                f"RECOVERED_FROM_CLICKHOUSE EventId={event_id}"
            )

            return

        # ----------------------------------------------------
        # CASE 3: No marker exists. Prepare a new SCD2 version.
        # ----------------------------------------------------

        if operation != "UPDATE":
            raise RuntimeError(
                f"Unsupported operation: {operation}"
            )

        changed_columns = [
            field
            for field in DIM_FIELDS
            if not same_value(
                before[field],
                after[field]
            )
        ]

        if not changed_columns:
            raise RuntimeError(
                "No Dimension attributes changed."
            )

        # Calculate hash BEFORE modifying ClickHouse.
        row_hash = calculate_row_hash(after)

        current_rows = read_current(customer_id)

        if len(current_rows) > 1:
            raise RuntimeError(
                "Multiple current versions detected."
            )

        if len(current_rows) == 1:

            old = current_rows[0]
            old_key = int(old["CustomerKey"])

            if ledger and ledger["old_key"] is not None:
                if int(ledger["old_key"]) != old_key:
                    raise RuntimeError(
                        "Current CustomerKey differs from reserved key."
                    )

            validate_before_image(before, old)

            if event_time <= parse_datetime(old["ValidFrom"]):
                raise RuntimeError(
                    "Event timestamp is stale."
                )

            # Reuse previously reserved key during recovery.
            if ledger and ledger["new_key"] is not None:

                new_key = int(ledger["new_key"])

            else:

                maximum = clickhouse("""
                    SELECT max(CustomerKey) AS MaxKey
                    FROM ChinookRecoveryLab.DimCustomer
                """, rows=True)

                new_key = int(maximum[0]["MaxKey"]) + 1

            # Reserve keys before modifying ClickHouse.
            reserve_processing(
                event_id,
                customer_id,
                old_key,
                new_key
            )

            ledger_started = True

            # Expire old version.
            clickhouse(f"""
                ALTER TABLE ChinookRecoveryLab.DimCustomer

                UPDATE
                    ValidTo = toDateTime64(
                        '{changed_at}', 3, 'UTC'
                    ),
                    IsCurrent = 0

                WHERE CustomerId = {customer_id}
                  AND CustomerKey = {old_key}
                  AND IsCurrent = 1

                SETTINGS mutations_sync = 1
            """)

            print(
                f"EXPIRED CustomerKey={old_key}"
            )

            if event_id == 900001:
                print(
                    "INJECTED_STOP_AFTER_EXPIRE EventId=900001",
                    flush=True
                )
                raise SystemExit(77)

        else:

            # Recovery after expiration but before insertion.
            if not ledger:
                raise RuntimeError(
                    "No current version and no processing ledger."
                )

            if (
                ledger["old_key"] is None
                or ledger["new_key"] is None
            ):
                raise RuntimeError(
                    "Recovery keys are missing from ledger."
                )

            old_key = int(ledger["old_key"])
            new_key = int(ledger["new_key"])

            old_rows = read_by_key(
                customer_id,
                old_key
            )

            if len(old_rows) != 1:
                raise RuntimeError(
                    "Cannot find the reserved old version."
                )

            old = old_rows[0]

            if int(old["IsCurrent"]) != 0:
                raise RuntimeError(
                    "Expected old version to be expired."
                )

            if parse_datetime(old["ValidTo"]) != event_time:
                raise RuntimeError(
                    "Expired version timestamp mismatch."
                )

            validate_before_image(before, old)

            reserve_processing(
                event_id,
                customer_id,
                old_key,
                new_key
            )

            ledger_started = True

            print(
                f"RECOVERY_MODE OldKey={old_key}"
            )

        # ----------------------------------------------------
        # VERIFY OLD VERSION EXPIRATION
        # ----------------------------------------------------

        expired_rows = read_by_key(
            customer_id,
            old_key
        )

        if len(expired_rows) != 1:
            raise RuntimeError(
                "Old version is missing or duplicated."
            )

        expired = expired_rows[0]

        if int(expired["IsCurrent"]) != 0:
            raise RuntimeError(
                "Old version was not expired."
            )

        if parse_datetime(expired["ValidTo"]) != event_time:
            raise RuntimeError(
                "Old version expiration time is incorrect."
            )

        # ----------------------------------------------------
        # CHECK NEW KEY AVAILABILITY
        # ----------------------------------------------------

        key_check = clickhouse(f"""
            SELECT count() AS n
            FROM ChinookRecoveryLab.DimCustomer
            WHERE CustomerKey = {new_key}
        """, rows=True)

        if int(key_check[0]["n"]) != 0:
            raise RuntimeError(
                f"New CustomerKey {new_key} is already occupied."
            )

        # ----------------------------------------------------
        # PREPARE NEW DIMENSION VERSION
        # ----------------------------------------------------

        new_row = {
            "CustomerKey": new_key,
            "CustomerId": customer_id,

            "FirstName": after["FirstName"],
            "LastName": after["LastName"],
            "Company": after["Company"],
            "Address": after["Address"],
            "City": after["City"],
            "State": after["State"],
            "Country": after["Country"],
            "PostalCode": after["PostalCode"],
            "Email": after["Email"],
            "SupportRepId": after["SupportRepId"],

            "ValidFrom": changed_at,
            "ValidTo": None,
            "IsCurrent": 1,

            "RowHash": row_hash,
            "SourceEventId": event_id,
        }

        # ----------------------------------------------------
        # INSERT NEW VERSION
        # ----------------------------------------------------

        clickhouse_insert(new_row)

        print(
            f"INSERTED CustomerKey={new_key}"
        )

        # ----------------------------------------------------
        # VERIFY INSERTED VERSION AND RECOVERY MARKER
        # ----------------------------------------------------

        markers = read_event_version(event_id)

        if len(markers) != 1:
            raise RuntimeError(
                "New version was not inserted exactly once."
            )

        verified_old_key, verified_new_key = validate_event_marker(
            marker=markers[0],
            event_id=event_id,
            customer_id=customer_id,
            event_time=event_time,
            changed_at=changed_at,
            before=before,
            after=after,
            expected_hash=row_hash,
            ledger=get_ledger(event_id),
            require_current=True,
        )

        if (
            verified_old_key != old_key
            or verified_new_key != new_key
        ):
            raise RuntimeError(
                "Inserted version keys do not match reserved keys."
            )

        # ----------------------------------------------------
        # COMPLETE PROCESSING LEDGER
        # ----------------------------------------------------

        mark_applied(
            event_id,
            old_key,
            new_key
        )

        print(
            f"POSTGRES_LEDGER_APPLIED EventId={event_id}"
        )

        print(
            f"SCD2_EVENT_COMPLETED EventId={event_id}"
        )

    except Exception as exc:

        if ledger_started:

            try:
                mark_failed(event_id, str(exc))

                print(
                    f"POSTGRES_LEDGER_FAILED EventId={event_id}"
                )

            except Exception as ledger_error:

                print(
                    "WARNING: Could not update failure status:",
                    str(ledger_error)
                )

        raise


# ============================================================
# 10. MAIN
# ============================================================

try:

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
                    source_created_at_utc
                FROM staging.customer_change_events
                WHERE entity_name = 'Customer'
                ORDER BY source_created_at_utc, event_id
            ) AS events
            """
        )
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .withColumn(
            "changed_at",
            F.date_format(
                "source_created_at_utc",
                "yyyy-MM-dd HH:mm:ss.SSS"
            )
        )
    )

    events = [
        row.asDict(recursive=True)
        for row in events_df.orderBy("source_created_at_utc", "event_id").collect()
    ]

    print("Total Customer Events:", len(events))

    for event in events:
        process_event(event)

    # ========================================================
    # GLOBAL DIMCUSTOMER VALIDATION
    # ========================================================

    print("\n=== GLOBAL DIMCUSTOMER VALIDATION ===")

    summary = clickhouse("""
        SELECT
            count() AS TotalVersions,
            uniqExact(CustomerKey) AS UniqueKeys,
            uniqExact(CustomerId) AS TotalCustomers,
            countIf(IsCurrent = 1) AS CurrentRows,
            uniqExactIf(CustomerId, IsCurrent = 1) AS CurrentCustomers,
            countIf(IsCurrent = 1 AND ValidTo IS NOT NULL) AS BadCurrent,
            countIf(IsCurrent = 0 AND ValidTo IS NULL) AS BadHistorical
        FROM ChinookRecoveryLab.DimCustomer
    """, rows=True)[0]

    print("Dimension Summary:", summary)

    if int(summary["TotalVersions"]) != int(summary["UniqueKeys"]):
        raise RuntimeError("Duplicate CustomerKey detected.")

    if (
        int(summary["CurrentRows"]) != int(summary["TotalCustomers"])
        or int(summary["CurrentCustomers"]) != int(summary["TotalCustomers"])
    ):
        raise RuntimeError("Every customer must have exactly one current row.")

    if int(summary["BadCurrent"]) != 0:
        raise RuntimeError("Current rows must have NULL ValidTo.")

    if int(summary["BadHistorical"]) != 0:
        raise RuntimeError("Historical rows must have a ValidTo.")

    print("GLOBAL_DIMCUSTOMER_VALID")

    # ========================================================
    # EVENT-LEVEL VALIDATION
    # ========================================================

    print("\n=== EVENT-LEVEL VALIDATION ===")

    for event in events:

        event_id = int(event["event_id"])
        customer_id = int(event["entity_id"])
        changed_at = event["changed_at"]

        markers = read_event_version(event_id)
        ledger = get_ledger(event_id)

        if len(markers) != 1:
            raise RuntimeError(
                f"Event {event_id}: expected exactly one ClickHouse marker."
            )

        if not ledger or ledger["status"] != "APPLIED":
            raise RuntimeError(
                f"Event {event_id}: PostgreSQL ledger is not APPLIED."
            )

        old_key, new_key = validate_event_marker(
            marker=markers[0],
            event_id=event_id,
            customer_id=customer_id,
            event_time=parse_datetime(changed_at),
            changed_at=changed_at,
            before=parse_json(event["before_data"]),
            after=parse_json(event["after_data"]),
            expected_hash=calculate_row_hash(
                parse_json(event["after_data"])
            ),
            ledger=ledger,
            require_current=False,
        )

        if (
            ledger["old_key"] is None
            or int(ledger["old_key"]) != old_key
            or ledger["new_key"] is None
            or int(ledger["new_key"]) != new_key
        ):
            raise RuntimeError(
                f"Event {event_id}: ledger keys do not match ClickHouse."
            )

        print(
            f"EVENT_VERIFIED EventId={event_id} "
            f"OldKey={old_key} NewKey={new_key}"
        )

    # ========================================================
    # FACTSALES MONITOR
    # ========================================================

    sales = clickhouse("""
        SELECT
            count() AS TotalSales,
            sum(SalesAmount) AS TotalRevenue
        FROM ChinookDW.FactSales
    """, rows=True)[0]

    print("FactSales Monitor:", sales)
    print("\nSCD2_POSTGRES_APPLY_COMPLETED")

finally:

    spark.stop()
