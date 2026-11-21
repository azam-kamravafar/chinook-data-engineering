"""Chinook SCD Type 2 lab: apply one pending customer-country event.

Requires Spark 4.0.x, ClickHouse HTTP access, and CH_USER / CH_PASSWORD.
Single-worker teaching version: no concurrent writer or event sequencing guarantees.
"""

import base64
import json
import os
import urllib.error
import urllib.request
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

CH_URL = "http://chinook-clickhouse:8123/"


def clickhouse(sql, rows=False):
    """Run ClickHouse SQL using HTTP POST. Never print credentials."""
    user = os.environ["CH_USER"]
    password = os.environ["CH_PASSWORD"]
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    statement = sql.strip().rstrip(";")
    if rows:
        statement += "\nFORMAT JSONEachRow"
    request = urllib.request.Request(
        CH_URL,
        data=statement.encode("utf-8"),
        headers={"Authorization": f"Basic {token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse query failed: {detail}") from exc
    return [json.loads(line) for line in body.splitlines() if line] if rows else None


def require_one(sql, label):
    result = clickhouse(sql, rows=True)
    if len(result) != 1:
        raise RuntimeError(f"Expected one row for {label}; got {len(result)}")
    return result[0]


spark = (SparkSession.builder
         .appName("Chinook-SCD2-Apply-Lab")
         .master("local[2]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")

try:
    current_rows = clickhouse("""
        SELECT CustomerId, CustomerKey, Country, ValidFrom
        FROM ChinookDW.DimCustomer
        WHERE IsCurrent = 1
    """, rows=True)
    event_rows = clickhouse("""
        SELECT CustomerId, NewCountry, ChangedAt
        FROM ChinookDW.StgCustomerChange
    """, rows=True)
    if not current_rows or not event_rows:
        raise RuntimeError("Current customers or staged events are missing")

    current = spark.createDataFrame(
        current_rows,
        "CustomerId INT, CustomerKey LONG, Country STRING, ValidFrom STRING",
    )
    events = spark.createDataFrame(
        event_rows,
        "CustomerId INT, NewCountry STRING, ChangedAt STRING",
    )

    if current.groupBy("CustomerId").count().filter("count > 1").count():
        raise RuntimeError("More than one current version for a customer")
    if events.groupBy("CustomerId", "ChangedAt").count().filter("count > 1").count():
        raise RuntimeError("Two events share a customer and timestamp")

    order = Window.partitionBy("CustomerId").orderBy(F.col("ChangedAt").desc())
    latest = (events.withColumn("rn", F.row_number().over(order))
             .filter(F.col("rn") == 1).drop("rn"))
    plan = (latest.alias("e")
            .join(current.alias("c"), F.col("e.CustomerId") == F.col("c.CustomerId"), "left")
            .select(
                F.col("e.CustomerId").alias("CustomerId"),
                F.col("e.NewCountry").alias("NewCountry"),
                F.col("e.ChangedAt").alias("ChangedAt"),
                F.col("c.CustomerKey").alias("OldKey"),
                F.col("c.Country").alias("OldCountry"),
                F.col("c.ValidFrom").alias("OldValidFrom"),
            )
            .withColumn(
                "Action",
                F.when(F.col("OldKey").isNull(), "NO_CURRENT")
                 .when(F.col("OldCountry").eqNullSafe(F.col("NewCountry")), "ALREADY_APPLIED")
                 .when(F.to_timestamp("ChangedAt") <= F.to_timestamp("OldValidFrom"), "STALE_EVENT")
                 .otherwise("UPDATE_REQUIRED"),
            ))

    print("\n=== SCD2 plan ===")
    plan.orderBy("CustomerId").show(truncate=False)
    tasks = [row.asDict() for row in plan.collect()
             if row["Action"] != "ALREADY_APPLIED"]
    if not tasks:
        print("Nothing to apply: all latest events already match current versions.")
    else:
        if len(tasks) != 1:
            raise RuntimeError("Lab script supports exactly one pending event per run")
        task = tasks[0]
        customer_id = int(task["CustomerId"])
        changed_at = str(task["ChangedAt"])
        datetime.strptime(changed_at, "%Y-%m-%d %H:%M:%S.%f")
        if task["Action"] == "STALE_EVENT":
            raise RuntimeError("Event timestamp predates current dimension version")

        # If the prior run closed the old record but failed before inserting
        # the new one, recover from the matching closed record.
        if task["Action"] == "NO_CURRENT":
            recovery = clickhouse(f"""
                SELECT CustomerKey, Country
                FROM ChinookDW.DimCustomer
                WHERE CustomerId = {customer_id}
                  AND IsCurrent = 0
                  AND ValidTo = toDateTime64('{changed_at}', 3, 'UTC')
                ORDER BY CustomerKey DESC LIMIT 2
            """, rows=True)
            if len(recovery) != 1:
                raise RuntimeError("No current version; cannot safely recover. Inspect manually.")
            old_key = int(recovery[0]["CustomerKey"])
            print(f"Recovery mode: former CustomerKey={old_key} is already expired")
        elif task["Action"] == "UPDATE_REQUIRED":
            old_key = int(task["OldKey"])
            clickhouse(f"""
                ALTER TABLE ChinookDW.DimCustomer
                UPDATE
                    ValidTo = toDateTime64('{changed_at}', 3, 'UTC'),
                    IsCurrent = 0
                WHERE CustomerId = {customer_id}
                  AND CustomerKey = {old_key}
                  AND IsCurrent = 1
                SETTINGS mutations_sync = 1
            """)
            print(f"Expired old version: CustomerKey={old_key}")
        else:
            raise RuntimeError(f"Unsupported action: {task['Action']}")

        expired = require_one(f"""
            SELECT count() AS n
            FROM ChinookDW.DimCustomer
            WHERE CustomerId = {customer_id} AND CustomerKey = {old_key}
              AND IsCurrent = 0
              AND ValidTo = toDateTime64('{changed_at}', 3, 'UTC')
        """, "expired version")
        if int(expired["n"]) != 1:
            raise RuntimeError("Old version was not expired exactly once; stopped")

        new_key = int(require_one("""
            SELECT max(CustomerKey) AS MaxKey FROM ChinookDW.DimCustomer
        """, "key allocator")["MaxKey"]) + 1

        # The source row supplies unchanged attributes, while the staging
        # event supplies the new country and its timestamp. The hash uses
        # the SAME ClickHouse expression as the initial dimension load.
        clickhouse(f"""
            INSERT INTO ChinookDW.DimCustomer
            (
                CustomerKey, CustomerId, FirstName, LastName,
                Company, Address, City, State, Country, PostalCode,
                Email, SupportRepId, ValidFrom, ValidTo, IsCurrent, RowHash
            )
            SELECT
                toUInt64({new_key}), c.CustomerId, c.FirstName, c.LastName,
                c.Company, c.Address, c.City, c.State, e.NewCountry,
                c.PostalCode, c.Email, c.SupportRepId,
                e.ChangedAt, NULL, toUInt8(1),
                lower(hex(SHA256(toJSONString(tuple(
                    c.FirstName, c.LastName, c.Company, c.Address,
                    c.City, c.State, e.NewCountry, c.PostalCode,
                    c.Email, c.SupportRepId
                )))))
            FROM ChinookDW.DimCustomer AS c
            INNER JOIN ChinookDW.StgCustomerChange AS e
                ON c.CustomerId = e.CustomerId
            WHERE c.CustomerId = {customer_id}
              AND c.CustomerKey = {old_key}
              AND c.IsCurrent = 0
              AND c.ValidTo = e.ChangedAt
              AND e.ChangedAt = toDateTime64('{changed_at}', 3, 'UTC')
              AND NOT EXISTS (
                  SELECT 1 FROM ChinookDW.DimCustomer
                  WHERE CustomerId = {customer_id}
                    AND ValidFrom = toDateTime64('{changed_at}', 3, 'UTC')
              )
        """)
        print(f"Inserted new version: CustomerKey={new_key}")

    print("\n=== Validation ===")
    for row in clickhouse("""
        SELECT CustomerKey, CustomerId, Country, ValidFrom, ValidTo, IsCurrent
        FROM ChinookDW.DimCustomer WHERE CustomerId = 5
        ORDER BY CustomerKey
    """, rows=True):
        print(row)
    audit = require_one("""
        SELECT count() AS TotalVersions,
               countIf(IsCurrent = 1) AS CurrentVersions,
               uniqExact(CustomerKey) AS UniqueKeys
        FROM ChinookDW.DimCustomer WHERE CustomerId = 5
    """, "SCD audit")
    sales = require_one("""
        SELECT count() AS TotalSales, sum(SalesAmount) AS TotalRevenue
        FROM ChinookDW.FactSales
    """, "sales audit")
    print("Customer 5 audit:", audit)
    print("FactSales audit:", sales)
    if int(audit["CurrentVersions"]) != 1 or int(audit["TotalVersions"]) != int(audit["UniqueKeys"]):
        raise RuntimeError("SCD version validation failed")
    if int(sales["TotalSales"]) != 2241 or str(sales["TotalRevenue"]) != "2329.59":
        raise RuntimeError("FactSales reconciliation failed")
    print("SCD2 LAB APPLY COMPLETED")
finally:
    spark.stop()
