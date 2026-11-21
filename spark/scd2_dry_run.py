
import os
import json
import base64
import urllib.request

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# Start Spark
spark = (
    SparkSession.builder
    .appName("Chinook-SCD2-Dry-Run")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")


# Read data from ClickHouse
def read_clickhouse(sql):
    username = os.environ["CH_USER"]
    password = os.environ["CH_PASSWORD"]

    credentials = base64.b64encode(
        f"{username}:{password}".encode()
    ).decode()

    request = urllib.request.Request(
        "http://chinook-clickhouse:8123/",
        data=(sql + "\nFORMAT JSONEachRow").encode(),
        headers={"Authorization": f"Basic {credentials}"}
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        return [
            json.loads(line)
            for line in response.read().decode().splitlines()
            if line
        ]


try:
    # 1. Read current customer versions
    current_rows = read_clickhouse("""
        SELECT
            CustomerId,
            CustomerKey,
            Country,
            ValidFrom
        FROM ChinookDW.DimCustomer
        WHERE IsCurrent = 1
    """)

    current = spark.createDataFrame(
        current_rows,
        schema="""
            CustomerId INT,
            CustomerKey LONG,
            Country STRING,
            ValidFrom STRING
        """
    )

    # 2. Read staged events
    event_rows = read_clickhouse("""
        SELECT CustomerId, NewCountry, ChangedAt
        FROM ChinookDW.StgCustomerChange
    """)

    events = spark.createDataFrame(
        event_rows,
        schema="""
            CustomerId INT,
            NewCountry STRING,
            ChangedAt STRING
        """
    )

    # 3. Validate data before processing
    duplicate_current = (
        current.groupBy("CustomerId")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_current > 0:
        raise RuntimeError("Multiple current versions detected")

    duplicate_times = (
        events.groupBy("CustomerId", "ChangedAt")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_times > 0:
        raise RuntimeError("Ambiguous event ordering detected")

    # 4. Select latest event per customer
    window = (
        Window.partitionBy("CustomerId")
        .orderBy(F.col("ChangedAt").desc())
    )

    latest = (
        events
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # 5. Compare with current dimension
    comparison = (
        latest.alias("e")
        .join(
            current.alias("c"),
            F.col("e.CustomerId") == F.col("c.CustomerId"),
            "left"
        )
        .select(
            F.col("e.CustomerId").alias("CustomerId"),
            F.col("c.CustomerKey").alias("OldCustomerKey"),
            F.col("c.Country").alias("OldCountry"),
            F.col("e.NewCountry").alias("NewCountry"),
            F.col("c.ValidFrom").alias("CurrentValidFrom"),
            F.col("e.ChangedAt").alias("ChangedAt")
        )
        .withColumn(
            "Action",
            F.when(
                F.col("OldCustomerKey").isNull(),
                "NEW_CUSTOMER"
            )
            .when(
                F.col("OldCountry").eqNullSafe(F.col("NewCountry")),
                "ALREADY_APPLIED"
            )
            .when(
                F.to_timestamp("ChangedAt")
                <= F.to_timestamp("CurrentValidFrom"),
                "INVALID_EVENT_TIME"
            )
            .otherwise("UPDATE_REQUIRED")
        )
    )

    # 6. Find the next available key
    max_key = int(read_clickhouse("""
        SELECT max(CustomerKey) AS MaxKey
        FROM ChinookDW.DimCustomer
    """)[0]["MaxKey"])

    update_count = comparison.filter(
        F.col("Action") == "UPDATE_REQUIRED"
    ).count()

    # This first test supports one update at a time.
    if update_count > 1:
        raise RuntimeError("Multi-customer loading is not enabled yet")

    plan = comparison.withColumn(
        "NewCustomerKey",
        F.when(
            F.col("Action") == "UPDATE_REQUIRED",
            F.lit(max_key + 1)
        )
    )

    print("\n=== SCD TYPE 2 DRY RUN ===")
    print("Current customer versions:", current.count())
    print("Incoming events:", events.count())
    print("Updates required:", update_count)

    plan.select(
        "CustomerId",
        "OldCustomerKey",
        "NewCustomerKey",
        "OldCountry",
        "NewCountry",
        "ChangedAt",
        "Action"
    ).show(truncate=False)

    print("DRY RUN COMPLETED - No database changes made")

finally:
    spark.stop()
