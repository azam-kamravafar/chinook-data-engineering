
import os
import json
import base64
import urllib.request

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# 1. Start Spark
spark = (
    SparkSession.builder
    .appName("Chinook-SCD-Latest-Changes")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# 2. Read data from ClickHouse
def read_clickhouse(sql):
    username = os.environ["CH_USER"]
    password = os.environ["CH_PASSWORD"]

    credentials = base64.b64encode(
        f"{username}:{password}".encode()
    ).decode()

    request = urllib.request.Request(
        "http://chinook-clickhouse:8123/",
        data=(sql + "\nFORMAT JSONEachRow").encode(),
        headers={
            "Authorization": f"Basic {credentials}"
        }
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        return [
            json.loads(line)
            for line in response.read().decode().splitlines()
            if line
        ]


# 3. Read current customer versions
current_rows = read_clickhouse("""
SELECT CustomerId, Country
FROM ChinookDW.DimCustomer
WHERE IsCurrent = 1
""")

current = spark.createDataFrame(
    current_rows,
    schema="CustomerId INT, Country STRING"
)


# 4. Read all staged changes
change_rows = read_clickhouse("""
SELECT CustomerId, NewCountry, ChangedAt
FROM ChinookDW.StgCustomerChange
""")

changes = spark.createDataFrame(
    change_rows,
    schema="CustomerId INT, NewCountry STRING, ChangedAt STRING"
)


# 5. Select the latest event per customer
window = (
    Window
    .partitionBy("CustomerId")
    .orderBy(F.col("ChangedAt").desc())
)

latest_changes = (
    changes
    .withColumn("RowNumber", F.row_number().over(window))
    .filter(F.col("RowNumber") == 1)
    .drop("RowNumber")
)


# 6. Compare latest changes with current dimension
result = (
    latest_changes.alias("e")
    .join(
        current.alias("c"),
        F.col("e.CustomerId") == F.col("c.CustomerId"),
        "left"
    )
    .select(
        F.col("e.CustomerId").alias("CustomerId"),
        F.col("c.Country").alias("CurrentCountry"),
        F.col("e.NewCountry").alias("EventCountry"),
        F.col("e.ChangedAt").alias("ChangedAt"),

        F.when(
            F.col("c.CustomerId").isNull(),
            F.lit("NEW_CUSTOMER")
        )
        .when(
            F.col("c.Country").eqNullSafe(F.col("e.NewCountry")),
            F.lit("ALREADY_APPLIED")
        )
        .otherwise(
            F.lit("UPDATE_REQUIRED")
        )
        .alias("Action")
    )
)

print("Current customers:", current.count())
print("Total staged events:", changes.count())
print("Latest events:", latest_changes.count())

result.show(truncate=False)

spark.stop()
