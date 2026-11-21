
import os
import json
import base64
import urllib.request

from pyspark.sql import SparkSession

# Step 1: Start Spark
spark = (
    SparkSession.builder
    .appName("Chinook-Read-DimCustomer")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Step 2: Query ClickHouse
query = """
SELECT
    CustomerKey,
    CustomerId,
    Country,
    IsCurrent
FROM ChinookDW.DimCustomer
FORMAT JSONEachRow
"""

# Step 3: Authenticate
username = os.environ["CH_USER"]
password = os.environ["CH_PASSWORD"]

credentials = base64.b64encode(
    f"{username}:{password}".encode()
).decode()

request = urllib.request.Request(
    "http://chinook-clickhouse:8123/",
    data=query.encode(),
    headers={"Authorization": f"Basic {credentials}"}
)

# Step 4: Read data
with urllib.request.urlopen(request, timeout=30) as response:
    lines = response.read().decode().splitlines()

rows = [json.loads(line) for line in lines if line]

# Step 5: Create Spark DataFrame
schema = """
    CustomerKey BIGINT,
    CustomerId INT,
    Country STRING,
    IsCurrent INT
"""

df = spark.createDataFrame(rows, schema=schema)

# Step 6: Validate results
print("Total dimension rows:", df.count())
print("Unique customers:", df.select("CustomerId").distinct().count())

df.filter(df.CustomerId == 5).orderBy("CustomerKey").show(
    truncate=False
)

spark.stop()
