from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType

# 1. Start Spark
spark = (
    SparkSession.builder
    .appName("Chinook-Customer-Change-Detection")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. Define source schema
columns = [
    "CustomerId", "FirstName", "LastName",
    "Company", "Address", "City",
    "State", "Country", "PostalCode",
    "Email", "SupportRepId"
]

schema = StructType([
    StructField(name, StringType(), True)
    for name in columns
])

# 3. Read customer snapshot
customers = (
    spark.read
    .option("header", "false")
    .option("nullValue", "NULL")
    .schema(schema)
    .csv("/data/customer_snapshot.csv")
)

# 4. Simulate a customer update
updated_customers = customers.withColumn(
    "Country",
    when(
        col("CustomerId") == "5",
        "SCD_TEST_COUNTRY"
    ).otherwise(col("Country"))
)

# 5. Detect changes
changes = (
    customers.alias("old")
    .join(
        updated_customers.alias("new"),
        on="CustomerId",
        how="inner"
    )
    .filter(
        ~col("old.Country").eqNullSafe(col("new.Country"))
    )
    .select(
        "CustomerId",
        col("old.Country").alias("OldCountry"),
        col("new.Country").alias("NewCountry")
    )
)

print("Total customers:", customers.count())
print("Changed customers:", changes.count())

changes.show(truncate=False)

spark.stop()
