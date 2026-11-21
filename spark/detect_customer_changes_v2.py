
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .appName("Chinook-Change-Detection-V2")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

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

# Read original customers
old_customers = (
    spark.read
    .option("header", "false")
    .option("nullValue", "NULL")
    .schema(schema)
    .csv("/data/customer_snapshot.csv")
)

# Simulate an UPDATE
updated = old_customers.withColumn(
    "Country",
    F.when(
        F.col("CustomerId") == "5",
        F.lit("SCD_TEST_COUNTRY")
    ).otherwise(F.col("Country"))
)

# Simulate an INSERT
new_customer = (
    old_customers
    .filter(F.col("CustomerId") == "5")
    .withColumn("CustomerId", F.lit("9999"))
    .withColumn("FirstName", F.lit("Test"))
    .withColumn("LastName", F.lit("Customer"))
    .withColumn("Email", F.lit("test9999@example.invalid"))
)

new_customers = updated.unionByName(new_customer)

# Generate hashes from descriptive attributes
attributes = [c for c in columns if c != "CustomerId"]

def add_hash(df):
    return df.withColumn(
        "RowHash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(c) for c in attributes]),
                options={"ignoreNullFields": "false"}
            ),
            256
        )
    )

old_df = add_hash(old_customers).alias("old")
new_df = add_hash(new_customers).alias("new")

# Compare both snapshots
joined = old_df.join(
    new_df,
    F.col("old.CustomerId") == F.col("new.CustomerId"),
    "full_outer"
)

changes = joined.select(
    F.coalesce(
        F.col("old.CustomerId"),
        F.col("new.CustomerId")
    ).alias("CustomerId"),

    F.when(
        F.col("old.CustomerId").isNull(), "INSERT"
    ).when(
        F.col("new.CustomerId").isNull(), "DELETE"
    ).when(
        F.col("old.RowHash") != F.col("new.RowHash"), "UPDATE"
    ).otherwise("UNCHANGED").alias("ChangeType"),

    F.col("old.Country").alias("OldCountry"),
    F.col("new.Country").alias("NewCountry")
)

result = changes.filter(
    F.col("ChangeType") != "UNCHANGED"
)

print("Old customers:", old_customers.count())
print("New customers:", new_customers.count())

result.orderBy("CustomerId").show(truncate=False)

spark.stop()
