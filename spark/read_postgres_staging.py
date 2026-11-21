
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

POSTGRES_HOST = "chinook-postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "chinook_staging"

POSTGRES_USER = "chinook_user"
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

if not POSTGRES_PASSWORD:
    raise RuntimeError("POSTGRES_PASSWORD environment variable is missing.")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/{POSTGRES_DB}"
)


# --------------------------------------------------
# 2. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Chinook-PostgreSQL-Staging-Reader")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


try:
    # --------------------------------------------------
    # 3. Read PostgreSQL Staging
    # --------------------------------------------------

    query = """
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
            kafka_offset,
            ingested_at_utc
        FROM staging.customer_change_events
    ) AS events
    """

    print("\n=== Reading PostgreSQL Staging ===")

    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", query)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # --------------------------------------------------
    # 4. Basic Validation
    # --------------------------------------------------

    total_events = df.count()

    print(f"\nTotal Events: {total_events}")

    print("\n=== Data Schema ===")
    df.printSchema()

    # --------------------------------------------------
    # 5. Operation Summary
    # --------------------------------------------------

    print("\n=== Events By Operation ===")

    (
        df.groupBy("operation_type")
        .count()
        .orderBy("operation_type")
        .show(truncate=False)
    )

    # --------------------------------------------------
    # 6. Extract Customer Changes From JSON
    # --------------------------------------------------

    result = (
        df
        .withColumn(
            "old_country",
            F.get_json_object("before_data", "$.Country")
        )
        .withColumn(
            "new_country",
            F.get_json_object("after_data", "$.Country")
        )
    )

    print("\n=== Customer Change Events ===")

    (
        result.select(
            "event_id",
            "entity_id",
            "operation_type",
            "old_country",
            "new_country",
            "kafka_partition",
            "kafka_offset"
        )
        .show(truncate=False)
    )

    print("\n=== SPARK POSTGRESQL READ COMPLETED ===")


finally:
    spark.stop()
