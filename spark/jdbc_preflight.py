import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Chinook-JDBC-Preflight")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    driver = spark._jvm.java.lang.Class.forName(
        "org.postgresql.Driver"
    )
    print("JDBC_DRIVER_LOADED:", driver.getName())

    conn = spark._jvm.java.sql.DriverManager.getConnection(
        "jdbc:postgresql://chinook-postgres:5432/chinook_staging",
        "chinook_user",
        os.environ["POSTGRES_PASSWORD"]
    )

    try:
        stmt = conn.createStatement()
        rs = stmt.executeQuery(
            "SELECT count(*) FROM staging.event_processing WHERE event_id = 1"
        )
        rs.next()
        print("EVENT1_LEDGER_ROWS:", rs.getLong(1))
        rs.close()
        stmt.close()
    finally:
        conn.close()

    print("JDBC_PREFLIGHT_OK")

finally:
    spark.stop()
