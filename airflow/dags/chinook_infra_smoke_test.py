from datetime import datetime, timezone
import socket

from airflow.sdk import dag, task


@dag(
    dag_id="chinook_infra_smoke_test",
    description="Check Chinook infrastructure connectivity",
    start_date=datetime(2026, 11, 21, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["chinook", "infrastructure", "smoke-test"],
)
def chinook_infra_smoke_test():

    def check_connection(host, port, service):
        with socket.create_connection((host, port), timeout=10):
            print(f"{service}_TCP_CONNECTION_OK")
        return f"{service}: OK"

    @task
    def check_postgresql():
        return check_connection(
            "chinook-postgres", 5432, "POSTGRESQL"
        )

    @task
    def check_kafka():
        return check_connection(
            "chinook-kafka", 9092, "KAFKA"
        )

    @task
    def check_clickhouse():
        return check_connection(
            "chinook-clickhouse", 8123, "CLICKHOUSE"
        )

    @task
    def report_results(postgres, kafka, clickhouse):
        print("=== CHINOOK INFRASTRUCTURE ===")
        print(postgres)
        print(kafka)
        print(clickhouse)
        print("CHINOOK_SMOKE_TEST_COMPLETED")

    report_results(
        check_postgresql(),
        check_kafka(),
        check_clickhouse(),
    )


chinook_infra_smoke_test()
