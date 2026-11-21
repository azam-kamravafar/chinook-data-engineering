from datetime import datetime, timezone

from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator


@dag(
    dag_id="chinook_docker_smoke_test",
    description="Test DockerOperator and Chinook Docker network",
    start_date=datetime(2026, 11, 21, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["chinook", "docker", "smoke-test"],
)
def chinook_docker_smoke_test():

    DockerOperator(
        task_id="spark_container_network_check",

        image="spark:4.0.1-python3",

        entrypoint=["python3"],

        command=[
            "-c",
            """
import socket

services = [
    ("chinook-postgres", 5432),
    ("chinook-kafka", 9092),
    ("chinook-clickhouse", 8123),
]

for host, port in services:
    with socket.create_connection((host, port), timeout=10):
        print(f"CONNECTED {host}:{port}", flush=True)

print("DOCKER_OPERATOR_SMOKE_OK", flush=True)
"""
        ],

        docker_url="unix:///var/run/docker.sock",

        network_mode="chinook-net",

        mount_tmp_dir=False,

        auto_remove="success",

        force_pull=False,

        do_xcom_push=False,
    )


chinook_docker_smoke_test()
