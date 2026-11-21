from datetime import datetime, timezone

from airflow.sdk import dag, task, TriggerRule
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from docker.types import Mount


SPARK_HOST_DIR = "/home/mina/chinook-data-engineering/spark"
PG_ENV_HOST_FILE = "/home/mina/chinook-data-engineering/postgres/.env"


PREFLIGHT_CODE = """
from pathlib import Path
import subprocess

files = [
    "/app/scd2_postgres_apply.py",
    "/app/jars/postgresql-42.7.14.jar",
]

for filename in files:
    if not Path(filename).is_file():
        raise FileNotFoundError(filename)
    print(f"FILE_OK: {filename}", flush=True)

result = subprocess.run(
    ["/opt/spark/bin/spark-submit", "--version"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    timeout=90,
)

if result.returncode != 0:
    raise RuntimeError("spark-submit version check failed")

print("SPARK_DOCKER_PREFLIGHT_OK", flush=True)
"""


PG_CHECK_COMMAND = """
set -eu

set -a
. /tmp/chinook_postgres.env
set +a

: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD missing}"

export PGPASSWORD="$POSTGRES_PASSWORD"
unset POSTGRES_PASSWORD
export PGCONNECT_TIMEOUT=10

psql \
  -h chinook-postgres \
  -p 5432 \
  -U chinook_user \
  -d chinook_staging \
  -X \
  -v ON_ERROR_STOP=1 <<'SQL'

BEGIN TRANSACTION READ ONLY;

SELECT COUNT(*) AS total_cdc_events
FROM staging.customer_change_events;

SELECT
    status,
    COUNT(*) AS event_count
FROM staging.event_processing
GROUP BY status
ORDER BY status;

COMMIT;

SQL

echo "POSTGRES_STAGING_VALIDATION_OK"
"""


@dag(
    dag_id="chinook_cdc_to_warehouse",
    description="Chinook CDC to Warehouse - incremental pipeline",
    start_date=datetime(2026, 11, 21, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["chinook", "pipeline", "spark"],
)
def chinook_pipeline():

    spark_preflight = DockerOperator(
        task_id="spark_preflight",
        image="spark:4.0.1-python3",
        entrypoint=["python3"],
        command=["-c", PREFLIGHT_CODE],
        docker_url="unix:///var/run/docker.sock",
        network_mode="chinook-net",
        mounts=[
            Mount(
                type="bind",
                source=SPARK_HOST_DIR,
                target="/app",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        do_xcom_push=False,
        retries=0,
    )

    validate_pg_staging = DockerOperator(
        task_id="validate_pg_staging",
        image="postgres:16-alpine",
        entrypoint=["/bin/sh", "-ec"],
        command=[PG_CHECK_COMMAND],
        docker_url="unix:///var/run/docker.sock",
        network_mode="chinook-net",
        mounts=[
            Mount(
                type="bind",
                source=PG_ENV_HOST_FILE,
                target="/tmp/chinook_postgres.env",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        do_xcom_push=False,
        retries=0,
    )

    validate_pg_staging.template_ext = ()


    run_spark_scd2 = DockerOperator(
        task_id="run_spark_scd2",
        image="spark:4.0.1-python3",
        entrypoint=["/bin/sh", "-ec"],
        command=["""
set -eu
set -a
. /tmp/chinook_postgres.env
. /tmp/chinook_clickhouse.env
set +a

: "${POSTGRES_PASSWORD:?Missing PostgreSQL password}"
: "${CH_USER:?Missing ClickHouse user}"
: "${CH_PASSWORD:?Missing ClickHouse password}"

exec /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --driver-memory 1g --conf spark.jars.ivy=/tmp/chinook-ivy \
  --driver-class-path /app/jars/postgresql-42.7.14.jar \
  --jars /app/jars/postgresql-42.7.14.jar \
  /app/scd2_postgres_apply.py
"""],
        docker_url="unix:///var/run/docker.sock",
        network_mode="chinook-net",
        user="1000:0",
        environment={
            "HOME": "/tmp",
            "JAVA_TOOL_OPTIONS": "-Duser.home=/tmp",
        },
        mounts=[
            Mount(
                type="bind",
                source="/etc/passwd",
                target="/etc/passwd",
                read_only=True,
            ),
            Mount(
                type="bind",
                source=SPARK_HOST_DIR,
                target="/app",
                read_only=True,
            ),
            Mount(
                type="bind",
                source=PG_ENV_HOST_FILE,
                target="/tmp/chinook_postgres.env",
                read_only=True,
            ),
            Mount(
                type="bind",
                source="/home/mina/chinook-data-engineering/clickhouse/.env",
                target="/tmp/chinook_clickhouse.env",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        do_xcom_push=False,
        retries=0,
    )

    run_spark_scd2.template_ext = ()


    validate_clickhouse_dw = DockerOperator(
        task_id="validate_clickhouse_dw",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        image="spark:4.0.1-python3",
        entrypoint=["/bin/sh", "-ec"],
        command=["""
set -eu
set -a
. /app/.env
set +a

: "${CH_USER:?CH_USER missing}"
: "${CH_PASSWORD:?CH_PASSWORD missing}"

exec python3 /app/validate_dw.py
"""],
        docker_url="unix:///var/run/docker.sock",
        network_mode="chinook-net",
        user="1000:1000",
        mounts=[
            Mount(
                type="bind",
                source="/home/mina/chinook-data-engineering/clickhouse",
                target="/app",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        do_xcom_push=False,
        retries=0,
    )

    validate_clickhouse_dw.template_ext = ()


    consume_kafka_events = DockerOperator(
        task_id="consume_kafka_events",
        image="chinook-kafka-consumer:lab-v1",
        entrypoint=["/bin/sh", "-ec"],
        command=["""
set -eu
set -a
. /tmp/chinook_postgres.env
set +a

: "${POSTGRES_PASSWORD:?Missing PostgreSQL password}"

exec python3 -u /app/kafka_consumer.py
"""],
        docker_url="unix:///var/run/docker.sock",
        network_mode="chinook-net",
        user="1000:1000",
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "chinook-kafka:19092",
            "PG_HOST": "chinook-postgres",
            "PG_PORT": "5432",
        },
        mounts=[
            Mount(
                type="bind",
                source="/home/mina/chinook-data-engineering/kafka-consumer",
                target="/app",
                read_only=True,
            ),
            Mount(
                type="bind",
                source="/home/mina/chinook-data-engineering/postgres/.env",
                target="/tmp/chinook_postgres.env",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        do_xcom_push=False,
        retries=0,
    )

    consume_kafka_events.template_ext = ()


    check_pending_events = DockerOperator(
        task_id="check_pending_events",
        image="postgres:16-alpine",
        entrypoint=["/bin/sh", "-ec"],
        command=["""
set -eu

set -a
. /tmp/chinook_postgres.env
set +a

: "${POSTGRES_PASSWORD:?Password missing}"

export PGPASSWORD="$POSTGRES_PASSWORD"
unset POSTGRES_PASSWORD
export PGOPTIONS="-c default_transaction_read_only=on"

psql -X -q -t -A -v ON_ERROR_STOP=1 \
  -h chinook-postgres \
  -p 5432 \
  -U chinook_user \
  -d chinook_staging <<'SQL'
SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM staging.event_processing
        WHERE status = 'PROCESSING'
    ) THEN 'BLOCKED_PROCESSING'
    ELSE (
        SELECT COUNT(*)::text
        FROM staging.customer_change_events e
        WHERE NOT EXISTS (
            SELECT 1
            FROM staging.event_processing p
            WHERE p.event_id = e.event_id
              AND p.status = 'APPLIED'
        )
    )
END;
SQL
"""],
        docker_url="unix:///var/run/docker.sock",
        network_mode="chinook-net",
        mounts=[
            Mount(
                type="bind",
                source=PG_ENV_HOST_FILE,
                target="/tmp/chinook_postgres.env",
                read_only=True,
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        do_xcom_push=True,
        xcom_all=False,
        retries=0,
    )

    check_pending_events.template_ext = ()


    @task.branch(task_id="choose_processing_path")
    def choose_processing_path(pending_count):
        value = str(pending_count).strip()

        if value == "BLOCKED_PROCESSING":
            raise RuntimeError(
                "PROCESSING events require manual review"
            )

        try:
            pending = int(value)
        except ValueError:
            raise RuntimeError(
                f"Invalid pending event count: {value!r}"
            )

        if pending < 0:
            raise RuntimeError("Negative pending event count")

        if pending > 0:
            print(f"PENDING_EVENTS: {pending}", flush=True)
            return "spark_preflight"

        print("NO_PENDING_EVENTS_SKIP_SPARK", flush=True)
        return "skip_spark"

    skip_spark = EmptyOperator(task_id="skip_spark")

    branch = choose_processing_path(
        check_pending_events.output
    )

    consume_kafka_events >> validate_pg_staging
    validate_pg_staging >> check_pending_events
    check_pending_events >> branch

    branch >> spark_preflight >> run_spark_scd2
    branch >> skip_spark

    [run_spark_scd2, skip_spark] >> validate_clickhouse_dw







chinook_pipeline()
