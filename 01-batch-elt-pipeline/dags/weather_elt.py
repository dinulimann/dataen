"""DAG harian: Open-Meteo -> MinIO -> PostgreSQL -> dbt.

Setiap run memproses tanggal `ds` (hari kemarin relatif terhadap jadwal),
sehingga data harian sudah lengkap saat diambil. Semua langkah idempoten,
jadi aman untuk di-retry maupun di-backfill:

    airflow dags backfill weather_elt -s 2026-09-01 -e 2026-09-30
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from platform_ops.notify import on_task_failure

DBT_CMD = (
    "cd /opt/dbt && /home/airflow/dbt-venv/bin/dbt {cmd} "
    "--profiles-dir /opt/dbt --target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
)


@dag(
    dag_id="weather_elt",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_task_failure,
    },
    tags=["portfolio", "elt", "open-meteo"],
    doc_md=__doc__,
)
def weather_elt():
    @task
    def extract(ds: str | None = None) -> list[str]:
        from weather_pipeline.config import LakeSettings, load_cities
        from weather_pipeline.lake import Lake
        from weather_pipeline.pipeline import extract_to_lake

        lake = Lake(LakeSettings.from_env())
        lake.ensure_bucket()
        return extract_to_lake(pendulum.parse(ds).date(), load_cities(), lake)

    @task
    def load(keys: list[str]) -> int:
        from weather_pipeline.config import LakeSettings, WarehouseSettings
        from weather_pipeline.lake import Lake
        from weather_pipeline.load import connect
        from weather_pipeline.pipeline import load_from_lake

        conn = connect(WarehouseSettings.from_env())
        try:
            return load_from_lake(keys, Lake(LakeSettings.from_env()), conn)
        finally:
            conn.close()

    dbt_build = BashOperator(task_id="dbt_build", bash_command=DBT_CMD.format(cmd="build"))

    load(extract()) >> dbt_build


weather_elt()
