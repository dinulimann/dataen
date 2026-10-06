"""DAG bulanan: lakehouse NYC Taxi (proyek 03).

Airflow tidak menjalankan Spark sendiri. Setiap tahap berjalan di container
image `taxi-lakehouse` lewat DockerOperator, jadi image Airflow tetap ramping
dan dependensi Spark/Java terisolasi.

    resolve_month -> wait_for_source -> fetch -> bronze -> silver -> gold -> maintain

Trigger manual untuk bulan tertentu atau memakai data contoh:

    airflow dags trigger lakehouse_monthly -c '{"month": "2024-01", "source": "sample"}'
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from platform_ops.lakehouse import STAGES, stage_command
from platform_ops.notify import on_task_failure

LAKEHOUSE_IMAGE = os.getenv("LAKEHOUSE_IMAGE", "taxi-lakehouse:local")
# Path *di host* ke folder 03-lakehouse; diisi otomatis oleh `make up`.
LAKEHOUSE_HOST_PATH = os.getenv("LAKEHOUSE_HOST_PATH", "")
DOCKER_URL = os.getenv("DOCKER_URL", "tcp://docker-proxy:2375")
LAG_MONTHS = int(os.getenv("LAKEHOUSE_LAG_MONTHS", "2"))
MONTH = "{{ ti.xcom_pull(task_ids='resolve_month') }}"


def lakehouse_task(task_id: str, command: str, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=LAKEHOUSE_IMAGE,
        command=command,
        docker_url=DOCKER_URL,
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(target="/data", source=f"{LAKEHOUSE_HOST_PATH}/data", type="bind"),
            Mount(target="/lakehouse", source=f"{LAKEHOUSE_HOST_PATH}/lakehouse", type="bind"),
        ],
        environment={
            "RAW_DIR": "/data/raw",
            "LAKEHOUSE_DIR": "/lakehouse",
            "SPARK_DRIVER_MEMORY": os.getenv("LAKEHOUSE_DRIVER_MEMORY", "4g"),
        },
        **kwargs,
    )


@dag(
    dag_id="lakehouse_monthly",
    # Tanggal 10 setiap bulan, 04:00 WIB.
    schedule="0 4 10 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": on_task_failure,
    },
    params={
        "month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Bulan YYYY-MM. Kosong = bulan run dikurangi LAKEHOUSE_LAG_MONTHS.",
        ),
        "source": Param(
            "download",
            enum=["download", "sample"],
            description="download = data asli NYC TLC, sample = data contoh berskema sama.",
        ),
    },
    tags=["portfolio", "lakehouse", "spark", "cross-project"],
    doc_md=__doc__,
)
def lakehouse_monthly():
    @task
    def resolve_month(params=None, data_interval_end=None, dag_run=None) -> str:
        from platform_ops.lakehouse import resolve_month as resolve

        if not LAKEHOUSE_HOST_PATH.startswith("/"):
            raise AirflowFailException(
                "LAKEHOUSE_HOST_PATH harus berisi path absolut folder 03-lakehouse di host. "
                "Jalankan stack lewat `make up` agar terisi otomatis."
            )
        # Run terjadwal: akhir interval (tanggal 10 bulan berjalan). Trigger manual:
        # tanggal saat ini, bukan akhir interval jadwal sebelumnya.
        scheduled = dag_run.run_type == "scheduled"
        run_date = data_interval_end if scheduled else pendulum.now("Asia/Jakarta")
        return resolve(params["month"], run_date.date(), LAG_MONTHS)

    @task.sensor(poke_interval=6 * 3600, timeout=14 * 24 * 3600, mode="reschedule")
    def wait_for_source(month: str, params=None) -> bool:
        """Data TLC terbit tidak pada tanggal pasti; tunggu sampai file tersedia."""
        from platform_ops.lakehouse import is_published

        return params["source"] == "sample" or is_published(month)

    month = resolve_month()
    fetch = lakehouse_task("fetch", "{{ params.source }} --months " + MONTH)
    stages = [lakehouse_task(stage, stage_command(stage, MONTH)) for stage in STAGES]
    maintain = lakehouse_task("maintain", "maintain")

    wait_for_source(month) >> fetch >> stages[0] >> stages[1] >> stages[2] >> maintain


lakehouse_monthly()
