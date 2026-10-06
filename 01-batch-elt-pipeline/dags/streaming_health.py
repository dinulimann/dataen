"""DAG kesehatan pipeline streaming (proyek 02), setiap 10 menit.

Memeriksa output Spark job di Postgres proyek 02: apakah data masih segar
dan volumenya tidak anjlok. Task gagal (merah di Airflow) bila ada
pemeriksaan yang tidak lolos, sehingga masalah terlihat walau job Spark
tidak crash (mis. producer berhenti atau consumer macet).

DAG ini dalam keadaan pause saat pertama kali dibuat; aktifkan setelah
stack proyek 02 berjalan (`make up` di root sudah melakukannya).
"""

from __future__ import annotations

import logging
import os

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from platform_ops.notify import on_failure_state_change, on_recovery

log = logging.getLogger(__name__)


@dag(
    dag_id="streaming_health",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(minutes=5),
    default_args={"retries": 0},
    tags=["portfolio", "streaming", "monitoring", "cross-project"],
    doc_md=__doc__,
)
def streaming_health():
    @task(on_failure_callback=on_failure_state_change, on_success_callback=on_recovery)
    def check_streaming_output() -> dict:
        import psycopg2

        from platform_ops.streaming_health import evaluate, fetch_snapshot

        conn = psycopg2.connect(
            host=os.getenv("STREAMING_PG_HOST", "host.docker.internal"),
            port=int(os.getenv("STREAMING_PG_PORT", "5433")),
            dbname=os.getenv("STREAMING_PG_DB", "streaming"),
            user=os.getenv("STREAMING_PG_USER", "postgres"),
            password=os.getenv("STREAMING_PG_PASSWORD", "postgres"),
            connect_timeout=10,
        )
        try:
            snapshot = fetch_snapshot(conn)
        except psycopg2.errors.UndefinedTable as exc:
            raise AirflowFailException("tabel stream.order_metrics_1m belum ada") from exc
        finally:
            conn.close()

        checks = evaluate(
            snapshot,
            max_lag_minutes=float(os.getenv("STREAMING_MAX_LAG_MINUTES", "5")),
            min_volume_ratio=float(os.getenv("STREAMING_MIN_VOLUME_RATIO", "0.5")),
        )
        for c in checks:
            log.info("[%s] %s: %s", "PASS" if c.passed else "FAIL", c.name, c.detail)
        failed = [c for c in checks if not c.passed]
        if failed:
            raise AirflowFailException("; ".join(f"{c.name}: {c.detail}" for c in failed))
        return {c.name: c.detail for c in checks}

    check_streaming_output()


streaming_health()
