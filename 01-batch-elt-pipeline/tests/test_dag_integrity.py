"""DAG integrity test: semua DAG bisa di-parse dan strukturnya sesuai harapan.

Butuh Airflow terpasang (job `dags` di CI); di-skip bila tidak ada.
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_present(dagbag):
    assert set(dagbag.dag_ids) == {"weather_elt", "lakehouse_monthly", "streaming_health"}


@pytest.mark.parametrize("dag_id", ["weather_elt", "lakehouse_monthly", "streaming_health"])
def test_dags_are_tagged_and_documented(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    assert "portfolio" in dag.tags
    assert dag.doc_md
    assert dag.catchup is False


def test_lakehouse_runs_medallion_stages_in_order(dagbag):
    dag = dagbag.dags["lakehouse_monthly"]
    order = [t.task_id for t in dag.topological_sort()]
    assert order == [
        "resolve_month",
        "wait_for_source",
        "fetch",
        "bronze",
        "silver",
        "gold",
        "maintain",
    ]
    assert dag.get_task("silver").command.endswith("--stage silver")
    assert dag.get_task("fetch").command.startswith("{{ params.source }}")


def test_lakehouse_params_validate_input(dagbag):
    params = dagbag.dags["lakehouse_monthly"].params
    assert params.get_param("source").schema["enum"] == ["download", "sample"]
    assert params.get_param("month").value is None


def test_every_task_alerts_on_failure(dagbag):
    missing = [
        f"{dag_id}.{task.task_id}"
        for dag_id, dag in dagbag.dags.items()
        for task in dag.tasks
        if not task.on_failure_callback
    ]
    assert missing == []


def test_streaming_health_alerts_only_on_state_change(dagbag):
    from platform_ops.notify import on_failure_state_change, on_recovery

    task = dagbag.dags["streaming_health"].get_task("check_streaming_output")
    assert task.on_failure_callback in (on_failure_state_change, [on_failure_state_change])
    assert task.on_success_callback in (on_recovery, [on_recovery])
