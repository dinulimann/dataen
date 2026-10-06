from datetime import UTC, date, datetime, timedelta

import pytest

from platform_ops.lakehouse import (
    is_published,
    resolve_month,
    source_url,
    stage_command,
)
from platform_ops.streaming_health import StreamingSnapshot, evaluate

NOW = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("run_date", "lag", "expected"),
    [
        (date(2026, 10, 10), 2, "2026-08"),
        (date(2026, 2, 10), 2, "2025-12"),  # melewati pergantian tahun
        (date(2026, 1, 10), 0, "2026-01"),
        (date(2026, 3, 10), 14, "2025-01"),
    ],
)
def test_resolve_month_from_run_date(run_date, lag, expected):
    assert resolve_month(None, run_date, lag) == expected


def test_resolve_month_prefers_requested_month():
    assert resolve_month("2024-01", date(2026, 10, 10), 2) == "2024-01"


@pytest.mark.parametrize("bad", ["2024-13", "2024-1", "Januari", "2024/01"])
def test_resolve_month_rejects_bad_format(bad):
    with pytest.raises(ValueError, match="YYYY-MM"):
        resolve_month(bad, date(2026, 10, 10), 2)


def test_commands_match_lakehouse_cli():
    assert stage_command("silver", "2024-01") == "run --months 2024-01 --stage silver"
    with pytest.raises(ValueError):
        stage_command("platinum", "2024-01")


def test_is_published_checks_http_status():
    class FakeSession:
        def __init__(self, status):
            self.status = status

        def head(self, url, timeout, allow_redirects):
            self.url = url
            return type("R", (), {"status_code": self.status})()

    ok = FakeSession(200)
    assert is_published("2024-01", session=ok)
    assert ok.url == source_url("2024-01")
    assert not is_published("2099-01", session=FakeSession(403))


def snapshot(lag_minutes=0.5, recent=(1000, 5), baseline=(12000, 60)):
    return StreamingSnapshot(
        checked_at=NOW,
        latest_window_end=NOW - timedelta(minutes=lag_minutes),
        orders_recent=recent[0],
        recent_minutes=recent[1],
        orders_baseline=baseline[0],
        baseline_minutes=baseline[1],
    )


def results(checks):
    return {c.name: c.passed for c in checks}


def test_healthy_stream_passes():
    assert results(evaluate(snapshot())) == {"freshness": True, "volume": True}


def test_in_progress_window_in_future_counts_as_fresh():
    assert results(evaluate(snapshot(lag_minutes=-0.8)))["freshness"]


def test_stale_stream_fails_freshness():
    checks = evaluate(snapshot(lag_minutes=12))
    assert results(checks)["freshness"] is False
    assert "12.0 menit" in checks[0].detail


def test_volume_drop_fails():
    # 50/menit vs baseline 200/menit = 25% < 50%
    checks = evaluate(snapshot(recent=(250, 5), baseline=(12000, 60)))
    assert results(checks)["volume"] is False


def test_volume_check_skipped_without_baseline():
    assert results(evaluate(snapshot(baseline=(0, 0))))["volume"] is True


def test_no_data_at_all_fails():
    empty = StreamingSnapshot(NOW, None, 0, 0, 0, 0)
    assert results(evaluate(empty)) == {"freshness": False}
