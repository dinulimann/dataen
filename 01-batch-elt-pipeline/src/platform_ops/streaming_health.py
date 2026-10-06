"""Pemeriksaan kesehatan pipeline streaming (proyek 02) dari sisi hasilnya.

Daripada memantau proses Spark, kita periksa *output*-nya di Postgres:
- freshness: apakah jendela terbaru masih baru (job hidup & mengikuti data)?
- volume: apakah jumlah pesanan 5 menit terakhir tidak anjlok dibanding
  rata-rata satu jam sebelumnya?
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

HEALTH_SQL = """
-- Semua batas diukur dari awal menit berjalan (yang jendelanya belum lengkap),
-- sehingga "5 menit terakhir" dan baseline 60 menit sebelumnya tidak tumpang tindih.
with b as (select date_trunc('minute', now()) as cur)
select
    now() as checked_at,
    max(window_end) as latest_window_end,
    coalesce(sum(orders) filter (where recent), 0) as orders_recent,
    count(distinct window_start) filter (where recent) as recent_minutes,
    coalesce(sum(orders) filter (where baseline), 0) as orders_baseline,
    count(distinct window_start) filter (where baseline) as baseline_minutes
from (
    select
        m.*,
        m.window_start >= b.cur - interval '5 minutes' and m.window_start < b.cur as recent,
        m.window_start >= b.cur - interval '65 minutes'
            and m.window_start < b.cur - interval '5 minutes' as baseline
    from stream.order_metrics_1m m, b
) t
"""


@dataclass(frozen=True)
class StreamingSnapshot:
    checked_at: datetime
    latest_window_end: datetime | None
    orders_recent: int
    recent_minutes: int
    orders_baseline: int
    baseline_minutes: int

    @property
    def recent_per_minute(self) -> float | None:
        return self.orders_recent / self.recent_minutes if self.recent_minutes else None

    @property
    def baseline_per_minute(self) -> float | None:
        return self.orders_baseline / self.baseline_minutes if self.baseline_minutes else None


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


def fetch_snapshot(conn) -> StreamingSnapshot:
    with conn.cursor() as cur:
        cur.execute(HEALTH_SQL)
        return StreamingSnapshot(*cur.fetchone())


def evaluate(
    snap: StreamingSnapshot, max_lag_minutes: float = 5, min_volume_ratio: float = 0.5
) -> list[Check]:
    if snap.latest_window_end is None:
        return [Check("freshness", False, "belum ada data di stream.order_metrics_1m")]

    lag_min = (snap.checked_at - snap.latest_window_end).total_seconds() / 60
    checks = [
        Check(
            "freshness",
            lag_min <= max_lag_minutes,
            f"jendela terbaru berakhir {max(lag_min, 0):.1f} menit lalu (maks {max_lag_minutes})",
        )
    ]

    recent, baseline = snap.recent_per_minute, snap.baseline_per_minute
    if recent is None or baseline is None or baseline == 0:
        checks.append(Check("volume", True, "data belum cukup untuk membandingkan volume"))
    else:
        ratio = recent / baseline
        checks.append(
            Check(
                "volume",
                ratio >= min_volume_ratio,
                f"{recent:.0f} pesanan/menit vs baseline {baseline:.0f} "
                f"({ratio:.0%}, min {min_volume_ratio:.0%})",
            )
        )
    return checks
