"""Pemeriksaan kualitas data sederhana yang dijalankan setelah setiap lapisan dibangun.

Pipeline berhenti (raise) bila ada pemeriksaan yang gagal, supaya data yang
rusak tidak mengalir ke lapisan gold.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


class DataQualityError(RuntimeError):
    def __init__(self, failures: list[CheckResult]):
        super().__init__("; ".join(f"{f.name}: {f.detail}" for f in failures))
        self.failures = failures


def check_silver(trips: DataFrame) -> list[CheckResult]:
    total, unique_ids = trips.agg(F.count("*"), F.countDistinct("trip_id")).first()
    nulls = trips.where(
        F.col("pickup_at").isNull()
        | F.col("pu_location_id").isNull()
        | F.col("total_amount").isNull()
    ).count()
    bad_ranges = trips.where(
        (F.col("duration_min") <= 0) | (F.col("distance_mi") <= 0) | (F.col("fare_amount") < 0)
    ).count()
    return [
        CheckResult("silver_not_empty", total > 0, f"{total} rows"),
        CheckResult(
            "silver_trip_id_unique", total == unique_ids, f"{total - unique_ids} duplicates"
        ),
        CheckResult("silver_required_not_null", nulls == 0, f"{nulls} rows with nulls"),
        CheckResult("silver_values_in_range", bad_ranges == 0, f"{bad_ranges} rows out of range"),
    ]


def check_rejection_rate(
    month: str, input_rows: int, rejected: int, max_ratio: float
) -> CheckResult:
    ratio = rejected / input_rows if input_rows else 1.0
    return CheckResult(
        f"rejection_rate_{month}", ratio <= max_ratio, f"{ratio:.2%} rejected (max {max_ratio:.0%})"
    )


def check_gold_reconciles(trips: DataFrame, daily_zone_metrics: DataFrame) -> CheckResult:
    silver_count = trips.count()
    gold_count = daily_zone_metrics.agg(F.sum("trips")).first()[0] or 0
    return CheckResult(
        "gold_reconciles_with_silver",
        silver_count == gold_count,
        f"silver={silver_count} gold={gold_count}",
    )


def enforce(results: list[CheckResult]) -> None:
    for r in results:
        log.info("[%s] %s: %s", "PASS" if r.passed else "FAIL", r.name, r.detail)
    failures = [r for r in results if not r.passed]
    if failures:
        raise DataQualityError(failures)
