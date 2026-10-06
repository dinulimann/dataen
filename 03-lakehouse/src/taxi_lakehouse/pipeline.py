"""Orkestrasi lapisan bronze -> silver -> gold untuk daftar bulan tertentu."""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from taxi_lakehouse import bronze, gold, quality, silver
from taxi_lakehouse.config import (
    BRONZE_TRIPS,
    SILVER_QUARANTINE,
    SILVER_TRIPS,
    SILVER_ZONES,
    Paths,
)

log = logging.getLogger(__name__)


def run_bronze(spark: SparkSession, paths: Paths, months: list[str]) -> None:
    for month in months:
        src = paths.raw_trips(month)
        if not src.exists():
            raise FileNotFoundError(
                f"{src} tidak ditemukan. Jalankan `download` atau `sample` dulu."
            )
        n = bronze.ingest_month(spark, str(src), month, paths.table(*BRONZE_TRIPS))
        log.info("bronze %s: %d rows", month, n)


def run_silver(
    spark: SparkSession, paths: Paths, months: list[str], max_reject_ratio: float = 0.10
) -> list[silver.SilverResult]:
    n_zones = silver.load_zones(spark, str(paths.raw_zones), paths.table(*SILVER_ZONES))
    log.info("silver zones: %d rows", n_zones)

    results, checks = [], []
    for month in months:
        r = silver.build_month(
            spark,
            paths.table(*BRONZE_TRIPS),
            paths.table(*SILVER_TRIPS),
            paths.table(*SILVER_QUARANTINE),
            month,
        )
        log.info(
            "silver %s: input=%d valid=%d duplicates=%d rejected=%s",
            month,
            r.input_rows,
            r.valid_rows,
            r.duplicate_rows,
            r.rejected,
        )
        checks.append(
            quality.check_rejection_rate(
                month, r.input_rows, sum(r.rejected.values()), max_reject_ratio
            )
        )
        results.append(r)

    trips = spark.read.format("delta").load(paths.table(*SILVER_TRIPS))
    quality.enforce(checks + quality.check_silver(trips))
    return results


def run_gold(spark: SparkSession, paths: Paths) -> dict[str, int]:
    trips = spark.read.format("delta").load(paths.table(*SILVER_TRIPS))
    zones = spark.read.format("delta").load(paths.table(*SILVER_ZONES))
    counts = {}
    tables = gold.build_all(trips, zones)
    for name, df in tables.items():
        path = paths.table("gold", name)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)
        counts[name] = spark.read.format("delta").load(path).count()
        log.info("gold %s: %d rows", name, counts[name])

    daily = spark.read.format("delta").load(paths.table("gold", "daily_zone_metrics"))
    quality.enforce([quality.check_gold_reconciles(trips, daily)])
    return counts


def run_maintenance(spark: SparkSession, paths: Paths, vacuum_hours: int = 168) -> None:
    """Gabungkan file kecil dan susun data agar query per tanggal/zona lebih cepat."""
    table = DeltaTable.forPath(spark, paths.table(*SILVER_TRIPS))
    table.optimize().executeZOrderBy("pickup_date", "pu_location_id")
    table.vacuum(vacuum_hours)
    log.info("silver trips optimized (Z-ORDER pickup_date, pu_location_id)")
