"""CLI lakehouse.

python -m taxi_lakehouse sample   --months 2024-01 2024-02
python -m taxi_lakehouse download --months 2024-01
python -m taxi_lakehouse run      --months 2024-01 2024-02 [--stage bronze|silver|gold|all]
python -m taxi_lakehouse maintain
python -m taxi_lakehouse query    "select * from gold_hourly_demand limit 10"
python -m taxi_lakehouse history  silver/trips
"""

from __future__ import annotations

import argparse
import logging

from taxi_lakehouse.config import (
    BRONZE_TRIPS,
    GOLD_TABLES,
    SILVER_QUARANTINE,
    SILVER_TRIPS,
    SILVER_ZONES,
    Paths,
)


def _register_views(spark, paths: Paths) -> list[str]:
    from delta.tables import DeltaTable

    tables = [BRONZE_TRIPS, SILVER_TRIPS, SILVER_QUARANTINE, SILVER_ZONES]
    tables += [("gold", name) for name in GOLD_TABLES]
    names = []
    for layer, name in tables:
        path = paths.table(layer, name)
        if DeltaTable.isDeltaTable(spark, path):
            spark.read.format("delta").load(path).createOrReplaceTempView(f"{layer}_{name}")
            names.append(f"{layer}_{name}")
    return names


def main() -> None:
    parser = argparse.ArgumentParser(prog="taxi_lakehouse")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("sample", help="buat data contoh berskema TLC")
    p.add_argument("--months", nargs="+", required=True)
    p.add_argument("--rows", type=int, default=50_000)

    p = sub.add_parser("download", help="unduh data asli NYC TLC")
    p.add_argument("--months", nargs="+", required=True)

    p = sub.add_parser("run", help="bangun lapisan bronze/silver/gold")
    p.add_argument("--months", nargs="+", required=True)
    p.add_argument("--stage", choices=["bronze", "silver", "gold", "all"], default="all")
    p.add_argument("--max-reject-ratio", type=float, default=0.10)

    sub.add_parser("maintain", help="OPTIMIZE + Z-ORDER + VACUUM tabel silver")

    p = sub.add_parser("query", help="jalankan SQL terhadap semua tabel")
    p.add_argument("sql")
    p.add_argument("-n", type=int, default=20)

    p = sub.add_parser("history", help="riwayat versi tabel Delta (time travel)")
    p.add_argument("table", help="mis. silver/trips")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    paths = Paths.from_env()

    if args.cmd == "sample":
        from taxi_lakehouse.sample import write_month, write_zones

        write_zones(paths.raw_zones)
        for month in args.months:
            n = write_month(paths.raw_trips(month), month, args.rows)
            logging.info("sample %s: %d rows -> %s", month, n, paths.raw_trips(month))
        return

    if args.cmd == "download":
        from taxi_lakehouse.download import download

        download(args.months, paths.raw_dir)
        return

    from taxi_lakehouse import pipeline
    from taxi_lakehouse.spark import build_spark

    spark = build_spark()
    if args.cmd == "run":
        if args.stage in ("bronze", "all"):
            pipeline.run_bronze(spark, paths, args.months)
        if args.stage in ("silver", "all"):
            pipeline.run_silver(spark, paths, args.months, args.max_reject_ratio)
        if args.stage in ("gold", "all"):
            pipeline.run_gold(spark, paths)
    elif args.cmd == "maintain":
        pipeline.run_maintenance(spark, paths)
    elif args.cmd == "query":
        views = _register_views(spark, paths)
        logging.info("tabel tersedia: %s", ", ".join(views))
        spark.sql(args.sql).show(args.n, truncate=False)
    elif args.cmd == "history":
        from delta.tables import DeltaTable

        layer, name = args.table.split("/")
        (
            DeltaTable.forPath(spark, paths.table(layer, name))
            .history()
            .select("version", "timestamp", "operation", "operationMetrics")
            .show(truncate=False)
        )


if __name__ == "__main__":
    main()
