"""Gold: tabel agregat siap pakai untuk BI dan analisis."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def _with_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    def side(prefix: str) -> DataFrame:
        return zones.select(
            F.col("location_id").alias(f"{prefix}_location_id"),
            F.col("borough").alias(f"{prefix}_borough"),
            F.col("zone").alias(f"{prefix}_zone"),
        )

    # Tabel zona kecil (265 baris): broadcast join menghindari shuffle pada tabel perjalanan.
    return trips.join(F.broadcast(side("pu")), "pu_location_id", "left").join(
        F.broadcast(side("do")), "do_location_id", "left"
    )


def _card_tip_pct() -> Column:
    # Tip tunai tidak tercatat di data TLC, jadi rata-rata tip hanya dari pembayaran kartu.
    return F.round(F.avg(F.when(F.col("payment_type") == 1, F.col("tip_pct"))), 2)


def daily_zone_metrics(trips: DataFrame, zones: DataFrame) -> DataFrame:
    return (
        _with_zones(trips, zones)
        .groupBy("pickup_date", "pu_location_id", "pu_borough", "pu_zone")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue_usd"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
            F.round(F.avg("distance_mi"), 2).alias("avg_distance_mi"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            _card_tip_pct().alias("avg_card_tip_pct"),
        )
    )


def hourly_demand(trips: DataFrame) -> DataFrame:
    """Rata-rata perjalanan per hari untuk setiap kombinasi hari-dalam-minggu x jam."""
    n_days = trips.select("pickup_date", "pickup_dow").distinct().groupBy("pickup_dow").count()
    return (
        trips.groupBy("pickup_dow", "pickup_hour")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
            F.round(F.percentile_approx("avg_speed_mph", 0.5), 2).alias("median_speed_mph"),
        )
        .join(n_days.withColumnRenamed("count", "n_days"), "pickup_dow")
        .withColumn("avg_trips_per_day", F.round(F.col("total_trips") / F.col("n_days"), 1))
        .drop("n_days")
    )


def payment_summary(trips: DataFrame) -> DataFrame:
    per_month = Window.partitionBy("pickup_month")
    return (
        trips.groupBy("pickup_month", "payment_type_desc")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue_usd"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
        )
        .withColumn("share_pct", F.round(100 * F.col("trips") / F.sum("trips").over(per_month), 2))
    )


def top_routes(trips: DataFrame, zones: DataFrame, top_n: int = 20) -> DataFrame:
    routes = (
        _with_zones(trips, zones)
        .groupBy("pickup_month", "pu_zone", "do_zone")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("duration_min"), 1).alias("avg_duration_min"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_usd"),
        )
    )
    rank = F.row_number().over(
        Window.partitionBy("pickup_month").orderBy(F.desc("trips"), "pu_zone", "do_zone")
    )
    return routes.withColumn("rank", rank).where(F.col("rank") <= top_n)


def build_all(trips: DataFrame, zones: DataFrame) -> dict[str, DataFrame]:
    return {
        "daily_zone_metrics": daily_zone_metrics(trips, zones),
        "hourly_demand": hourly_demand(trips),
        "payment_summary": payment_summary(trips),
        "top_routes": top_routes(trips, zones),
    }
