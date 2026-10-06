"""Silver: data bersih, tervalidasi, tanpa duplikat, dan diperkaya.

Baris yang gagal validasi tidak dibuang diam-diam, melainkan dipindah ke
tabel karantina beserta alasannya.
"""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

PAYMENT_TYPES = {
    0: "Flex Fare",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

MAX_TRIP_HOURS = 6
MAX_DISTANCE_MI = 200
MAX_PASSENGERS = 8

NATURAL_KEY = [
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pulocationid",
    "dolocationid",
    "trip_distance",
    "total_amount",
]


def rejection_reason() -> Column:
    c = F.col
    duration_s = c("tpep_dropoff_datetime").cast("long") - c("tpep_pickup_datetime").cast("long")
    return (
        F.when(
            c("tpep_pickup_datetime").isNull() | c("tpep_dropoff_datetime").isNull(),
            "missing_timestamp",
        )
        .when(
            F.date_format("tpep_pickup_datetime", "yyyy-MM") != c("_source_month"),
            "pickup_outside_file_month",
        )
        .when(duration_s <= 0, "non_positive_duration")
        .when(duration_s > MAX_TRIP_HOURS * 3600, "duration_too_long")
        .when(c("trip_distance").isNull() | (c("trip_distance") <= 0), "non_positive_distance")
        .when(c("trip_distance") > MAX_DISTANCE_MI, "distance_too_long")
        .when((c("fare_amount") < 0) | (c("total_amount") < 0), "negative_amount")
        .when(c("passenger_count") > MAX_PASSENGERS, "too_many_passengers")
        .when(
            ~c("pulocationid").between(1, 265) | ~c("dolocationid").between(1, 265),
            "unknown_location",
        )
    )


def with_trip_id(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "trip_id", F.sha2(F.concat_ws("|", *[F.col(k).cast("string") for k in NATURAL_KEY]), 256)
    )


def enrich(valid: DataFrame) -> DataFrame:
    duration_min = (
        F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")
    ) / 60
    payment_map = F.create_map(*[x for k, v in PAYMENT_TYPES.items() for x in (F.lit(k), F.lit(v))])
    return valid.select(
        "trip_id",
        F.col("vendorid").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_at"),
        F.col("tpep_dropoff_datetime").alias("dropoff_at"),
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("pickup_month"),
        F.hour("tpep_pickup_datetime").alias("pickup_hour"),
        F.dayofweek("tpep_pickup_datetime").alias("pickup_dow"),  # 1 = Minggu
        "passenger_count",
        F.col("trip_distance").alias("distance_mi"),
        F.round(duration_min, 2).alias("duration_min"),
        F.round(F.col("trip_distance") / (duration_min / 60), 2).alias("avg_speed_mph"),
        F.col("pulocationid").alias("pu_location_id"),
        F.col("dolocationid").alias("do_location_id"),
        F.col("payment_type"),
        F.coalesce(payment_map[F.col("payment_type")], F.lit("Unknown")).alias("payment_type_desc"),
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        F.when(
            F.col("fare_amount") > 0, F.round(100 * F.col("tip_amount") / F.col("fare_amount"), 2)
        ).alias("tip_pct"),
        "_source_month",
    )


@dataclass(frozen=True)
class SilverResult:
    month: str
    input_rows: int
    valid_rows: int
    duplicate_rows: int
    rejected: dict[str, int]


def build_month(
    spark: SparkSession, bronze_path: str, silver_path: str, quarantine_path: str, month: str
) -> SilverResult:
    bronze = spark.read.format("delta").load(bronze_path).where(F.col("_source_month") == month)
    checked = with_trip_id(bronze).withColumn("rejection_reason", rejection_reason()).cache()

    rejected = checked.where(F.col("rejection_reason").isNotNull())
    valid = checked.where(F.col("rejection_reason").isNull())
    deduped = valid.dropDuplicates(["trip_id"])
    trips = enrich(deduped)

    _upsert(spark, trips, silver_path)
    (
        rejected.drop("_ingested_at")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_source_month = '{month}'")
        .partitionBy("_source_month")
        .save(quarantine_path)
    )

    reasons = {
        r["rejection_reason"]: r["n"]
        for r in rejected.groupBy("rejection_reason").agg(F.count("*").alias("n")).collect()
    }
    input_rows, valid_rows, unique_rows = checked.count(), valid.count(), deduped.count()
    checked.unpersist()
    return SilverResult(month, input_rows, unique_rows, valid_rows - unique_rows, reasons)


def _upsert(spark: SparkSession, trips: DataFrame, silver_path: str) -> None:
    """MERGE berdasarkan trip_id: menjalankan ulang bulan yang sama tidak menambah baris."""
    if not DeltaTable.isDeltaTable(spark, silver_path):
        trips.write.format("delta").partitionBy("pickup_month").save(silver_path)
        return
    (
        DeltaTable.forPath(spark, silver_path)
        .alias("t")
        .merge(trips.alias("s"), "t.trip_id = s.trip_id AND t.pickup_month = s.pickup_month")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def load_zones(spark: SparkSession, csv_path: str, zones_path: str) -> int:
    zones = (
        spark.read.option("header", True)
        .csv(csv_path)
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone"),
        )
    )
    zones.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(zones_path)
    return zones.count()
