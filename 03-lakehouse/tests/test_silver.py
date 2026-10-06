from datetime import timedelta

import pytest
from conftest import T0, trip
from pyspark.sql import functions as F

from taxi_lakehouse.silver import build_month, enrich, rejection_reason, with_trip_id


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"tpep_dropoff_datetime": None}, "missing_timestamp"),
        (
            {
                "tpep_pickup_datetime": T0.replace(year=2002),
                "tpep_dropoff_datetime": T0.replace(year=2002) + timedelta(minutes=5),
            },
            "pickup_outside_file_month",
        ),
        ({"tpep_dropoff_datetime": T0}, "non_positive_duration"),
        ({"tpep_dropoff_datetime": T0 + timedelta(hours=7)}, "duration_too_long"),
        ({"trip_distance": 0.0}, "non_positive_distance"),
        ({"trip_distance": 350.0}, "distance_too_long"),
        ({"fare_amount": -5.0}, "negative_amount"),
        ({"passenger_count": 12}, "too_many_passengers"),
        ({"pulocationid": 999}, "unknown_location"),
        ({}, None),
    ],
)
def test_rejection_reason(make_bronze, overrides, reason):
    df = make_bronze([trip(**overrides)]).withColumn("reason", rejection_reason())
    assert df.first()["reason"] == reason


def test_trip_id_is_stable_and_detects_duplicates(make_bronze):
    df = with_trip_id(make_bronze([trip(), trip(), trip(total_amount=99.0)]))
    ids = [r["trip_id"] for r in df.collect()]

    assert ids[0] == ids[1] != ids[2]
    assert len(ids[0]) == 64  # sha256 hex


def test_enrich_derives_columns(make_bronze):
    row = enrich(with_trip_id(make_bronze([trip()]))).first()

    assert row["duration_min"] == 15.0
    assert row["avg_speed_mph"] == 12.0  # 3 mil dalam 15 menit
    assert row["tip_pct"] == 20.0
    assert row["payment_type_desc"] == "Credit card"
    assert row["pickup_month"] == "2024-01"
    assert (row["pickup_hour"], row["pickup_dow"]) == (8, 2)  # Senin


def test_build_month_splits_dedupes_and_is_idempotent(spark, make_bronze, tmp_path):
    bronze, silver, quarantine = (str(tmp_path / n) for n in ("bronze", "silver", "quarantine"))
    rows = [
        trip(),
        trip(),  # duplikat persis
        trip(total_amount=30.0),
        trip(trip_distance=0.0),
        trip(fare_amount=-1.0),
    ]
    make_bronze(rows).withColumn("_ingested_at", F.current_timestamp()).write.format("delta").save(
        bronze
    )

    first = build_month(spark, bronze, silver, quarantine, "2024-01")
    second = build_month(spark, bronze, silver, quarantine, "2024-01")

    assert (first.input_rows, first.valid_rows, first.duplicate_rows) == (5, 2, 1)
    assert first.rejected == {"non_positive_distance": 1, "negative_amount": 1}
    assert second == first
    assert spark.read.format("delta").load(silver).count() == 2
    assert spark.read.format("delta").load(quarantine).count() == 2
