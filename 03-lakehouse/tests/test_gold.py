from datetime import datetime, timedelta

import pytest
from conftest import trip

from taxi_lakehouse import gold, quality
from taxi_lakehouse.silver import enrich, with_trip_id


@pytest.fixture
def trips(make_bronze):
    mon = datetime(2024, 1, 15, 8)  # Senin
    tue = datetime(2024, 1, 16, 8)
    rows = [
        trip(
            tpep_pickup_datetime=mon,
            tpep_dropoff_datetime=mon + timedelta(minutes=10),
            pulocationid=161,
            payment_type=1,
            fare_amount=10.0,
            tip_amount=2.0,
            total_amount=15.0,
        ),
        trip(
            tpep_pickup_datetime=mon,
            tpep_dropoff_datetime=mon + timedelta(minutes=20),
            pulocationid=161,
            payment_type=2,
            fare_amount=20.0,
            tip_amount=0.0,
            total_amount=25.0,
            dolocationid=132,
        ),
        trip(
            tpep_pickup_datetime=tue,
            tpep_dropoff_datetime=tue + timedelta(minutes=30),
            pulocationid=132,
            payment_type=1,
            fare_amount=50.0,
            tip_amount=5.0,
            total_amount=70.0,
        ),
    ]
    return enrich(with_trip_id(make_bronze(rows)))


@pytest.fixture
def zones(spark):
    return spark.createDataFrame(
        [
            (161, "Manhattan", "Midtown Center"),
            (132, "Queens", "JFK Airport"),
            (237, "Manhattan", "Upper East Side South"),
        ],
        "location_id int, borough string, zone string",
    )


def test_daily_zone_metrics(trips, zones):
    rows = {
        (str(r["pickup_date"]), r["pu_zone"]): r
        for r in gold.daily_zone_metrics(trips, zones).collect()
    }

    midtown = rows[("2024-01-15", "Midtown Center")]
    assert (midtown["trips"], midtown["revenue_usd"], midtown["avg_fare_usd"]) == (2, 40.0, 15.0)
    assert midtown["avg_card_tip_pct"] == 20.0  # tip tunai tidak dihitung
    assert rows[("2024-01-16", "JFK Airport")]["pu_borough"] == "Queens"


def test_hourly_demand_averages_per_day(trips):
    rows = {(r["pickup_dow"], r["pickup_hour"]): r for r in gold.hourly_demand(trips).collect()}
    assert rows[(2, 8)]["avg_trips_per_day"] == 2.0  # 2 perjalanan dalam 1 hari Senin
    assert rows[(3, 8)]["avg_trips_per_day"] == 1.0


def test_payment_share_sums_to_100(trips):
    rows = gold.payment_summary(trips).collect()
    assert sum(r["share_pct"] for r in rows) == pytest.approx(100.0)
    assert {r["payment_type_desc"]: r["trips"] for r in rows} == {"Credit card": 2, "Cash": 1}


def test_top_routes_ranked(trips, zones):
    rows = gold.top_routes(trips, zones).orderBy("rank").collect()
    assert [r["rank"] for r in rows] == [1, 2, 3]
    assert all(r["trips"] == 1 for r in rows)


def test_quality_enforce_raises_on_failure(trips, zones):
    daily = gold.daily_zone_metrics(trips, zones)
    quality.enforce([quality.check_gold_reconciles(trips, daily)])  # lolos

    with pytest.raises(quality.DataQualityError, match="rejection_rate_2024-01"):
        quality.enforce([quality.check_rejection_rate("2024-01", 100, 50, max_ratio=0.1)])
