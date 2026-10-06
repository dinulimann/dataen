from datetime import datetime, timedelta

import pytest

from taxi_lakehouse.bronze import BRONZE_COLUMNS
from taxi_lakehouse.spark import build_spark


@pytest.fixture(scope="session")
def spark():
    session = build_spark("taxi-lakehouse-tests", shuffle_partitions=2)
    yield session
    session.stop()


T0 = datetime(2024, 1, 15, 8, 0, 0)


def trip(**overrides) -> dict:
    """Satu baris berbentuk tabel bronze yang valid; override untuk membuat kasus kotor."""
    row = {name: None for name in BRONZE_COLUMNS}
    row.update(
        vendorid=1,
        tpep_pickup_datetime=T0,
        tpep_dropoff_datetime=T0 + timedelta(minutes=15),
        passenger_count=1,
        trip_distance=3.0,
        pulocationid=161,
        dolocationid=237,
        payment_type=1,
        fare_amount=15.0,
        tip_amount=3.0,
        tolls_amount=0.0,
        total_amount=22.0,
        _source_month="2024-01",
    )
    row.update(overrides)
    return row


BRONZE_DDL = ", ".join(
    [f"{name} {dtype}" for name, dtype in BRONZE_COLUMNS.items()] + ["_source_month string"]
)


@pytest.fixture
def make_bronze(spark):
    def _make(rows: list[dict]):
        return spark.createDataFrame(
            [tuple(r[c] for c in [*BRONZE_COLUMNS, "_source_month"]) for r in rows], BRONZE_DDL
        )

    return _make
