from datetime import datetime

from pyspark.sql import functions as F

from taxi_lakehouse.bronze import BRONZE_COLUMNS, conform, ingest_month
from taxi_lakehouse.sample import write_month


def test_conform_handles_schema_drift(spark):
    # Nama kolom beda huruf besar/kecil, tipe beda, dan kolom yang hilang.
    raw = spark.createDataFrame(
        [(2, datetime(2024, 1, 1, 10), 1.0, 7.5)],
        "VendorID long, tpep_pickup_datetime timestamp, passenger_count double, Airport_fee double",
    )
    row = conform(raw).collect()[0]

    assert conform(raw).columns == list(BRONZE_COLUMNS)
    assert row["vendorid"] == 2
    assert row["passenger_count"] == 1 and isinstance(row["passenger_count"], int)
    assert row["airport_fee"] == 7.5
    assert row["total_amount"] is None


def test_ingest_is_idempotent_per_month(spark, tmp_path):
    table = str(tmp_path / "bronze")
    jan, feb = tmp_path / "jan.parquet", tmp_path / "feb.parquet"
    n_jan = write_month(jan, "2024-01", rows=300)
    n_feb = write_month(feb, "2024-02", rows=200)

    ingest_month(spark, str(jan), "2024-01", table)
    ingest_month(spark, str(feb), "2024-02", table)
    ingest_month(spark, str(jan), "2024-01", table)  # diulang

    counts = {
        r["_source_month"]: r["count"]
        for r in spark.read.format("delta").load(table).groupBy("_source_month").count().collect()
    }
    assert counts == {"2024-01": n_jan, "2024-02": n_feb}


def test_ingest_adds_lineage_columns(spark, tmp_path):
    table = str(tmp_path / "bronze")
    src = tmp_path / "yellow_tripdata_2024-01.parquet"
    write_month(src, "2024-01", rows=10)
    ingest_month(spark, str(src), "2024-01", table)

    row = (
        spark.read.format("delta")
        .load(table)
        .select("_source_file", "_source_month", F.col("_ingested_at").isNotNull().alias("has_ts"))
        .first()
    )
    assert (row["_source_file"], row["_source_month"], row["has_ts"]) == (
        "yellow_tripdata_2024-01.parquet",
        "2024-01",
        True,
    )
