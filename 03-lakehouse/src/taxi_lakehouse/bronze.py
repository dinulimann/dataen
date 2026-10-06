"""Bronze: salin data mentah apa adanya + metadata ingestion.

Satu-satunya perubahan adalah *schema enforcement*: nama kolom diseragamkan
(huruf kecil) dan tipe di-cast ke skema tetap, karena file TLC antarbulan
tidak konsisten (mis. `passenger_count` kadang double, `Airport_fee` vs
`airport_fee`). Ini membuat tabel bronze bisa di-append lintas bulan.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# kolom (huruf kecil) -> tipe di bronze
BRONZE_COLUMNS = {
    "vendorid": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "ratecodeid": "int",
    "store_and_fwd_flag": "string",
    "pulocationid": "int",
    "dolocationid": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}


def conform(raw: DataFrame) -> DataFrame:
    lower = {c.lower(): c for c in raw.columns}
    return raw.select(
        *[
            (F.col(lower[name]) if name in lower else F.lit(None)).cast(dtype).alias(name)
            for name, dtype in BRONZE_COLUMNS.items()
        ]
    )


def ingest_month(spark: SparkSession, source_path: str, month: str, table_path: str) -> int:
    """Muat satu file bulanan. Idempoten: partisi bulan itu ditimpa, bulan lain tidak tersentuh."""
    df = (
        conform(spark.read.parquet(source_path))
        .withColumn("_source_file", F.lit(source_path.rsplit("/", 1)[-1]))
        .withColumn("_source_month", F.lit(month))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_source_month = '{month}'")
        .partitionBy("_source_month")
        .save(table_path)
    )
    return (
        spark.read.format("delta").load(table_path).where(F.col("_source_month") == month).count()
    )
