"""Transformasi Spark yang murni (tanpa I/O), sehingga bisa dites dengan DataFrame biasa."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from orders_stream.schema import ORDER_SCHEMA, PAYMENT_METHODS

KAFKA_META = ["topic", "partition", "offset", "kafka_timestamp"]


def parse_orders(raw: DataFrame) -> DataFrame:
    """Kolom `value` (bytes/string dari Kafka) -> kolom-kolom event + `error_reason`."""
    df = raw.select(
        F.col("value").cast("string").alias("raw_value"),
        *[c for c in ["topic", "partition", "offset"] if c in raw.columns],
        *([F.col("timestamp").alias("kafka_timestamp")] if "timestamp" in raw.columns else []),
    )
    parsed = df.withColumn("o", F.from_json("raw_value", ORDER_SCHEMA)).select("*", "o.*").drop("o")
    return parsed.withColumn("error_reason", _error_reason())


def _error_reason() -> Column:
    c = F.col
    return (
        F.when(
            c("order_id").isNull() & c("event_time").isNull() & c("amount_idr").isNull(),
            "malformed_json",
        )
        .when(c("order_id").isNull(), "missing_order_id")
        .when(c("event_time").isNull(), "invalid_event_time")
        .when(c("city").isNull(), "missing_city")
        .when(
            ~c("payment_method").isin(*PAYMENT_METHODS) | c("payment_method").isNull(),
            "unknown_payment_method",
        )
        .when(c("amount_idr").isNull() | (c("amount_idr") <= 0), "non_positive_amount")
        .when(c("quantity").isNull() | (c("quantity") <= 0), "invalid_quantity")
    )


def valid_orders(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("error_reason").isNull()).drop("error_reason", "raw_value")


def invalid_orders(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("error_reason").isNotNull())


def to_dead_letter(invalid: DataFrame) -> DataFrame:
    """Bentuk pesan untuk topik DLQ: payload asli + alasan + posisi di Kafka."""
    meta = [c for c in ["raw_value", "error_reason", *KAFKA_META] if c in invalid.columns]
    return invalid.select(
        F.col("order_id").alias("key"),
        F.to_json(F.struct(*meta)).alias("value"),
    )


def aggregate_metrics(valid: DataFrame, window: str, watermark: str) -> DataFrame:
    """Metrik per jendela waktu event-time, kota, dan metode pembayaran.

    Watermark membatasi berapa lama state disimpan: event yang datang lebih
    terlambat dari `watermark` dibuang dan dilaporkan oleh ProgressLogger.
    """
    w = F.window("event_time", window)
    return (
        valid.withWatermark("event_time", watermark)
        .groupBy(w.alias("w"), "city", "payment_method")
        .agg(
            F.count("*").alias("orders"),
            F.sum("quantity").alias("items"),
            F.sum("amount_idr").alias("gmv_idr"),
        )
        .select(
            # String dengan offset zona waktu: aman dikirim ke kolom timestamptz Postgres
            F.date_format("w.start", "yyyy-MM-dd'T'HH:mm:ssXXX").alias("window_start"),
            F.date_format("w.end", "yyyy-MM-dd'T'HH:mm:ssXXX").alias("window_end"),
            "city",
            "payment_method",
            "orders",
            "items",
            "gmv_idr",
        )
    )
