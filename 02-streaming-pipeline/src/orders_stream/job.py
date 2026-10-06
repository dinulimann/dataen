"""Spark Structured Streaming job.

Satu sumber (topik Kafka `orders`) -> tiga query streaming:
  1. order_metrics : agregasi per menit -> Postgres (upsert per micro-batch)
  2. dead_letter   : event tidak valid  -> topik Kafka `orders.dlq`
  3. raw_archive   : event valid        -> Parquet terpartisi per tanggal (untuk replay)
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from orders_stream.config import KafkaSettings, PostgresSettings, StreamSettings
from orders_stream.sink import ensure_tables, make_batch_writer
from orders_stream.transforms import (
    aggregate_metrics,
    invalid_orders,
    parse_orders,
    to_dead_letter,
    valid_orders,
)

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"

log = logging.getLogger("orders_stream")


class ProgressLogger(StreamingQueryListener):
    """Log throughput dan jumlah event yang dibuang karena terlambat di luar watermark."""

    def onQueryStarted(self, event):
        log.info("query started: %s", event.name)

    def onQueryProgress(self, event):
        p = event.progress
        dropped = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        log.info(
            "%s batch=%d input_rows=%d rows_per_sec=%.1f dropped_late=%d",
            p.name,
            p.batchId,
            p.numInputRows,
            p.processedRowsPerSecond or 0,
            dropped,
        )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.warning("query terminated: %s exception=%s", event.id, event.exception)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("orders-stream")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "Asia/Jakarta")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    kafka, pg, cfg = (
        KafkaSettings.from_env(),
        PostgresSettings.from_env(),
        StreamSettings.from_env(),
    )

    ensure_tables(pg)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", kafka.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
    )
    parsed = parse_orders(raw)
    valid = valid_orders(parsed)

    (
        aggregate_metrics(valid, cfg.window, cfg.watermark)
        .writeStream.queryName("order_metrics")
        .outputMode("update")
        .foreachBatch(make_batch_writer(pg))
        .option("checkpointLocation", f"{cfg.checkpoint_dir}/order_metrics")
        .trigger(processingTime=cfg.trigger_interval)
        .start()
    )
    (
        to_dead_letter(invalid_orders(parsed))
        .writeStream.queryName("dead_letter")
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("topic", kafka.dlq_topic)
        .option("checkpointLocation", f"{cfg.checkpoint_dir}/dead_letter")
        .trigger(processingTime=cfg.trigger_interval)
        .start()
    )
    (
        valid.withColumn("event_date", F.to_date("event_time"))
        .writeStream.queryName("raw_archive")
        .format("parquet")
        .partitionBy("event_date")
        .option("path", cfg.archive_dir)
        .option("checkpointLocation", f"{cfg.checkpoint_dir}/raw_archive")
        .trigger(processingTime="1 minute")
        .start()
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
