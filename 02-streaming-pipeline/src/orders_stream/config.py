"""Konfigurasi dari environment variable."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic: str
    dlq_topic: str

    @classmethod
    def from_env(cls) -> KafkaSettings:
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("KAFKA_TOPIC", "orders"),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "orders.dlq"),
        )


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> PostgresSettings:
        return cls(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432")),
            dbname=os.getenv("PG_DB", "streaming"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD", "postgres"),
        )


@dataclass(frozen=True)
class StreamSettings:
    window: str
    watermark: str
    trigger_interval: str
    checkpoint_dir: str
    archive_dir: str

    @classmethod
    def from_env(cls) -> StreamSettings:
        return cls(
            window=os.getenv("STREAM_WINDOW", "1 minute"),
            watermark=os.getenv("STREAM_WATERMARK", "10 minutes"),
            trigger_interval=os.getenv("STREAM_TRIGGER", "10 seconds"),
            checkpoint_dir=os.getenv("CHECKPOINT_DIR", "/tmp/orders-stream/checkpoints"),
            archive_dir=os.getenv("ARCHIVE_DIR", "/tmp/orders-stream/archive"),
        )


@dataclass(frozen=True)
class ProducerSettings:
    events_per_second: float
    late_ratio: float
    very_late_ratio: float
    invalid_ratio: float

    @classmethod
    def from_env(cls) -> ProducerSettings:
        return cls(
            events_per_second=float(os.getenv("EVENTS_PER_SECOND", "20")),
            late_ratio=float(os.getenv("LATE_RATIO", "0.05")),
            very_late_ratio=float(os.getenv("VERY_LATE_RATIO", "0.01")),
            invalid_ratio=float(os.getenv("INVALID_RATIO", "0.02")),
        )
