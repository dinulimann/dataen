"""Konfigurasi pipeline yang dibaca dari environment variable."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path

# Seed dbt dipakai sebagai satu-satunya sumber daftar kota,
# sehingga ingestion dan dimensi di warehouse selalu konsisten.
DEFAULT_CITIES_CSV = Path(__file__).resolve().parents[2] / "dbt" / "seeds" / "cities.csv"


@dataclass(frozen=True)
class City:
    city_id: str
    city_name: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class LakeSettings:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str

    @classmethod
    def from_env(cls) -> LakeSettings:
        return cls(
            endpoint_url=os.getenv("LAKE_ENDPOINT_URL", "http://localhost:9000"),
            access_key=os.getenv("LAKE_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("LAKE_SECRET_KEY", "minioadmin"),
            bucket=os.getenv("LAKE_BUCKET", "raw"),
        )


@dataclass(frozen=True)
class WarehouseSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> WarehouseSettings:
        return cls(
            host=os.getenv("WAREHOUSE_HOST", "localhost"),
            port=int(os.getenv("WAREHOUSE_PORT", "5432")),
            dbname=os.getenv("WAREHOUSE_DB", "warehouse"),
            user=os.getenv("WAREHOUSE_USER", "postgres"),
            password=os.getenv("WAREHOUSE_PASSWORD", "postgres"),
        )


def load_cities(path: str | Path | None = None) -> list[City]:
    csv_path = Path(path or os.getenv("CITIES_CSV") or DEFAULT_CITIES_CSV)
    with csv_path.open(newline="", encoding="utf-8") as f:
        return [
            City(
                city_id=row["city_id"],
                city_name=row["city_name"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
            )
            for row in csv.DictReader(f)
        ]
