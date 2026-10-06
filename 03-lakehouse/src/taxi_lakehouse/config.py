"""Lokasi data dan tabel. Tabel Delta disimpan berbasis path (tanpa metastore)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    raw_dir: Path
    lakehouse_dir: Path

    @classmethod
    def from_env(cls) -> Paths:
        return cls(
            raw_dir=Path(os.getenv("RAW_DIR", "data/raw")),
            lakehouse_dir=Path(os.getenv("LAKEHOUSE_DIR", "lakehouse")),
        )

    def raw_trips(self, month: str) -> Path:
        return self.raw_dir / f"yellow_tripdata_{month}.parquet"

    @property
    def raw_zones(self) -> Path:
        return self.raw_dir / "taxi_zone_lookup.csv"

    def table(self, layer: str, name: str) -> str:
        return str(self.lakehouse_dir / layer / name)


BRONZE_TRIPS = ("bronze", "yellow_trips")
SILVER_TRIPS = ("silver", "trips")
SILVER_QUARANTINE = ("silver", "trips_quarantine")
SILVER_ZONES = ("silver", "zones")
GOLD_TABLES = ("daily_zone_metrics", "hourly_demand", "payment_summary", "top_routes")
