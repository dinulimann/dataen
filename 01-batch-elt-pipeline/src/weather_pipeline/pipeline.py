"""Langkah-langkah pipeline tingkat tinggi yang dipanggil oleh DAG Airflow."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from datetime import date
from typing import Any, Protocol

from weather_pipeline.config import City
from weather_pipeline.extract import fetch_daily
from weather_pipeline.lake import raw_object_key
from weather_pipeline.load import ensure_raw_table, upsert_rows
from weather_pipeline.transform import flatten_daily

log = logging.getLogger(__name__)


class LakeLike(Protocol):
    def put_json(self, key: str, payload: dict[str, Any]) -> None: ...
    def get_json(self, key: str) -> dict[str, Any]: ...


def extract_to_lake(
    day: date,
    cities: Iterable[City],
    lake: LakeLike,
    fetch: Callable[[City, date], dict[str, Any]] = fetch_daily,
) -> list[str]:
    keys = []
    for city in cities:
        payload = fetch(city, day)
        key = raw_object_key(city.city_id, day)
        lake.put_json(key, payload)
        log.info("saved %s", key)
        keys.append(key)
    return keys


def city_id_from_key(key: str) -> str:
    # .../city=<id>.json
    return key.rsplit("city=", 1)[1].removesuffix(".json")


def load_from_lake(keys: Iterable[str], lake: LakeLike, conn) -> int:
    """Muat semua file dalam satu transaksi: sukses semua atau tidak sama sekali."""
    total = 0
    with conn:
        ensure_raw_table(conn)
        for key in keys:
            rows = flatten_daily(lake.get_json(key), city_id_from_key(key))
            total += upsert_rows(conn, rows, source_file=key)
    log.info("upserted %d rows", total)
    return total
