"""Load: upsert baris ke tabel raw di PostgreSQL (idempoten)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from weather_pipeline.config import WarehouseSettings
from weather_pipeline.transform import COLUMN_MAP

RAW_SCHEMA = "raw"
RAW_TABLE = "open_meteo_daily"

DDL = f"""
create schema if not exists {RAW_SCHEMA};
create table if not exists {RAW_SCHEMA}.{RAW_TABLE} (
    city_id             text        not null,
    obs_date            date        not null,
    weather_code        integer,
    temperature_max     numeric(5, 2),
    temperature_min     numeric(5, 2),
    temperature_mean    numeric(5, 2),
    precipitation_sum   numeric(7, 2),
    precipitation_hours numeric(5, 2),
    wind_speed_max      numeric(6, 2),
    source_file         text        not null,
    ingested_at         timestamptz not null default now(),
    primary key (city_id, obs_date)
);
"""

COLUMNS = ["city_id", "obs_date", *COLUMN_MAP.values(), "source_file"]


def connect(settings: WarehouseSettings):
    return psycopg2.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
    )


def ensure_raw_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)


def upsert_rows(conn, rows: Sequence[dict[str, Any]], source_file: str) -> int:
    """Insert atau update berdasarkan (city_id, obs_date).

    Menjalankan ulang DAG untuk tanggal yang sama tidak membuat duplikat;
    ingested_at diperbarui supaya model incremental dbt ikut memproses ulang.
    """
    if not rows:
        return 0
    values = [[*(row[c] for c in COLUMNS[:-1]), source_file] for row in rows]
    updates = ", ".join(f"{c} = excluded.{c}" for c in COLUMNS[2:])
    sql = (
        f"insert into {RAW_SCHEMA}.{RAW_TABLE} ({', '.join(COLUMNS)}) values %s "
        f"on conflict (city_id, obs_date) do update set {updates}, ingested_at = now()"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    return len(values)
