"""Sink Postgres: upsert hasil agregasi tiap micro-batch."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import psycopg2
from psycopg2.extras import execute_values

from orders_stream.config import PostgresSettings

DDL = """
create schema if not exists stream;
create table if not exists stream.order_metrics_1m (
    window_start    timestamptz not null,
    window_end      timestamptz not null,
    city            text        not null,
    payment_method  text        not null,
    orders          bigint      not null,
    items           bigint      not null,
    gmv_idr         bigint      not null,
    updated_at      timestamptz not null default now(),
    primary key (window_start, city, payment_method)
);
create index if not exists order_metrics_1m_window_idx on stream.order_metrics_1m (window_start);
"""

COLUMNS = ["window_start", "window_end", "city", "payment_method", "orders", "items", "gmv_idr"]

UPSERT = f"""
insert into stream.order_metrics_1m ({", ".join(COLUMNS)}) values %s
on conflict (window_start, city, payment_method) do update set
    orders = excluded.orders,
    items = excluded.items,
    gmv_idr = excluded.gmv_idr,
    updated_at = now()
"""


def connect(settings: PostgresSettings):
    return psycopg2.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
    )


def ensure_tables(settings: PostgresSettings) -> None:
    conn = connect(settings)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL)
    finally:
        conn.close()


def upsert_metrics(conn, rows: Iterable[Sequence]) -> int:
    """Mode output `update` mengirim nilai agregat terbaru per key, jadi upsert
    (bukan increment) memberi hasil yang benar dan aman bila batch diulang."""
    values = [list(r) for r in rows]
    if not values:
        return 0
    with conn, conn.cursor() as cur:
        execute_values(cur, UPSERT, values)
    return len(values)


def make_batch_writer(settings: PostgresSettings):
    def write_batch(batch_df, batch_id: int) -> None:
        rows = [tuple(r[c] for c in COLUMNS) for r in batch_df.select(*COLUMNS).collect()]
        conn = connect(settings)
        try:
            upsert_metrics(conn, rows)
        finally:
            conn.close()

    return write_batch
