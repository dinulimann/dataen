"""Muat payload contoh (tests/fixtures) ke warehouse tanpa memanggil API.

Dipakai di CI dan untuk mencoba model dbt secara lokal:
    python scripts/load_fixtures.py
"""

from __future__ import annotations

import json
from pathlib import Path

from weather_pipeline.config import WarehouseSettings
from weather_pipeline.load import connect, ensure_raw_table, upsert_rows
from weather_pipeline.transform import flatten_daily

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures"


def main() -> None:
    conn = connect(WarehouseSettings.from_env())
    try:
        with conn:
            ensure_raw_table(conn)
            for path in sorted(FIXTURES.glob("open_meteo_*.json")):
                city_id = path.stem.removeprefix("open_meteo_")
                rows = flatten_daily(json.loads(path.read_text()), city_id)
                n = upsert_rows(conn, rows, source_file=f"fixtures/{path.name}")
                print(f"{path.name}: {n} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
