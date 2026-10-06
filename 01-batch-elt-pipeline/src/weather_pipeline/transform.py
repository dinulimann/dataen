"""Flatten payload Open-Meteo (array paralel per variabel) menjadi baris per hari."""

from __future__ import annotations

from typing import Any

# nama variabel API -> nama kolom di tabel raw
COLUMN_MAP = {
    "weather_code": "weather_code",
    "temperature_2m_max": "temperature_max",
    "temperature_2m_min": "temperature_min",
    "temperature_2m_mean": "temperature_mean",
    "precipitation_sum": "precipitation_sum",
    "precipitation_hours": "precipitation_hours",
    "wind_speed_10m_max": "wind_speed_max",
}


class PayloadError(ValueError):
    """Payload dari API tidak sesuai kontrak yang diharapkan."""


def flatten_daily(payload: dict[str, Any], city_id: str) -> list[dict[str, Any]]:
    daily = payload.get("daily")
    if not daily or "time" not in daily:
        raise PayloadError(f"payload untuk {city_id!r} tidak punya blok 'daily.time'")

    dates = daily["time"]
    for api_name in COLUMN_MAP:
        values = daily.get(api_name)
        if values is None:
            raise PayloadError(f"variabel {api_name!r} hilang untuk {city_id!r}")
        if len(values) != len(dates):
            raise PayloadError(
                f"panjang {api_name!r} ({len(values)}) != jumlah tanggal ({len(dates)})"
            )

    return [
        {
            "city_id": city_id,
            "obs_date": day,
            **{col: daily[api_name][i] for api_name, col in COLUMN_MAP.items()},
        }
        for i, day in enumerate(dates)
    ]
