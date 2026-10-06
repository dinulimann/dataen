"""Extract: ambil data cuaca harian dari Open-Meteo API (gratis, tanpa API key)."""

from __future__ import annotations

from datetime import date
from typing import Any

import requests

from weather_pipeline.config import City

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TIMEZONE = "Asia/Jakarta"

DAILY_VARIABLES = (
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
)


def build_params(city: City, start: date, end: date | None = None) -> dict[str, Any]:
    return {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": TIMEZONE,
        "start_date": start.isoformat(),
        "end_date": (end or start).isoformat(),
    }


def fetch_daily(
    city: City,
    start: date,
    end: date | None = None,
    session: requests.Session | None = None,
    timeout: float = 30,
) -> dict[str, Any]:
    """Kembalikan payload JSON mentah apa adanya; parsing dilakukan saat load."""
    http = session or requests.Session()
    response = http.get(OPEN_METEO_URL, params=build_params(city, start, end), timeout=timeout)
    response.raise_for_status()
    return response.json()
