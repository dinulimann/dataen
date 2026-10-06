"""Penjadwalan lakehouse NYC Taxi (proyek 03) dari Airflow."""

from __future__ import annotations

import re
from datetime import date

import requests

TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"
STAGES = ("bronze", "silver", "gold")


def resolve_month(requested: str | None, run_date: date, lag_months: int) -> str:
    """Bulan yang diproses: yang diminta saat trigger manual, atau `run_date` mundur
    `lag_months` bulan (NYC TLC menerbitkan data dengan jeda sekitar 2 bulan)."""
    if requested:
        if not re.match(MONTH_PATTERN, requested):
            raise ValueError(f"format bulan harus YYYY-MM, bukan {requested!r}")
        return requested
    index = run_date.year * 12 + (run_date.month - 1) - lag_months
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def source_url(month: str) -> str:
    return TLC_URL.format(month=month)


def is_published(month: str, session: requests.Session | None = None, timeout: float = 30) -> bool:
    http = session or requests.Session()
    response = http.head(source_url(month), timeout=timeout, allow_redirects=True)
    return response.status_code == 200


def stage_command(stage: str, month: str) -> str:
    if stage not in STAGES:
        raise ValueError(f"stage harus salah satu dari {STAGES}, bukan {stage!r}")
    return f"run --months {month} --stage {stage}"
