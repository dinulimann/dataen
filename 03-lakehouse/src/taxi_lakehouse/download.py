"""Unduh data asli NYC TLC Yellow Taxi (format Parquet, ~50 MB per bulan)."""

from __future__ import annotations

import logging
import shutil
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
log = logging.getLogger(__name__)


def _fetch(url: str, dest: Path) -> None:
    if dest.exists():
        log.info("sudah ada, dilewati: %s", dest)
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    log.info("mengunduh %s", url)
    with urllib.request.urlopen(url, timeout=60) as resp, tmp.open("wb") as f:
        shutil.copyfileobj(resp, f)
    tmp.rename(dest)  # file baru terlihat setelah unduhan lengkap


def download(months: list[str], raw_dir: Path) -> None:
    _fetch(f"{BASE_URL}/misc/taxi_zone_lookup.csv", raw_dir / "taxi_zone_lookup.csv")
    for month in months:
        name = f"yellow_tripdata_{month}.parquet"
        _fetch(f"{BASE_URL}/trip-data/{name}", raw_dir / name)
