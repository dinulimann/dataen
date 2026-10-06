"""Pembuat data contoh dengan skema yang sama persis dengan file NYC TLC.

Dipakai untuk test, CI, dan mencoba pipeline tanpa mengunduh ~50 MB per bulan.
Sengaja menyisipkan baris kotor (jarak 0, durasi negatif, tanggal salah,
nilai negatif, duplikat) agar aturan di lapisan silver benar-benar teruji.
"""

from __future__ import annotations

import calendar
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Beberapa zona nyata yang ramai; sisanya diberi nama generik.
KNOWN_ZONES = {
    1: ("EWR", "Newark Airport", "EWR"),
    48: ("Manhattan", "Clinton East", "Yellow Zone"),
    68: ("Manhattan", "East Chelsea", "Yellow Zone"),
    79: ("Manhattan", "East Village", "Yellow Zone"),
    132: ("Queens", "JFK Airport", "Airports"),
    138: ("Queens", "LaGuardia Airport", "Airports"),
    142: ("Manhattan", "Lincoln Square East", "Yellow Zone"),
    161: ("Manhattan", "Midtown Center", "Yellow Zone"),
    162: ("Manhattan", "Midtown East", "Yellow Zone"),
    186: ("Manhattan", "Penn Station/Madison Sq West", "Yellow Zone"),
    230: ("Manhattan", "Times Sq/Theatre District", "Yellow Zone"),
    236: ("Manhattan", "Upper East Side North", "Yellow Zone"),
    237: ("Manhattan", "Upper East Side South", "Yellow Zone"),
    264: ("Unknown", "N/A", "N/A"),
    265: ("N/A", "Outside of NYC", "N/A"),
}
BUSY = [161, 237, 236, 162, 230, 186, 142, 48, 68, 79, 132, 138]
BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
HOUR_WEIGHTS = [2, 1, 1, 1, 1, 2, 4, 6, 7, 6, 6, 6, 7, 7, 7, 7, 8, 9, 10, 9, 8, 7, 5, 3]


def write_zones(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        for i in range(1, 266):
            borough, zone, service = KNOWN_ZONES.get(
                i, (BOROUGHS[i % len(BOROUGHS)], f"Sample Zone {i}", "Boro Zone")
            )
            w.writerow([i, borough, zone, service])


def _clean_trip(rng: random.Random, year: int, month: int) -> dict:
    days = calendar.monthrange(year, month)[1]
    hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
    pickup = datetime(year, month, rng.randint(1, days), hour, rng.randrange(60), rng.randrange(60))
    distance = round(min(rng.lognormvariate(0.6, 0.8), 60), 2)
    speed_mph = max(3.0, rng.gauss(12 if hour in range(7, 20) else 18, 3))
    dropoff = pickup + timedelta(minutes=max(1.0, distance / speed_mph * 60 + rng.uniform(0, 4)))
    fare = round(3 + 2.5 * distance + 0.35 * (dropoff - pickup).seconds / 60, 2)
    payment = rng.choices([1, 2, 3, 4], weights=[75, 22, 2, 1])[0]
    tip = round(fare * rng.uniform(0.1, 0.3), 2) if payment == 1 else 0.0
    pu = rng.choice(BUSY) if rng.random() < 0.7 else rng.randint(1, 263)
    do = rng.choice(BUSY) if rng.random() < 0.6 else rng.randint(1, 263)
    airport_fee = 1.75 if pu in (132, 138) else 0.0
    return {
        "VendorID": rng.choice([1, 2]),
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": dropoff,
        "passenger_count": float(rng.choices([1, 2, 3, 4, 5, 6], weights=[70, 15, 5, 3, 4, 3])[0])
        if rng.random() > 0.03
        else None,
        "trip_distance": distance,
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": pu,
        "DOLocationID": do,
        "payment_type": payment,
        "fare_amount": fare,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": tip,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": round(fare + 1.0 + 0.5 + tip + 1.0 + 2.5 + airport_fee, 2),
        "congestion_surcharge": 2.5,
        "Airport_fee": airport_fee,
    }


def _make_dirty(rng: random.Random, trip: dict) -> dict:
    kind = rng.randrange(5)
    if kind == 0:
        trip["trip_distance"] = 0.0
    elif kind == 1:
        trip["tpep_dropoff_datetime"] = trip["tpep_pickup_datetime"] - timedelta(minutes=3)
    elif kind == 2:
        # Data TLC asli kadang memuat tanggal nyasar (mis. tahun 2002) di file bulan lain.
        shift = timedelta(days=22 * 365)
        trip["tpep_pickup_datetime"] -= shift
        trip["tpep_dropoff_datetime"] -= shift
    elif kind == 3:
        trip["fare_amount"] = -trip["fare_amount"]
        trip["total_amount"] = -trip["total_amount"]
    else:
        trip["tpep_dropoff_datetime"] = trip["tpep_pickup_datetime"] + timedelta(hours=9)
    return trip


def generate_month(
    month: str, rows: int, seed: int = 0, dirty_ratio: float = 0.03, duplicate_ratio: float = 0.005
) -> list[dict]:
    year, mon = map(int, month.split("-"))
    rng = random.Random(f"{month}-{seed}")
    trips = []
    for _ in range(rows):
        trip = _clean_trip(rng, year, mon)
        trips.append(_make_dirty(rng, trip) if rng.random() < dirty_ratio else trip)
    trips += [dict(t) for t in rng.sample(trips, int(rows * duplicate_ratio))]
    return trips


SCHEMA = pa.schema(
    [
        ("VendorID", pa.int32()),
        ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("passenger_count", pa.float64()),
        ("trip_distance", pa.float64()),
        ("RatecodeID", pa.float64()),
        ("store_and_fwd_flag", pa.string()),
        ("PULocationID", pa.int32()),
        ("DOLocationID", pa.int32()),
        ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()),
        ("extra", pa.float64()),
        ("mta_tax", pa.float64()),
        ("tip_amount", pa.float64()),
        ("tolls_amount", pa.float64()),
        ("improvement_surcharge", pa.float64()),
        ("total_amount", pa.float64()),
        ("congestion_surcharge", pa.float64()),
        ("Airport_fee", pa.float64()),
    ]
)


def write_month(path: Path, month: str, rows: int, seed: int = 0) -> int:
    trips = generate_month(month, rows, seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(trips, schema=SCHEMA), path)
    return len(trips)
