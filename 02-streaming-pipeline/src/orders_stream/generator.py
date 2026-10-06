"""Generator event pesanan sintetis.

Sengaja menyisipkan tiga jenis "gangguan" yang umum di sistem nyata:
- event terlambat (masih di dalam watermark -> tetap dihitung),
- event sangat terlambat (di luar watermark -> dibuang oleh Spark),
- event rusak (JSON rusak / field tidak valid -> masuk dead-letter queue).
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from orders_stream.schema import CATEGORIES, CITIES, PAYMENT_METHODS

WIB = timezone(timedelta(hours=7))

ON_TIME, LATE, VERY_LATE, INVALID = "on_time", "late", "very_late", "invalid"


@dataclass(frozen=True)
class GeneratedEvent:
    key: str
    value: str
    kind: str


def _weighted(rng: random.Random, weights: dict[str, int]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def make_order(rng: random.Random, event_time: datetime) -> dict:
    quantity = rng.randint(1, 5)
    unit_price = int(rng.lognormvariate(11.5, 0.8)) // 500 * 500 + 500  # kelipatan Rp500
    return {
        "order_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "customer_id": f"C{rng.randint(1, 50_000):06d}",
        "event_time": event_time.isoformat(timespec="milliseconds"),
        "city": _weighted(rng, CITIES),
        "payment_method": _weighted(rng, PAYMENT_METHODS),
        "category": rng.choice(CATEGORIES),
        "quantity": quantity,
        "amount_idr": quantity * unit_price,
    }


def _corrupt(rng: random.Random, order: dict) -> str:
    variant = rng.randrange(4)
    if variant == 0:
        return json.dumps(order)[:-7]  # JSON terpotong
    if variant == 1:
        order.pop("order_id")
    elif variant == 2:
        order["amount_idr"] = -order["amount_idr"]
    else:
        order["payment_method"] = "barter"
    return json.dumps(order)


def generate_event(
    rng: random.Random,
    now: datetime | None = None,
    late_ratio: float = 0.05,
    very_late_ratio: float = 0.01,
    invalid_ratio: float = 0.02,
) -> GeneratedEvent:
    now = now or datetime.now(WIB)
    roll = rng.random()
    if roll < invalid_ratio:
        kind, event_time = INVALID, now
    elif roll < invalid_ratio + very_late_ratio:
        kind, event_time = VERY_LATE, now - timedelta(minutes=rng.uniform(15, 60))
    elif roll < invalid_ratio + very_late_ratio + late_ratio:
        kind, event_time = LATE, now - timedelta(minutes=rng.uniform(1, 8))
    else:
        kind, event_time = ON_TIME, now - timedelta(seconds=rng.uniform(0, 5))

    order = make_order(rng, event_time)
    value = _corrupt(rng, order) if kind == INVALID else json.dumps(order)
    return GeneratedEvent(key=order.get("order_id", "unknown"), value=value, kind=kind)
