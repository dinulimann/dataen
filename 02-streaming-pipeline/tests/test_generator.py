import json
import random
from collections import Counter
from datetime import datetime

from orders_stream.generator import (
    INVALID,
    LATE,
    ON_TIME,
    VERY_LATE,
    WIB,
    generate_event,
    make_order,
)
from orders_stream.schema import CITIES, PAYMENT_METHODS

NOW = datetime(2026, 10, 6, 12, 0, tzinfo=WIB)


def test_make_order_has_valid_fields():
    order = make_order(random.Random(1), NOW)

    assert order["city"] in CITIES
    assert order["payment_method"] in PAYMENT_METHODS
    assert 1 <= order["quantity"] <= 5
    assert order["amount_idr"] > 0 and order["amount_idr"] % 500 == 0
    assert order["event_time"] == "2026-10-06T12:00:00.000+07:00"


def test_generator_is_deterministic_with_seed():
    a = [generate_event(random.Random(7), NOW) for _ in range(5)]
    b = [generate_event(random.Random(7), NOW) for _ in range(5)]
    assert a == b


def test_event_kind_ratios_roughly_match_config():
    rng = random.Random(42)
    kinds = Counter(
        generate_event(rng, NOW, late_ratio=0.1, very_late_ratio=0.05, invalid_ratio=0.1).kind
        for _ in range(10_000)
    )
    assert abs(kinds[INVALID] / 10_000 - 0.10) < 0.02
    assert abs(kinds[VERY_LATE] / 10_000 - 0.05) < 0.02
    assert abs(kinds[LATE] / 10_000 - 0.10) < 0.02
    assert kinds[ON_TIME] > 6_000


def test_late_events_are_in_the_past():
    rng = random.Random(3)
    for _ in range(500):
        event = generate_event(rng, NOW, late_ratio=0.5, very_late_ratio=0.5, invalid_ratio=0)
        delay = (NOW - datetime.fromisoformat(json.loads(event.value)["event_time"])).seconds / 60
        if event.kind == LATE:
            assert 1 <= delay <= 8
        elif event.kind == VERY_LATE:
            assert 15 <= delay <= 60
