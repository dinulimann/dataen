import json
import random
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from orders_stream.generator import WIB, make_order
from orders_stream.transforms import (
    aggregate_metrics,
    invalid_orders,
    parse_orders,
    to_dead_letter,
    valid_orders,
)

T0 = datetime(2026, 10, 6, 12, 0, 10, tzinfo=WIB)


def order(**overrides):
    o = make_order(random.Random(0), T0)
    o.update(overrides)
    return o


def to_df(spark, values):
    return spark.createDataFrame([(v,) for v in values], "value string")


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ('{"order_id": "x", "amount', "malformed_json"),
        (json.dumps({k: v for k, v in order().items() if k != "order_id"}), "missing_order_id"),
        (json.dumps(order(event_time="kemarin sore")), "invalid_event_time"),
        (json.dumps(order(city=None)), "missing_city"),
        (json.dumps(order(payment_method="barter")), "unknown_payment_method"),
        (json.dumps(order(amount_idr=-5000)), "non_positive_amount"),
        (json.dumps(order(quantity=0)), "invalid_quantity"),
        (json.dumps(order()), None),
    ],
)
def test_error_reason(spark, payload, reason):
    row = parse_orders(to_df(spark, [payload])).collect()[0]
    assert row["error_reason"] == reason


def test_valid_and_invalid_split_is_complete(spark):
    values = [json.dumps(order(order_id=str(i))) for i in range(5)] + ["not json", "{}"]
    parsed = parse_orders(to_df(spark, values))

    assert valid_orders(parsed).count() == 5
    assert invalid_orders(parsed).count() == 2
    assert "error_reason" not in valid_orders(parsed).columns


def test_dead_letter_keeps_original_payload(spark):
    parsed = parse_orders(to_df(spark, ["not json"]))
    msg = json.loads(to_dead_letter(invalid_orders(parsed)).collect()[0]["value"])

    assert msg == {"raw_value": "not json", "error_reason": "malformed_json"}


def test_timestamp_parsed_in_wib(spark):
    # Format di sisi Spark: collect() mengonversi ke zona waktu lokal Python, bukan sesi Spark.
    valid = valid_orders(parse_orders(to_df(spark, [json.dumps(order())])))
    row = valid.select(F.date_format("event_time", "HH:mm:ss").alias("t")).collect()[0]
    assert row["t"] == "12:00:10"  # session TZ = Asia/Jakarta


def test_aggregate_metrics_per_window_city_payment(spark):
    events = [
        order(order_id="a", city="Jakarta", payment_method="qris", quantity=1, amount_idr=10_000),
        order(order_id="b", city="Jakarta", payment_method="qris", quantity=2, amount_idr=30_000),
        order(order_id="c", city="Bandung", payment_method="cod", quantity=1, amount_idr=5_000),
        # menit berikutnya -> jendela berbeda
        order(
            order_id="d",
            city="Jakarta",
            payment_method="qris",
            quantity=1,
            amount_idr=7_000,
            event_time="2026-10-06T12:01:30.000+07:00",
        ),
    ]
    valid = valid_orders(parse_orders(to_df(spark, [json.dumps(e) for e in events])))
    rows = {
        (r["window_start"], r["city"], r["payment_method"]): r
        for r in aggregate_metrics(valid, "1 minute", "10 minutes").collect()
    }

    jkt = rows[("2026-10-06T12:00:00+07:00", "Jakarta", "qris")]
    assert (jkt["orders"], jkt["items"], jkt["gmv_idr"]) == (2, 3, 40_000)
    assert jkt["window_end"] == "2026-10-06T12:01:00+07:00"
    assert rows[("2026-10-06T12:00:00+07:00", "Bandung", "cod")]["gmv_idr"] == 5_000
    assert rows[("2026-10-06T12:01:00+07:00", "Jakarta", "qris")]["orders"] == 1
    assert len(rows) == 3
