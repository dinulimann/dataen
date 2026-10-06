import pytest

from weather_pipeline.transform import COLUMN_MAP, PayloadError, flatten_daily


def test_flatten_returns_one_row_per_day(jakarta_payload):
    rows = flatten_daily(jakarta_payload, "jakarta")

    assert len(rows) == len(jakarta_payload["daily"]["time"])
    first = rows[0]
    assert first["city_id"] == "jakarta"
    assert first["obs_date"] == jakarta_payload["daily"]["time"][0]
    assert first["temperature_max"] == jakarta_payload["daily"]["temperature_2m_max"][0]
    assert set(first) == {"city_id", "obs_date", *COLUMN_MAP.values()}


def test_flatten_rejects_missing_daily_block():
    with pytest.raises(PayloadError, match="daily.time"):
        flatten_daily({"latitude": 0}, "jakarta")


def test_flatten_rejects_missing_variable(jakarta_payload):
    del jakarta_payload["daily"]["precipitation_sum"]
    with pytest.raises(PayloadError, match="precipitation_sum"):
        flatten_daily(jakarta_payload, "jakarta")


def test_flatten_rejects_length_mismatch(jakarta_payload):
    jakarta_payload["daily"]["temperature_2m_max"].pop()
    with pytest.raises(PayloadError, match="panjang"):
        flatten_daily(jakarta_payload, "jakarta")
