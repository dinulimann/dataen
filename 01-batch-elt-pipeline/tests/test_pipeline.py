from datetime import date

from weather_pipeline.config import City
from weather_pipeline.lake import raw_object_key
from weather_pipeline.pipeline import city_id_from_key, extract_to_lake

CITIES = [City("jakarta", "Jakarta", -6.2, 106.8), City("medan", "Medan", 3.6, 98.7)]


def test_raw_object_key_is_partitioned_by_date():
    key = raw_object_key("jakarta", date(2026, 10, 1))
    assert key == "open_meteo/daily/date=2026-10-01/city=jakarta.json"
    assert city_id_from_key(key) == "jakarta"


def test_extract_to_lake_writes_one_object_per_city(fake_lake, jakarta_payload):
    calls = []

    def fake_fetch(city, day):
        calls.append((city.city_id, day))
        return jakarta_payload

    day = date(2026, 10, 1)
    keys = extract_to_lake(day, CITIES, fake_lake, fetch=fake_fetch)

    assert calls == [("jakarta", day), ("medan", day)]
    assert keys == [raw_object_key("jakarta", day), raw_object_key("medan", day)]
    assert set(fake_lake.objects) == set(keys)


def test_extract_to_lake_is_idempotent(fake_lake, jakarta_payload):
    day = date(2026, 10, 1)
    for _ in range(2):
        extract_to_lake(day, CITIES, fake_lake, fetch=lambda c, d: jakarta_payload)

    assert len(fake_lake.objects) == len(CITIES)
