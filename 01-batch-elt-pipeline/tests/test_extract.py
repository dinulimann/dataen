from datetime import date

from weather_pipeline.config import City, load_cities
from weather_pipeline.extract import DAILY_VARIABLES, build_params, fetch_daily

JAKARTA = City("jakarta", "Jakarta", -6.2088, 106.8456)


def test_build_params_single_day():
    params = build_params(JAKARTA, date(2026, 10, 1))

    assert params["start_date"] == params["end_date"] == "2026-10-01"
    assert params["timezone"] == "Asia/Jakarta"
    assert params["daily"].split(",") == list(DAILY_VARIABLES)


def test_fetch_daily_returns_json(jakarta_payload):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return jakarta_payload

    class FakeSession:
        def get(self, url, params, timeout):
            self.params = params
            return FakeResponse()

    session = FakeSession()
    assert fetch_daily(JAKARTA, date(2026, 10, 1), session=session) == jakarta_payload
    assert session.params["latitude"] == JAKARTA.latitude


def test_cities_seed_is_valid():
    cities = load_cities()
    ids = [c.city_id for c in cities]

    assert len(ids) == len(set(ids)), "city_id harus unik"
    assert all(-11 <= c.latitude <= 6 and 95 <= c.longitude <= 141 for c in cities), (
        "koordinat harus berada di wilayah Indonesia"
    )
