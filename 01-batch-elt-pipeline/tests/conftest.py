import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def jakarta_payload() -> dict:
    return json.loads((FIXTURES / "open_meteo_jakarta.json").read_text())


class FakeLake:
    """Pengganti MinIO di memori untuk unit test."""

    def __init__(self):
        self.objects: dict[str, dict] = {}

    def put_json(self, key, payload):
        self.objects[key] = payload

    def get_json(self, key):
        return self.objects[key]


@pytest.fixture
def fake_lake() -> FakeLake:
    return FakeLake()
