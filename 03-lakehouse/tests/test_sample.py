from collections import Counter

import pyarrow.parquet as pq

from taxi_lakehouse.sample import SCHEMA, generate_month, write_month, write_zones


def test_sample_file_matches_tlc_schema(tmp_path):
    path = tmp_path / "yellow_tripdata_2024-02.parquet"
    n = write_month(path, "2024-02", rows=1_000)

    table = pq.read_table(path)
    assert table.schema.equals(SCHEMA)
    assert table.num_rows == n == 1_005  # + 0.5% duplikat


def test_sample_contains_dirty_rows_and_duplicates():
    trips = generate_month("2024-01", rows=5_000, dirty_ratio=0.05)

    zero_distance = sum(t["trip_distance"] == 0 for t in trips)
    outside_month = sum(t["tpep_pickup_datetime"].year != 2024 for t in trips)
    keys = Counter((t["tpep_pickup_datetime"], t["PULocationID"], t["total_amount"]) for t in trips)

    assert zero_distance > 0
    assert outside_month > 0
    assert sum(c - 1 for c in keys.values() if c > 1) >= 25


def test_zone_lookup_has_all_265_ids(tmp_path):
    path = tmp_path / "zones.csv"
    write_zones(path)
    lines = path.read_text().splitlines()

    assert lines[0] == "LocationID,Borough,Zone,service_zone"
    assert len(lines) == 266
    assert "132,Queens,JFK Airport,Airports" in lines
