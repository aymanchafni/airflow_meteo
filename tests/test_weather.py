import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from weather.extract import endpoint_for, fetch_city_day
from weather.storage import rolling_report, upsert
from weather.transform import DataQualityError, to_frame, validate


def write_raw(tmp_path, city, day, tmax, tmin, precip, source="archive"):
    payload = {"city": city, "source": source, "requested_date": day,
               "response": {"daily": {"time": [day], "temperature_2m_max": [tmax],
                                      "temperature_2m_min": [tmin], "precipitation_sum": [precip]}}}
    path = tmp_path / f"{city}_{day}.json"
    path.write_text(json.dumps(payload))
    return str(path)


def test_endpoint_switches_from_forecast_to_archive():
    today = date(2026, 11, 9)
    assert endpoint_for(date(2026, 11, 8), today)[0] == "forecast"
    assert endpoint_for(date(2026, 10, 1), today)[0] == "archive"


def test_fetch_lands_raw_payload(tmp_path):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"daily": {"time": ["2026-10-01"], "temperature_2m_max": [19.7],
                              "temperature_2m_min": [13.6], "precipitation_sum": [0.0]}}

    class FakeSession:
        def get(self, url, params, timeout):
            assert params["start_date"] == params["end_date"] == "2026-10-01"
            return FakeResponse()

    city = {"name": "montreal", "latitude": 45.5, "longitude": -73.6}
    path = fetch_city_day(city, date(2026, 10, 1), str(tmp_path), session=FakeSession())
    assert Path(path).parts[-2:] == ("date=2026-10-01", "montreal.json")
    assert to_frame([path]).iloc[0]["temp_max_c"] == 19.7


def test_validate_rejects_bad_data(tmp_path):
    paths = [write_raw(tmp_path, "montreal", "2026-10-01", 5.0, 10.0, 1.0)]   # min > max
    with pytest.raises(DataQualityError, match="temp_min_c > temp_max_c"):
        validate(to_frame(paths), {"montreal"})

    paths = [write_raw(tmp_path, "paris", "2026-10-01", 15.0, 9.0, 0.0)]
    with pytest.raises(DataQualityError, match="missing cities"):
        validate(to_frame(paths), {"paris", "montreal"})

    paths = [write_raw(tmp_path, "paris", "2026-10-02", None, 9.0, 0.0)]
    with pytest.raises(DataQualityError, match="null values"):
        validate(to_frame(paths), {"paris"})


def test_upsert_is_idempotent_and_archive_overrides_forecast(tmp_path):
    db = str(tmp_path / "w.duckdb")
    forecast = to_frame([write_raw(tmp_path, "paris", "2026-10-01", 15.0, 9.0, 0.0, "forecast")])
    archive = to_frame([write_raw(tmp_path, "paris", "2026-10-01", 16.0, 9.5, 2.5, "archive")])

    upsert(forecast, db)
    upsert(forecast, db)   # re-run: no duplicate
    upsert(archive, db)    # later, authoritative values replace the forecast

    report = rolling_report(db, date(2026, 10, 1))
    assert len(report) == 1
    row = report.iloc[0]
    assert (row.days_available, row.avg_temp_max_c, row.total_precipitation_mm) == (1, 16.0, 2.5)


def test_rolling_report_window(tmp_path):
    db = str(tmp_path / "w.duckdb")
    days = pd.date_range("2026-10-01", "2026-10-10").strftime("%Y-%m-%d")
    paths = [write_raw(tmp_path, "montreal", d, 10.0 + i, 0.0, float(i)) for i, d in enumerate(days)]
    upsert(to_frame(paths), db)

    report = rolling_report(db, date(2026, 10, 10), days=7)
    row = report.iloc[0]
    assert row.days_available == 7                       # Oct 4..10
    assert row.total_precipitation_mm == sum(range(3, 10))
    assert pd.Timestamp(row.rainiest_day).date() == date(2026, 10, 10)
