"""Extract: one API call per (city, day), raw response landed as-is (bronze layer)."""

import json
import os
from datetime import date, timedelta

import requests

from weather.config import ARCHIVE_LAG_DAYS, ARCHIVE_URL, DAILY_VARIABLES, DATA_DIR, FORECAST_URL


def endpoint_for(day: date, today: date | None = None) -> tuple[str, str]:
    today = today or date.today()
    if day <= today - timedelta(days=ARCHIVE_LAG_DAYS):
        return "archive", ARCHIVE_URL
    return "forecast", FORECAST_URL


def raw_path(day: date, city: str, data_dir: str = DATA_DIR) -> str:
    return os.path.join(data_dir, "raw", f"date={day.isoformat()}", f"{city}.json")


def fetch_city_day(city: dict, day: date, data_dir: str = DATA_DIR, session=None) -> str:
    """Fetch daily weather for one city and one day; returns the path of the raw JSON file."""
    source, url = endpoint_for(day)
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": day.isoformat(),
        "end_date": day.isoformat(),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "auto",
    }
    response = (session or requests).get(url, params=params, timeout=30)
    response.raise_for_status()

    payload = {"city": city["name"], "source": source, "requested_date": day.isoformat(),
               "response": response.json()}
    path = raw_path(day, city["name"], data_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, path)  # atomic: a retry never sees a half-written file
    return path
