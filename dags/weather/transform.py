"""Transform + validate: raw JSON payloads -> one tidy row per (city, date)."""

import json

import pandas as pd

COLUMNS = ["city", "date", "temp_max_c", "temp_min_c", "precipitation_mm", "source"]


class DataQualityError(ValueError):
    pass


def to_frame(paths: list[str]) -> pd.DataFrame:
    rows = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        daily = payload["response"]["daily"]
        for i, day in enumerate(daily["time"]):
            rows.append({
                "city": payload["city"],
                "date": day,
                "temp_max_c": daily["temperature_2m_max"][i],
                "temp_min_c": daily["temperature_2m_min"][i],
                "precipitation_mm": daily["precipitation_sum"][i],
                "source": payload["source"],
            })
    df = pd.DataFrame(rows, columns=COLUMNS)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def validate(df: pd.DataFrame, expected_cities: set[str]) -> pd.DataFrame:
    """Fail loudly instead of loading bad data. Returns df unchanged when valid."""
    problems = []
    missing = expected_cities - set(df["city"])
    if missing:
        problems.append(f"missing cities: {sorted(missing)}")
    if df.duplicated(["city", "date"]).any():
        problems.append("duplicate (city, date) rows")
    nulls = df[["temp_max_c", "temp_min_c", "precipitation_mm"]].isna().sum()
    if nulls.any():
        problems.append(f"null values: {nulls[nulls > 0].to_dict()}")
    valid = df.dropna(subset=["temp_max_c", "temp_min_c", "precipitation_mm"])
    if (valid["temp_min_c"] > valid["temp_max_c"]).any():
        problems.append("temp_min_c > temp_max_c")
    if (~valid["temp_max_c"].between(-80, 60) | ~valid["temp_min_c"].between(-90, 60)).any():
        problems.append("temperature outside physical range")
    if (valid["precipitation_mm"] < 0).any():
        problems.append("negative precipitation")
    if problems:
        raise DataQualityError("; ".join(problems))
    return df
