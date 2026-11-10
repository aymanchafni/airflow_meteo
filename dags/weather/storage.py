"""Load: idempotent upsert into DuckDB, plus a rolling 7-day report."""

import os
from datetime import date

import duckdb
import pandas as pd

SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_weather (
    city             VARCHAR NOT NULL,
    date             DATE    NOT NULL,
    temp_max_c       DOUBLE  NOT NULL,
    temp_min_c       DOUBLE  NOT NULL,
    precipitation_mm DOUBLE  NOT NULL,
    source           VARCHAR NOT NULL,
    loaded_at        TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (city, date)
)
"""


def upsert(df: pd.DataFrame, db_path: str) -> int:
    """Insert or replace rows on (city, date): re-running a day never duplicates data,
    and a day first loaded from the forecast API is overwritten by archive values later."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    with duckdb.connect(db_path) as con:
        con.execute(SCHEMA)
        con.register("incoming", df)
        con.execute(
            """
            INSERT INTO daily_weather (city, date, temp_max_c, temp_min_c, precipitation_mm, source)
            SELECT city, date, temp_max_c, temp_min_c, precipitation_mm, source FROM incoming
            ON CONFLICT (city, date) DO UPDATE SET
                temp_max_c = excluded.temp_max_c,
                temp_min_c = excluded.temp_min_c,
                precipitation_mm = excluded.precipitation_mm,
                source = excluded.source,
                loaded_at = now()
            """
        )
    return len(df)


def rolling_report(db_path: str, end: date, days: int = 7) -> pd.DataFrame:
    """Per-city stats over the `days` days ending at `end` (inclusive)."""
    with duckdb.connect(db_path, read_only=True) as con:
        return con.execute(
            """
            SELECT
                city,
                COUNT(*)                         AS days_available,
                ROUND(AVG(temp_max_c), 2)        AS avg_temp_max_c,
                ROUND(AVG(temp_min_c), 2)        AS avg_temp_min_c,
                ROUND(SUM(precipitation_mm), 2)  AS total_precipitation_mm,
                arg_max(date, precipitation_mm)  AS rainiest_day
            FROM daily_weather
            WHERE date BETWEEN ?::DATE - (? - 1) * INTERVAL 1 DAY AND ?::DATE
            GROUP BY city
            ORDER BY city
            """,
            [end, days, end],
        ).fetchdf()
