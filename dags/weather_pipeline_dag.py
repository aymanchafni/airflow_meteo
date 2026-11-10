"""
Daily multi-city weather pipeline (Open-Meteo -> DuckDB).

    fetch[montreal] ─┐
    fetch[toronto]  ─┤
    fetch[...]      ─┼─► transform_validate ─► load (upsert) ─► report
    fetch[paris]    ─┘

* One run per day (`ds`); every task only touches that day's data, so runs are
  idempotent and the DAG can be backfilled: `airflow dags backfill weather_pipeline -s ... -e ...`
* `fetch` is dynamically mapped over the cities in `weather/config.py`.
* Bad data fails the run in `transform_validate` before anything is loaded.
"""

from datetime import date, datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="weather_pipeline",
    description="Open-Meteo daily weather for several cities -> validated -> DuckDB",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=True,
    max_active_runs=1,  # DuckDB allows a single writer process at a time
    default_args={
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["weather", "duckdb", "open-meteo"],
)
def weather_pipeline():
    @task
    def list_cities() -> list[dict]:
        from weather.config import CITIES

        return CITIES

    @task
    def fetch(city: dict, ds: str | None = None) -> str:
        from weather.extract import fetch_city_day

        return fetch_city_day(city, date.fromisoformat(ds))

    @task
    def transform_validate(raw_paths: list[str], ds: str | None = None) -> str:
        import os

        from weather.config import CITIES, DATA_DIR
        from weather.transform import to_frame, validate

        df = validate(to_frame(list(raw_paths)), {c["name"] for c in CITIES})
        out = os.path.join(DATA_DIR, "clean", f"date={ds}", "daily_weather.parquet")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        df.to_parquet(out, index=False)
        return out

    @task
    def load(clean_path: str) -> int:
        import pandas as pd
        from weather.config import DB_PATH
        from weather.storage import upsert

        return upsert(pd.read_parquet(clean_path), DB_PATH)

    @task
    def report(rows_loaded: int, ds: str | None = None) -> str:
        import os

        from weather.config import DATA_DIR, DB_PATH
        from weather.storage import rolling_report

        stats = rolling_report(DB_PATH, date.fromisoformat(ds))
        out = os.path.join(DATA_DIR, "reports", f"weekly_stats_{ds}.csv")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        stats.to_csv(out, index=False)
        print(f"{rows_loaded} rows loaded for {ds}\n{stats.to_string(index=False)}")
        return out

    raw = fetch.expand(city=list_cities())
    report(load(transform_validate(raw)))


weather_pipeline()
