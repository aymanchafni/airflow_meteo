import os

# Add a city here and the DAG fans out one more fetch task automatically (dynamic task mapping).
CITIES = [
    {"name": "montreal", "latitude": 45.5088, "longitude": -73.5878},
    {"name": "toronto", "latitude": 43.6532, "longitude": -79.3832},
    {"name": "vancouver", "latitude": 49.2827, "longitude": -123.1207},
    {"name": "paris", "latitude": 48.8566, "longitude": 2.3522},
    {"name": "casablanca", "latitude": 33.5731, "longitude": -7.5898},
]

DATA_DIR = os.environ.get("WEATHER_DATA_DIR", "/opt/airflow/data")
DB_PATH = os.path.join(DATA_DIR, "weather.duckdb")

DAILY_VARIABLES = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]

# Open-Meteo: the archive (reanalysis) API is authoritative for past days but lags a few
# days behind; recent days are taken from the forecast API, which also serves past dates.
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_LAG_DAYS = 6
