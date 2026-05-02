"""
DAG: weather_pipeline
TP2 - INF8200 Hiver 2026

Pipeline de données météo multi-villes basé sur l'API Open-Meteo (gratuite, sans clé).

Graphe de dépendances :
    fetch_montreal ──┐
                     ├──► merge_cities ──► transform ──► save_results
    fetch_paris ─────┘
"""

import json
import os
from datetime import datetime, timedelta

import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = "/opt/airflow/data"  # répertoire monté via volume Docker

CITIES = {
    "montreal": {"latitude": 45.5088, "longitude": -73.5878},
    "paris":    {"latitude": 48.8566, "longitude":   2.3522},
}

API_BASE = "https://api.open-meteo.com/v1/forecast"
API_PARAMS = {
    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
    "timezone": "auto",
    "past_days": 7,
}

# ---------------------------------------------------------------------------
# Fonctions des tâches
# ---------------------------------------------------------------------------

def fetch_city(city_name: str, latitude: float, longitude: float) -> None:
    """
    Télécharge les données météo des 7 derniers jours pour une ville donnée
    via l'API Open-Meteo et sauvegarde le résultat brut en JSON.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        **API_PARAMS,
    }

    response = requests.get(API_BASE, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{city_name}_raw.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[{city_name}] Données sauvegardées dans {out_path}")
    print(f"[{city_name}] Jours reçus : {data['daily']['time']}")


def fetch_montreal() -> None:
    fetch_city("montreal", **CITIES["montreal"])


def fetch_paris() -> None:
    fetch_city("paris", **CITIES["paris"])


def merge_cities() -> None:
    """
    Fusionne les fichiers JSON bruts des deux villes en un seul DataFrame
    et le sauvegarde en CSV intermédiaire.

    Cette tâche dépend de fetch_montreal ET fetch_paris.
    Elle ne peut s'exécuter qu'une fois les deux extractions terminées.
    """
    dfs = []

    for city_name in CITIES:
        raw_path = os.path.join(OUTPUT_DIR, f"{city_name}_raw.json")

        if not os.path.exists(raw_path):
            raise FileNotFoundError(
                f"Fichier manquant pour {city_name} : {raw_path}. "
                "Vérifiez que les tâches fetch ont bien été exécutées."
            )

        with open(raw_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        daily = data["daily"]
        df = pd.DataFrame({
            "date":            daily["time"],
            "temp_max":        daily["temperature_2m_max"],
            "temp_min":        daily["temperature_2m_min"],
            "precipitation":   daily["precipitation_sum"],
            "city":            city_name,
        })
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"])

    merged_path = os.path.join(OUTPUT_DIR, "merged.csv")
    merged.to_csv(merged_path, index=False)

    print(f"Fusion terminée : {len(merged)} lignes → {merged_path}")
    print(merged.to_string(index=False))


def transform() -> None:
    """
    Calcule des statistiques par ville à partir du CSV fusionné :
      - Température maximale moyenne sur la période
      - Température minimale moyenne sur la période
      - Jour le plus pluvieux
      - Total des précipitations
    """
    merged_path = os.path.join(OUTPUT_DIR, "merged.csv")

    if not os.path.exists(merged_path):
        raise FileNotFoundError(f"Fichier fusionné introuvable : {merged_path}")

    df = pd.read_csv(merged_path, parse_dates=["date"])

    stats_rows = []

    for city, group in df.groupby("city"):
        rainiest_idx = group["precipitation"].idxmax()
        rainiest_day = group.loc[rainiest_idx, "date"].strftime("%Y-%m-%d")

        stats_rows.append({
            "city":                city,
            "avg_temp_max":        round(group["temp_max"].mean(), 2),
            "avg_temp_min":        round(group["temp_min"].mean(), 2),
            "total_precipitation": round(group["precipitation"].sum(), 2),
            "rainiest_day":        rainiest_day,
        })

    stats = pd.DataFrame(stats_rows)

    stats_path = os.path.join(OUTPUT_DIR, "stats.csv")
    stats.to_csv(stats_path, index=False)

    print("Statistiques calculées :")
    print(stats.to_string(index=False))


def save_results() -> None:
    """
    Vérifie la présence des fichiers de sortie et affiche un résumé final
    dans les logs Airflow. Dans un contexte de production, cette tâche
    pourrait envoyer les résultats vers un entrepôt de données ou un dashboard.
    """
    expected_files = [
        "montreal_raw.json",
        "paris_raw.json",
        "merged.csv",
        "stats.csv",
    ]

    print("=== Vérification des fichiers produits ===")
    all_ok = True

    for filename in expected_files:
        path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(path):
            size = os.path.getsize(path)
            print(f"  ✓ {filename}  ({size} octets)")
        else:
            print(f"  ✗ {filename}  MANQUANT")
            all_ok = False

    if not all_ok:
        raise RuntimeError("Certains fichiers de sortie sont manquants. Consultez les logs.")

    # Affichage du rapport final
    stats_path = os.path.join(OUTPUT_DIR, "stats.csv")
    stats = pd.read_csv(stats_path)
    print("\n=== Rapport final ===")
    print(stats.to_string(index=False))
    print("\nPipeline complété avec succès.")


# ---------------------------------------------------------------------------
# Définition du DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "tp2_inf8200",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_pipeline",
    description="Pipeline météo multi-villes : Open-Meteo → merge → stats → CSV",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # déclenchement manuel via l'UI Airflow
    catchup=False,
    tags=["tp2", "inf8200", "météo"],
) as dag:

    t_fetch_montreal = PythonOperator(
        task_id="fetch_montreal",
        python_callable=fetch_montreal,
    )

    t_fetch_paris = PythonOperator(
        task_id="fetch_paris",
        python_callable=fetch_paris,
    )

    t_merge = PythonOperator(
        task_id="merge_cities",
        python_callable=merge_cities,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t_save = PythonOperator(
        task_id="save_results",
        python_callable=save_results,
    )

    # Graphe de dépendances :
    #   fetch_montreal ──┐
    #                    ├──► merge_cities ──► transform ──► save_results
    #   fetch_paris ─────┘
    [t_fetch_montreal, t_fetch_paris] >> t_merge >> t_transform >> t_save
