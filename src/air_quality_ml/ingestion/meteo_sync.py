import asyncio
from datetime import datetime
from pathlib import Path
import httpx
import pandas as pd
from sqlalchemy import text
from air_quality_ml.database import engine
from tqdm.asyncio import tqdm

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
SEMAPHORE_LIMIT = 5


def get_stations_from_db() -> list[dict]:
    """Pobiera stacje i ich współrzędne z bazy danych."""
    stations = []
    with engine.begin() as conn:
        result = conn.execute(text("""
                                   SELECT station_id, station_name, latitude, longitude
                                   FROM stations
                                   WHERE latitude IS NOT NULL
                                     AND longitude IS NOT NULL;
                                   """))
        for row in result:
            stations.append({
                "station_id": row.station_id,
                "station_name": row.station_name,
                "latitude": row.latitude,
                "longitude": row.longitude,
            })
    return stations


def get_meteo_watermarks() -> dict[int, str]:
    """Pobiera wodoznak (najnowszą datę) dla każdej stacji z tabeli meteo."""
    watermarks = {}
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                                       SELECT station_id, MAX(datetime) as max_date
                                       FROM meteo_measurements
                                       GROUP BY station_id;
                                       """))
            for row in result:
                if row.max_date:
                    watermarks[row.station_id] = row.max_date.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Błąd odczytu wodoznaków meteo: {e}")
    return watermarks


async def fetch_station_meteo(
        client: httpx.AsyncClient,
        station: dict,
        start_date: str,
        end_date: str,
        semaphore: asyncio.Semaphore
) -> list[dict]:
    """Pobiera pełny wachlarz danych pogodowych dla stacji z Open-Meteo API."""
    params = {
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": (
            "temperature_2m,relative_humidity_2m,dew_point_2m,"
            "surface_pressure,wind_speed_10m,wind_direction_10m,"
            "wind_gusts_10m,precipitation,cloud_cover,"
            "boundary_layer_height,shortwave_radiation"
        )
    }

    async with semaphore:
        try:
            response = await client.get(OPEN_METEO_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            hourly = data.get("hourly", {})
            times = hourly.get("time", [])

            records = []
            for i, t in enumerate(times):
                records.append({
                    "station_id": station["station_id"],
                    "datetime": t,
                    "temperature_2m": hourly.get("temperature_2m", [None])[i],
                    "relative_humidity_2m": hourly.get("relative_humidity_2m", [None])[i],
                    "dew_point_2m": hourly.get("dew_point_2m", [None])[i],
                    "surface_pressure": hourly.get("surface_pressure", [None])[i],
                    "wind_speed_10m": hourly.get("wind_speed_10m", [None])[i],
                    "wind_direction_10m": hourly.get("wind_direction_10m", [None])[i],
                    "wind_gusts_10m": hourly.get("wind_gusts_10m", [None])[i],
                    "precipitation": hourly.get("precipitation", [None])[i],
                    "cloud_cover": hourly.get("cloud_cover", [None])[i],
                    "boundary_layer_height": hourly.get("boundary_layer_height", [None])[i],
                    "shortwave_radiation": hourly.get("shortwave_radiation", [None])[i],
                })
            return records
        except Exception:
            return []


def save_meteo_to_landing_zone(df: pd.DataFrame) -> Path:
    now = datetime.now()
    dir_path = Path("data") / "raw_meteo" / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
    dir_path.mkdir(parents=True, exist_ok=True)

    file_path = dir_path / f"meteo_{now.strftime('%H%M%S')}.parquet"
    df.to_parquet(file_path, index=False)
    print(f"Zapisano surowy backup meteo do: {file_path}")
    return file_path


def upsert_meteo_to_db(df: pd.DataFrame) -> None:
    if df.empty:
        return

    print("Zapisywanie danych meteo do bazy PostgreSQL (UPSERT)...")
    records = df.to_dict(orient="records")

    sql = text("""
               INSERT INTO meteo_measurements (station_id, datetime, temperature_2m, relative_humidity_2m,
                                               dew_point_2m, surface_pressure, wind_speed_10m, wind_direction_10m,
                                               wind_gusts_10m, precipitation, cloud_cover, boundary_layer_height,
                                               shortwave_radiation)
               VALUES (:station_id, :datetime, :temperature_2m, :relative_humidity_2m,
                       :dew_point_2m, :surface_pressure, :wind_speed_10m, :wind_direction_10m,
                       :wind_gusts_10m, :precipitation, :cloud_cover, :boundary_layer_height, :shortwave_radiation) ON CONFLICT (station_id, datetime) 
        DO
               UPDATE SET
                   temperature_2m = EXCLUDED.temperature_2m,
                   relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                   dew_point_2m = EXCLUDED.dew_point_2m,
                   surface_pressure = EXCLUDED.surface_pressure,
                   wind_speed_10m = EXCLUDED.wind_speed_10m,
                   wind_direction_10m = EXCLUDED.wind_direction_10m,
                   wind_gusts_10m = EXCLUDED.wind_gusts_10m,
                   precipitation = EXCLUDED.precipitation,
                   cloud_cover = EXCLUDED.cloud_cover,
                   boundary_layer_height = EXCLUDED.boundary_layer_height,
                   shortwave_radiation = EXCLUDED.shortwave_radiation;
               """)

    with engine.begin() as conn:
        conn.execute(sql, records)

    print(f"Pomyślnie zsynchronizowano {len(records)} rekordów meteo w bazie!")


async def run_meteo_sync() -> None:
    print("--- START PRZYROSTOWEGO PROCESU ETL METEO ---")

    stations = get_stations_from_db()
    if not stations:
        print("Brak stacji ze współrzędnymi w bazie!")
        return

    watermarks = get_meteo_watermarks()
    default_start_date = "2026-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async with httpx.AsyncClient(limits=limits, timeout=60) as client:
        tasks = []
        for station in stations:
            start_date = watermarks.get(station["station_id"], default_start_date)
            if start_date <= end_date:
                tasks.append(
                    fetch_station_meteo(client, station, start_date, end_date, semaphore)
                )

        print(f"Pobieranie danych pogodowych dla {len(stations)} stacji...")
        results = await tqdm.gather(*tasks, desc="Pobieranie Meteo")

        all_records = []
        for station_records in results:
            all_records.extend(station_records)

    if not all_records:
        print("Brak nowych danych pogodowych do pobrania.")
        return

    df = pd.DataFrame(all_records)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.drop_duplicates(subset=["station_id", "datetime"]).reset_index(drop=True)

    print(f"Pobrano łącznie {len(df)} rekordów meteo.")

    save_meteo_to_landing_zone(df)
    upsert_meteo_to_db(df)
    print("--- KONIEC PROCESU ETL METEO ---")


if __name__ == "__main__":
    asyncio.run(run_meteo_sync())