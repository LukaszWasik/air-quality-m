import asyncio
from datetime import datetime
from pathlib import Path
import httpx
import pandas as pd
from sqlalchemy import text
from air_quality_ml.database import engine
from tqdm.asyncio import tqdm

GIOS_API_URL = "https://api.gios.gov.pl/pjp-api/v1/rest"

TARGET_VOIVODESHIPS = [
    "PODKARPACKIE",
    "MAŁOPOLSKIE",
    "LUBELSKIE",
    "ŚWIĘTOKRZYSKIE",
]

SEMAPHORE_LIMIT = 5


async def fetch_json(client: httpx.AsyncClient, url: str, params: dict = None) -> dict | list | None:
    try:
        response = await client.get(url, params=params, timeout=45)
        response.raise_for_status()
        return response.json()
    except (httpx.HTTPStatusError, httpx.HTTPError):
        return None


async def get_all_stations(client: httpx.AsyncClient) -> list[dict]:
    url = f"{GIOS_API_URL}/station/findAll"
    data = await fetch_json(client, url, params={"size": 1000})
    if not data:
        return []

    stations = data.get("Lista stacji pomiarowych", [])
    filtered = []
    for s in stations:
        voivodeship = s.get("Województwo", "").strip().upper()
        if any(target in voivodeship for target in TARGET_VOIVODESHIPS):
            filtered.append(s)
    return filtered


async def get_sensors(client: httpx.AsyncClient, station_id: int) -> list[dict]:
    url = f"{GIOS_API_URL}/station/sensors/{station_id}"
    data = await fetch_json(client, url, params={"size": 100})
    if not data:
        return []
    return data.get("Lista stanowisk pomiarowych dla podanej stacji", [])


def get_sensor_watermarks() -> dict[int, str]:
    watermarks = {}
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                                       SELECT sensor_id, MAX(datetime) as max_date
                                       FROM measurements
                                       GROUP BY sensor_id;
                                       """))
            for row in result:
                if row.max_date:
                    watermarks[row.sensor_id] = row.max_date.strftime("%Y-%m-%d %H:%M")
    except Exception as e:
        print(f"Błąd odczytu wodoznaków z bazy: {e}")
    return watermarks


async def get_archival_data_incremental(
        client: httpx.AsyncClient, sensor_id: int, date_from: str, date_to: str, semaphore: asyncio.Semaphore
) -> list[dict]:
    url = f"{GIOS_API_URL}/archivalData/getDataBySensor/{sensor_id}"
    all_measurements = []
    page = 0
    size = 500

    async with semaphore:
        while True:
            params = {
                "dateFrom": date_from,
                "dateTo": date_to,
                "size": size,
                "page": page,
            }
            data = await fetch_json(client, url, params=params)
            if not data:
                break

            measurements = data.get("Lista archiwalnych wyników pomiarów", [])
            if not measurements:
                break

            all_measurements.extend(measurements)
            if len(measurements) < size:
                break

            page += 1
            await asyncio.sleep(0.05)

    return all_measurements


def save_to_raw_landing_zone(df: pd.DataFrame) -> Path:
    now = datetime.now()
    dir_path = Path("data") / "raw" / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
    dir_path.mkdir(parents=True, exist_ok=True)

    file_path = dir_path / f"measurements_{now.strftime('%H%M%S')}.parquet"
    df.to_parquet(file_path, index=False)
    print(f"Zapisano surowy backup do: {file_path}")
    return file_path


def upsert_measurements_to_db(df: pd.DataFrame) -> None:
    if df.empty:
        return

    print("Zapisywanie danych do bazy PostgreSQL (UPSERT)...")
    records = df[["sensor_id", "datetime", "value"]].to_dict(orient="records")

    sql = text("""
               INSERT INTO measurements (sensor_id, datetime, value)
               VALUES (:sensor_id, :datetime, :value) ON CONFLICT (sensor_id, datetime) 
        DO
               UPDATE SET value = EXCLUDED.value;
               """)

    with engine.begin() as conn:
        conn.execute(sql, records)

    print(f"Pomyślnie zsynchronizowano {len(records)} rekordów w bazie!")


async def run_incremental_sync() -> None:
    """Główna funkcja orkiestrująca pobieranie przyrostowe GIOŚ."""
    print("--- START PRZYROSTOWEGO PROCESU ETL GIOŚ ---")

    watermarks = get_sensor_watermarks()
    default_date_from = "2026-01-01 00:00"
    date_to = datetime.now().strftime("%Y-%m-%d %H:%M")

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async with httpx.AsyncClient(limits=limits, timeout=60) as client:
        print("Pobieranie listy stacji...")
        stations = await get_all_stations(client)
        print(f"Znaleziono {len(stations)} stacji.")

        if not stations:
            return

        print("Pobieranie sensorów dla stacji...")
        station_sensor_tasks = [get_sensors(client, s.get("Identyfikator stacji")) for s in stations]
        sensors_results = await asyncio.gather(*station_sensor_tasks)

        download_tasks = []
        task_metadata = []

        for station, sensors in zip(stations, sensors_results):
            s_id = station.get("Identyfikator stacji")
            s_name = station.get("Nazwa stacji")
            voivodeship = station.get("Województwo")

            for sensor in sensors:
                sensor_id = sensor.get("Identyfikator stanowiska")
                indicator = sensor.get("Wskaźnik")
                if sensor_id is not None:
                    date_from = watermarks.get(sensor_id, default_date_from)
                    download_tasks.append(
                        get_archival_data_incremental(client, sensor_id, date_from, date_to, semaphore)
                    )
                    task_metadata.append({
                        "sensor_id": sensor_id,
                    })

        print(f"Pobieranie nowych pomiarów dla {len(download_tasks)} sensorów...")
        measurements_results = await tqdm.gather(*download_tasks, desc="Pobieranie sensorów")

        all_records = []
        for meta, measurements in zip(task_metadata, measurements_results):
            for m in measurements:
                all_records.append({
                    "sensor_id": meta["sensor_id"],
                    "datetime": m.get("Data"),
                    "value": m.get("Wartość"),
                })

    if not all_records:
        print("Brak nowych danych do pobrania. Baza jest aktualna.")
        return

    df = pd.DataFrame(all_records)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.drop_duplicates(subset=["sensor_id", "datetime"]).reset_index(drop=True)

    print(f"Pobrano łącznie {len(df)} nowych/zaktualizowanych rekordów.")

    save_to_raw_landing_zone(df)
    upsert_measurements_to_db(df)
    print("--- KONIEC PROCESU ETL ---")