import asyncio
import calendar
from datetime import datetime, timedelta
from pathlib import Path
import httpx
import pandas as pd
from sqlalchemy import text
from tqdm.asyncio import tqdm
from air_quality_ml.database import engine
from air_quality_ml.ingestion.stations_sync import (
    GIOS_API_URL,
    fetch_json,
    sync_stations_and_sensors,
)

SEMAPHORE_LIMIT = 5


def iter_month_windows(start_dt: datetime, end_dt: datetime):
    """Generuje pary (month_from, month_to) w paczkach po 1 miesiącu."""
    current = datetime(start_dt.year, start_dt.month, 1, 0, 0)
    while current <= end_dt:
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_start = current if current >= start_dt else start_dt
        month_end = datetime(current.year, current.month, last_day, 23, 59)
        if month_end > end_dt:
            month_end = end_dt

        if month_start <= month_end:
            yield month_start, month_end

        if current.month == 12:
            current = datetime(current.year + 1, 1, 1, 0, 0)
        else:
            current = datetime(current.year, current.month + 1, 1, 0, 0)


def get_sensor_watermarks() -> dict[int, datetime]:
    watermarks: dict[int, datetime] = {}
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT sensor_id, MAX(datetime) AS max_date
                FROM measurements
                GROUP BY sensor_id;
            """))
            for row in result:
                if row.max_date:
                    watermarks[int(row.sensor_id)] = row.max_date
    except Exception as e:
        print(f"Błąd odczytu wodoznaków z bazy: {e}")
    return watermarks


async def get_archival_data_for_window(
    client: httpx.AsyncClient,
    sensor_id: int,
    date_from: str,
    date_to: str,
    semaphore: asyncio.Semaphore,
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


def save_to_raw_landing_zone(df: pd.DataFrame, tag: str = "measurements") -> Path:
    now = datetime.now()
    dir_path = Path("data") / "raw" / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
    dir_path.mkdir(parents=True, exist_ok=True)

    file_path = dir_path / f"{tag}_{now.strftime('%H%M%S_%f')}.parquet"
    df.to_parquet(file_path, index=False)
    return file_path


def upsert_measurements_to_db(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    records = df[["sensor_id", "datetime", "value"]].to_dict(orient="records")
    sql = text("""
        INSERT INTO measurements (sensor_id, datetime, value)
        VALUES (:sensor_id, :datetime, :value)
        ON CONFLICT (sensor_id, datetime)
        DO UPDATE SET value = EXCLUDED.value;
    """)

    with engine.begin() as conn:
        conn.execute(sql, records)

    return len(records)


async def _sync_date_range(
    client: httpx.AsyncClient,
    sensor_ids: list[int],
    date_from_dt: datetime,
    date_to_dt: datetime,
    semaphore: asyncio.Semaphore,
    label: str,
) -> int:
    date_from_str = date_from_dt.strftime("%Y-%m-%d %H:%M")
    date_to_str = date_to_dt.strftime("%Y-%m-%d %H:%M")

    tasks = [
        get_archival_data_for_window(client, s_id, date_from_str, date_to_str, semaphore)
        for s_id in sensor_ids
    ]
    results = await tqdm.gather(*tasks, desc=f"GIOŚ {label}", leave=False)

    all_records = []
    for s_id, measurements in zip(sensor_ids, results):
        for m in measurements:
            all_records.append({
                "sensor_id": s_id,
                "datetime": m.get("Data"),
                "value": m.get("Wartość"),
            })

    if not all_records:
        print(f"[{label}] Pobrano 0 rekordów.")
        return 0

    df = pd.DataFrame(all_records)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df = df.drop_duplicates(subset=["sensor_id", "datetime"]).reset_index(drop=True)

    parquet_path = save_to_raw_landing_zone(df, tag=f"gios_{label}")
    synced = upsert_measurements_to_db(df)
    print(f"[{label}] Pobrano i zsynchronizowano {synced} rekordów (backup: {parquet_path.name}).")
    return synced


async def run_history_backfill_months(months_back: int = 60, end_dt: datetime | None = None) -> None:
    """Dociąga dane historyczne (domyślnie ~5 lat = 60 miesięcy) paczkami po 1 miesiącu."""
    print(f"--- START BACKFILLU GIOŚ (ostatnie {months_back} mies., paczki po 1 mies.) ---")
    stations, sensors_by_station = await sync_stations_and_sensors()
    if not stations:
        print("Brak stacji do pobrania.")
        return

    sensor_ids = [
        int(sensor["Identyfikator stanowiska"])
        for sens_list in sensors_by_station.values()
        for sensor in sens_list
        if sensor.get("Identyfikator stanowiska") is not None
    ]

    now = end_dt or datetime.now()
    # Przybliżone cofnięcie o months_back miesięcy od początku bieżącego miesiąca
    total_months = now.year * 12 + (now.month - 1) - months_back
    start_year = total_months // 12
    start_month = (total_months % 12) + 1
    start_dt = datetime(start_year, start_month, 1, 0, 0)

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    total_synced = 0
    async with httpx.AsyncClient(limits=limits, timeout=60) as client:
        for m_from, m_to in iter_month_windows(start_dt, now):
            label = m_from.strftime("%Y-%m")
            synced = await _sync_date_range(client, sensor_ids, m_from, m_to, semaphore, label)
            total_synced += synced

    print(f"--- KONIEC BACKFILLU GIOŚ (łącznie zsynchronizowano: {total_synced} rekordów) ---")


async def run_incremental_sync() -> None:
    """Lekki tryb przyrostowy dla schedulera co godzinę."""
    print("--- START PRZYROSTOWEGO PROCESU ETL GIOŚ ---")
    stations, sensors_by_station = await sync_stations_and_sensors()
    if not stations:
        return

    watermarks = get_sensor_watermarks()
    now = datetime.now()
    default_from = now - timedelta(hours=48)
    date_to_str = now.strftime("%Y-%m-%d %H:%M")

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async with httpx.AsyncClient(limits=limits, timeout=60) as client:
        tasks = []
        task_ids = []
        for sens_list in sensors_by_station.values():
            for sensor in sens_list:
                s_id = sensor.get("Identyfikator stanowiska")
                if s_id is None:
                    continue
                sensor_id = int(s_id)
                wm = watermarks.get(sensor_id)
                # Mały zakładnik 2h wstecz na wypadek opóźnionych pomiarów
                from_dt = (wm - timedelta(hours=2)) if wm else default_from
                if from_dt <= now:
                    tasks.append(
                        get_archival_data_for_window(
                            client, sensor_id, from_dt.strftime("%Y-%m-%d %H:%M"), date_to_str, semaphore
                        )
                    )
                    task_ids.append(sensor_id)

        print(f"Pobieranie nowych pomiarów dla {len(tasks)} sensorów...")
        results = await tqdm.gather(*tasks, desc="Pobieranie sensorów")

        all_records = []
        for sensor_id, measurements in zip(task_ids, results):
            for m in measurements:
                all_records.append({
                    "sensor_id": sensor_id,
                    "datetime": m.get("Data"),
                    "value": m.get("Wartość"),
                })

    if not all_records:
        print("Brak nowych danych do pobrania. Baza jest aktualna.")
        return

    df = pd.DataFrame(all_records)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df = df.drop_duplicates(subset=["sensor_id", "datetime"]).reset_index(drop=True)

    print(f"Pobrano łącznie {len(df)} nowych/zaktualizowanych rekordów.")
    save_to_raw_landing_zone(df)
    upsert_measurements_to_db(df)
    print("--- KONIEC PROCESU ETL ---")