import asyncio
import calendar
from datetime import datetime, timedelta
from pathlib import Path
import httpx
import pandas as pd
from sqlalchemy import text
from tqdm.asyncio import tqdm
from air_quality_ml.database import engine
from air_quality_ml.ingestion.stations_sync import sync_stations_and_sensors

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
SEMAPHORE_LIMIT = 5

HOURLY_VARS = (
    "temperature_2m,relative_humidity_2m,dew_point_2m,"
    "surface_pressure,wind_speed_10m,wind_direction_10m,"
    "wind_gusts_10m,precipitation,cloud_cover,"
    "boundary_layer_height,shortwave_radiation"
)


def iter_month_days(start_dt: datetime, end_dt: datetime):
    """Generuje pary (YYYY-MM-DD, YYYY-MM-DD, YYYY-MM) w paczkach po 1 miesiącu."""
    current = datetime(start_dt.year, start_dt.month, 1)
    while current <= end_dt:
        last_day = calendar.monthrange(current.year, current.month)[1]
        m_start = current if current >= start_dt else start_dt
        m_end = datetime(current.year, current.month, last_day)
        if m_end > end_dt:
            m_end = end_dt

        if m_start <= m_end:
            yield m_start.strftime("%Y-%m-%d"), m_end.strftime("%Y-%m-%d"), current.strftime("%Y-%m")

        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)


def get_stations_from_db() -> list[dict]:
    stations = []
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT station_id, station_name, latitude, longitude
            FROM stations
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
        """))
        for row in result:
            stations.append({
                "station_id": int(row.station_id),
                "station_name": row.station_name,
                "latitude": float(row.latitude),
                "longitude": float(row.longitude),
            })
    return stations


def get_meteo_watermarks() -> dict[int, datetime]:
    watermarks: dict[int, datetime] = {}
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT station_id, MAX(datetime) AS max_date
                FROM meteo_measurements
                GROUP BY station_id;
            """))
            for row in result:
                if row.max_date:
                    watermarks[int(row.station_id)] = row.max_date
    except Exception as e:
        print(f"Błąd odczytu wodoznaków meteo: {e}")
    return watermarks


async def fetch_station_meteo(
    client: httpx.AsyncClient,
    station: dict,
    start_date: str,
    end_date: str,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    params = {
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARS,
    }

    async with semaphore:
        for attempt in range(3):
            try:
                response = await client.get(OPEN_METEO_URL, params=params, timeout=40)
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
            except (httpx.HTTPStatusError, httpx.HTTPError):
                if attempt == 2:
                    return []
                await asyncio.sleep(2 * (attempt + 1))
    return []


def save_meteo_to_landing_zone(df: pd.DataFrame, tag: str = "meteo") -> Path:
    now = datetime.now()
    dir_path = Path("data") / "raw_meteo" / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
    dir_path.mkdir(parents=True, exist_ok=True)

    file_path = dir_path / f"{tag}_{now.strftime('%H%M%S_%f')}.parquet"
    df.to_parquet(file_path, index=False)
    return file_path


def upsert_meteo_to_db(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    records = df.to_dict(orient="records")
    sql = text("""
        INSERT INTO meteo_measurements (
            station_id, datetime, temperature_2m, relative_humidity_2m,
            dew_point_2m, surface_pressure, wind_speed_10m, wind_direction_10m,
            wind_gusts_10m, precipitation, cloud_cover, boundary_layer_height, shortwave_radiation
        )
        VALUES (
            :station_id, :datetime, :temperature_2m, :relative_humidity_2m,
            :dew_point_2m, :surface_pressure, :wind_speed_10m, :wind_direction_10m,
            :wind_gusts_10m, :precipitation, :cloud_cover, :boundary_layer_height, :shortwave_radiation
        )
        ON CONFLICT (station_id, datetime)
        DO UPDATE SET
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

    return len(records)


async def run_history_backfill_months(months_back: int = 60, end_dt: datetime | None = None) -> None:
    """Dociąga dane meteo z ostatnich ~5 lat (60 mies.) paczkami po 1 miesiącu."""
    print(f"--- START BACKFILLU METEO (ostatnie {months_back} mies., paczki po 1 mies.) ---")
    await sync_stations_and_sensors()
    stations = get_stations_from_db()
    if not stations:
        print("Brak stacji ze współrzędnymi w bazie.")
        return

    now = end_dt or datetime.now()
    total_months = now.year * 12 + (now.month - 1) - months_back
    start_dt = datetime(total_months // 12, (total_months % 12) + 1, 1)

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    total_synced = 0
    async with httpx.AsyncClient(limits=limits, timeout=60) as client:
        for d_from, d_to, label in iter_month_days(start_dt, now):
            tasks = [fetch_station_meteo(client, st, d_from, d_to, semaphore) for st in stations]
            results = await tqdm.gather(*tasks, desc=f"Meteo {label}", leave=False)

            all_records = [rec for st_recs in results for rec in st_recs]
            if not all_records:
                print(f"[{label}] Pobrano 0 rekordów meteo.")
                continue

            df = pd.DataFrame(all_records)
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df = df.dropna(subset=["datetime"])
            df = df.drop_duplicates(subset=["station_id", "datetime"]).reset_index(drop=True)

            parquet_path = save_meteo_to_landing_zone(df, tag=f"meteo_{label}")
            synced = upsert_meteo_to_db(df)
            total_synced += synced
            print(f"[{label}] Pobrano i zsynchronizowano {synced} rekordów meteo (backup: {parquet_path.name}).")

    print(f"--- KONIEC BACKFILLU METEO (łącznie zsynchronizowano: {total_synced} rekordów) ---")


async def run_meteo_sync() -> None:
    """Lekki tryb przyrostowy dla schedulera co godzinę."""
    print("--- START PRZYROSTOWEGO PROCESU ETL METEO ---")
    await sync_stations_and_sensors()
    stations = get_stations_from_db()
    if not stations:
        print("Brak stacji ze współrzędnymi w bazie!")
        return

    watermarks = get_meteo_watermarks()
    now = datetime.now()
    default_start = (now - timedelta(days=2)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async with httpx.AsyncClient(limits=limits, timeout=60) as client:
        tasks = []
        for station in stations:
            wm = watermarks.get(station["station_id"])
            start_date = (wm - timedelta(days=1)).strftime("%Y-%m-%d") if wm else default_start
            if start_date <= end_date:
                tasks.append(fetch_station_meteo(client, station, start_date, end_date, semaphore))

        print(f"Pobieranie danych pogodowych dla {len(stations)} stacji...")
        results = await tqdm.gather(*tasks, desc="Pobieranie Meteo")
        all_records = [rec for st_recs in results for rec in st_recs]

    if not all_records:
        print("Brak nowych danych pogodowych do pobrania.")
        return

    df = pd.DataFrame(all_records)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df = df.drop_duplicates(subset=["station_id", "datetime"]).reset_index(drop=True)

    print(f"Pobrano łącznie {len(df)} rekordów meteo.")
    save_meteo_to_landing_zone(df)
    upsert_meteo_to_db(df)
    print("--- KONIEC PROCESU ETL METEO ---")