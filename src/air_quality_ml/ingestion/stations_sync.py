import asyncio
import httpx
from sqlalchemy import text
from air_quality_ml.database import engine

GIOS_API_URL = "https://api.gios.gov.pl/pjp-api/v1/rest"

TARGET_VOIVODESHIPS = [
    "PODKARPACKIE",
    "MAŁOPOLSKIE",
    "LUBELSKIE",
    "ŚWIĘTOKRZYSKIE",
]


async def fetch_json(client: httpx.AsyncClient, url: str, params: dict | None = None) -> dict | list | None:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = await client.get(url, params=params, timeout=45)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPStatusError, httpx.HTTPError):
            if attempt == max_retries - 1:
                return None
            await asyncio.sleep(2 * (attempt + 1))
    return None


async def fetch_filtered_stations(client: httpx.AsyncClient) -> list[dict]:
    url = f"{GIOS_API_URL}/station/findAll"
    data = await fetch_json(client, url, params={"size": 1000})
    if not data:
        return []

    stations = data.get("Lista stacji pomiarowych", [])
    filtered = []
    for s in stations:
        voivodeship = (s.get("Województwo") or "").strip().upper()
        if any(target in voivodeship for target in TARGET_VOIVODESHIPS):
            filtered.append(s)
    return filtered


async def fetch_station_sensors(client: httpx.AsyncClient, station_id: int) -> list[dict]:
    url = f"{GIOS_API_URL}/station/sensors/{station_id}"
    data = await fetch_json(client, url, params={"size": 100})
    if not data:
        return []
    return data.get("Lista stanowisk pomiarowych dla podanej stacji", [])


def upsert_stations_and_sensors(stations: list[dict], sensors_by_station: dict[int, list[dict]]) -> None:
    station_rows = []
    sensor_rows = []

    for s in stations:
        station_id = s.get("Identyfikator stacji")
        if not station_id:
            continue

        lat_raw = s.get("WGS84 φ N")
        lon_raw = s.get("WGS84 λ E")
        lat = float(lat_raw) if lat_raw is not None else None
        lon = float(lon_raw) if lon_raw is not None else None

        station_rows.append({
            "station_id": int(station_id),
            "station_name": (s.get("Nazwa stacji") or "").strip(),
            "voivodeship": (s.get("Województwo") or "").strip().upper(),
            "latitude": lat,
            "longitude": lon,
        })

        for sensor in sensors_by_station.get(int(station_id), []):
            sensor_id = sensor.get("Identyfikator stanowiska")
            if sensor_id is not None:
                sensor_rows.append({
                    "sensor_id": int(sensor_id),
                    "station_id": int(station_id),
                    "indicator": sensor.get("Wskaźnik") or "nieznany",
                    "sensor_code": sensor.get("Kod stanowiska"),
                })

    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))

        for row in station_rows:
            conn.execute(
                text("""
                    INSERT INTO stations (station_id, station_name, voivodeship, latitude, longitude, geom)
                    VALUES (
                        :station_id,
                        :station_name,
                        :voivodeship,
                        :latitude,
                        :longitude,
                        CASE
                            WHEN :longitude IS NOT NULL AND :latitude IS NOT NULL
                            THEN ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)
                            ELSE NULL
                        END
                    )
                    ON CONFLICT (station_id) DO UPDATE SET
                        station_name = EXCLUDED.station_name,
                        voivodeship = EXCLUDED.voivodeship,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        geom = EXCLUDED.geom;
                """),
                row,
            )

        for s_row in sensor_rows:
            conn.execute(
                text("""
                    INSERT INTO sensors (sensor_id, station_id, indicator, sensor_code)
                    VALUES (:sensor_id, :station_id, :indicator, :sensor_code)
                    ON CONFLICT (sensor_id) DO UPDATE SET
                        station_id = EXCLUDED.station_id,
                        indicator = EXCLUDED.indicator,
                        sensor_code = EXCLUDED.sensor_code;
                """),
                s_row,
            )

    print(f"Zsynchronizowano {len(station_rows)} stacji (z PostGIS geom) oraz {len(sensor_rows)} sensorów.")


async def sync_stations_and_sensors() -> tuple[list[dict], dict[int, list[dict]]]:
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    async with httpx.AsyncClient(limits=limits, timeout=60) as client:
        stations = await fetch_filtered_stations(client)
        if not stations:
            return [], {}

        tasks = [fetch_station_sensors(client, int(s["Identyfikator stacji"])) for s in stations]
        results = await asyncio.gather(*tasks)

        sensors_by_station: dict[int, list[dict]] = {}
        for s, sens in zip(stations, results):
            s_id = int(s["Identyfikator stacji"])
            sensors_by_station[s_id] = sens

        upsert_stations_and_sensors(stations, sensors_by_station)
        return stations, sensors_by_station


if __name__ == "__main__":
    asyncio.run(sync_stations_and_sensors())