import asyncio
import httpx
import pandas as pd

GIOS_API_URL = "https://api.gios.gov.pl/pjp-api/v1/rest"

# Zakres dat do testu (zmień na pełny rok, gdy wszystko zadziała)
DATE_FROM = "2026-01-01 00:00"
DATE_TO = "2026-11-16 23:00"

TARGET_VOIVODESHIPS = [
    "PODKARPACKIE",
    "MAŁOPOLSKIE",
    "LUBELSKIE",
    "ŚWIĘTOKRZYSKIE",
]

# Semafor ograniczający liczbę *jednocześnie* wysyłanych zapytań do API (np. do 5)
# Chroni przed błędami połączenia (Too Many Requests / Connection Reset)
SEMAPHORE_LIMIT = 5


async def fetch_json(
        client: httpx.AsyncClient, url: str, params: dict = None
) -> dict | list | None:
    """Pomocnicza funkcja asynchroniczna z obsługą ponowień i błędów."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = await client.get(url, params=params, timeout=45)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPStatusError, httpx.HTTPError) as e:
            if attempt == max_retries - 1:
                # Ostatnia próba nie powiodła się
                # print(f"Błąd dla {url}: {e}")
                return None
            await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
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


async def get_archival_data_paginated(
        client: httpx.AsyncClient, sensor_id: int, semaphore: asyncio.Semaphore
) -> list[dict]:
    """Pobiera dane archiwalne z paginacją, kontrolowane przez semafor."""
    url = f"{GIOS_API_URL}/archivalData/getDataBySensor/{sensor_id}"
    all_measurements = []
    page = 0
    size = 500

    async with semaphore:
        while True:
            params = {
                "dateFrom": DATE_FROM,
                "dateTo": DATE_TO,
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


async def main() -> None:
    print("--- ROZPOCZĘCIE POBIERANIA DANYCH Z GIOŚ ---")

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async with httpx.AsyncClient(limits=limits, timeout=60) as client:
        print("Pobieranie listy stacji dla 4 województw...")
        stations = await get_all_stations(client)
        print(f"Znaleziono łącznie {len(stations)} stacji w regionie.")

        if not stations:
            print("Brak stacji spełniających kryteria.")
            return

        print("Pobieranie sensorów dla stacji...")
        station_sensor_tasks = [
            get_sensors(client, s.get("Identyfikator stacji")) for s in stations
        ]
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
                    # Przekazujemy semafor do zadań pobierania
                    download_tasks.append(
                        get_archival_data_paginated(client, sensor_id, semaphore)
                    )
                    task_metadata.append(
                        {
                            "voivodeship": voivodeship,
                            "station_id": s_id,
                            "station_name": s_name,
                            "sensor_id": sensor_id,
                            "indicator": indicator,
                        }
                    )

        print(
            f"\nUruchamiam kontrolowane pobieranie pomiarów dla {len(download_tasks)} sensorów (max {SEMAPHORE_LIMIT} naraz)...")
        measurements_results = await asyncio.gather(*download_tasks)

        print("Przetwarzanie i agregowanie wyników...")
        all_records = []
        for meta, measurements in zip(task_metadata, measurements_results):
            for m in measurements:
                all_records.append(
                    {
                        "voivodeship": meta["voivodeship"],
                        "station_id": meta["station_id"],
                        "station_name": meta["station_name"],
                        "sensor_id": meta["sensor_id"],
                        "indicator": meta["indicator"],
                        "sensor_code": m.get("Kod stanowiska"),
                        "datetime": m.get("Data"),
                        "value": m.get("Wartość"),
                    }
                )

    if not all_records:
        print("\nNie pobrano żadnych danych pomiarowych.")
        return

    df = pd.DataFrame(all_records)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values(["voivodeship", "station_name", "indicator", "datetime"]).reset_index(drop=True)

    print(f"\nSukces! Pobrano łącznie {len(df)} rekordów pomiarowych.")
    print(df.head(5))

    # Zapis bezpieczny z obsługą braku pyarrow
    csv_filename = "gios_4_voivodeships.csv"
    df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
    print(f"\nZapisano do CSV: {csv_filename}")

    try:
        parquet_filename = "gios_4_voivodeships.parquet"
        df.to_parquet(parquet_filename, index=False)
        print(f"Zapisano do Parquet: {parquet_filename}")
    except ImportError:
        print(
            "Uwaga: Brak biblioteki 'pyarrow'. Pominięto zapis do Parquet (zainstaluj przez `uv add pyarrow`, jeśli potrzebny).")


if __name__ == "__main__":
    asyncio.run(main())