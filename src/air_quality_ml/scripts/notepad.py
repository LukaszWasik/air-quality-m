# import httpx
#
# response = httpx.get(
#     "https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll",
#     params={"size": 1}
# )
# station_sample = response.json().get("Lista stacji pomiarowych", [])[0]
#
# print("Klucze dostępne w stacji:")
# for key in station_sample.keys():
#     print(f" - {key}: {station_sample[key]}")
import asyncio
from datetime import datetime
import httpx
import pandas as pd
from tqdm.asyncio import tqdm
from air_quality_ml.ingestion.meteo_sync import (
    iter_month_days,
    get_stations_from_db,
    fetch_station_meteo,
    save_meteo_to_landing_zone,
    upsert_meteo_to_db,
)

async def retry_2024():
    stations = get_stations_from_db()
    sem = asyncio.Semaphore(2)
    start_dt = datetime(2024, 3, 1)
    end_dt = datetime(2024, 8, 31)
    total = 0

    async with httpx.AsyncClient(timeout=60) as client:
        for d_from, d_to, label in iter_month_days(start_dt, end_dt):
            tasks = [fetch_station_meteo(client, st, d_from, d_to, sem) for st in stations]
            results = await tqdm.gather(*tasks, desc=f"Meteo fix {label}", leave=False)
            records = [rec for st_recs in results for rec in st_recs]

            if not records:
                print(f"[{label}] Nadal 0 rekordów.")
                continue

            df = pd.DataFrame(records)
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
            df = df.dropna(subset=["datetime"])
            df = df.drop_duplicates(subset=["station_id", "datetime"]).reset_index(drop=True)

            backup = save_meteo_to_landing_zone(df, tag=f"meteo_fix_{label}")
            synced = upsert_meteo_to_db(df)
            total += synced
            print(f"[{label}] Zsynchronizowano {synced} rekordów (backup: {backup.name}).")

    print(f"Gotowe! Łącznie przetworzono w poprawce: {total} rekordów.")

asyncio.run(retry_2024())