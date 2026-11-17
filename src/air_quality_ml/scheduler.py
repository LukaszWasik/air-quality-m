import asyncio
from datetime import datetime
from air_quality_ml.ingestion.gios_sync import run_incremental_sync


async def main_scheduler():
    print("--- URUCHOMIONO SCHEDULER ETL (Tryb ciągły: co 1h) ---")

    while True:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{current_time}] Sprawdzanie i uruchamianie synchronizacji przyrostowej...")

        try:
            # Uruchamiamy nasz proces ETL
            await run_incremental_sync()
        except Exception as e:
            print(f"[BŁĄD] Wystąpił problem podczas automatycznej synchronizacji: {e}")

        # Czekaj 1 godzinę (3600 sekund) do kolejnego sprawdzenia
        print("Cykl zakończony. Usypiam scheduler na 1 godzinę...")
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main_scheduler())