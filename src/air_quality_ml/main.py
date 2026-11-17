import asyncio
from air_quality_ml.ingestion.gios_sync import run_incremental_sync
def main() -> None:
    # Główny punkt wejścia do aplikacji
    asyncio.run(run_incremental_sync())

if __name__ == "__main__":
    main()