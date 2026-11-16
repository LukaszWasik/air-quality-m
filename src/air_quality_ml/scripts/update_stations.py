import httpx
from sqlalchemy import text
from air_quality_ml.database import engine

GIOS_API_URL = "https://api.gios.gov.pl/pjp-api/v1/rest"


def update_station_coordinates() -> None:
    """Pobiera stacje z API GIOŚ i aktualizuje ich współrzędne oraz kolumnę PostGIS

    w bazie danych, nie naruszając istniejących pomiarów.
    """
    print("Pobieranie aktualnej listy stacji z API GIOŚ...")
    response = httpx.get(
        f"{GIOS_API_URL}/station/findAll", params={"size": 1000}, timeout=30
    )
    response.raise_for_status()
    data = response.json()
    stations = data.get("Lista stacji pomiarowych", [])

    print(
        f"Pobrano {len(stations)} stacji. Aktualizacja bazy danych PostgreSQL..."
    )

    with engine.begin() as conn:
        # 1. Upewnij się, że rozszerzenie PostGIS i kolumny współrzędnych istnieją
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.execute(
            text(
                """
            ALTER TABLE stations 
            ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS geom GEOMETRY(Point, 4326);
        """
            )
        )

        updated_count = 0
        for station in stations:
            station_id = station.get("Identyfikator stacji")
            if not station_id:
                continue

            # Dokładne klucze wykryte w API GIOŚ
            lat = station.get("WGS84 φ N")
            lon = station.get("WGS84 λ E")

            if lat is not None and lon is not None:
                try:
                    lat = float(lat)
                    lon = float(lon)

                    # Aktualizujemy stację o konkretnym ID
                    conn.execute(
                        text(
                            """
                            UPDATE stations 
                            SET latitude = :lat, 
                                longitude = :lon,
                                geom = ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
                            WHERE station_id = :station_id;
                        """
                        ),
                        {"lat": lat, "lon": lon, "station_id": station_id},
                    )
                    updated_count += 1
                except ValueError:
                    print(
                        f"Nie udało się przekonwertować współrzędnych dla stacji ID {station_id}"
                    )

    print(
        f"\nSukces! Zaktualizowano współrzędne dla {updated_count} stacji w bazie."
    )


if __name__ == "__main__":
    update_station_coordinates()