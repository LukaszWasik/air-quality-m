from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from air_quality_ml.database import engine

app = FastAPI(
    title="Air Quality & Meteo API",
    description="Backend dla projektu: Jakość powietrza + Dane Meteo + ML",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {
        "status": "online",
        "message": "Witaj w API systemu jakości powietrza!",
    }


@app.get("/api/stations")
def get_stations():
    stations = []
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                SELECT station_id, station_name, voivodeship, latitude, longitude
                FROM stations
                ORDER BY voivodeship, station_name;
            """)
        )
        for row in result:
            stations.append(
                {
                    "station_id": row.station_id,
                    "station_name": row.station_name,
                    "voivodeship": row.voivodeship,
                    "latitude": row.latitude,
                    "longitude": row.longitude,
                }
            )

    return {
        "count": len(stations),
        "stations": stations,
    }


@app.get("/api/stations/{station_id}/sensors")
def get_station_sensors(station_id: int):
    """Zwraca dostępne wskaźniki/sensory dla wybranej stacji."""
    with engine.begin() as conn:
        station_exists = conn.execute(
            text("SELECT 1 FROM stations WHERE station_id = :station_id;"),
            {"station_id": station_id},
        ).first()
        if not station_exists:
            raise HTTPException(status_code=404, detail="Nie znaleziono stacji")

        rows = conn.execute(
            text("""
                SELECT sensor_id, indicator, sensor_code
                FROM sensors
                WHERE station_id = :station_id
                ORDER BY indicator;
            """),
            {"station_id": station_id},
        ).fetchall()

    return {
        "station_id": station_id,
        "sensors": [
            {
                "sensor_id": r.sensor_id,
                "indicator": r.indicator,
                "sensor_code": r.sensor_code,
            }
            for r in rows
        ],
    }


@app.get("/api/stations/{station_id}/measurements")
def get_station_measurements(
    station_id: int,
    indicator: Optional[str] = Query(default=None, description="np. 'pył zawieszony PM10'"),
    limit: int = Query(default=168, ge=1, le=5000, description="Liczba najnowszych rekordów (domyślnie 168h = 7 dni)"),
):
    """Zwraca pomiary zanieczyszczeń dla stacji (chronologicznie rosnąco pod wykresy)."""
    query = """
        SELECT m.datetime, m.value, s.indicator, s.sensor_id
        FROM measurements m
        JOIN sensors s ON m.sensor_id = s.sensor_id
        WHERE s.station_id = :station_id
    """
    params: dict = {"station_id": station_id, "limit": limit}

    if indicator:
        query += " AND s.indicator = :indicator"
        params["indicator"] = indicator

    # Pobieramy najnowsze N rekordów, a w podzapytaniu odwracamy kolejność pod oś X wykresu
    wrapped_query = f"""
        SELECT datetime, value, indicator, sensor_id
        FROM (
            {query}
            ORDER BY m.datetime DESC
            LIMIT :limit
        ) sub
        ORDER BY datetime ASC;
    """

    with engine.begin() as conn:
        rows = conn.execute(text(wrapped_query), params).fetchall()

    return {
        "station_id": station_id,
        "indicator": indicator,
        "count": len(rows),
        "measurements": [
            {
                "datetime": r.datetime.isoformat() if r.datetime else None,
                "value": r.value,
                "indicator": r.indicator,
                "sensor_id": r.sensor_id,
            }
            for r in rows
        ],
    }


@app.get("/api/stations/{station_id}/meteo")
def get_station_meteo(
    station_id: int,
    limit: int = Query(default=168, ge=1, le=5000, description="Liczba najnowszych godzin pogodowych"),
):
    """Zwraca dane meteorologiczne dla stacji."""
    sql = text("""
        SELECT
            datetime,
            temperature_2m,
            relative_humidity_2m,
            dew_point_2m,
            surface_pressure,
            wind_speed_10m,
            wind_direction_10m,
            wind_gusts_10m,
            precipitation,
            cloud_cover,
            boundary_layer_height,
            shortwave_radiation
        FROM (
            SELECT *
            FROM meteo_measurements
            WHERE station_id = :station_id
            ORDER BY datetime DESC
            LIMIT :limit
        ) sub
        ORDER BY datetime ASC;
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, {"station_id": station_id, "limit": limit}).fetchall()

    return {
        "station_id": station_id,
        "count": len(rows),
        "meteo": [
            {
                "datetime": r.datetime.isoformat() if r.datetime else None,
                "temperature_2m": r.temperature_2m,
                "relative_humidity_2m": r.relative_humidity_2m,
                "dew_point_2m": r.dew_point_2m,
                "surface_pressure": r.surface_pressure,
                "wind_speed_10m": r.wind_speed_10m,
                "wind_direction_10m": r.wind_direction_10m,
                "wind_gusts_10m": r.wind_gusts_10m,
                "precipitation": r.precipitation,
                "cloud_cover": r.cloud_cover,
                "boundary_layer_height": r.boundary_layer_height,
                "shortwave_radiation": r.shortwave_radiation,
            }
            for r in rows
        ],
    }