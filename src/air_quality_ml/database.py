from sqlalchemy import URL, create_engine, text

from air_quality_ml.config import settings

database_url = URL.create(
    drivername="postgresql+psycopg",
    username=settings.postgres_user,
    password=settings.postgres_password,
    host=settings.postgres_host,
    port=settings.postgres_port,
    database=settings.postgres_db,
)

engine = create_engine(
    database_url,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 5},
)


def check_database_connection() -> None:
    """Sprawdza poprawność połączenia z bazą danych."""
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        print(f"Database connection successful: {result.scalar()}")


def init_db() -> None:
    """Inicjalizuje bazę danych: włącza PostGIS, tworzy tabele oraz indeksy

    zapewniające unikalność pomiarów pod mechanizm UPSERT.
    """
    print("Inicjalizacja struktury bazy danych PostgreSQL...")

    with engine.begin() as conn:
        # 1. Włączenie rozszerzenia geoprzestrzennego PostGIS
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))

        # 2. Tabela stacji pomiarowych
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS stations
                          (
                              station_id
                              INT
                              PRIMARY
                              KEY,
                              station_name
                              VARCHAR
                          (
                              255
                          ) NOT NULL,
                              voivodeship VARCHAR
                          (
                              100
                          ) NOT NULL,
                              latitude DOUBLE PRECISION,
                              longitude DOUBLE PRECISION,
                              geom GEOMETRY
                          (
                              Point,
                              4326
                          )
                              );
                          """))

        # 3. Tabela sensorów (stanowisk pomiarowych)
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS sensors
                          (
                              sensor_id
                              INT
                              PRIMARY
                              KEY,
                              station_id
                              INT
                              REFERENCES
                              stations
                          (
                              station_id
                          ) ON DELETE CASCADE,
                              indicator VARCHAR
                          (
                              50
                          ) NOT NULL,
                              sensor_code VARCHAR
                          (
                              100
                          )
                              );
                          """))

        # 4. Tabela pomiarów czasowych
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS measurements
                          (
                              id
                              SERIAL
                              PRIMARY
                              KEY,
                              sensor_id
                              INT
                              REFERENCES
                              sensors
                          (
                              sensor_id
                          ) ON DELETE CASCADE,
                              datetime TIMESTAMP NOT NULL,
                              value REAL
                              );
                          """))
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS meteo_measurements
                          (
                              id
                              SERIAL
                              PRIMARY
                              KEY,
                              station_id
                              INT
                              REFERENCES
                              stations
                          (
                              station_id
                          ) ON DELETE CASCADE,
                              datetime TIMESTAMP NOT NULL,
                              temperature_2m REAL,
                              relative_humidity_2m REAL,
                              dew_point_2m REAL,
                              surface_pressure REAL,
                              wind_speed_10m REAL,
                              wind_direction_10m REAL,
                              wind_gusts_10m REAL,
                              precipitation REAL,
                              cloud_cover REAL,
                              boundary_layer_height REAL,
                              shortwave_radiation REAL
                              );
                          """))

        # 5. Kluczowy indeks unikalny dla mechanizmu UPSERT oraz przyspieszenia zapytań
        conn.execute(text("""
                          CREATE UNIQUE INDEX IF NOT EXISTS idx_measurements_sensor_datetime
                              ON measurements(sensor_id, datetime);
                          """))
        conn.execute(text("""
                          CREATE UNIQUE INDEX IF NOT EXISTS idx_meteo_station_datetime
                              ON meteo_measurements(station_id, datetime);
                          """))

    print("Struktura bazy danych została pomyślnie skonfigurowana!")


if __name__ == "__main__":
    check_database_connection()
    init_db()