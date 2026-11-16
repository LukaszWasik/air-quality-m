import pandas as pd
from air_quality_ml.database import engine  # Importujesz swój silnik z database.py
from sqlalchemy import text


def init_database():
    """Tworzy tabele i włącza PostGIS, jeśli jeszcze nie istnieją."""
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))

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
                          ) NOT NULL
                              );
                          """))

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
                          CREATE INDEX IF NOT EXISTS idx_measurements_sensor_date
                              ON measurements(sensor_id, datetime);
                          """))

    print("Tabele zostały pomyślnie zainicjalizowane w bazie.")


def load_parquet_to_db(file_path: str = "gios_4_voivodeships.parquet"):
    """Wczytuje lokalny plik Parquet i ładuje dane do PostgreSQL."""
    try:
        df = pd.read_parquet(file_path)
    except FileNotFoundError:
        print(f"Nie znaleziono pliku {file_path}. Najpierw pobierz dane.")
        return

    print(f"Wczytano {len(df)} wierszy. Ładowanie do bazy PostgreSQL...")

    init_database()

    with engine.begin() as conn:
        # 1. Stacje
        stations_df = df[["station_id", "station_name", "voivodeship"]].drop_duplicates(subset=["station_id"])
        stations_df.to_sql("stations", con=conn, if_exists="append", index=False, method="multi")

        # 2. Sensory
        sensors_df = df[["sensor_id", "station_id", "indicator", "sensor_code"]].drop_duplicates(subset=["sensor_id"])
        sensors_df.to_sql("sensors", con=conn, if_exists="append", index=False, method="multi")

        # 3. Pomiary czasowe
        measurements_df = df[["sensor_id", "datetime", "value"]]
        measurements_df.to_sql(
            "measurements",
            con=conn,
            if_exists="append",
            index=False,
            chunksize=10000,
            method="multi"
        )

    print("Dane zostały w pełni załadowane do bazy!")


if __name__ == "__main__":
    load_parquet_to_db()