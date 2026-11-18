import httpx
import json

# Współrzędne dla testu (np. Rzeszów - al. Rejtana)
LAT = 50.0349
LON = 22.0058

url = "https://archive-api.open-meteo.com/v1/archive"

# Sprawdzamy szerszy wachlarz parametrów, które mogą mieć kluczowe znaczenie dla jakości powietrza (smogu)
params = {
    "latitude": LAT,
    "longitude": LON,
    "start_date": "2026-01-01",
    "end_date": "2026-01-01",
    "hourly": (
        "temperature_2m,relative_humidity_2m,dew_point_2m,"
        "surface_pressure,wind_speed_10m,wind_direction_10m,"
        "wind_gusts_10m,precipitation,cloud_cover,"
        "boundary_layer_height,shortwave_radiation"
    )
}


def test_fetch():
    print("Wysyłam zapytanie testowe do Open-Meteo API...")
    response = httpx.get(url, params=params, timeout=30)

    if response.status_code == 200:
        data = response.json()
        print("\nSukces! Otrzymano dane.")
        print(f"Strefa czasowa: {data.get('timezone')}")
        print(f"Wysokość n.p.m.: {data.get('elevation')} m")

        hourly_data = data.get("hourly", {})
        print("\nLista zwróconych zmiennych godzinowych i ich pierwsze wartości:")
        for key, values in hourly_data.items():
            first_val = values[0] if values else "Brak danych"
            print(f" - {key}: np. {first_val}")
    else:
        print(f"Błąd HTTP: {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    test_fetch()