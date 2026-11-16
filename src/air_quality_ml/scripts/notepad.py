import httpx

response = httpx.get(
    "https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll",
    params={"size": 1}
)
station_sample = response.json().get("Lista stacji pomiarowych", [])[0]

print("Klucze dostępne w stacji:")
for key in station_sample.keys():
    print(f" - {key}: {station_sample[key]}")