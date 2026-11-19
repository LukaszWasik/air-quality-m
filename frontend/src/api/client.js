const API_BASE_URL = "http://127.0.0.1:8000";

export async function fetchStations() {
  const res = await fetch(`${API_BASE_URL}/api/stations`);
  if (!res.ok) throw new Error("Nie udało się pobrać listy stacji");
  return res.json();
}

export async function fetchStationSensors(stationId) {
  const res = await fetch(`${API_BASE_URL}/api/stations/${stationId}/sensors`);
  if (!res.ok) throw new Error("Nie udało się pobrać sensorów stacji");
  return res.json();
}

export async function fetchStationMeasurements(stationId, indicator = null, limit = 168) {
  const params = new URLSearchParams({ limit: String(limit) });
  if (indicator) params.set("indicator", indicator);

  const res = await fetch(
    `${API_BASE_URL}/api/stations/${stationId}/measurements?${params.toString()}`
  );
  if (!res.ok) throw new Error("Nie udało się pobrać pomiarów");
  return res.json();
}