import { useEffect, useState } from "react";
import { Wind, MapPin, Activity } from "lucide-react";
import {
  fetchStations,
  fetchStationSensors,
  fetchStationMeasurements,
} from "./api/client";
import StationsMap from "./components/StationsMap";
import MeasurementChart from "./components/MeasurementChart";

export default function App() {
  const [stations, setStations] = useState([]);
  const [selectedStation, setSelectedStation] = useState(null);
  const [sensors, setSensors] = useState([]);
  const [selectedIndicator, setSelectedIndicator] = useState("");
  const [measurements, setMeasurements] = useState([]);
  const [loadingStations, setLoadingStations] = useState(true);
  const [loadingChart, setLoadingChart] = useState(false);
  const [error, setError] = useState(null);

  // 1. Pobranie listy stacji przy starcie
  useEffect(() => {
    async function loadInitial() {
      try {
        setLoadingStations(true);
        const data = await fetchStations();
        const list = data.stations || [];
        setStations(list);
        if (list.length > 0) {
          setSelectedStation(list[0]);
        }
      } catch (err) {
        setError(err.message);
      } finally {
        setLoadingStations(false);
      }
    }
    loadInitial();
  }, []);

  // 2. Pobranie sensorów po zmianie stacji
  useEffect(() => {
    if (!selectedStation) return;

    async function loadSensors() {
      try {
        const data = await fetchStationSensors(selectedStation.station_id);
        const list = data.sensors || [];
        setSensors(list);

        // Preferuj PM10 jeśli jest dostępne, w przeciwnym razie pierwszy sensor
        const pm10 = list.find((s) => s.indicator.toLowerCase().includes("pm10"));
        setSelectedIndicator(pm10 ? pm10.indicator : list[0]?.indicator || "");
      } catch (err) {
        setError(err.message);
      }
    }
    loadSensors();
  }, [selectedStation]);

  // 3. Pobranie pomiarów po zmianie stacji lub wskaźnika
  useEffect(() => {
    if (!selectedStation || !selectedIndicator) {
      setMeasurements([]);
      return;
    }

    async function loadMeasurements() {
      try {
        setLoadingChart(true);
        const data = await fetchStationMeasurements(
          selectedStation.station_id,
          selectedIndicator,
          168
        );
        setMeasurements(data.measurements || []);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoadingChart(false);
      }
    }
    loadMeasurements();
  }, [selectedStation, selectedIndicator]);

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <header className="bg-white border-b border-slate-200 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-blue-600 text-white rounded-lg">
            <Wind className="w-5 h-5" />
          </div>
          <div>
            <h1 className="text-lg font-bold tracking-tight">Air Quality Monitor</h1>
            <p className="text-xs text-slate-500">
              GIOŚ + Open-Meteo • Podstawa pod analizę i ML
            </p>
          </div>
        </div>
        <span className="text-xs font-medium bg-slate-100 text-slate-700 px-3 py-1 rounded-full">
          Stacji w bazie: {stations.length}
        </span>
      </header>

      <main className="max-w-7xl mx-auto p-6 space-y-6">
        {error && (
          <div className="p-4 rounded-xl bg-red-50 border border-red-200 text-red-700 text-sm">
            {error}
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          {/* Lewa kolumna: mapa + szybki wybór stacji */}
          <div className="lg:col-span-6 space-y-4">
            <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm space-y-3">
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-2 font-semibold text-sm">
                  <MapPin className="w-4 h-4 text-blue-600" />
                  <span>Mapa stacji pomiarowych</span>
                </div>

                <select
                  value={selectedStation?.station_id || ""}
                  onChange={(e) => {
                    const found = stations.find(
                      (s) => String(s.station_id) === e.target.value
                    );
                    if (found) setSelectedStation(found);
                  }}
                  disabled={loadingStations}
                  className="text-sm border border-slate-300 rounded-lg px-2.5 py-1.5 bg-white max-w-[260px]"
                >
                  {stations.map((s) => (
                    <option key={s.station_id} value={s.station_id}>
                      {s.station_name} ({s.voivodeship})
                    </option>
                  ))}
                </select>
              </div>

              <StationsMap
                stations={stations}
                selectedStationId={selectedStation?.station_id}
                onSelectStation={(st) => setSelectedStation(st)}
              />
            </div>
          </div>

          {/* Prawa kolumna: wykres dla wybranej stacji i wskaźnika */}
          <div className="lg:col-span-6">
            <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm space-y-4 h-full">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <div className="flex items-center gap-2 font-semibold text-base">
                    <Activity className="w-4 h-4 text-blue-600" />
                    <span>{selectedStation?.station_name || "Wybierz stację"}</span>
                  </div>
                  <p className="text-xs text-slate-500">
                    {selectedStation?.voivodeship} • Ostatnie 168 pomiarów
                  </p>
                </div>

                <select
                  value={selectedIndicator}
                  onChange={(e) => setSelectedIndicator(e.target.value)}
                  disabled={sensors.length === 0}
                  className="text-sm border border-slate-300 rounded-lg px-3 py-1.5 bg-white"
                >
                  {sensors.map((sensor) => (
                    <option key={sensor.sensor_id} value={sensor.indicator}>
                      {sensor.indicator}
                    </option>
                  ))}
                </select>
              </div>

              {loadingChart ? (
                <div className="h-72 flex items-center justify-center text-slate-500 text-sm">
                  Ładowanie danych wykresu...
                </div>
              ) : (
                <MeasurementChart
                  measurements={measurements}
                  indicator={selectedIndicator}
                />
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}