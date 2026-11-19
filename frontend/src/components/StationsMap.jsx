import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
import L from "leaflet";
import markerIcon2x from "leaflet/dist/images/marker-icon-2x.png";
import markerIcon from "leaflet/dist/images/marker-icon.png";
import markerShadow from "leaflet/dist/images/marker-shadow.png";

// Naprawa domyślnych ikon markerów Leaflet w bundlerach typu Vite
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: markerIcon2x,
  iconUrl: markerIcon,
  shadowUrl: markerShadow,
});

export default function StationsMap({ stations, selectedStationId, onSelectStation }) {
  // Centrum na południowo-wschodnią Polskę (nasze 4 województwa)
  const defaultCenter = [50.3, 21.5];

  return (
    <div className="h-[420px] w-full rounded-xl overflow-hidden border border-slate-200 shadow-sm">
      <MapContainer center={defaultCenter} zoom={7} className="h-full w-full">
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        {stations
          .filter((s) => s.latitude != null && s.longitude != null)
          .map((station) => (
            <Marker
              key={station.station_id}
              position={[station.latitude, station.longitude]}
              eventHandlers={{
                click: () => onSelectStation(station),
              }}
            >
              <Popup>
                <div className="text-sm">
                  <p className="font-semibold text-slate-900">{station.station_name}</p>
                  <p className="text-slate-600">{station.voivodeship}</p>
                  <button
                    onClick={() => onSelectStation(station)}
                    className="mt-2 px-2.5 py-1 bg-blue-600 text-white rounded text-xs hover:bg-blue-700 cursor-pointer"
                  >
                    {selectedStationId === station.station_id ? "Wybrano" : "Pokaż wykres"}
                  </button>
                </div>
              </Popup>
            </Marker>
          ))}
      </MapContainer>
    </div>
  );
}