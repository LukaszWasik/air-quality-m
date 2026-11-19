import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";

export default function MeasurementChart({ measurements, indicator }) {
  if (!measurements || measurements.length === 0) {
    return (
      <div className="h-72 flex items-center justify-center text-slate-500 border border-dashed border-slate-300 rounded-xl">
        Brak danych pomiarowych dla wybranego wskaźnika.
      </div>
    );
  }

  const formattedData = measurements.map((m) => ({
    ...m,
    label: m.datetime
      ? new Date(m.datetime).toLocaleString("pl-PL", {
          day: "2-digit",
          month: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
        })
      : "",
  }));

  return (
    <div className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={formattedData} margin={{ top: 10, right: 20, left: 0, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="label" tick={{ fontSize: 11 }} minTickGap={25} />
          <YAxis tick={{ fontSize: 12 }} />
          <Tooltip />
          <Line
            type="monotone"
            dataKey="value"
            name={indicator || "Wartość"}
            stroke="#2563eb"
            strokeWidth={2}
            dot={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}