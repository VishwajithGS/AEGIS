import React from "react";
import { Bar, CartesianGrid, ComposedChart, Legend, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { Card, Empty, KV } from "./ui.jsx";
import { fmt, fmtTime } from "../constants.js";

const CATEGORY_ORDER = ["no_rain", "light", "moderate", "heavy", "very_heavy", "extremely_heavy"];

// Real-data layers: shown only when the API serves a real-data root.
export default function RealDataPanels({ real }) {
  if (!real || !real.available) return null;
  const v = real.validation || {};
  const e5 = v.era5_vs_imd_region_daily_mean || {};
  const g = real.gauge;
  return (
    <>
      <Card title="Observed daily rainfall (IMD gridded) vs ERA5 reanalysis"
            subtitle={`${real.scope} · IMD day = 24 h read at 08:30 IST (start/end attribution assumed, see validation report) · IMD = official gauge-based grid; ERA5 = reanalysis (model + observations)`}>
        <ResponsiveContainer width="100%" height={260}>
          <ComposedChart data={real.daily_rainfall}>
            <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="date" fontSize={11} /><YAxis unit=" mm" /><Tooltip /><Legend />
            <Bar dataKey="imd_mm" name="IMD observed (mean)" fill="#1565c0" isAnimationActive={false} />
            <Line dataKey="era5_mm" name="ERA5 same window (mean)" stroke="#ef6c00" dot={false} isAnimationActive={false} />
            <Line dataKey="imd_max_mm" name="IMD wettest zone" stroke="#6a1b9a" strokeDasharray="4 3" dot={false} isAnimationActive={false} />
          </ComposedChart>
        </ResponsiveContainer>
        <KV items={[
          ["Region-mean agreement", e5.pairs ? `r = ${fmt(e5.correlation, 3)}, mean difference ERA5 − IMD ${fmt(e5.mean_difference_mm, 2)} mm/day, MAE ${fmt(e5.mae_mm, 2)} mm` : "—"],
          ["Period totals (region mean)", e5.pairs ? `IMD ${fmt(e5.total_a_mm, 0)} mm · ERA5 ${fmt(e5.total_b_mm, 0)} mm` : "—"],
          ["IMD intensity classes (zone-days)", CATEGORY_ORDER.filter((c) => real.imd_category_counts && real.imd_category_counts[c])
            .map((c) => `${c.replace("_", " ")} ${real.imd_category_counts[c]}`).join(" · ") || "—"],
          ["Heavy zone-days (IMD ≥ 64.5 mm)", v.heavy_days ? `${v.heavy_days.imd_heavy_zone_days}; ERA5 also heavy on ${v.heavy_days.era5_also_heavy}` : "—"],
        ]} />
      </Card>
      {g && (
        <Card title={`Rain gauge: ${g.station || "NOAA GHCN-Daily"} (zone ${g.zone_id})`}
              subtitle="NOAA GHCN-Daily station observations vs the IMD cell and ERA5 for the same zone; gaps = no gauge report">
          <ResponsiveContainer width="100%" height={220}>
            <ComposedChart data={g.daily}>
              <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="date" fontSize={11} /><YAxis unit=" mm" /><Tooltip /><Legend />
              <Bar dataKey="gauge_mm" name="gauge" fill="#2e7d32" isAnimationActive={false} />
              <Line dataKey="imd_mm" name="IMD cell" stroke="#1565c0" dot={false} isAnimationActive={false} />
              <Line dataKey="era5_mm" name="ERA5" stroke="#ef6c00" dot={false} isAnimationActive={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </Card>
      )}
      <Card title="Officially reported events" subtitle="Dartmouth Flood Observatory archive and GDACS: regional events, not street-level incidents">
        {!real.official_events || real.official_events.length === 0 ? <Empty>No official events in the imported data.</Empty> : (
          <table className="table small">
            <thead><tr><th>Began</th><th>Event</th><th>Source</th><th>Severity / alert</th><th>Deaths</th><th>Displaced</th><th>Scope</th></tr></thead>
            <tbody>
              {real.official_events.map((e) => (
                <tr key={e.event_id}>
                  <td>{fmtTime(e.began)}</td>
                  <td>{e.name}</td>
                  <td>{e.data_source}</td>
                  <td>{e.severity_label ? `DFO ${e.severity_label}` : e.alert_level ? `GDACS ${e.alert_level}` : "—"}</td>
                  <td>{e.dead ?? "—"}</td>
                  <td>{e.displaced ?? "—"}</td>
                  <td className="muted">{e.spatial_scope}{e.distance_to_region_km != null ? ` · ${fmt(e.distance_to_region_km, 0)} km from the study area` : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>
    </>
  );
}
