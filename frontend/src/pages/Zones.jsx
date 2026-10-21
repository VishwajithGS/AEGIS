import React, { useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api } from "../services/api.js";
import { useApi } from "../hooks/useApi.js";
import { Bar, Card, Empty, ErrorBox, KV, Loading, OriginBadge, RiskBadge } from "../components/ui.jsx";
import { RISK_COLORS, RISK_ORDER, fmt, fmtTime } from "../constants.js";

const FEATURE_COLOR = { rainfall: "#1565c0", water_level: "#00838f", incidents: "#6a1b9a", traffic: "#8d6e63", historical: "#546e7a" };

function ZoneDetail({ zoneId, refreshKey }) {
  const { data, error, loading } = useApi(() => api.zone(zoneId), [zoneId, refreshKey]);
  if (loading && !data) return <Loading what={`zone ${zoneId}`} />;
  if (error) return <ErrorBox error={error} what={`zone ${zoneId}`} />;
  const a = data.assessment;
  const contrib = (a && typeof a.contributions === "object" && a.contributions) || {};
  const live = (data.live_history_hbase || []).slice().reverse().map((h) => ({ t: fmtTime(h.window_start), score: h.risk_score }));
  const batch = (data.batch_history_72h || []).map((h) => ({ t: fmtTime(h.hour), score: h.risk_score }));
  return (
    <div>
      <h3>
        {zoneId} {a && <RiskBadge category={a.risk_category} score={a.risk_score} />} {a && <OriginBadge origin={a.data_origin} stale={a.stale} />}
      </h3>
      {!a ? <Empty>No assessment yet for this zone.</Empty> : (
        <>
          <p className="explain">{a.explanation}</p>
          <KV items={[
            ["Window", `${fmtTime(a.window_start)} → ${fmtTime(a.window_end)}`],
            ["Confidence / coverage", `${a.confidence} · ${Math.round((a.coverage || 0) * 100)}% of indicator weight observed`],
            ["Missing indicators", (a.missing_features || []).join(", ") || "none"],
            ["Observations", `weather ${a.weather_obs ?? "—"} · water ${a.water_obs ?? "—"} · traffic ${a.traffic_obs ?? "—"}`],
            ["Read from", a.read_from],
          ]} />
          <h4>Contributions to the rule-based score (points)</h4>
          {Object.keys(contrib).length === 0 ? <p className="muted small">No contributions (insufficient data).</p> :
            Object.entries(contrib).sort((x, y) => y[1] - x[1]).map(([k, v]) => (
              <Bar key={k} label={k} value={v} max={100} color={FEATURE_COLOR[k] || "#455a64"} />
            ))}
          <KV items={[
            ["Rainfall (mean)", `${fmt(a.rainfall_avg_mm_hr, 2)} mm/hr`],
            ["Water level (max)", a.level_ratio_max != null ? `${fmt(a.level_ratio_max, 2)} × danger level` : "not observed"],
            ["Weighted reports", fmt(a.weighted_reports, 1)],
            ["Traffic disrupted", a.disrupted_ratio != null ? `${Math.round(a.disrupted_ratio * 100)}%` : "not observed"],
            ["Historical hazard index", fmt(a.historical_hazard_index, 2)],
          ]} />
        </>
      )}
      <h4>Spatial assignment</h4>
      <KV items={[
        ["Weather station", data.weather_station ? `${data.weather_station[0]} (${data.weather_station[1]} km)` : "none within radius"],
        ["River gauge", data.water_gauge ? `${data.water_gauge[0]} (${data.water_gauge[1]} km)` : "none within radius → water level not observed"],
      ]} />
      <h4>ML-predicted flood-incident severity <span className="muted small">(separate from the rule-based score)</span></h4>
      {data.ml_prediction ? (
        <div className="ml-box">
          <strong>{data.ml_prediction.predicted_severity || "Insufficient data"}</strong>{data.ml_prediction.prediction_status && data.ml_prediction.prediction_status !== "ok" ? ` (${data.ml_prediction.prediction_status})` : ""} · model {data.ml_prediction.model_version} · features as of {fmtTime(data.ml_prediction.features_as_of)}
          <div className="muted small">Model trained on SYNTHETIC labels. Not an observed severity and not a warning.</div>
        </div>
      ) : <p className="muted small">No zone prediction yet (run the inference job).</p>}
      <h4>Risk history</h4>
      {live.length > 0 && (
        <>
          <div className="muted small">Live windows from HBase zone_risk_history (newest {live.length})</div>
          <ResponsiveContainer width="100%" height={160}>
            <LineChart data={live}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="t" hide /><YAxis domain={[0, 100]} />
              <Tooltip /><Line dataKey="score" stroke="#c62828" dot={false} isAnimationActive={false} /></LineChart>
          </ResponsiveContainer>
        </>
      )}
      {(data.latest_row_score_versions || []).length > 1 && (
        <p className="muted small">
          HBase cell versions of the latest window's risk:risk_score (newest first):{" "}
          {data.latest_row_score_versions.map((v) => fmt(v.value)).join(" ← ")}
        </p>
      )}
      {data.hbase_error && <p className="warn small">HBase history unavailable: {data.hbase_error}</p>}
      {batch.length > 0 && (
        <>
          <div className="muted small">Batch historical, last 72 hours of the archive</div>
          <ResponsiveContainer width="100%" height={160}>
            <LineChart data={batch}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="t" hide /><YAxis domain={[0, 100]} />
              <Tooltip /><Line dataKey="score" stroke="#1565c0" dot={false} isAnimationActive={false} /></LineChart>
          </ResponsiveContainer>
        </>
      )}
    </div>
  );
}

export default function Zones({ zonesData, error, selected, onSelect, refreshKey }) {
  const [category, setCategory] = useState("");
  const [sortKey, setSortKey] = useState("risk_score");
  const [desc, setDesc] = useState(true);
  const rows = useMemo(() => {
    const zs = (zonesData ? zonesData.zones : []).filter((z) => !category || ((z.assessment || {}).risk_category || "No assessment") === category);
    const val = (z) => (sortKey === "zone_id" ? z.zone_id : (z.assessment || {})[sortKey]);
    return zs.slice().sort((a, b) => {
      const x = val(a), y = val(b);
      if (x == null && y == null) return 0;
      if (x == null) return 1;
      if (y == null) return -1;
      return (x < y ? -1 : x > y ? 1 : 0) * (desc ? -1 : 1);
    });
  }, [zonesData, category, sortKey, desc]);
  const head = (k, label) => (
    <th className="sortable" onClick={() => (sortKey === k ? setDesc(!desc) : (setSortKey(k), setDesc(true)))}>
      {label} {sortKey === k ? (desc ? "▼" : "▲") : ""}
    </th>
  );
  if (!zonesData) return error ? <ErrorBox error={error} what="zones" /> : <Loading what="zones" />;
  return (
    <div className="page grid-zones">
      <Card title="Zone risk explorer" subtitle={zonesData.note}
            actions={
              <select value={category} onChange={(e) => setCategory(e.target.value)}>
                <option value="">all categories</option>
                {RISK_ORDER.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            }>
        {Object.keys(zonesData.source_errors || {}).length > 0 && (
          <div className="banner warn">Some sources unavailable: {Object.entries(zonesData.source_errors).map(([k, v]) => `${k}: ${v}`).join("; ")}</div>
        )}
        <table className="table">
          <thead><tr>{head("zone_id", "Zone")}{head("risk_score", "Score")}<th>Category</th>{head("coverage", "Coverage")}<th>Confidence</th><th>Origin</th><th>Window end</th></tr></thead>
          <tbody>
            {rows.map((z) => {
              const a = z.assessment || {};
              return (
                <tr key={z.zone_id} className={selected === z.zone_id ? "selected" : ""} onClick={() => onSelect(z.zone_id)}>
                  <td><i className="sw" style={{ background: RISK_COLORS[a.risk_category || "No assessment"] }} />{z.zone_id}</td>
                  <td>{fmt(a.risk_score)}</td>
                  <td>{a.risk_category || "No assessment"}</td>
                  <td>{a.coverage != null ? `${Math.round(a.coverage * 100)}%` : "—"}</td>
                  <td>{a.confidence || "—"}</td>
                  <td>{a.data_origin ? <OriginBadge origin={a.data_origin} stale={a.stale} /> : "—"}</td>
                  <td className="small">{fmtTime(a.window_end)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </Card>
      <Card title="Zone detail">
        {selected ? <ZoneDetail zoneId={selected} refreshKey={refreshKey} /> : <Empty>Select a zone in the table or on the map.</Empty>}
      </Card>
    </div>
  );
}
