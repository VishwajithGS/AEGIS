import React, { useState } from "react";
import { api } from "../services/api.js";
import { useApi } from "../hooks/useApi.js";
import { Card, ErrorBox, Loading, OriginBadge } from "../components/ui.jsx";
import { CATEGORY_COLORS, dataMode, fmt, fmtInt, fmtTime } from "../constants.js";

const SUBTITLE = {
  synthetic: "Archived incidents (batch, synthetic) and public reports (live stream + historical). Severity exists only for archived incidents.",
  "real-historical": "Real-data mode: no verified open source of incidents or citizen reports exists, so this list is empty. Officially reported regional flood events are under Historical analytics.",
  hybrid: "Hybrid mode: every incident and report here is SYNTHETIC, simulated from the real ERA5 rainfall of the period. Severity (synthetic label) exists only for archived incidents. Real regional flood events are under Historical analytics.",
};

const PAGE = 25;

export default function Incidents() {
  const [f, setF] = useState({ category: "", source_type: "", verification: "", origin: "", q: "", zone_id: "" });
  const [query, setQuery] = useState(f);
  const [offset, setOffset] = useState(0);
  const { data, error, loading } = useApi(() => api.incidents({ ...query, limit: PAGE, offset }), [query, offset], 30000);
  const dataset = useApi(() => api.dataset(), [], 0);
  const mode = dataMode(dataset.data);
  const set = (k) => (e) => setF({ ...f, [k]: e.target.value });
  const apply = (e) => {
    e.preventDefault();
    setOffset(0);
    setQuery(f);
  };
  return (
    <div className="page">
      <Card title="Incident explorer" subtitle={SUBTITLE[mode] || SUBTITLE.synthetic}>
        <form className="toolbar" onSubmit={apply}>
          <label>Category<select value={f.category} onChange={set("category")}><option value="">all</option>
            {Object.keys(CATEGORY_COLORS).map((c) => <option key={c}>{c}</option>)}</select></label>
          <label>Source<select value={f.source_type} onChange={set("source_type")}><option value="">all</option>
            <option value="public_report">public report</option><option value="historical_incident">archived incident</option></select></label>
          <label>Verification<select value={f.verification} onChange={set("verification")}><option value="">all</option>
            <option>verified</option><option>unverified</option><option>rejected</option></select></label>
          <label>Origin<select value={f.origin} onChange={set("origin")}><option value="">all</option>
            <option value="live-stream">live stream</option><option value="batch-historical">batch historical</option></select></label>
          <label>Zone<input value={f.zone_id} onChange={set("zone_id")} placeholder="Z-R04-C02" size={10} /></label>
          <label>Search<input value={f.q} onChange={set("q")} placeholder="text / id" /></label>
          <button type="submit">Apply</button>
        </form>
        <ErrorBox error={error} what="incidents" />
        {loading && !data ? <Loading what="incidents" /> : data && (
          <>
            <div className="muted small">{fmtInt(data.total)} matching · showing {data.total ? offset + 1 : 0}–{Math.min(offset + PAGE, data.total)}</div>
            <table className="table">
              <thead><tr><th>Time</th><th>Category</th><th>Source</th><th>Verification</th><th>Severity (observed)</th><th>Zone</th><th>Location</th><th>Origin</th><th>Description</th></tr></thead>
              <tbody>
                {data.items.map((i) => (
                  <tr key={i.incident_id}>
                    <td className="small">{fmtTime(i.event_time)}</td>
                    <td><i className="sw" style={{ background: CATEGORY_COLORS[i.category] || "#999" }} />{i.category}</td>
                    <td className="small">{i.source_type === "historical_incident" ? "archive" : `report (${i.channel || "?"})`}</td>
                    <td>{i.verification_status}</td>
                    <td>{i.severity || "—"}</td>
                    <td>{i.zone_id || "—"}</td>
                    <td className="small">{i.lat != null ? `${fmt(i.lat, 4)}, ${fmt(i.lon, 4)}` : "no coordinates"}</td>
                    <td><OriginBadge origin={i.data_origin} /></td>
                    <td className="small">{i.description}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className="pager">
              <button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))}>← Prev</button>
              <button disabled={offset + PAGE >= data.total} onClick={() => setOffset(offset + PAGE)}>Next →</button>
            </div>
          </>
        )}
      </Card>
    </div>
  );
}
