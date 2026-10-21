import React, { useState } from "react";
import { api } from "../services/api.js";
import { useApi } from "../hooks/useApi.js";
import RiskMap from "../components/RiskMap.jsx";
import { Card, ErrorBox, Loading, OriginBadge, RiskBadge } from "../components/ui.jsx";
import { CATEGORY_COLORS, RISK_COLORS, RISK_ORDER, fmt, fmtTime } from "../constants.js";

export default function LiveMap({ zonesData, onOpenZone }) {
  const [origin, setOrigin] = useState("live-stream");
  const [category, setCategory] = useState("");
  const [radius, setRadius] = useState(2);
  const [nearby, setNearby] = useState(null);
  const [nearErr, setNearErr] = useState(null);
  const inc = useApi(() => api.incidents({ origin, category, located_only: true, limit: 500,
    since_hours: origin === "live-stream" ? 6 : 72 }), [origin, category], 30000);

  const search = async (latlng) => {
    try {
      setNearErr(null);
      setNearby(await api.nearby({ lat: latlng.lat.toFixed(5), lon: latlng.lng.toFixed(5), radius_km: radius, limit: 50 }));
    } catch (e) {
      setNearErr(e);
    }
  };

  return (
    <div className="page">
      <div className="toolbar">
        <label>
          Incidents
          <select value={origin} onChange={(e) => setOrigin(e.target.value)}>
            <option value="live-stream">live reports (last 6 h)</option>
            <option value="batch-historical">historical (last 72 h of archive)</option>
          </select>
        </label>
        <label>
          Category
          <select value={category} onChange={(e) => setCategory(e.target.value)}>
            <option value="">all</option>
            {Object.keys(CATEGORY_COLORS).map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </label>
        <label>
          Nearby radius (km)
          <input type="number" min="0.5" max="20" step="0.5" value={radius} onChange={(e) => setRadius(Number(e.target.value))} />
        </label>
        <span className="muted small">Click the map to search hazards within the radius (Haversine distance). Click a zone for details.</span>
      </div>
      <div className="grid-map">
        <Card className="map-card">
          {!zonesData ? <Loading what="zones" /> : (
            <RiskMap zones={zonesData.zones} incidents={inc.data ? inc.data.items : []} nearby={nearby}
                     onMapClick={search} onZoneClick={onOpenZone} />
          )}
          <ErrorBox error={inc.error} what="incidents" />
          <div className="legend-row">
            {RISK_ORDER.map((c) => (
              <span key={c}><i className="sw" style={{ background: RISK_COLORS[c] }} />{c}</span>
            ))}
            <span className="muted small">Dashed / faint = stale or historical assessment</span>
          </div>
        </Card>
        <div className="side">
          <Card title="Nearby hazards" subtitle={nearby ? `${nearby.total} within ${nearby.radius_km} km of ${nearby.center.map((v) => Number(v).toFixed(4)).join(", ")}` : "Click the map"}>
            <ErrorBox error={nearErr} what="nearby hazards" />
            {nearby && (
              <>
                <div className="chips">
                  {Object.entries(nearby.by_category).map(([k, v]) => <span key={k} className="chip">{k}: {v}</span>)}
                </div>
                <ul className="list">
                  {nearby.items.slice(0, 15).map((i) => (
                    <li key={i.incident_id}>
                      <strong>{fmt(i.distance_km, 2)} km</strong> · {i.category} · {i.verification_status}
                      <OriginBadge origin={i.data_origin} />
                      <div className="muted small">{fmtTime(i.event_time)} · {i.zone_id}</div>
                    </li>
                  ))}
                </ul>
              </>
            )}
          </Card>
          <Card title="Highest-risk zones">
            <ul className="list">
              {(zonesData ? zonesData.zones : []).filter((z) => z.assessment && z.assessment.risk_score != null)
                .sort((a, b) => b.assessment.risk_score - a.assessment.risk_score).slice(0, 8).map((z) => (
                  <li key={z.zone_id} className="clickable" onClick={() => onOpenZone(z.zone_id)}>
                    <RiskBadge category={z.assessment.risk_category} score={z.assessment.risk_score} /> <strong>{z.zone_id}</strong>
                    <OriginBadge origin={z.assessment.data_origin} stale={z.assessment.stale} />
                  </li>
                ))}
            </ul>
          </Card>
        </div>
      </div>
    </div>
  );
}
