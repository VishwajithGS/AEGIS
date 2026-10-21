import React from "react";
import { Circle, CircleMarker, MapContainer, Popup, Rectangle, TileLayer, Tooltip, useMapEvents } from "react-leaflet";
import { CATEGORY_COLORS, fmt, fmtTime, riskColor } from "../constants.js";
import { OriginBadge, RiskBadge } from "./ui.jsx";

function ClickCapture({ onClick }) {
  useMapEvents({ click: (e) => onClick && onClick(e.latlng) });
  return null;
}

// Zones are drawn as grid rectangles coloured by rule-based risk category;
// incidents are circle markers coloured by incident category.
export default function RiskMap({ zones = [], incidents = [], nearby, onMapClick, onZoneClick, height = 560 }) {
  const center = [13.05, 80.225];
  return (
    <MapContainer center={center} zoom={11} style={{ height, width: "100%" }} scrollWheelZoom>
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <ClickCapture onClick={onMapClick} />
      {zones.map((z) => {
        const a = z.assessment;
        return (
          <Rectangle
            key={z.zone_id}
            bounds={z.bounds}
            pathOptions={{ color: riskColor(a), weight: 1, fillOpacity: a ? (a.stale ? 0.18 : 0.38) : 0.06, dashArray: a && a.stale ? "4 4" : null }}
            eventHandlers={{ click: () => onZoneClick && onZoneClick(z.zone_id) }}
          >
            <Tooltip sticky>
              <strong>{z.zone_id}</strong> — {a ? `${a.risk_category}${a.risk_score != null ? ` (${fmt(a.risk_score)})` : ""}` : "no assessment"}
            </Tooltip>
          </Rectangle>
        );
      })}
      {incidents
        .filter((i) => i.lat != null && i.lon != null)
        .map((i) => (
          <CircleMarker
            key={i.incident_id}
            center={[i.lat, i.lon]}
            radius={i.source_type === "historical_incident" ? 5 : 4}
            pathOptions={{ color: CATEGORY_COLORS[i.category] || "#555", fillOpacity: 0.85, weight: 1 }}
          >
            <Popup>
              <div className="popup">
                <strong>{i.category}</strong> <OriginBadge origin={i.data_origin} />
                <div>{fmtTime(i.event_time)}</div>
                <div>
                  {i.source_type === "historical_incident" ? "Archived incident" : "Public report"} · {i.verification_status}
                </div>
                {i.severity && <div>Observed severity (synthetic archive): {i.severity}</div>}
                <div className="muted small">{i.description}</div>
                <div className="muted small">
                  {i.zone_id} · {fmt(i.lat, 4)}, {fmt(i.lon, 4)}
                </div>
                {zones.find((z) => z.zone_id === i.zone_id)?.assessment && (
                  <div>
                    Zone risk: <RiskBadge category={zones.find((z) => z.zone_id === i.zone_id).assessment.risk_category}
                      score={zones.find((z) => z.zone_id === i.zone_id).assessment.risk_score} />
                  </div>
                )}
              </div>
            </Popup>
          </CircleMarker>
        ))}
      {nearby && (
        <Circle center={nearby.center} radius={nearby.radius_km * 1000} pathOptions={{ color: "#1a237e", weight: 2, fillOpacity: 0.05 }} />
      )}
    </MapContainer>
  );
}
