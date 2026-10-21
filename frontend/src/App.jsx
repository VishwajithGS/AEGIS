import React, { useCallback, useEffect, useRef, useState } from "react";
import { api, API_BASE } from "./services/api.js";
import { useApi } from "./hooks/useApi.js";
import { useLiveStream } from "./hooks/useLiveStream.js";
import Overview from "./pages/Overview.jsx";
import LiveMap from "./pages/LiveMap.jsx";
import Zones from "./pages/Zones.jsx";
import Incidents from "./pages/Incidents.jsx";
import Analytics from "./pages/Analytics.jsx";
import ModelPanel from "./pages/ModelPanel.jsx";
import Pipeline from "./pages/Pipeline.jsx";
import { Dot } from "./components/ui.jsx";
import { timeAgo } from "./constants.js";

const TABS = [
  ["overview", "Overview"], ["map", "Live map"], ["zones", "Zone risk"], ["incidents", "Incidents"],
  ["analytics", "Historical analytics"], ["model", "ML model"], ["pipeline", "Pipeline"],
];

const TAB_IDS = TABS.map(([id]) => id);
const tabFromHash = () => {
  const h = (window.location.hash || "").replace(/^#/, "");
  return TAB_IDS.includes(h) ? h : "overview";
};

export default function App() {
  // The active tab is mirrored in the URL fragment (#map, #zones, ...) so tabs can be
  // bookmarked or linked and the browser back button works.
  const [tab, setTabState] = useState(tabFromHash);
  const setTab = useCallback((id) => {
    setTabState(id);
    if (window.location.hash !== `#${id}`) window.history.pushState(null, "", `#${id}`);
  }, []);
  useEffect(() => {
    const onHash = () => setTabState(tabFromHash());
    window.addEventListener("hashchange", onHash);
    window.addEventListener("popstate", onHash);
    return () => {
      window.removeEventListener("hashchange", onHash);
      window.removeEventListener("popstate", onHash);
    };
  }, []);
  const [zone, setZone] = useState(null);
  const [alerts, setAlerts] = useState([]);
  const [refreshKey, setRefreshKey] = useState(0);
  const debounce = useRef(null);

  // Shared zone data; refreshed on live updates (SSE) or every 30 s as a fallback.
  const zones = useApi(() => api.zones(), [refreshKey], 30000);

  useEffect(() => {
    api.alerts().then((d) => setAlerts(d.alerts || [])).catch(() => {});
  }, []);

  const onEvent = useCallback((type, data) => {
    if (type === "alert") setAlerts((a) => [data, ...a.filter((x) => x.alert_id !== data.alert_id)].slice(0, 50));
    if (type === "zone_risk") {
      clearTimeout(debounce.current);
      debounce.current = setTimeout(() => setRefreshKey((k) => k + 1), 3000);
    }
  }, []);
  const live = useLiveStream(onEvent);

  const openZone = (id) => {
    setZone(id);
    setTab("zones");
  };

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">AEGIS</span>
          <span className="tagline">Real-time disaster intelligence & risk analytics · synthetic demo data</span>
        </div>
        <div className="status">
          <span title="Server-Sent Events from the API (Kafka zone-risk-updates)">
            <Dot ok={live.connected} /> live updates {live.connected ? "connected" : "offline (polling)"}
            {live.lastEventAt ? ` · last event ${timeAgo(live.lastEventAt.toISOString())}` : ""}
          </span>
          <span className="muted small">API {API_BASE}</span>
        </div>
      </header>
      <nav className="tabs">
        {TABS.map(([id, label]) => (
          <button key={id} className={tab === id ? "active" : ""} onClick={() => setTab(id)}>
            {label}
            {id === "overview" && alerts.length > 0 && <span className="pill">{alerts.length}</span>}
          </button>
        ))}
      </nav>
      <main>
        {tab === "overview" && <Overview alerts={alerts} refreshKey={refreshKey} onOpenZone={openZone} />}
        {tab === "map" && <LiveMap zonesData={zones.data} onOpenZone={openZone} />}
        {tab === "zones" && <Zones zonesData={zones.data} error={zones.error} selected={zone} onSelect={setZone} refreshKey={refreshKey} />}
        {tab === "incidents" && <Incidents />}
        {tab === "analytics" && <Analytics zones={zones.data ? zones.data.zones : []} />}
        {tab === "model" && <ModelPanel zonesData={zones.data} />}
        {tab === "pipeline" && <Pipeline liveConnected={live.connected} />}
      </main>
      <footer className="foot muted small">
        AEGIS — Kafka · Spark (RDD, SQL, Structured Streaming, MLlib) · HBase · Parquet · FastAPI · React/Leaflet.
        Zones are an analysis grid, not official boundaries. No alert leaves this dashboard.
      </footer>
    </div>
  );
}
