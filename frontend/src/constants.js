export const RISK_COLORS = {
  Low: "#2e7d32",
  Moderate: "#f9a825",
  High: "#ef6c00",
  Critical: "#c62828",
  "Insufficient data": "#90a4ae",
  "No assessment": "#cfd8dc",
};
export const RISK_ORDER = ["Critical", "High", "Moderate", "Low", "Insufficient data", "No assessment"];
export const SEVERITY_ORDER = ["Low", "Moderate", "High", "Critical"];

export const CATEGORY_COLORS = {
  flood: "#1565c0",
  waterlogging: "#4fc3f7",
  tree_fall: "#558b2f",
  power_outage: "#6a1b9a",
  road_blockage: "#8d6e63",
  structure_damage: "#d84315",
  other: "#78909c",
};

export const fmt = (v, digits = 1) =>
  v === null || v === undefined || Number.isNaN(v) ? "—" : typeof v === "number" ? v.toFixed(digits) : String(v);

export const fmtInt = (v) => (v === null || v === undefined ? "—" : Number(v).toLocaleString());

export const fmtTime = (iso) => {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
};

export const timeAgo = (iso) => {
  if (!iso) return "never";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (Number.isNaN(s)) return "unknown";
  if (s < 0) return "in the future";
  if (s < 90) return `${Math.round(s)} s ago`;
  if (s < 5400) return `${Math.round(s / 60)} min ago`;
  if (s < 172800) return `${Math.round(s / 3600)} h ago`;
  return `${Math.round(s / 86400)} days ago`;
};

export const riskColor = (a) => RISK_COLORS[(a && a.risk_category) || "No assessment"] || RISK_COLORS["No assessment"];

// Data mode served by the API (/api/dataset): "synthetic" (default), "real-historical" or "hybrid".
export const dataMode = (dataset) => (dataset && dataset.data_mode) || "synthetic";
export const MODE_TAGLINE = {
  synthetic: "synthetic demo data",
  "real-historical": "real historical data",
  hybrid: "hybrid: real rainfall + synthetic fill",
};
