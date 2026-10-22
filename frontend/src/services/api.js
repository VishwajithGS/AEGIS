// All backend access goes through this module.
export const API_BASE = (import.meta.env.VITE_API_BASE_URL || "http://localhost:8000").replace(/\/$/, "");

async function request(path, options = {}) {
  let res;
  try {
    res = await fetch(`${API_BASE}${path}`, {
      headers: { "Content-Type": "application/json" },
      ...options,
    });
  } catch (e) {
    throw new Error(`Cannot reach the AEGIS API at ${API_BASE} (${e.message})`);
  }
  const text = await res.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { detail: text };
  }
  if (!res.ok) {
    const detail = body && body.detail ? (typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail)) : res.statusText;
    const err = new Error(`${res.status}: ${detail}`);
    err.status = res.status;
    throw err;
  }
  return body;
}

const qs = (params) => {
  const p = new URLSearchParams();
  Object.entries(params || {}).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") p.set(k, v);
  });
  const s = p.toString();
  return s ? `?${s}` : "";
};

export const api = {
  health: () => request("/health"),
  overview: () => request("/api/overview"),
  zones: (params) => request(`/api/zones${qs(params)}`),
  zone: (id) => request(`/api/zones/${encodeURIComponent(id)}`),
  incidents: (params) => request(`/api/incidents${qs(params)}`),
  nearby: (params) => request(`/api/incidents/nearby${qs(params)}`),
  historical: (params) => request(`/api/analytics/historical${qs(params)}`),
  dataQuality: () => request("/api/analytics/data-quality"),
  pipeline: () => request("/api/pipeline/status"),
  alerts: () => request("/api/alerts"),
  modelStatus: () => request("/api/model/status"),
  modelPredictions: () => request("/api/model/predictions"),
  dataset: () => request("/api/dataset"),
  predict: (body) => request("/api/model/predict", { method: "POST", body: JSON.stringify(body) }),
  streamUrl: () => `${API_BASE}/api/stream`,
};
