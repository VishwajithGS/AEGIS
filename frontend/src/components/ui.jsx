import React from "react";
import { RISK_COLORS } from "../constants.js";

export function Card({ title, subtitle, children, actions, className = "" }) {
  return (
    <section className={`card ${className}`}>
      {(title || actions) && (
        <header className="card-head">
          <div>
            {title && <h3>{title}</h3>}
            {subtitle && <p className="muted small">{subtitle}</p>}
          </div>
          {actions && <div className="card-actions">{actions}</div>}
        </header>
      )}
      {children}
    </section>
  );
}

export function Stat({ label, value, hint, tone }) {
  return (
    <div className={`stat ${tone ? `tone-${tone}` : ""}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {hint && <div className="stat-hint">{hint}</div>}
    </div>
  );
}

export function RiskBadge({ category, score }) {
  const c = category || "No assessment";
  return (
    <span className="badge" style={{ background: RISK_COLORS[c] || "#999", color: c === "Moderate" ? "#222" : "#fff" }}>
      {c}
      {score !== undefined && score !== null ? ` · ${Number(score).toFixed(1)}` : ""}
    </span>
  );
}

const ORIGIN_LABEL = {
  "live-stream": ["LIVE STREAM", "origin-live"],
  "batch-historical": ["BATCH HISTORICAL", "origin-batch"],
  synthetic: ["SYNTHETIC", "origin-synth"],
};

export function OriginBadge({ origin, stale }) {
  const [label, cls] = ORIGIN_LABEL[origin] || [origin || "unknown", "origin-unknown"];
  return (
    <span className={`origin ${cls}`} title="Where this value comes from">
      {label}
      {stale ? " · STALE" : ""}
    </span>
  );
}

export function Loading({ what = "data" }) {
  return <div className="loading">Loading {what}…</div>;
}

export function ErrorBox({ error, what }) {
  if (!error) return null;
  return (
    <div className="error-box">
      <strong>Could not load {what || "data"}.</strong> {error.message || String(error)}
    </div>
  );
}

export function Empty({ children }) {
  return <div className="empty">{children}</div>;
}

export function KV({ items }) {
  return (
    <dl className="kv">
      {items.map(([k, v]) => (
        <React.Fragment key={k}>
          <dt>{k}</dt>
          <dd>{v}</dd>
        </React.Fragment>
      ))}
    </dl>
  );
}

export function Dot({ ok, warn }) {
  return <span className={`dot ${ok ? "dot-ok" : warn ? "dot-warn" : "dot-bad"}`} />;
}

export function Bar({ value, max = 100, color = "#1565c0", label }) {
  const pct = Math.max(0, Math.min(100, (100 * (value || 0)) / (max || 1)));
  return (
    <div className="bar-row">
      {label && <span className="bar-label">{label}</span>}
      <span className="bar-track">
        <span className="bar-fill" style={{ width: `${pct}%`, background: color }} />
      </span>
      <span className="bar-value">{value === null || value === undefined ? "—" : Number(value).toFixed(1)}</span>
    </div>
  );
}
