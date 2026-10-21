import React, { useState } from "react";
import { api } from "../services/api.js";
import { useApi } from "../hooks/useApi.js";
import { Bar, Card, Empty, ErrorBox, KV, Loading, RiskBadge } from "../components/ui.jsx";
import { fmt, fmtTime } from "../constants.js";

const DEFAULTS = { rain_3h_mm: 45, rain_max_1h_mm_hr: 28, level_ratio_max_3h: 0.9, humidity_avg_3h: 94,
  disrupted_ratio_1h: 0.4, speed_ratio_avg_1h: 0.5, reports_6h: 3, water_obs_available: 1, traffic_obs_available: 1 };

function Confusion({ cm }) {
  if (!cm) return null;
  const max = Math.max(1, ...cm.rows_actual_cols_predicted.flat());
  return (
    <table className="table cm">
      <thead><tr><th>actual \ predicted</th>{cm.labels.map((l) => <th key={l}>{l}</th>)}</tr></thead>
      <tbody>
        {cm.rows_actual_cols_predicted.map((row, i) => (
          <tr key={cm.labels[i]}><th>{cm.labels[i]}</th>
            {row.map((v, j) => <td key={j} style={{ background: `rgba(21,101,192,${(0.08 + 0.7 * v / max).toFixed(2)})`, color: v / max > 0.55 ? "#fff" : "#111" }}>{v}</td>)}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default function ModelPanel({ zonesData }) {
  const status = useApi(() => api.modelStatus(), [], 15000);
  const preds = useApi(() => api.modelPredictions(), [], 30000);
  const [form, setForm] = useState(DEFAULTS);
  const [zone, setZone] = useState("");
  const [result, setResult] = useState(null);
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);
  if (status.loading && !status.data) return <Loading what="model status" />;
  if (status.error && !status.data) return <ErrorBox error={status.error} what="model status" />;
  const s = status.data;
  const zonePreds = (preds.data && preds.data.zone_predictions && preds.data.zone_predictions.predictions) || [];

  const fillFromZone = (zid) => {
    setZone(zid);
    const p = zonePreds.find((x) => x.zone_id === zid);
    if (p) {
      const next = {};
      Object.keys(DEFAULTS).forEach((k) => (next[k] = p[k] === null || p[k] === undefined ? "" : p[k]));
      setForm(next);
    }
  };
  const submit = async (e) => {
    e.preventDefault();
    setBusy(true);
    setErr(null);
    try {
      const body = {};
      Object.entries(form).forEach(([k, v]) => (body[k] = v === "" || v === null ? null : Number(v)));
      if (zone) body.zone_id = zone;
      body.input_source = zone ? `dashboard form (prefilled from ${zone})` : "dashboard form";
      setResult(await api.predict(body));
    } catch (ex) {
      setErr(ex);
    } finally {
      setBusy(false);
    }
  };
  const zoneRisk = zone && zonesData ? (zonesData.zones.find((z) => z.zone_id === zone) || {}).assessment : null;
  const ev = s.evaluation || {};
  return (
    <div className="page">
      <div className="banner warn">
        Supporting ML component. Labels are <strong>{s.labels_are_synthetic === false ? "real" : "SYNTHETIC"}</strong> ({(s.label_provenance || "no model trained").replace(/^SYNTHETIC - /, "")}).
        Predictions are ML estimates of flood-incident severity, <strong>not observed severities and not official warnings</strong>, and are independent of the rule-based zone risk score.
      </div>
      <div className="grid-2">
        <Card title="Model status" subtitle={s.kind}>
          <KV items={[
            ["State", <span key="s">{s.state}{s.reason ? ` — ${s.reason}` : ""}</span>],
            ["Version", s.version || "—"], ["Trained", fmtTime(s.trained_at)], ["Algorithm", s.algorithm || "—"],
            ["Split", s.split ? `${s.split.method}; train ${s.split.train_rows} / test ${s.split.test_rows}; cutoff ${fmtTime(s.split.cutoff_utc)}` : "—"],
            ["Params", s.params ? Object.entries(s.params).map(([k, v]) => `${k}=${v}`).join(", ") : "—"],
            ["Reload check", s.reload_check ? (s.reload_check.passed ? "passed" : "FAILED") : "—"],
          ]} />
          {ev.accuracy !== undefined && (
            <>
              <h4>Held-out evaluation (time-based test split)</h4>
              <div className="chips">
                <span className="chip">accuracy <strong>{fmt(ev.accuracy, 3)}</strong></span>
                <span className="chip">weighted precision <strong>{fmt(ev.weightedPrecision, 3)}</strong></span>
                <span className="chip">weighted recall <strong>{fmt(ev.weightedRecall, 3)}</strong></span>
                <span className="chip">weighted F1 <strong>{fmt(ev.weighted_f1, 3)}</strong></span>
                {ev.baseline_majority_class && <span className="chip">majority-class baseline <strong>{fmt(ev.baseline_majority_class.accuracy, 3)}</strong></span>}
              </div>
              <Confusion cm={ev.confusion_matrix} />
              <table className="table small">
                <thead><tr><th>class</th><th>precision</th><th>recall</th><th>F1</th><th>support</th></tr></thead>
                <tbody>{Object.entries(ev.per_class || {}).map(([k, v]) => (
                  <tr key={k}><td>{k}</td><td>{fmt(v.precision, 3)}</td><td>{fmt(v.recall, 3)}</td><td>{fmt(v.f1, 3)}</td><td>{v.support}</td></tr>))}</tbody>
              </table>
              <h4>Feature importances</h4>
              {Object.entries(s.feature_importances || {}).map(([k, v]) => <Bar key={k} label={k} value={v * 100} max={100} color="#3949ab" />)}
            </>
          )}
          {(s.limitations || []).length > 0 && <ul className="small muted">{s.limitations.map((l) => <li key={l}>{l}</li>)}</ul>}
        </Card>
        <Card title="Predict severity" subtitle="Enter a feature record or prefill it from a zone's latest computed conditions">
          {!s.available && <div className="banner warn">Prediction unavailable: {s.reason || s.state}</div>}
          <form onSubmit={submit} className="form">
            <label>Prefill from zone
              <select value={zone} onChange={(e) => fillFromZone(e.target.value)}>
                <option value="">— none —</option>
                {zonePreds.map((p) => <option key={p.zone_id} value={p.zone_id}>{p.zone_id}</option>)}
              </select>
            </label>
            {(s.features || []).map((f) => (
              <label key={f.name} title={f.description}>{f.name} <span className="muted small">({f.unit})</span>
                <input type="number" step="any" value={form[f.name] ?? ""} onChange={(e) => setForm({ ...form, [f.name]: e.target.value })} placeholder="null = not observed" />
              </label>
            ))}
            <button type="submit" disabled={busy || !s.available}>{busy ? "Predicting…" : "Predict"}</button>
          </form>
          <ErrorBox error={err} what="prediction" />
          {result && (
            <div className="ml-result">
              <div className="small muted">ML-predicted flood-incident severity (model {result.model_version}, {fmtTime(result.predicted_at)})</div>
              <div className="big">{result.predicted_severity}</div>
              {Object.entries(result.probabilities || {}).map(([k, v]) => <Bar key={k} label={k} value={v * 100} max={100} color="#5e35b1" />)}
              {result.imputed_features && result.imputed_features.length > 0 && (
                <div className="small warn">Imputed (not observed): {result.imputed_features.join(", ")}</div>
              )}
              <div className="small muted">{result.note}</div>
              <div className="small">Persisted to HBase: {result.persisted && result.persisted.hbase ? `yes (${result.persisted.row_key})` : `no ${result.persisted && result.persisted.error ? "— " + result.persisted.error : ""}`}</div>
              {zoneRisk && (
                <div className="compare">
                  For comparison, rule-based zone risk of {zone}: <RiskBadge category={zoneRisk.risk_category} score={zoneRisk.risk_score} />
                </div>
              )}
            </div>
          )}
        </Card>
      </div>
      <Card title="Latest per-zone ML predictions" subtitle={preds.data && preds.data.zone_predictions ? `model ${preds.data.zone_predictions.model_version} · features as of ${fmtTime(preds.data.zone_predictions.features_as_of)} · source ${preds.data.zone_predictions.source}` : ""}>
        {zonePreds.length === 0 ? <Empty>No zone predictions yet (run the inference job: scripts/train_model.ps1 -Infer).</Empty> : (
          <div className="chips">
            {zonePreds.slice().sort((a, b) => a.zone_id.localeCompare(b.zone_id)).map((p) => (
              <span key={p.zone_id} className="chip clickable" onClick={() => fillFromZone(p.zone_id)}>{p.zone_id}: <strong>{p.predicted_severity || "Insufficient data"}</strong></span>
            ))}
          </div>
        )}
      </Card>
    </div>
  );
}
