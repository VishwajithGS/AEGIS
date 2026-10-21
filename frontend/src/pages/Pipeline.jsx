import React from "react";
import { api } from "../services/api.js";
import { useApi } from "../hooks/useApi.js";
import { Card, Dot, ErrorBox, KV, Loading } from "../components/ui.jsx";
import { fmt, fmtInt, fmtTime, timeAgo } from "../constants.js";

export default function Pipeline({ liveConnected }) {
  const st = useApi(() => api.pipeline(), [], 15000);
  const dq = useApi(() => api.dataQuality(), [], 30000);
  if (st.loading && !st.data) return <Loading what="pipeline status" />;
  if (st.error && !st.data) return <ErrorBox error={st.error} what="pipeline status" />;
  const s = st.data;
  const k = s.services.kafka, h = s.services.hbase, sp = s.services.streaming || {};
  const live = dq.data && dq.data.live;
  const batch = dq.data && dq.data.batch;
  return (
    <div className="page">
      <div className={`banner ${s.overall === "ok" ? "ok" : "warn"}`}>
        Overall: <strong>{s.overall}</strong>{s.issues.length ? ` — ${s.issues.join("; ")}` : ""} · checked {fmtTime(s.generated_at)}
      </div>
      <div className="grid-3 grid-pipeline">
        <Card title="Services">
          <ul className="list">
            <li><Dot ok={k.available} /> Kafka {k.bootstrap} {k.error && <span className="small warn">{k.error}</span>}</li>
            <li><Dot ok={h.available} /> HBase Thrift {h.endpoint} {h.available ? `(${h.latency_ms} ms, ${h.tables.length} tables)` : <span className="small warn">{h.error}</span>}</li>
            <li><Dot ok={sp.running} warn={sp.available} /> Spark Structured Streaming: {sp.state || "never run"} {sp.status_age_s != null ? `(status ${fmt(sp.status_age_s, 0)} s old)` : ""}</li>
            <li><Dot ok={s.services.live_feed.connected} /> API Kafka live feed: {s.services.live_feed.state} ({s.services.live_feed.messages} msgs)</li>
            <li><Dot ok={liveConnected} /> Browser live stream (SSE): {liveConnected ? "connected" : "polling fallback"}</li>
            <li><Dot ok={s.services.model.state === "ready"} warn /> ML model: {s.services.model.state} {s.services.model.version || ""}</li>
          </ul>
          {sp.available && <KV items={[["HBase sink", sp.hbase_sink], ["Watermark", sp.watermark], ["Window", sp.window], ["Mode", sp.mode]]} />}
        </Card>
        <Card title="Streaming queries" subtitle="from StreamingQueryListener progress events; records and rates are Kafka records (offset ranges), scanned counts every plan branch">
          <div className="table-scroll"><table className="table small">
            <thead><tr><th>query</th><th>status</th><th>batch</th><th title="Kafka records in the micro-batch (from offset ranges)">records</th><th title="rows Spark scanned across all plan branches">scanned</th><th>in/s</th><th>proc/s</th><th>state rows</th><th>late dropped</th><th>last progress</th></tr></thead>
            <tbody>
              {Object.entries(sp.queries || {}).map(([n, q]) => (
                <tr key={n}><td>{n}</td><td>{q.status}{q.restarts ? ` (${q.restarts} restarts)` : ""}</td><td>{q.batch_id ?? "—"}</td>
                  <td>{q.num_input_rows ?? "—"}</td><td className="muted">{q.rows_scanned ?? "—"}</td><td>{fmt(q.input_rows_per_second, 1)}</td><td>{fmt(q.processed_rows_per_second, 1)}</td>
                  <td>{q.state_rows ?? "—"}</td><td>{q.dropped_by_watermark ?? "—"}</td><td>{timeAgo(q.last_progress_at)}</td></tr>
              ))}
            </tbody>
          </table></div>
          {sp.last_silver && <p className="small muted">Last silver batch {sp.last_silver.batch_id}: {JSON.stringify(sp.last_silver.rows)} · HBase {JSON.stringify(sp.last_silver.hbase && (sp.last_silver.hbase.verify || sp.last_silver.hbase.error || ""))}</p>}
        </Card>
        <Card title="Producers (synthetic sources)">
          {s.producers.length === 0 ? <p className="muted">No producer status yet.</p> : (
            <ul className="list">{s.producers.map((p) => (
              <li key={p.name}><Dot ok={p.running} warn /> {p.name} — {p.running ? `running @ ${p.rate_per_sec}/s` : "stopped"} · sent {fmtInt(p.producer && p.producer.sent)} · delivered {fmtInt(p.producer && p.producer.delivered)} · failed {fmtInt((p.producer && p.producer.failed) || 0)}
                <div className="small muted">updated {fmt(p.updated_age_s, 0)} s ago</div></li>))}</ul>
          )}
        </Card>
      </div>
      <div className="grid-2">
        <Card title="Kafka topics" subtitle="retained messages (high - low watermark offsets)">
          {!k.available ? <p className="warn">Kafka unreachable</p> : (
            <table className="table small"><thead><tr><th>topic</th><th>partitions</th><th>messages</th></tr></thead>
              <tbody>{Object.entries(k.topics).map(([t, v]) => <tr key={t}><td>{t}</td><td>{v.partitions ?? "missing"}</td><td>{fmtInt(v.messages)}</td></tr>)}</tbody></table>
          )}
          <h4>Lake row counts</h4>
          <KV items={[
            ["bronze (Kafka raw archive)", fmtInt(s.lake_row_counts.bronze_kafka_raw)],
            ["silver_stream", Object.entries(s.lake_row_counts.silver_stream).map(([d, n]) => `${d} ${fmtInt(n)}`).join(" · ")],
            ["silver (batch)", Object.entries(s.lake_row_counts.silver_batch).map(([d, n]) => `${d} ${fmtInt(n)}`).join(" · ")],
            ["quarantine stream / batch", `${fmtInt(s.lake_row_counts.quarantine_stream)} / ${fmtInt(s.lake_row_counts.quarantine_batch)}`],
            ["live zone-risk rows", fmtInt(s.lake_row_counts.live_zone_risk_rows)],
          ]} />
        </Card>
        <Card title="Data quality">
          <ErrorBox error={dq.error} what="data quality" />
          {live && (
            <>
              <h4>Live stream ({live.batches} micro-batches)</h4>
              <KV items={[["records", fmtInt(live.records)], ["valid", fmtInt(live.valid)], ["rejected", `${fmtInt(live.rejected)} (${fmt(live.rejected_pct, 2)}%)`]]} />
              <div className="chips">{Object.entries(live.rejected_by_reason).slice(0, 12).map(([r, n]) => <span key={r} className="chip">{r}: {n}</span>)}</div>
            </>
          )}
          {batch && (
            <>
              <h4>Batch ETL ({fmtTime(batch.generated_at)}, {fmt(batch.duration_s, 0)} s)</h4>
              <KV items={[["records", fmtInt(batch.totals.records)], ["valid", fmtInt(batch.totals.valid)], ["rejected", fmtInt(batch.totals.rejected)],
                ["duplicates removed", fmtInt(batch.totals.duplicates_removed)], ["silver rows", fmtInt(batch.totals.silver_rows)]]} />
              <table className="table small"><thead><tr><th>verification check</th><th>result</th></tr></thead>
                <tbody>{batch.reconciliation.map((c) => <tr key={c.check}><td>{c.check}</td><td className={c.passed ? "ok" : "warn"}>{c.passed ? "passed" : "FAILED"}</td></tr>)}</tbody></table>
              {batch.injected_vs_detected && (
                <table className="table small"><thead><tr><th>injected defect</th><th>injected</th><th>detected</th></tr></thead>
                  <tbody>{Object.entries(batch.injected_vs_detected).map(([kk, v]) => <tr key={kk}><td>{kk}</td><td>{v.injected}</td><td className={v.injected === v.detected ? "ok" : "warn"}>{v.detected}</td></tr>)}</tbody></table>
              )}
            </>
          )}
          {dq.data && dq.data.quarantine_sample && (
            <>
              <h4>Recently quarantined (stream)</h4>
              <ul className="list small">{dq.data.quarantine_sample.slice(0, 6).map((q, i) => (
                <li key={i}><strong>{(q.reasons || []).join(", ")}</strong> · {q.topic}<div className="mono">{q.raw_value}</div></li>))}</ul>
            </>
          )}
        </Card>
      </div>
    </div>
  );
}
