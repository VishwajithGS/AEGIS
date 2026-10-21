import React, { useState } from "react";
import {
  Area, AreaChart, Bar, BarChart, CartesianGrid, ComposedChart, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import { api } from "../services/api.js";
import { useApi } from "../hooks/useApi.js";
import { Card, Empty, ErrorBox, Loading, OriginBadge } from "../components/ui.jsx";
import { CATEGORY_COLORS, RISK_COLORS, fmt, fmtTime } from "../constants.js";

export default function Analytics({ zones }) {
  const [days, setDays] = useState(30);
  const [zone, setZone] = useState("");
  const { data, error, loading } = useApi(() => api.historical({ days, zone_id: zone }), [days, zone], 60000);
  if (loading && !data) return <Loading what="historical analytics" />;
  if (error && !data) return <ErrorBox error={error} what="historical analytics" />;
  if (!data.available) return <Empty>{data.reason}</Empty>;
  const live = data.live_trend || {};
  const liveRows = (live.windows || []).map((w) => ({ ...w, t: fmtTime(w.window_start) }));
  return (
    <div className="page">
      <div className="toolbar">
        <label>Period<select value={days} onChange={(e) => setDays(Number(e.target.value))}>
          {[7, 14, 30, 60].map((d) => <option key={d} value={d}>last {d} days of the archive</option>)}</select></label>
        <label>Zone<select value={zone} onChange={(e) => setZone(e.target.value)}><option value="">whole region</option>
          {(zones || []).map((z) => <option key={z.zone_id}>{z.zone_id}</option>)}</select></label>
        <OriginBadge origin="batch-historical" /> <span className="muted small">computed by the Spark batch ETL from the synthetic 60-day backfill</span>
      </div>
      <div className="grid-2">
        <Card title="Daily incidents and reports by category">
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={data.daily_incidents_by_category}>
              <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="event_date" fontSize={11} /><YAxis /><Tooltip /><Legend />
              {data.categories.map((c) => <Bar key={c} dataKey={c} stackId="a" fill={CATEGORY_COLORS[c] || "#999"} isAnimationActive={false} />)}
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Rainfall and peak river level (daily)" subtitle="mean daily rain across zones (mm) and max water level / danger level">
          <ResponsiveContainer width="100%" height={260}>
            <ComposedChart data={data.daily_environment}>
              <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="event_date" fontSize={11} />
              <YAxis yAxisId="l" /><YAxis yAxisId="r" orientation="right" domain={[0, "auto"]} /><Tooltip /><Legend />
              <Bar yAxisId="l" dataKey="rain_mm" name="rain (mm)" fill="#64b5f6" isAnimationActive={false} />
              <Line yAxisId="r" dataKey="level_ratio_peak" name="peak level ratio" stroke="#00838f" dot={false} isAnimationActive={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Rule-based risk trend (daily)" subtitle="mean and peak zone risk score">
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={data.daily_environment}>
              <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="event_date" fontSize={11} /><YAxis domain={[0, 100]} /><Tooltip /><Legend />
              <Line dataKey="risk_avg" name="mean risk" stroke="#ef6c00" dot={false} isAnimationActive={false} />
              <Line dataKey="risk_peak" name="peak risk" stroke="#c62828" dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Zone-hours by risk category">
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart data={data.daily_zone_hours_by_risk_category || []}>
              <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="event_date" fontSize={11} /><YAxis /><Tooltip /><Legend />
              {["Low", "Moderate", "High", "Critical", "Insufficient data"].map((c) => (
                <Area key={c} dataKey={c} stackId="1" stroke={RISK_COLORS[c]} fill={RISK_COLORS[c]} isAnimationActive={false} />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Flood-related incident timing" subtitle="incidents by hour of day (IST)">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={data.incidents_by_hour_of_day_ist || []}>
              <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="hour_of_day_ist" /><YAxis /><Tooltip />
              <Bar dataKey="incidents" fill="#5c6bc0" isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Traffic disruption vs rainfall band" subtitle={`corr(rain, disruption) over all zone-hours: ${fmt((data.traffic_hazard_correlation_all || [])[0]?.corr_rain_disruption, 3)}`}>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={data.rainfall_disruption_bands || []}>
              <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="rain_band" fontSize={11} /><YAxis domain={[0, 1]} /><Tooltip /><Legend />
              <Bar dataKey="avg_disrupted_ratio" name="disrupted share" fill="#8d6e63" isAnimationActive={false} />
              <Bar dataKey="avg_speed_ratio" name="speed / free-flow" fill="#90a4ae" isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </div>
      <Card title="Live stream trend" subtitle="per event-time window, latest assessment per zone (Structured Streaming output)">
        {!live.available ? <Empty>{live.reason}</Empty> : (
          <ResponsiveContainer width="100%" height={240}>
            <ComposedChart data={liveRows}>
              <CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="t" fontSize={11} /><YAxis yAxisId="l" domain={[0, 100]} />
              <YAxis yAxisId="r" orientation="right" /><Tooltip /><Legend />
              <Line yAxisId="l" dataKey="mean_risk" name="mean risk" stroke="#ef6c00" dot={false} isAnimationActive={false} />
              <Line yAxisId="l" dataKey="max_risk" name="max risk" stroke="#c62828" dot={false} isAnimationActive={false} />
              <Bar yAxisId="r" dataKey="reports" name="reports" fill="#ce93d8" isAnimationActive={false} />
            </ComposedChart>
          </ResponsiveContainer>
        )}
      </Card>
    </div>
  );
}
