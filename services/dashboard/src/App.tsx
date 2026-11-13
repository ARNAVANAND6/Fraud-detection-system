import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Alert, AlertDetail, AlertStatus, LEVELS, Pct, Policy, RiskLevel, SOURCE_LABEL, Source, Stats, Txn, TxnDetail, api,
} from "./api";
import { LEVEL_COLOR, LEVEL_ICON, MinuteSeries, RiskDistribution } from "./charts";
import { Live, useLive } from "./live";

type Tab = "overview" | "alerts" | "performance" | "ingest" | "settings";
type Health = "live" | "idle" | "delayed" | "disconnected" | "offline" | "connecting";

const inr = (n: number) => "₹" + Number(n).toLocaleString("en-IN", { maximumFractionDigits: 0 });
const num = (n: number) => n.toLocaleString("en-IN");
const clock = (iso: string | null | undefined) => (iso ? new Date(iso).toLocaleTimeString() : "never");
const ms = (v: number | null | undefined) => (v == null ? "n/a" : v >= 1000 ? `${(v / 1000).toFixed(2)} s` : `${v.toFixed(v < 10 ? 2 : 0)} ms`);

function ago(iso: string | null | undefined, now: number): string {
  if (!iso) return "never";
  const s = Math.max(0, (now - Date.parse(iso)) / 1000);
  if (s < 2) return "just now";
  if (s < 90) return `${Math.round(s)} s ago`;
  if (s < 5400) return `${Math.round(s / 60)} min ago`;
  return `${Math.round(s / 3600)} h ago`;
}

function useNow(): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const t = window.setInterval(() => setNow(Date.now()), 1000); return () => window.clearInterval(t); }, []);
  return now;
}

/** One honest answer to "is what I am looking at live?" */
function health(live: Live): { state: Health; title: string; detail: string } {
  const lastSeen = live.lastMessageAt ? new Date(live.lastMessageAt).toLocaleTimeString() : "never";
  if (live.conn === "connecting" && !live.status) return { state: "connecting", title: "CONNECTING", detail: "Waiting for the first message from the server." };
  if (live.conn !== "connected") {
    return { state: "offline", title: "NO CONNECTION TO SERVER",
             detail: `This browser lost its connection to the VYROX API. Everything below is what was last received at ${lastSeen} and is not live. Reconnecting automatically.` };
  }
  const st = live.status;
  if (!st) return { state: "connecting", title: "CONNECTING", detail: "Connected. Waiting for pipeline status." };
  const why = st.reasons.join(". ");
  if (st.stream_status === "DISCONNECTED") {
    return { state: "disconnected", title: "STREAM DISCONNECTED",
             detail: `${why}. No new transactions are being scored. Figures below are stored records; the last event was ${clock(st.last_event_at)}.` };
  }
  if (st.stream_status === "DELAYED") return { state: "delayed", title: "STREAM DELAYED", detail: `${why}. Figures below lag behind what has been ingested.` };
  if (st.stream_status === "IDLE") return { state: "idle", title: "IDLE", detail: `${why}. Figures below are stored records; the last event was ${clock(st.last_event_at)}.` };
  return { state: "live", title: "LIVE", detail: "" };
}

const HEALTH_ICON: Record<Health, string> = { live: "●", idle: "○", delayed: "▲", disconnected: "✕", offline: "✕", connecting: "…" };

function SourceBadge({ source }: { source: Source }) {
  return <span className={`src src-${source}`}>{SOURCE_LABEL[source] ?? source}</span>;
}
function LevelBadge({ level }: { level: RiskLevel }) {
  return <span className="level"><i style={{ color: LEVEL_COLOR[level] }}>{LEVEL_ICON[level]}</i> {level}</span>;
}
function Chips({ items }: { items: string[] }) {
  return <>{items.map((c) => <span className="chip" key={c}>{c}</span>)}</>;
}

export default function App() {
  const [tab, setTab] = useState<Tab>("overview");
  const [source, setSource] = useState<Source | null>(null);
  const seed = useCallback(() => api.recent(100, null), []);
  const live = useLive(seed);
  const now = useNow();
  const h = health(live);
  const st = live.status;
  const [openTxn, setOpenTxn] = useState<string | null>(null);

  // With a source filter, analytics are re-read from stored rows each time the backend reports new data.
  const [filtered, setFiltered] = useState<Stats | null>(null);
  useEffect(() => {
    if (!source) { setFiltered(null); return; }
    let cancelled = false;
    api.stats(source).then((s) => { if (!cancelled) setFiltered(s); }).catch(() => undefined);
    return () => { cancelled = true; };
  }, [source, live.stats]);
  const stats = source ? filtered : live.stats;
  const feed = useMemo(() => (source ? live.feed.filter((t) => t.source === source) : live.feed), [live.feed, source]);
  const env = st?.environment;
  const perf = live.stats?.performance;

  return (
    <div className="app">
      <header>
        <div className="brand">VYROX</div>
        <div className={`env env-${env ?? "unknown"}`} title="Environment of the server this dashboard is connected to">
          {env === "demo" ? "DEMO ENVIRONMENT · simulated data" : env === "production" ? "PRODUCTION" : "ENVIRONMENT UNKNOWN"}
        </div>
        <nav className="tabs">
          {(["overview", "alerts", "performance", "ingest", "settings"] as Tab[]).map((t) => (
            <button key={t} className={tab === t ? "on" : ""} onClick={() => setTab(t)}>
              {t === "alerts" && live.stats ? `Alerts (${live.stats.alerts.open} open)` : t[0].toUpperCase() + t.slice(1)}
            </button>
          ))}
        </nav>
        <label className="filter">Data source
          <select value={source ?? ""} onChange={(e) => setSource((e.target.value || null) as Source | null)}>
            <option value="">All sources</option>
            {(Object.keys(SOURCE_LABEL) as Source[]).map((s) => <option key={s} value={s}>{SOURCE_LABEL[s]}</option>)}
          </select>
        </label>
        <div className={`pill pill-${h.state}`} data-testid="stream-pill"><span>{HEALTH_ICON[h.state]}</span> {h.title}</div>
      </header>

      <section className="statusbar" data-testid="statusbar">
        <Fact label="Stream status" value={st && live.conn === "connected" ? st.stream_status : "UNKNOWN"} testid="stream-status" />
        <Fact label="Connection" testid="connection"
              value={live.conn === "connected" ? "Connected" : live.conn === "connecting" ? "Connecting" : "Disconnected"}
              sub={live.conn === "connected" ? `last message ${live.lastMessageAt ? ago(new Date(live.lastMessageAt).toISOString(), now) : "never"}` : `retrying, ${live.reconnects} attempts`} />
        <Fact label="Data source" value={st?.active_sources.length ? st.active_sources.map((s) => SOURCE_LABEL[s]).join(", ") : "none active"}
              sub="sources with events in the last minute" />
        <Fact label="Last event received" value={clock(st?.last_event_at)} sub={ago(st?.last_event_at, now)} testid="last-event" />
        <Fact label="Last successful inference" value={clock(st?.last_inference_at)} sub={st?.model_version ? `model ${st.model_version}` : "model unknown"} />
        <Fact label="Processing latency" value={perf?.pipeline_ms ? ms(perf.pipeline_ms.p50) : "n/a"}
              sub={perf?.pipeline_ms ? `P50, ingest to stored, last ${perf.window_minutes} min` : "no events in the last 5 min"} />
      </section>

      {h.state !== "live" && (
        <div className={`banner banner-${h.state}`} role="status" data-testid="banner">
          <b>{HEALTH_ICON[h.state]} {h.title}.</b> {h.detail}
        </div>
      )}

      <main className={h.state === "live" ? "" : "stale"}>
        {tab === "overview" && <Overview stats={stats} feed={feed} live={live} source={source} now={now} onOpen={setOpenTxn} isLive={h.state === "live"} />}
        {tab === "alerts" && <Alerts live={live} now={now} onOpenTxn={setOpenTxn} />}
        {tab === "performance" && <PerformanceTab live={live} />}
        {tab === "ingest" && <Ingest env={env} />}
        {tab === "settings" && <Settings />}
      </main>
      {openTxn && <TxnDrawer id={openTxn} onClose={() => setOpenTxn(null)} />}
      <footer>
        Risk scores are model estimates on {env === "production" ? "ingested" : "simulated"} data, not banking or regulatory decisions.
        Every figure on this page is read from stored records or measured by the running system.
      </footer>
    </div>
  );
}

function Fact({ label, value, sub, testid }: { label: string; value: string; sub?: string; testid?: string }) {
  return (
    <div className="fact">
      <div className="fact-label">{label}</div>
      <div className="fact-value" data-testid={testid}>{value}</div>
      {sub && <div className="fact-sub">{sub}</div>}
    </div>
  );
}

function Kpi({ label, value, sub, testid }: { label: string; value: string; sub?: string; testid?: string }) {
  return (
    <div className="kpi">
      <div className="kpi-label">{label}</div>
      <div className="kpi-value" data-testid={testid}>{value}</div>
      {sub && <div className="kpi-sub">{sub}</div>}
    </div>
  );
}

function Overview({ stats, feed, live, source, now, onOpen, isLive }: {
  stats: Stats | null; feed: Txn[]; live: Live; source: Source | null; now: number; onOpen: (id: string) => void; isLive: boolean;
}) {
  const [suspicious, setSuspicious] = useState(false);
  if (!stats) return <div className="empty">No data received from the server yet.</div>;
  const rows = (suspicious ? feed.filter((t) => t.risk_level === "HIGH" || t.risk_level === "CRITICAL") : feed).slice(0, 40);
  const mixed = !source && stats.by_source.length > 1;
  const q = stats.label_quality;
  return (
    <>
      <div className="kpis">
        <Kpi label="Transactions processed" value={num(stats.processed)} testid="kpi-processed" sub={source ? SOURCE_LABEL[source] : "all sources, stored total"} />
        <Kpi label="Transactions per second" value={stats.tps_last_minute.toFixed(1)} sub="stored in the last 60 s" />
        <Kpi label="Total volume" value={inr(stats.total_volume)} sub="sum of stored amounts" />
        <Kpi label="High-risk transactions" value={num(stats.high_risk)} sub={`${num(stats.critical)} critical`} />
        <Kpi label="Alerts" value={num(live.stats?.alerts.open ?? 0)} testid="kpi-alerts"
             sub={`open · ${live.stats?.alerts.escalated ?? 0} escalated · ${live.stats?.alerts.total ?? 0} total`} />
        <Kpi label="Average risk score" value={stats.avg_risk_score.toFixed(4)} sub="mean of stored scores" />
      </div>

      {mixed && (
        <div className="notice">
          Totals above combine {stats.by_source.length} data sources. The table "By data source" below separates them, and the
          "Data source" filter in the header restricts every figure to one source.
        </div>
      )}

      <div className="grid2">
        <div className="panel">
          <h3>Transactions per minute <span className="muted">last 30 min, stored records</span></h3>
          <MinuteSeries kind="line" color="var(--series-1)" label="Transactions" points={stats.trend.map((t) => ({ minute: t.minute, value: t.txns }))} />
          <h3 className="spaced">High-risk transactions per minute</h3>
          <MinuteSeries kind="bars" color="var(--serious)" label="High-risk transactions" points={stats.trend.map((t) => ({ minute: t.minute, value: t.high_risk }))} />
        </div>
        <div className="panel">
          <h3>Risk distribution <span className="muted">{num(stats.processed)} stored transactions</span></h3>
          <RiskDistribution levels={stats.risk_levels} />
          <h3 className="spaced">By data source</h3>
          <table className="compact">
            <thead><tr><th>Source</th><th className="r">Transactions</th><th className="r">Volume</th><th className="r">High risk</th><th>Last event</th></tr></thead>
            <tbody>
              {live.stats?.by_source.map((s) => (
                <tr key={s.source}><td><SourceBadge source={s.source} /></td><td className="r">{num(s.txns)}</td>
                  <td className="r">{inr(s.volume)}</td><td className="r">{num(s.high_risk)}</td><td>{ago(s.last_event_at, now)}</td></tr>
              ))}
              {!live.stats?.by_source.length && <tr><td colSpan={5} className="muted">No stored transactions.</td></tr>}
            </tbody>
          </table>
          {q.labelled > 0 && (
            <p className="note">
              Simulator ground truth for {num(q.labelled)} labelled transactions: {q.tp} fraud flagged HIGH or CRITICAL, {q.fn} fraud
              missed, {q.fp} legitimate flagged. Synthetic data only; this says nothing about real-world accuracy.
            </p>
          )}
        </div>
      </div>

      <div className="panel">
        <div className="panel-head">
          <h3>{isLive ? "Live transaction feed" : "Transaction feed (not live)"} <span className="muted">
            {isLive ? `${num(live.received)} events pushed to this browser since it connected` : "most recent stored transactions"}</span></h3>
          <div className="tabs small">
            <button className={!suspicious ? "on" : ""} onClick={() => setSuspicious(false)}>All</button>
            <button className={suspicious ? "on" : ""} onClick={() => setSuspicious(true)}>Suspicious only</button>
          </div>
        </div>
        <table data-testid="feed">
          <thead><tr><th>Stored at</th><th>Source</th><th>Transaction</th><th>Account</th><th>Type</th><th className="r">Amount</th>
            <th className="r">Risk</th><th>Level</th><th>Decision</th><th>Reasons</th><th className="r">Latency</th></tr></thead>
          <tbody>
            {rows.map((t) => (
              <tr key={t.transaction_id} className="clickable" onClick={() => onOpen(t.transaction_id)}>
                <td>{clock(t.persisted_at)}</td><td><SourceBadge source={t.source} /></td>
                <td className="mono">{t.transaction_id}</td><td className="mono">{t.account_id}</td><td>{t.txn_type}</td>
                <td className="r">{inr(t.amount)}</td><td className="r">{t.risk_score.toFixed(3)}</td>
                <td><LevelBadge level={t.risk_level} /></td><td>{t.decision}</td><td><Chips items={t.reason_codes} /></td>
                <td className="r">{ms(t.pipeline_ms)}</td>
              </tr>
            ))}
            {!rows.length && <tr><td colSpan={11} className="muted">No transactions to show.</td></tr>}
          </tbody>
        </table>
      </div>
    </>
  );
}

const NEXT: Record<AlertStatus, AlertStatus[]> = {
  OPEN: ["IN_REVIEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE"], IN_REVIEW: ["CONFIRMED_FRAUD", "FALSE_POSITIVE", "OPEN"],
  CONFIRMED_FRAUD: [], FALSE_POSITIVE: [],
};

function Alerts({ live, now, onOpenTxn }: { live: Live; now: number; onOpenTxn: (id: string) => void }) {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [filter, setFilter] = useState<"ACTIVE" | "ALL">("ACTIVE");
  const [selected, setSelected] = useState<number | null>(null);
  const [detail, setDetail] = useState<AlertDetail | null>(null);
  const [error, setError] = useState("");
  // Re-read the queue whenever the backend reports an alert event (throttled to once a second).
  const tick = Math.floor(live.alertTick ? now / 1000 : 0);
  useEffect(() => { api.alerts().then(setAlerts).catch((e) => setError(String(e.message))); }, [tick, live.conn]);
  useEffect(() => {
    if (selected == null) { setDetail(null); return; }
    api.alert(selected).then(setDetail).catch((e) => setError(String(e.message)));
  }, [selected, live.alertTick]);
  const act = async (id: number, body: { status?: AlertStatus; assigned_to?: string }) => {
    try { setError(""); await api.updateAlert(id, body); setAlerts(await api.alerts()); if (selected === id) setDetail(await api.alert(id)); }
    catch (e) { setError(String((e as Error).message)); }
  };
  const rows = alerts.filter((a) => filter === "ALL" || a.status === "OPEN" || a.status === "IN_REVIEW");
  const sla = (a: Alert) => {
    if (a.status !== "OPEN") return "n/a";
    const left = (Date.parse(a.sla_due_at) - now) / 1000;
    return left < 0 ? `overdue ${Math.round(-left / 60)} min` : `${Math.ceil(left / 60)} min left`;
  };
  return (
    <div className="grid2 wide-left">
      <div className="panel">
        <div className="panel-head">
          <h3>Alert queue <span className="muted">sorted by priority; one alert per account while it stays open</span></h3>
          <div className="tabs small">
            <button className={filter === "ACTIVE" ? "on" : ""} onClick={() => setFilter("ACTIVE")}>Open and in review</button>
            <button className={filter === "ALL" ? "on" : ""} onClick={() => setFilter("ALL")}>All</button>
          </div>
        </div>
        {error && <div className="error">{error}</div>}
        <table data-testid="alerts">
          <thead><tr><th>Priority</th><th>Alert</th><th>Status</th><th>Account</th><th>Triggers</th><th className="r">Max risk</th>
            <th className="r">Events</th><th>Assigned to</th><th>Response time</th></tr></thead>
          <tbody>
            {rows.map((a) => (
              <tr key={a.alert_id} className={`clickable ${selected === a.alert_id ? "sel" : ""}`} onClick={() => setSelected(a.alert_id)}>
                <td><span className={`prio prio-${a.priority}`}>P{a.priority}</span></td><td>#{a.alert_id}</td>
                <td>{a.status.replace("_", " ")}{a.escalation_level > 0 && <span className="esc"> ▲ escalated ×{a.escalation_level}</span>}</td>
                <td className="mono">{a.account_id}</td><td><Chips items={a.trigger_types} /></td>
                <td className="r">{a.max_risk_score.toFixed(3)}</td><td className="r">{a.event_count}</td>
                <td>{a.assigned_to ?? "unassigned"}</td><td>{sla(a)}</td>
              </tr>
            ))}
            {!rows.length && <tr><td colSpan={9} className="muted">No alerts.</td></tr>}
          </tbody>
        </table>
      </div>
      <div className="panel">
        {!detail ? <div className="empty">Select an alert to investigate it.</div> : (
          <>
            <h3>Alert #{detail.alert_id} <span className={`prio prio-${detail.priority}`}>P{detail.priority}</span></h3>
            <p className="big mono">{detail.account_id}</p>
            <p>{detail.status.replace("_", " ")} · assigned to {detail.assigned_to ?? "nobody"} · {detail.event_count} transactions · {inr(detail.total_amount)}</p>
            <p><Chips items={detail.reason_codes} /></p>
            <div className="actions">
              {NEXT[detail.status].map((s) => <button key={s} onClick={() => act(detail.alert_id, { status: s })}>{s === "OPEN" ? "Reopen" : s.replace("_", " ").toLowerCase()}</button>)}
              <button onClick={() => act(detail.alert_id, { assigned_to: "dashboard-user" })}>Assign to me</button>
            </div>
            <h4>Transactions in this alert</h4>
            <table className="compact">
              <tbody>{detail.transactions.map((t) => (
                <tr key={t.transaction_id} className="clickable" onClick={() => onOpenTxn(t.transaction_id)}>
                  <td>{clock(t.event_time)}</td><td>{t.txn_type}</td><td className="r">{inr(t.amount)}</td>
                  <td className="r">{t.risk_score.toFixed(3)}</td><td><LevelBadge level={t.risk_level} /></td><td><SourceBadge source={t.source} /></td>
                </tr>))}</tbody>
            </table>
            <h4>History</h4>
            <table className="compact">
              <tbody>{detail.events.map((e, i) => (
                <tr key={i}><td>{clock(e.at)}</td><td>{e.kind}</td><td>{e.actor}</td><td className="mono small">{JSON.stringify(e.detail)}</td></tr>))}</tbody>
            </table>
          </>
        )}
      </div>
    </div>
  );
}

function PerformanceTab({ live }: { live: Live }) {
  const p = live.stats?.performance;
  if (!p) return <div className="empty">No measurements received yet.</div>;
  const row = (name: string, where: string, v: Pct | null | undefined, samples?: number) => (
    <tr><td>{name}</td><td className="muted">{where}</td>
      <td className="r">{v ? ms(v.p50) : "n/a"}</td><td className="r">{v ? ms(v.p95) : "n/a"}</td><td className="r">{v ? ms(v.p99) : "n/a"}</td>
      <td className="r">{samples != null ? num(samples) : "n/a"}</td></tr>
  );
  return (
    <>
      <div className="kpis">
        <Kpi label="Throughput" value={p.throughput_tps != null ? `${p.throughput_tps.toFixed(1)} /s` : "n/a"} sub={`stored rows over the last ${p.window_minutes} min`} />
        <Kpi label="Kafka consumer lag" value={p.consumer_lag != null ? num(p.consumer_lag) : "unknown"} sub="scorer group, end offset minus committed" />
        {live.status?.pipeline_mode === "spark" && <Kpi label="Spark backlog" value={p.spark_backlog != null ? num(p.spark_backlog) : "unknown"} sub="raw minus forwarded offsets, approximate" />}
        <Kpi label="Error rate" value={p.error_rate != null ? `${(100 * p.error_rate).toFixed(3)}%` : "n/a"}
             sub={`${num(p.events_dead_lettered ?? 0)} dead-lettered of ${num(p.events_received ?? 0)} received`} />
        <Kpi label="Duplicates skipped" value={live.status?.duplicates_skipped != null ? num(live.status.duplicates_skipped) : "n/a"} sub="by the scorer since it started" />
      </div>
      <div className="panel">
        <h3>Latency <span className="muted">measured by the running system, last {p.window_minutes} minutes</span></h3>
        <table>
          <thead><tr><th>Stage</th><th>What is measured</th><th className="r">P50</th><th className="r">P95</th><th className="r">P99</th><th className="r">Samples</th></tr></thead>
          <tbody>
            {row("Ingestion", "API request received to Kafka acknowledgement (REST and webhook, single events)", p.ingest_ms, p.ingest_ms?.samples ?? 0)}
            {row("Feature retrieval", "Redis read and update for one transaction", p.feature_ms, p.samples)}
            {row("Model inference", "rules and model scoring for one transaction", p.inference_ms, p.samples)}
            {row("End-to-end scoring", "ingestion timestamp to prediction stored in PostgreSQL", p.pipeline_ms, p.samples)}
            {row("Server push", "prediction stored to event handed to dashboard connections", p.push_ms, p.push_ms?.samples ?? 0)}
            {row("Dashboard delivery", "prediction stored to received by this browser (clock offset removed)", live.delivery, live.delivery?.samples ?? 0)}
          </tbody>
        </table>
        <p className="note">
          Measured at {clock(p.measured_at)}. "n/a" means no samples in the window, not zero. Historical imports are left out of
          latency because they are bulk-loaded. Pipeline mode: {live.status?.pipeline_mode ?? "unknown"}.
          {live.status?.pipeline_mode === "spark" && " In this mode every event waits for a Spark micro-batch before scoring, which dominates end-to-end latency."}
        </p>
      </div>
    </>
  );
}

function Ingest({ env }: { env: string | undefined }) {
  const [log, setLog] = useState<string[]>([]);
  const say = (m: string) => setLog((l) => [`${new Date().toLocaleTimeString()}  ${m}`, ...l].slice(0, 12));
  const run = async (label: string, f: () => Promise<string>) => {
    try { say(`${label}: ${await f()}`); } catch (e) { say(`${label} failed: ${(e as Error).message}`); }
  };
  const [amount, setAmount] = useState("48000");
  const sendOne = () => run("REST transaction", async () => {
    const id = `TXN_UI_${Date.now().toString(36)}`;
    const r = await api.ingest({ transaction_id: id, event_time: new Date().toISOString(), account_id: "C_UI_TEST", counterparty_id: "M_UI_TEST",
      txn_type: "TRANSFER", amount: Number(amount), old_balance_orig: Number(amount) * 1.02, old_balance_dest: 0, channel: "API" });
    return `${r.transaction_id} ${r.status}, labelled ${SOURCE_LABEL[r.source]}`;
  });
  return (
    <div className="grid2">
      <div className="panel">
        <h3>Send data into the pipeline</h3>
        {env === "demo" ? (
          <>
            <h4>Controlled fraud scenarios <SourceBadge source="SIMULATED_DEMO" /></h4>
            <p className="muted">Each button publishes real events to Kafka. They are scored by the same pipeline and labelled as simulated.</p>
            <div className="actions">
              {["RAPID_BURST", "ACCOUNT_TAKEOVER", "UNUSUAL_AMOUNT"].map((s) => (
                <button key={s} data-testid={`scenario-${s}`} onClick={() => run(s, async () => {
                  const r = await api.scenario(s); return `${r.transaction_ids.length} events for ${r.account_id}`; })}>{s.replace("_", " ").toLowerCase()}</button>
              ))}
            </div>
          </>
        ) : <p className="muted">Fraud scenarios and the synthetic generator do not exist in the production environment.</p>}
        <h4>Authenticated REST API <SourceBadge source="LIVE_API" /></h4>
        <div className="actions">
          <label>Amount <input value={amount} onChange={(e) => setAmount(e.target.value)} inputMode="decimal" /></label>
          <button onClick={sendOne}>Send one transaction</button>
        </div>
        <h4>Historical file <SourceBadge source="HISTORICAL_IMPORT" /></h4>
        <p className="muted">CSV or Parquet with the transaction schema columns, up to 50 MB.</p>
        <input type="file" accept=".csv,.parquet" onChange={(e) => {
          const f = e.target.files?.[0];
          if (f) run(`Import ${f.name}`, async () => { const r = await api.upload(f); return `${r.rows} rows: ${r.accepted} accepted, ${r.duplicates} duplicates, ${r.invalid} invalid`; });
          e.target.value = "";
        }} />
        <h4>Result</h4>
        <pre className="log">{log.length ? log.join("\n") : "Nothing sent yet."}</pre>
      </div>
      <div className="panel">
        <h3>Where data can come from</h3>
        <table className="compact">
          <tbody>
            <tr><td><SourceBadge source="LIVE_API" /></td><td>POST /v1/transactions with an API key</td></tr>
            <tr><td><SourceBadge source="LIVE_WEBHOOK" /></td><td>POST /v1/webhooks/generic, HMAC-SHA256 signed, replay-protected</td></tr>
            <tr><td><SourceBadge source="KAFKA_STREAM" /></td><td>events another system produces directly to the transactions topic</td></tr>
            <tr><td><SourceBadge source="HISTORICAL_IMPORT" /></td><td>CSV or Parquet upload</td></tr>
            <tr><td><SourceBadge source="SIMULATED_DEMO" /></td><td>built-in generator and fraud scenarios, demo environment only</td></tr>
          </tbody>
        </table>
        <p className="note">
          VYROX has no connection to live UPI, bank or card-network transactions. Real payment data can only arrive through an
          authorised provider integration that the operator sets up. No such integration is bundled.
        </p>
      </div>
    </div>
  );
}

function Settings() {
  const [policy, setPolicy] = useState<Policy | null>(null);
  const [msg, setMsg] = useState("");
  useEffect(() => { api.config().then(setPolicy).catch((e) => setMsg(String(e.message))); }, []);
  if (!policy) return <div className="empty">{msg || "Loading configuration."}</div>;
  const t = policy.thresholds;
  const setT = (k: keyof Policy["thresholds"], v: string) => setPolicy({ ...policy, thresholds: { ...t, [k]: Number(v) } });
  const setA = (k: "dedup_minutes" | "max_escalations", v: string) => setPolicy({ ...policy, alerts: { ...policy.alerts, [k]: Number(v) } });
  const save = async () => {
    try { setPolicy(await api.saveConfig(policy)); setMsg("Saved. The scorer applies it to new transactions within about 5 seconds."); }
    catch (e) { setMsg(`Not saved: ${(e as Error).message}`); }
  };
  return (
    <div className="panel narrow">
      <h3>Risk thresholds <span className="muted">applied to the model's risk probability</span></h3>
      <div className="form">
        {(["medium", "high", "critical"] as const).map((k) => (
          <label key={k}><LevelBadge level={k.toUpperCase() as RiskLevel} /> at or above
            <input type="number" step="0.01" min="0.01" max="1" value={t[k]} onChange={(e) => setT(k, e.target.value)} /></label>
        ))}
      </div>
      <p className="muted">Below MEDIUM is LOW. HIGH is sent for review, CRITICAL is held. Existing predictions keep the level they were scored with.</p>
      <h3 className="spaced">Alert engine</h3>
      <div className="form">
        <label>Group repeat alerts for one account within (minutes)
          <input type="number" min="1" value={policy.alerts.dedup_minutes} onChange={(e) => setA("dedup_minutes", e.target.value)} /></label>
        <label>Maximum escalations<input type="number" min="0" max="5" value={policy.alerts.max_escalations} onChange={(e) => setA("max_escalations", e.target.value)} /></label>
      </div>
      <p className="muted">
        Alerts are raised from {policy.alerts.score_alert_level} upward, or from {policy.alerts.rule_min_level} upward when a
        rule in these groups fires: {policy.alerts.rule_triggers.join(", ")}. Analysts: {policy.alerts.analysts.join(", ")}.
        Response-time targets in minutes by priority: {LEVELS.map((_, i) => `P${i + 1} ${policy.alerts.sla_minutes[String(i + 1)]}`).join(", ")}.
      </p>
      <div className="actions"><button className="on" onClick={save}>Save</button> <span>{msg}</span></div>
    </div>
  );
}

function TxnDrawer({ id, onClose }: { id: string; onClose: () => void }) {
  const [t, setT] = useState<TxnDetail | null>(null);
  const [err, setErr] = useState("");
  useEffect(() => { setT(null); api.transaction(id).then(setT).catch((e) => setErr(String(e.message))); }, [id]);
  return (
    <aside className="drawer">
      <div className="panel-head"><h3>Transaction</h3><button onClick={onClose}>Close</button></div>
      {err && <div className="error">{err}</div>}
      {t && (
        <>
          <p className="mono">{t.transaction_id}</p>
          <p><SourceBadge source={t.source} /> <LevelBadge level={t.risk_level} /> · {t.decision}</p>
          <table className="compact"><tbody>
            <tr><td>Risk probability</td><td className="r">{t.risk_score.toFixed(6)}</td></tr>
            <tr><td>Model version</td><td className="r">{t.model_version}</td></tr>
            <tr><td>Amount</td><td className="r">{inr(t.amount)} {t.txn_type}</td></tr>
            <tr><td>Account → counterparty</td><td className="r mono">{t.account_id} → {t.counterparty_id}</td></tr>
            <tr><td>Event time</td><td className="r">{new Date(t.event_time).toLocaleString()}</td></tr>
            <tr><td>Ingested</td><td className="r">{new Date(t.ingested_at).toLocaleString()}</td></tr>
            <tr><td>Inference timestamp</td><td className="r">{new Date(t.scored_at).toLocaleString()}</td></tr>
            <tr><td>Feature / inference / end-to-end</td><td className="r">{ms(t.feature_ms)} / {ms(t.inference_ms)} / {ms(t.pipeline_ms)}</td></tr>
            <tr><td>Alert</td><td className="r">{t.alert_id ? `#${t.alert_id} ${t.alert_status}` : "none"}</td></tr>
            <tr><td>Label</td><td className="r">{t.label == null ? "none" : `${t.label ? "fraud" : "legitimate"} (${t.label_source})`}</td></tr>
          </tbody></table>
          <h4>Reasons</h4>
          <p>{t.reason_codes.length ? <Chips items={t.reason_codes} /> : <span className="muted">No rule fired and the score is below MEDIUM.</span>}</p>
          <h4>Features used at scoring time</h4>
          <table className="compact"><tbody>
            {Object.entries(t.features).sort().map(([k, v]) => <tr key={k}><td className="mono">{k}</td><td className="r">{Number(v).toLocaleString("en-IN", { maximumFractionDigits: 3 })}</td></tr>)}
          </tbody></table>
        </>
      )}
    </aside>
  );
}
