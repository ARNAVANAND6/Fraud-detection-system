import { useCallback, useEffect, useMemo, useState } from "react";
import QRCode from "qrcode";
import {
  Alert, AlertDetail, AlertStatus, ApiKey, AuditRow, Connector, CustomerReport, CustomerTxn, Drift, DriftStatus, LEVELS, Me, Org, Pct,
  Policy, ReportKind, RiskLevel, Role, SOURCE_LABEL, ScoreResult, Source, Stats, Status, Txn, TxnDetail, User, api, session,
} from "./api";
import { LEVEL_COLOR, LEVEL_ICON, MinuteSeries, RiskDistribution } from "./charts";
import { Live, useLive } from "./live";
import { Scene3D } from "./Scene3D";
import { onNav } from "./router";
import { BANNER_TIPS, HELPLINE, REPORT_URL } from "./home/content";

type Tab = "platform" | "overview" | "alerts" | "check" | "performance" | "integrations" | "ingest" | "settings" | "admin" | "security";
// What each role sees. The server enforces the same rules; this only hides what would be refused.
// CUSTOMER never reaches the console: account holders get their own portal (CustomerPortal).
const TABS: Record<Role, Tab[]> = {
  VIEWER: ["overview", "alerts", "performance", "integrations", "security"],
  ANALYST: ["overview", "alerts", "check", "performance", "integrations", "security"],
  // Risk settings apply to every organisation, so only the platform operator changes them.
  ADMIN: ["overview", "alerts", "check", "performance", "integrations", "ingest", "admin", "security"],
  SUPERADMIN: ["platform", "overview", "alerts", "performance", "integrations", "settings", "admin", "security"],
  CUSTOMER: [],
};
const NAV: { group: string; tabs: Tab[] }[] = [
  { group: "Platform", tabs: ["platform"] },
  { group: "Monitor", tabs: ["overview", "alerts", "check"] },
  { group: "System", tabs: ["performance", "integrations"] },
  { group: "Administration", tabs: ["ingest", "settings", "admin"] },
];
const TAB_LABEL: Record<Tab, string> = {
  platform: "Organisations", overview: "Overview", alerts: "Alerts", check: "Check a transaction", performance: "Performance",
  integrations: "Integrations", ingest: "Data intake", settings: "Settings", admin: "People and audit", security: "Security",
};
const TAB_INTRO: Record<Tab, string> = {
  platform: "Every organisation on this platform: create, rename and suspend. Their data stays theirs.",
  overview: "Scored transactions, as stored by the pipeline.",
  alerts: "Work the queue. Every change is recorded in the audit log under your name.",
  check: "Score one transaction against the live model and feature store. Nothing is stored.",
  performance: "Latency, throughput and model health, measured by the running system.",
  integrations: "Where transactions come from, and whether each source is delivering.",
  ingest: "Send test data through the same pipeline as production traffic.",
  settings: "Risk thresholds and alert rules. Changes apply within about 5 seconds.",
  admin: "Who can sign in, what they may do, and what they did.",
  security: "Your own sign-in settings.",
};
type Health = "live" | "idle" | "delayed" | "disconnected" | "offline" | "connecting";

const inr = (n: number) => "₹" + Number(n).toLocaleString("en-IN", { maximumFractionDigits: 0 });
const num = (n: number) => n.toLocaleString("en-IN");
const clock = (iso: string | null | undefined) => (iso ? new Date(iso).toLocaleTimeString() : "never");
const ms = (v: number | null | undefined) => (v == null ? "n/a" : v >= 1000 ? `${(v / 1000).toFixed(2)} s` : `${v.toFixed(v < 10 ? 2 : 0)} ms`);
const notFound = (e: unknown) => String((e as Error)?.message ?? e).startsWith("404");

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
             detail: `This browser lost its connection to the FORESENTRY API. Everything below is what was last received at ${lastSeen} and is not live. Reconnecting automatically.` };
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

function SourceBadge({ source }: { source: Source }) {
  return <span className={`src src-${source}`}>{SOURCE_LABEL[source] ?? source}</span>;
}
function LevelBadge({ level }: { level: RiskLevel }) {
  return <span className="level"><i style={{ color: LEVEL_COLOR[level] }}>{LEVEL_ICON[level]}</i> {level}</span>;
}
function Chips({ items }: { items: string[] }) {
  return <>{items.map((c) => <span className="chip" key={c}>{c}</span>)}</>;
}
/** Risk score as a number with a short bar behind it, so a column of scores can be scanned. */
function ScoreBar({ score, level }: { score: number; level: RiskLevel }) {
  return (
    <span className="scorebar" title={`risk score ${score.toFixed(6)}`}>
      <span className="scorebar-track"><span style={{ width: `${Math.max(2, score * 100)}%`, background: LEVEL_COLOR[level] }} /></span>
      <span className="scorebar-num">{score.toFixed(3)}</span>
    </span>
  );
}

// ---- sign-in ---------------------------------------------------------------------------

/** Reads #sso_token / #sso_error that the API appends when it redirects back after single sign-on. */
function takeSsoResult(): { error: string | null } {
  const h = new URLSearchParams(window.location.hash.replace(/^#/, ""));
  const tok = h.get("sso_token");
  const err = h.get("sso_error");
  if (tok || err) history.replaceState(null, "", window.location.pathname + window.location.search);
  if (tok) session.set(tok);
  return { error: err };
}

export default function App() {
  const [ssoError] = useState(() => takeSsoResult().error);
  const [me, setMe] = useState<Me | null>(null);
  const [checking, setChecking] = useState(session.has());
  useEffect(() => {
    if (session.has()) api.me().then(setMe).catch(() => undefined).finally(() => setChecking(false));
    return session.onExpired(() => setMe(null));
  }, []);
  if (checking) return <div className="boot">Checking your session.</div>;
  // The sign-in answer has the role; /me adds the organisation, so read it once signed in.
  if (!me) return <SignIn onSignedIn={(m) => { setMe(m); api.me().then(setMe).catch(() => undefined); }} initialError={ssoError} />;
  const signOut = () => { api.logout().catch(() => undefined).finally(() => setMe(null)); };
  if (me.role === "CUSTOMER") return <CustomerPortal me={me} onSignOut={signOut} />;
  return <Console me={me} onSignOut={signOut} />;
}

function loginError(m: string): string {
  if (m.startsWith("401")) return "Wrong username or password.";
  if (m.startsWith("429")) return "Too many failed attempts. Try again in 15 minutes.";
  if (m.includes("Failed to fetch")) return "Cannot reach the FORESENTRY server.";
  return m;
}

function SignIn({ onSignedIn, initialError }: { onSignedIn: (me: Me) => void; initialError: string | null }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [code, setCode] = useState("");
  const [mfa, setMfa] = useState<{ token: string; until: number } | null>(null);
  const [error, setError] = useState(initialError ? `Single sign-on failed: ${initialError}` : "");
  const [busy, setBusy] = useState(false);
  const [sso, setSso] = useState<{ enabled: boolean; provider_name: string | null } | null>(null);
  useEffect(() => { api.sso().then(setSso).catch(() => setSso(null)); }, []);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true); setError("");
    try {
      const r = await api.login(username, password);
      if (r.kind === "done") onSignedIn(r.me);
      else { setMfa({ token: r.mfaToken, until: Date.now() + r.expiresIn * 1000 }); setPassword(""); }
    } catch (err) { setError(loginError((err as Error).message)); } finally { setBusy(false); }
  };
  const submitCode = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!mfa) return;
    if (Date.now() > mfa.until) { setMfa(null); setCode(""); setError("That step timed out. Sign in again."); return; }
    setBusy(true); setError("");
    try { onSignedIn(await api.loginMfa(mfa.token, code.trim())); }
    catch (err) {
      const m = (err as Error).message;
      setError(m.startsWith("401") ? "That code was not accepted. Codes change every 30 seconds." : loginError(m));
    } finally { setBusy(false); }
  };

  return (
    <div className="signin">
      <div className="signin-card glass">
        <a className="signin-brand" href="/" onClick={onNav("/")}><span className="mark" aria-hidden>F</span> FORESENTRY</a>
        {!mfa ? (
          <form onSubmit={submit} data-testid="signin">
            <h1>Sign in</h1>
            <p className="muted">Fraud teams, account holders and platform operators use the same sign-in.</p>
            <label>Username<input autoFocus autoComplete="username" value={username} onChange={(e) => setUsername(e.target.value)} /></label>
            <label>Password<input type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} /></label>
            {error && <div className="error" role="alert">{error}</div>}
            <button className="primary wide" type="submit" disabled={busy || !username || !password}>{busy ? "Signing in" : "Sign in"}</button>
            {sso?.enabled && (
              <>
                <div className="or"><span>or</span></div>
                <button type="button" className="wide" onClick={() => { window.location.href = api.ssoStartUrl(); }}>
                  Sign in with {sso.provider_name ?? "your organisation"}
                </button>
              </>
            )}
          </form>
        ) : (
          <form onSubmit={submitCode} data-testid="signin-mfa">
            <h1>Two-step verification</h1>
            <p className="muted">Enter the 6-digit code from your authenticator app for <b>{username}</b>, or one of your recovery codes.</p>
            <label>Code<input autoFocus inputMode="numeric" autoComplete="one-time-code" value={code} onChange={(e) => setCode(e.target.value)} /></label>
            {error && <div className="error" role="alert">{error}</div>}
            <button className="primary wide" type="submit" disabled={busy || code.trim().length < 6}>{busy ? "Checking" : "Verify"}</button>
            <button type="button" className="link" onClick={() => { setMfa(null); setCode(""); setError(""); }}>Use a different account</button>
          </form>
        )}
      </div>
    </div>
  );
}

// ---- console shell --------------------------------------------------------------------

function Console({ me, onSignOut }: { me: Me; onSignOut: () => void }) {
  const [tab, setTab] = useState<Tab>(me.role === "SUPERADMIN" ? "platform" : "overview");
  const [source, setSource] = useState<Source | null>(null);
  const seed = useCallback(() => api.recent(500, null), []);
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
  const allowed = TABS[me.role];

  return (
    <div className="shell">
      <aside className="side glass">
        <div className="side-brand">
          <span className="mark" aria-hidden>F</span>
          <span className="side-name">FORESENTRY</span>
        </div>
        {me.org_name && <div className="side-org" title={`Organisation ${me.org_id}`}>{me.org_name}</div>}
        {me.role === "SUPERADMIN" && <div className="side-org">Platform operator</div>}
        <div className={`env env-${env ?? "unknown"}`} title="Environment of the server this dashboard is connected to">
          {env === "demo" ? "DEMO · simulated data" : env === "production" ? "PRODUCTION" : "ENVIRONMENT UNKNOWN"}
        </div>
        <nav className="tabs" aria-label="Sections">
          {NAV.map((g) => {
            const items = g.tabs.filter((t) => allowed.includes(t));
            if (!items.length) return null;
            return (
              <div className="nav-group" key={g.group}>
                <div className="nav-label">{g.group}</div>
                {items.map((t) => (
                  <button key={t} className={tab === t ? "on" : ""} aria-current={tab === t ? "page" : undefined} onClick={() => setTab(t)}>
                    <span>{TAB_LABEL[t]}</span>
                    {t === "alerts" && live.stats ? <span className="count">{num(live.stats.alerts.open)}</span> : null}
                  </button>
                ))}
              </div>
            );
          })}
        </nav>
        <div className="side-foot">
          <button className={`side-link ${tab === "security" ? "on" : ""}`} onClick={() => setTab("security")}>Security</button>
          <div className="who" data-testid="who" title={`Signed in as ${me.username}`}>
            <span className="avatar" aria-hidden>{me.username.slice(0, 1).toUpperCase()}</span>
            <span className="who-text"><b>{me.username}</b><span className="muted">{me.role.toLowerCase()}</span></span>
            <button className="ghost" onClick={onSignOut}>Sign out</button>
          </div>
        </div>
      </aside>

      <div className="content">
        <header className="topbar">
          <div className="crumbs">
            <span className="muted">{me.role === "SUPERADMIN" ? "All organisations" : me.org_name ?? "Workspace"}</span>
            <span className="sep">/</span><h2>{TAB_LABEL[tab]}</h2>
            <p className="intro muted">{TAB_INTRO[tab]}</p>
          </div>
          <label className="filter">Data source
            <select value={source ?? ""} onChange={(e) => setSource((e.target.value || null) as Source | null)}>
              <option value="">All sources</option>
              {(Object.keys(SOURCE_LABEL) as Source[]).map((s) => <option key={s} value={s}>{SOURCE_LABEL[s]}</option>)}
            </select>
          </label>
          <div className={`pill pill-${h.state}`} data-testid="stream-pill"><span className="dot" aria-hidden /> {h.title}</div>
        </header>

        <section className="statusbar" data-testid="statusbar">
          <Fact label="Stream status" value={st && live.conn === "connected" ? st.stream_status : "UNKNOWN"} testid="stream-status" />
          <Fact label="Connection" testid="connection"
                value={live.conn === "connected" ? "Connected" : live.conn === "connecting" ? "Connecting" : "Disconnected"}
                sub={live.conn === "connected" ? `last message ${live.lastMessageAt ? ago(new Date(live.lastMessageAt).toISOString(), now) : "never"}` : `retrying, ${live.reconnects} attempts`} />
          <Fact label="Data source" value={st?.active_sources.length ? st.active_sources.map((s) => SOURCE_LABEL[s]).join(", ") : "none active"}
                sub="sources with events in the last minute" />
          <Fact label="Last event" value={clock(st?.last_event_at)} sub={ago(st?.last_event_at, now)} testid="last-event" />
          <Fact label="Last inference" value={clock(st?.last_inference_at)} sub={st?.model_version ? `model ${st.model_version}` : "model unknown"} />
          <Fact label="Processing latency" value={perf?.pipeline_ms ? ms(perf.pipeline_ms.p50) : "n/a"}
                sub={perf?.pipeline_ms ? `P50, ingest to stored, last ${perf.window_minutes} min` : "no events in the last 5 min"} />
        </section>

        {h.state !== "live" && (
          <div className={`banner banner-${h.state}`} role="status" data-testid="banner">
            <b>{h.title}.</b> {h.detail}
          </div>
        )}

        <main className={h.state === "live" ? "" : "stale"}>
          {tab === "platform" && <Platform now={now} />}
          {tab === "overview" && <Overview stats={stats} feed={feed} live={live} source={source} now={now} onOpen={setOpenTxn} isLive={h.state === "live"} />}
          {tab === "alerts" && <Alerts live={live} now={now} onOpenTxn={setOpenTxn} me={me} />}
          {tab === "check" && <CheckTransaction />}
          {tab === "performance" && <PerformanceTab live={live} now={now} />}
          {tab === "integrations" && <Integrations now={now} />}
          {tab === "ingest" && <Ingest env={env} />}
          {tab === "settings" && <Settings />}
          {tab === "admin" && <Admin me={me} />}
          {tab === "security" && <Security me={me} />}
        </main>
        {openTxn && <TxnDrawer id={openTxn} onClose={() => setOpenTxn(null)} />}
        <footer>
          Risk scores are model estimates on {env === "production" ? "ingested" : "simulated"} data, not banking or regulatory decisions.
          Every figure on this page is read from stored records or measured by the running system.
        </footer>
      </div>
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

// ---- overview -------------------------------------------------------------------------

/** Half-circle gauge: share of stored transactions passed (LOW, MEDIUM) against flagged (HIGH, CRITICAL). */
function ScreeningGauge({ levels }: { levels: Partial<Record<RiskLevel, number>> }) {
  const passed = (levels.LOW ?? 0) + (levels.MEDIUM ?? 0);
  const flagged = (levels.HIGH ?? 0) + (levels.CRITICAL ?? 0);
  const total = passed + flagged;
  if (!total) return <div className="empty">No scored transactions yet.</div>;
  const share = flagged / total;
  // Flagged is usually around 1%; a linear arc would be a sliver, so the arc uses a square-root scale and the
  // exact share is printed underneath.
  const arc = Math.min(1, Math.sqrt(share));
  const r = 70, len = Math.PI * r;
  return (
    <div className="gauge">
      <svg viewBox="0 0 180 104" role="img" aria-label={`${(100 * share).toFixed(2)}% of transactions flagged`}>
        <path d="M 20 92 A 70 70 0 0 1 160 92" className="gauge-track" />
        <path d="M 20 92 A 70 70 0 0 1 160 92" className="gauge-flag" strokeDasharray={`${arc * len} ${len}`} />
        <text x="90" y="78" textAnchor="middle" className="gauge-num">{(100 * share).toFixed(share < 0.1 ? 2 : 1)}%</text>
        <text x="90" y="95" textAnchor="middle" className="gauge-sub">flagged</text>
      </svg>
      <div className="gauge-legend">
        <div><span className="sw sw-pass" /> Passed <b>{num(passed)}</b><span className="muted">LOW and MEDIUM</span></div>
        <div><span className="sw sw-flag" /> Flagged <b>{num(flagged)}</b><span className="muted">HIGH and CRITICAL</span></div>
      </div>
      <p className="note">Arc length uses a square-root scale so a small share stays visible.</p>
    </div>
  );
}

function Overview({ stats, feed, live, source, now, onOpen, isLive }: {
  stats: Stats | null; feed: Txn[]; live: Live; source: Source | null; now: number; onOpen: (id: string) => void; isLive: boolean;
}) {
  const [suspicious, setSuspicious] = useState(false);
  const [thresholds, setThresholds] = useState<Policy["thresholds"] | null>(null);
  const [show3d, setShow3d] = useState(() => { try { return localStorage.getItem("vyrox-3d") !== "off"; } catch { return true; } });
  const toggle3d = () => setShow3d((v) => { try { localStorage.setItem("vyrox-3d", v ? "off" : "on"); } catch { /* preference not saved */ } return !v; });
  useEffect(() => { api.config().then((p) => setThresholds(p.thresholds)).catch(() => undefined); }, [live.conn]);
  if (!stats) return <div className="empty">No data received from the server yet.</div>;
  const rows = (suspicious ? feed.filter((t) => t.risk_level === "HIGH" || t.risk_level === "CRITICAL") : feed).slice(0, 40);
  const mixed = !source && stats.by_source.length > 1;
  const q = stats.label_quality;
  return (
    <>
      <div className="kpis">
        <Kpi label="Transactions processed" value={num(stats.processed)} testid="kpi-processed" sub={source ? SOURCE_LABEL[source] : "all sources, stored total"} />
        <Kpi label="Per second" value={stats.tps_last_minute.toFixed(1)} sub="stored in the last 60 s" />
        <Kpi label="Total volume" value={inr(stats.total_volume)} sub="sum of stored amounts" />
        <Kpi label="High risk" value={num(stats.high_risk)} sub={`${num(stats.critical)} critical`} />
        <Kpi label="Open alerts" value={num(live.stats?.alerts.open ?? 0)} testid="kpi-alerts"
             sub={`${live.stats?.alerts.escalated ?? 0} escalated · ${num(live.stats?.alerts.total ?? 0)} total`} />
        <Kpi label="Average risk score" value={stats.avg_risk_score.toFixed(4)} sub="mean of stored scores" />
      </div>

      {mixed && (
        <div className="notice">
          Totals combine {stats.by_source.length} data sources. "By data source" below separates them, and the
          "Data source" filter at the top restricts every figure to one source.
        </div>
      )}

      <div className="grid2">
        <div className="panel">
          <div className="panel-head"><h3>Transactions per minute</h3><span className="muted small">last 30 min, stored records</span></div>
          <MinuteSeries kind="line" color="var(--series-1)" label="Transactions" points={stats.trend.map((t) => ({ minute: t.minute, value: t.txns }))} />
          <div className="panel-head spaced"><h3>High risk per minute</h3></div>
          <MinuteSeries kind="bars" color="var(--serious)" label="High-risk transactions" points={stats.trend.map((t) => ({ minute: t.minute, value: t.high_risk }))} />
        </div>
        <div className="panel">
          <div className="panel-head"><h3>Screening outcome</h3><span className="muted small">{num(stats.processed)} stored</span></div>
          <ScreeningGauge levels={stats.risk_levels} />
          <div className="panel-head spaced"><h3>Risk distribution</h3></div>
          <RiskDistribution levels={stats.risk_levels} />
        </div>
      </div>

      <div className="panel">
        <div className="panel-head">
          <h3>Live transaction space</h3>
          <span className="muted small grow">each point is one stored transaction: amount across, risk score up, age into the distance</span>
          <button onClick={toggle3d}>{show3d ? "Hide 3D view" : "Show 3D view"}</button>
        </div>
        {show3d && <Scene3D feed={feed} isLive={isLive} frozenAt={live.lastMessageAt} clockOffset={live.clockOffset} thresholds={thresholds} onOpen={onOpen} />}
      </div>

      <div className="panel">
        <div className="panel-head">
          <h3>{isLive ? "Live transaction feed" : "Transaction feed (not live)"}</h3>
          <span className="muted small grow">
            {isLive ? `${num(live.received)} events pushed to this browser since it connected` : "most recent stored transactions"}</span>
          <div className="seg">
            <button className={!suspicious ? "on" : ""} onClick={() => setSuspicious(false)}>All</button>
            <button className={suspicious ? "on" : ""} onClick={() => setSuspicious(true)}>Suspicious only</button>
          </div>
        </div>
        <div className="table-wrap">
          <table data-testid="feed">
            <thead><tr><th>Stored at</th><th>Source</th><th>Transaction</th><th>Account</th><th>Type</th><th className="r">Amount</th>
              <th>Risk score</th><th>Level</th><th>Decision</th><th>Reasons</th><th className="r">Latency</th></tr></thead>
            <tbody>
              {rows.map((t) => (
                <tr key={t.transaction_id} className="clickable" onClick={() => onOpen(t.transaction_id)}>
                  <td>{clock(t.persisted_at)}</td><td><SourceBadge source={t.source} /></td>
                  <td className="mono">{t.transaction_id}</td><td className="mono">{t.account_id}</td><td>{t.txn_type}</td>
                  <td className="r">{inr(t.amount)}</td><td><ScoreBar score={t.risk_score} level={t.risk_level} /></td>
                  <td><LevelBadge level={t.risk_level} /></td><td><span className={`decision d-${t.decision}`}>{t.decision}</span></td>
                  <td><Chips items={t.reason_codes} /></td><td className="r">{ms(t.pipeline_ms)}</td>
                </tr>
              ))}
              {!rows.length && <tr><td colSpan={11} className="muted">No transactions to show.</td></tr>}
            </tbody>
          </table>
        </div>
      </div>

      <div className="panel">
        <div className="panel-head"><h3>By data source</h3></div>
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
            Simulator ground truth for {num(q.labelled)} labelled transactions: {num(q.tp)} fraud flagged HIGH or CRITICAL, {num(q.fn)} fraud
            missed, {num(q.fp)} legitimate flagged. Synthetic data only; this says nothing about real-world accuracy.
          </p>
        )}
      </div>
    </>
  );
}

// ---- alerts ---------------------------------------------------------------------------

const NEXT: Record<AlertStatus, AlertStatus[]> = {
  OPEN: ["IN_REVIEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE"], IN_REVIEW: ["CONFIRMED_FRAUD", "FALSE_POSITIVE", "OPEN"],
  CONFIRMED_FRAUD: [], FALSE_POSITIVE: [],
};

function Alerts({ live, now, onOpenTxn, me }: { live: Live; now: number; onOpenTxn: (id: string) => void; me: Me }) {
  const canWork = me.role !== "VIEWER";
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
    return left < 0 ? <span className="overdue">overdue {Math.round(-left / 60)} min</span> : `${Math.ceil(left / 60)} min left`;
  };
  return (
    <div className="grid2 wide-left">
      <div className="panel">
        <div className="panel-head">
          <h3>Alert queue</h3>
          <span className="muted small grow">sorted by priority; one alert per account while it stays open</span>
          <div className="seg">
            <button className={filter === "ACTIVE" ? "on" : ""} onClick={() => setFilter("ACTIVE")}>Open and in review</button>
            <button className={filter === "ALL" ? "on" : ""} onClick={() => setFilter("ALL")}>All</button>
          </div>
        </div>
        {error && <div className="error">{error}</div>}
        <div className="table-wrap">
          <table data-testid="alerts">
            <thead><tr><th>Priority</th><th>Alert</th><th>Status</th><th>Account</th><th>Triggers</th><th className="r">Max risk</th>
              <th className="r">Events</th><th>Assigned to</th><th>Response time</th></tr></thead>
            <tbody>
              {rows.map((a) => (
                <tr key={a.alert_id} className={`clickable ${selected === a.alert_id ? "sel" : ""}`} onClick={() => setSelected(a.alert_id)}>
                  <td><span className={`prio prio-${a.priority}`}>P{a.priority}</span></td><td>#{a.alert_id}</td>
                  <td><span className={`status s-${a.status}`}>{a.status.replace("_", " ").toLowerCase()}</span>
                    {a.escalation_level > 0 && <span className="esc"> escalated ×{a.escalation_level}</span>}</td>
                  <td className="mono">{a.account_id}</td><td><Chips items={a.trigger_types} /></td>
                  <td className="r">{a.max_risk_score.toFixed(3)}</td><td className="r">{a.event_count}</td>
                  <td>{a.assigned_to ?? <span className="muted">unassigned</span>}</td><td>{sla(a)}</td>
                </tr>
              ))}
              {!rows.length && <tr><td colSpan={9} className="muted">No alerts.</td></tr>}
            </tbody>
          </table>
        </div>
      </div>
      <div className="panel sticky">
        {!detail ? <div className="empty">Select an alert to investigate it.</div> : (
          <>
            <div className="panel-head"><h3>Alert #{detail.alert_id}</h3><span className={`prio prio-${detail.priority}`}>P{detail.priority}</span></div>
            <p className="big mono">{detail.account_id}</p>
            <dl className="facts">
              <dt>Status</dt><dd><span className={`status s-${detail.status}`}>{detail.status.replace("_", " ").toLowerCase()}</span></dd>
              <dt>Assigned to</dt><dd>{detail.assigned_to ?? "nobody"}</dd>
              <dt>Transactions</dt><dd>{detail.event_count}</dd>
              <dt>Amount</dt><dd>{inr(detail.total_amount)}</dd>
            </dl>
            <p><Chips items={detail.reason_codes} /></p>
            {canWork ? (
              <div className="actions">
                {NEXT[detail.status].map((s) => (
                  <button key={s} className={s === "CONFIRMED_FRAUD" ? "danger" : ""} onClick={() => act(detail.alert_id, { status: s })}>
                    {s === "OPEN" ? "Reopen" : s === "IN_REVIEW" ? "Start review" : s === "CONFIRMED_FRAUD" ? "Confirm fraud" : "Mark false positive"}
                  </button>
                ))}
                <button onClick={() => act(detail.alert_id, { assigned_to: me.username })}>Assign to me</button>
              </div>
            ) : <p className="muted">Your role can view alerts but not change them.</p>}
            <h4>Transactions in this alert</h4>
            <table className="compact">
              <tbody>{detail.transactions.map((t) => (
                <tr key={t.transaction_id} className="clickable" onClick={() => onOpenTxn(t.transaction_id)}>
                  <td>{clock(t.event_time)}</td><td>{t.txn_type}</td><td className="r">{inr(t.amount)}</td>
                  <td><ScoreBar score={t.risk_score} level={t.risk_level} /></td><td><SourceBadge source={t.source} /></td>
                </tr>))}</tbody>
            </table>
            <h4>History</h4>
            <ol className="timeline">
              {detail.events.map((e, i) => (
                <li key={i}><span className="t-when">{clock(e.at)}</span><span className="t-what">{e.kind.replace(/_/g, " ").toLowerCase()}</span>
                  <span className="t-who">{e.actor}</span>
                  {Object.keys(e.detail).length > 0 && <span className="t-detail mono small">{JSON.stringify(e.detail)}</span>}</li>))}
            </ol>
          </>
        )}
      </div>
    </div>
  );
}

// ---- check a transaction --------------------------------------------------------------

const TXN_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"] as const;
const DECISION_TEXT: Record<ScoreResult["decision"], string> = {
  ALLOW: "No action needed", REVIEW: "Send for review", HOLD: "Hold for investigation",
};

/** Ring showing the risk score (0 to 1). It is a model score, not a calibrated probability. */
function ScoreRing({ score, level }: { score: number; level: RiskLevel }) {
  const r = 46, c = 2 * Math.PI * r;
  return (
    <svg className="ring" viewBox="0 0 120 120" role="img" aria-label={`risk score ${score.toFixed(3)}`}>
      <circle cx="60" cy="60" r={r} className="ring-track" />
      <circle cx="60" cy="60" r={r} className="ring-val" stroke={LEVEL_COLOR[level]}
              strokeDasharray={`${Math.max(0.005, score) * c} ${c}`} transform="rotate(-90 60 60)" />
      <text x="60" y="58" textAnchor="middle" className="ring-num">{score.toFixed(3)}</text>
      <text x="60" y="76" textAnchor="middle" className="ring-sub">risk score</text>
    </svg>
  );
}

function CheckTransaction() {
  const blank = { account_id: "", counterparty_id: "", txn_type: "TRANSFER", amount: "", old_balance_orig: "", old_balance_dest: "",
                  channel: "APP", device_id: "" };
  const [f, setF] = useState(blank);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ sent: typeof blank; r: ScoreResult } | null>(null);
  const [error, setError] = useState("");
  const set = (k: keyof typeof blank) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => setF({ ...f, [k]: e.target.value });
  const ready = f.account_id && f.counterparty_id && Number(f.amount) > 0;
  // A blank balance means "not known" and is left out, as payment-gateway events are; it is not read as zero.
  const balance = (v: string) => (v.trim() === "" ? {} : { value: Number(v) });
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true); setError("");
    try {
      const r = await api.score({
        transaction_id: `CHECK_${Date.now().toString(36)}`, event_time: new Date().toISOString(),
        account_id: f.account_id.trim(), counterparty_id: f.counterparty_id.trim(), txn_type: f.txn_type,
        amount: Number(f.amount),
        ...("value" in balance(f.old_balance_orig) ? { old_balance_orig: Number(f.old_balance_orig) } : {}),
        ...("value" in balance(f.old_balance_dest) ? { old_balance_dest: Number(f.old_balance_dest) } : {}),
        channel: f.channel || "APP", ...(f.device_id.trim() ? { device_id: f.device_id.trim() } : {}),
      });
      setResult({ sent: f, r });
    } catch (err) {
      const m = (err as Error).message;
      setError(m.startsWith("403") ? "Only analysts, administrators and API clients can run checks." : m.startsWith("422") ? `The server rejected a field: ${m.slice(5)}` : m);
    } finally { setBusy(false); }
  };
  const r = result?.r;
  return (
    <div className="grid2 check">
      <form className="panel" onSubmit={submit}>
        <div className="panel-head"><h3>Transaction details</h3></div>
        <div className="fields">
          <label>Account<input value={f.account_id} onChange={set("account_id")} placeholder="C00000001" /></label>
          <label>Counterparty<input value={f.counterparty_id} onChange={set("counterparty_id")} placeholder="M000000001" /></label>
          <label>Type<select value={f.txn_type} onChange={set("txn_type")}>{TXN_TYPES.map((t) => <option key={t}>{t}</option>)}</select></label>
          <label>Amount (₹)<input inputMode="decimal" value={f.amount} onChange={set("amount")} /></label>
          <label><span>Sender balance before (₹) <span className="muted">(blank if unknown)</span></span><input inputMode="decimal" value={f.old_balance_orig} onChange={set("old_balance_orig")} /></label>
          <label><span>Receiver balance before (₹) <span className="muted">(blank if unknown)</span></span><input inputMode="decimal" value={f.old_balance_dest} onChange={set("old_balance_dest")} /></label>
          <label>Channel<input value={f.channel} onChange={set("channel")} /></label>
          <label><span>Device id <span className="muted">(optional)</span></span><input value={f.device_id} onChange={set("device_id")} /></label>
        </div>
        {error && <div className="error" role="alert">{error}</div>}
        <div className="actions">
          <button className="primary" type="submit" disabled={busy || !ready}>{busy ? "Scoring" : "Score transaction"}</button>
          <button type="button" onClick={() => { setF(blank); setResult(null); setError(""); }}>Clear</button>
        </div>
        <p className="note">
          The account's history in the feature store is used, so the same payment can score differently for different accounts.
          This is a dry run: nothing is stored, no alert is raised and the account's history is not changed.
        </p>
      </form>

      <div className="panel">
        <div className="panel-head"><h3>Result</h3>{r && <span className="muted small">{clock(r.scored_at)}</span>}</div>
        {!r ? <div className="empty">Fill in the details and score the transaction.</div> : (
          <div className="result">
            <div className={`verdict v-${r.decision}`}>
              <b>{DECISION_TEXT[r.decision]}</b>
              <span>{r.decision} · <LevelBadge level={r.risk_level} /></span>
            </div>
            <ScoreRing score={r.risk_score} level={r.risk_level} />
            <dl className="facts">
              <dt>Payment</dt><dd>{inr(Number(result!.sent.amount))} {result!.sent.txn_type}</dd>
              <dt>From → to</dt><dd className="mono">{result!.sent.account_id} → {result!.sent.counterparty_id}</dd>
              <dt>Model</dt><dd>{r.model_version}</dd>
              <dt>Inference</dt><dd>{ms(r.latency_ms)}</dd>
            </dl>
            <h4>Reasons</h4>
            <p>{r.reason_codes.length ? <Chips items={r.reason_codes} /> : <span className="muted">No rule fired and the score is below MEDIUM.</span>}</p>
            {r.disclaimer && <p className="note">{r.disclaimer}</p>}
          </div>
        )}
      </div>
    </div>
  );
}

// ---- performance ----------------------------------------------------------------------

function PerformanceTab({ live, now }: { live: Live; now: number }) {
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
        <div className="panel-head"><h3>Latency</h3><span className="muted small">measured by the running system, last {p.window_minutes} minutes</span></div>
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
      <Nodes status={live.status} now={now} />
      <ModelHealth />
    </>
  );
}

function Nodes({ status, now }: { status: Status | null; now: number }) {
  const nodes = status?.nodes;
  return (
    <div className="panel" data-testid="nodes">
      <div className="panel-head"><h3>Servers</h3><span className="muted small">
        API and scorer processes seen in the last hour{status?.node ? `; this page is served by ${status.node}` : ""}</span></div>
      {!nodes ? (
        <div className="empty">This server does not report its processes. It is running as a single node.</div>
      ) : (
        <table className="compact">
          <thead><tr><th>Process</th><th>Kind</th><th>State</th><th className="r">Partitions</th><th>Last heartbeat</th></tr></thead>
          <tbody>
            {nodes.map((n) => (
              <tr key={`${n.kind}:${n.id}`}><td className="mono">{n.id}</td><td>{n.kind}</td>
                <td><span className={`state ${n.alive ? "ok" : "bad"}`}>{n.alive ? "running" : "not responding"}</span></td>
                <td className="r">{n.kind === "scorer" && n.alive ? n.partitions ?? "" : ""}</td>
                <td>{ago(n.last_seen, now)}</td></tr>
            ))}
            {!nodes.length && <tr><td colSpan={5} className="muted">No processes reported.</td></tr>}
          </tbody>
        </table>
      )}
    </div>
  );
}

const DRIFT_TEXT: Record<DriftStatus, string> = {
  STABLE: "Stable", WATCH: "Watch", DRIFTED: "Drifted", NEEDS_24H: "Needs 24 h of traffic",
  NOT_ENOUGH_DATA: "Not enough data", NO_REFERENCE: "No reference",
};

function ModelHealth() {
  const [d, setD] = useState<Drift | null>(null);
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    let stop = false;
    const load = () => api.drift().then((r) => { if (!stop) { setD(r); setErr(null); } })
      .catch((e) => { if (!stop) setErr((e as Error).message); });
    load();
    const t = setInterval(load, 60_000);
    return () => { stop = true; clearInterval(t); };
  }, []);
  return (
    <div className="panel" data-testid="model-health">
      <div className="panel-head"><h3>Model health</h3><span className="muted small">live inputs compared with the data the model was trained on</span></div>
      {err && <p className="note">Could not load the comparison: {err}</p>}
      {!d && !err && <div className="empty">Loading.</div>}
      {d?.status === "NO_REFERENCE" && <div className="empty">Model {d.model_version} has no training reference, so drift cannot be measured. Run ml/build_reference.py for it.</div>}
      {d?.status === "NOT_ENOUGH_DATA" && <div className="empty">Only {num(d.rows ?? 0)} live transactions in the last 24 hours. At least {num(d.min_rows ?? 0)} are needed for a comparison.</div>}
      {d && d.features.length > 0 && (
        <>
          <p className="note">
            Overall: <span className={`drift ${d.status}`}>{DRIFT_TEXT[d.status]}</span>. Model {d.model_version}, {num(d.rows ?? 0)} live
            transactions over {(d.span_hours ?? 0).toFixed(1)} h against {num(d.reference_rows ?? 0)} training rows. Measured at {clock(d.measured_at)}.
          </p>
          <table>
            <thead><tr><th>Model input</th><th className="r">PSI</th><th>Reading</th></tr></thead>
            <tbody>
              {d.features.map((f) => (
                <tr key={f.feature}><td className="mono">{f.feature}</td><td className="r">{f.psi.toFixed(3)}</td>
                  <td><span className={`drift ${f.status}`}>{DRIFT_TEXT[f.status]}</span></td></tr>
              ))}
            </tbody>
          </table>
          <p className="note">
            PSI (population stability index) measures how far a model input has moved from training: above {d.watch_above} is worth
            watching, above {d.drifted_above} has drifted. Drift means the inputs changed. It does not by itself mean the model is
            wrong; confirmed labels decide that. Training data: {d.reference_data ?? "unknown"}.
          </p>
        </>
      )}
    </div>
  );
}

// ---- integrations ---------------------------------------------------------------------

const CONNECTOR_NAME: Record<string, string> = { razorpay: "Razorpay", stripe: "Stripe", generic: "Signed webhook" };

function Integrations({ now }: { now: number }) {
  const [rows, setRows] = useState<Connector[] | null>(null);
  const [alerting, setAlerting] = useState<boolean | null>(null);
  const [state, setState] = useState<"loading" | "ok" | "missing" | "error">("loading");
  const [err, setErr] = useState("");
  useEffect(() => {
    let stop = false;
    const load = () => api.connectors()
      .then((r) => { if (!stop) { setRows(r.connectors); setAlerting(r.alert_webhook_configured); setState("ok"); } })
      .catch((e) => { if (!stop) { setState(notFound(e) ? "missing" : "error"); setErr((e as Error).message); } });
    load();
    const t = setInterval(load, 15_000);
    return () => { stop = true; clearInterval(t); };
  }, []);
  return (
    <>
      <div className="panel">
        <div className="panel-head"><h3>Connected sources</h3><span className="muted small">refreshed every 15 s</span></div>
        {state === "loading" && <div className="empty">Loading.</div>}
        {state === "missing" && <div className="empty">This server does not report connectors. Update the API to a version with connector support.</div>}
        {state === "error" && <div className="error">{err}</div>}
        {state === "ok" && rows && (
          <table data-testid="connectors">
            <thead><tr><th>Name</th><th>Webhook path</th><th>State</th><th>Last event</th><th className="r">Events, 24 h</th><th>Last error</th></tr></thead>
            <tbody>
              {rows.map((c) => {
                // Failing only while the newest thing that happened is an error.
                const failing = !!c.last_error_at && (!c.last_event_at || c.last_error_at > c.last_event_at);
                return (
                  <tr key={c.name}>
                    <td>{CONNECTOR_NAME[c.name] ?? c.name}{!c.balance_known && <div className="muted small">no balances: scored mainly by rules</div>}</td>
                    <td className="mono small">{c.path}</td>
                    <td><span className={`state ${!c.enabled ? "off" : failing ? "bad" : "ok"}`}>
                      {!c.enabled ? "off" : failing ? "failing" : "on"}</span></td>
                    <td>{ago(c.last_event_at, now)}</td><td className="r">{num(c.events_24h)}</td>
                    <td className="small">{c.last_error ? <>{c.last_error} <span className="muted">({ago(c.last_error_at, now)})</span></>
                      : <span className="muted">none</span>}</td>
                  </tr>
                );
              })}
              {!rows.length && <tr><td colSpan={6} className="muted">No connectors configured.</td></tr>}
            </tbody>
          </table>
        )}
      </div>
      {alerting !== null && (
        <div className="panel">
          <div className="panel-head"><h3>Operational alerts</h3></div>
          <p className="note">
            {alerting
              ? "Posting to the configured chat webhook: new priority-1 alerts (at most 10 an hour), stopped servers, a disconnected stream and failed backups."
              : "Off. Set ALERT_WEBHOOK_URL to a Slack, Teams or other incoming webhook to be told about stopped servers, failed backups and priority-1 alerts."}
          </p>
        </div>
      )}
      <div className="panel">
        <div className="panel-head"><h3>Ways data can arrive</h3></div>
        <table className="compact">
          <tbody>
            <tr><td><SourceBadge source="LIVE_API" /></td><td>POST /v1/transactions with an API key</td></tr>
            <tr><td><SourceBadge source="LIVE_WEBHOOK" /></td><td>POST /v1/webhooks/generic, HMAC-SHA256 signed, replay-protected; or a Razorpay or Stripe webhook, checked with the provider's signature</td></tr>
            <tr><td><SourceBadge source="KAFKA_STREAM" /></td><td>events another system produces directly to the transactions topic</td></tr>
            <tr><td><SourceBadge source="HISTORICAL_IMPORT" /></td><td>CSV or Parquet upload</td></tr>
            <tr><td><SourceBadge source="SIMULATED_DEMO" /></td><td>built-in generator and fraud scenarios, demo environment only</td></tr>
          </tbody>
        </table>
        <p className="note">
          Real payment data only arrives through a provider integration the operator has authorised. Nothing here connects to UPI,
          a bank or a card network on its own.
        </p>
      </div>
    </>
  );
}

// ---- data intake ----------------------------------------------------------------------

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
        <div className="panel-head"><h3>Send data into the pipeline</h3></div>
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
          <label className="inline">Amount <input value={amount} onChange={(e) => setAmount(e.target.value)} inputMode="decimal" /></label>
          <button onClick={sendOne}>Send one transaction</button>
        </div>
        <h4>Historical file <SourceBadge source="HISTORICAL_IMPORT" /></h4>
        <p className="muted">CSV or Parquet with the transaction schema columns, up to 50 MB.</p>
        <input type="file" accept=".csv,.parquet" onChange={(e) => {
          const f = e.target.files?.[0];
          if (f) run(`Import ${f.name}`, async () => { const r = await api.upload(f); return `${r.rows} rows: ${r.accepted} accepted, ${r.duplicates} duplicates, ${r.invalid} invalid`; });
          e.target.value = "";
        }} />
      </div>
      <div className="panel">
        <div className="panel-head"><h3>Result</h3></div>
        <pre className="log">{log.length ? log.join("\n") : "Nothing sent yet."}</pre>
      </div>
    </div>
  );
}

// ---- settings -------------------------------------------------------------------------

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
      <div className="panel-head"><h3>Risk thresholds</h3><span className="muted small">applied to the model's risk score</span></div>
      <div className="form">
        {(["medium", "high", "critical"] as const).map((k) => (
          <label key={k}><LevelBadge level={k.toUpperCase() as RiskLevel} /> at or above
            <input type="number" step="0.01" min="0.01" max="1" value={t[k]} onChange={(e) => setT(k, e.target.value)} /></label>
        ))}
      </div>
      <p className="muted">Below MEDIUM is LOW. HIGH is sent for review, CRITICAL is held. Existing predictions keep the level they were scored with.</p>
      <div className="panel-head spaced"><h3>Alert engine</h3></div>
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
      <div className="actions"><button className="primary" onClick={save}>Save</button> <span>{msg}</span></div>
    </div>
  );
}

// ---- people and audit -----------------------------------------------------------------

const ORG_ROLES: Role[] = ["VIEWER", "ANALYST", "ADMIN", "CUSTOMER"];

function Admin({ me }: { me: Me }) {
  const platform = me.role === "SUPERADMIN";
  const ROLES: Role[] = platform ? [...ORG_ROLES, "SUPERADMIN"] : ORG_ROLES;
  const [users, setUsers] = useState<User[]>([]);
  const [audit, setAudit] = useState<AuditRow[]>([]);
  const [msg, setMsg] = useState("");
  const [form, setForm] = useState({ username: "", password: "", role: "ANALYST" as Role, org_id: "", accounts: "" });
  const extra = () => ({
    ...(platform && form.role !== "SUPERADMIN" && form.org_id.trim() ? { org_id: form.org_id.trim() } : {}),
    ...(form.role === "CUSTOMER" ? { account_ids: form.accounts.split(/[\s,]+/).filter(Boolean) } : {}),
  });
  const formReady = form.username && form.password.length >= 10
    && (form.role !== "CUSTOMER" || form.accounts.trim()) && (!platform || form.role === "SUPERADMIN" || form.org_id.trim());
  const load = useCallback(() => {
    Promise.all([api.users(), api.audit()]).then(([u, a]) => { setUsers(u); setAudit(a); }).catch((e) => setMsg(String(e.message)));
  }, []);
  useEffect(load, [load]);
  const run = async (what: string, fn: () => Promise<unknown>) => {
    try { await fn(); setMsg(`${what}: done.`); } catch (e) { setMsg(`${what}: ${(e as Error).message}`); }
    load();
  };
  const reportsMfa = users.some((u) => u.mfa_enabled !== undefined);
  return (
    <div className="grid2">
      <div className="panel">
        <div className="panel-head"><h3>People</h3><span className="muted small">who can sign in, and what they may do</span></div>
        {msg && <div className="notice" role="status">{msg}</div>}
        <table data-testid="users">
          <thead><tr><th>Username</th>{platform && <th>Organisation</th>}<th>Role</th>{reportsMfa && <th>Sign-in</th>}<th>Status</th><th></th></tr></thead>
          <tbody>{users.map((u) => (
            <tr key={u.username}>
              <td className="mono">{u.username}{u.username === me.username && <span className="muted"> (you)</span>}
                {u.role === "CUSTOMER" && u.account_ids?.length ? <div className="muted small">{u.account_ids.join(", ")}</div> : null}</td>
              {platform && <td className="mono small">{u.org_id ?? <span className="muted">platform</span>}</td>}
              <td>
                <select value={u.role} aria-label={`Role of ${u.username}`}
                        onChange={(e) => run(`Role of ${u.username}`, () => api.changeUser(u.username, { role: e.target.value as Role }))}>
                  {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
                </select>
              </td>
              {reportsMfa && (
                <td>
                  {u.sso ? <span className="state ok">SSO</span> : null}{" "}
                  <span className={`state ${u.mfa_enabled ? "ok" : "off"}`}>{u.mfa_enabled ? "2FA on" : "password only"}</span>
                </td>
              )}
              <td>{u.active ? "Active" : <span className="muted">Deactivated</span>}</td>
              <td className="r row-actions">
                {u.mfa_enabled && (
                  <button onClick={() => { if (window.confirm(`Reset two-step verification for ${u.username}? They will sign in with a password only until they set it up again.`))
                    run(`Reset 2FA for ${u.username}`, () => api.changeUser(u.username, { reset_mfa: true })); }}>Reset 2FA</button>
                )}
                <button onClick={() => run(`${u.active ? "Deactivate" : "Reactivate"} ${u.username}`, () => api.changeUser(u.username, { active: !u.active }))}>
                  {u.active ? "Deactivate" : "Reactivate"}
                </button>
              </td>
            </tr>
          ))}</tbody>
        </table>
        <h4>Add a person</h4>
        <form className="form" onSubmit={(e) => { e.preventDefault(); run(`Add ${form.username}`, async () => {
          await api.createUser(form.username, form.password, form.role, extra());
          setForm({ username: "", password: "", role: "ANALYST", org_id: form.org_id, accounts: "" }); }); }}>
          <label>Username<input value={form.username} autoComplete="off" onChange={(e) => setForm({ ...form, username: e.target.value })} /></label>
          <label>Initial password (10 or more characters)
            <input type="password" autoComplete="new-password" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} /></label>
          <label>Role
            <select value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value as Role })}>
              {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
            </select></label>
          {platform && form.role !== "SUPERADMIN" && (
            <label>Organisation id<input value={form.org_id} placeholder="demo" onChange={(e) => setForm({ ...form, org_id: e.target.value })} /></label>
          )}
          {form.role === "CUSTOMER" && (
            <label>Account ids (comma-separated)<input value={form.accounts} placeholder="C00000001" onChange={(e) => setForm({ ...form, accounts: e.target.value })} /></label>
          )}
          <div className="actions"><button className="primary" type="submit" disabled={!formReady}>Add person</button></div>
        </form>
        <p className="muted">Viewer: read only. Analyst: can also work alerts. Admin: can also change settings, import data and manage people.
          Customer: an account holder who sees only their own payments and can report one. Changing a role or deactivating someone
          signs them out everywhere.</p>
        {me.role === "ADMIN" && <ApiKeys />}
      </div>
      <div className="panel">
        <div className="panel-head"><h3>Audit log</h3><span className="muted small grow">latest {audit.length} entries, newest first</span>
          <button onClick={load}>Refresh</button></div>
        <div className="table-wrap tall">
          <table className="compact" data-testid="audit">
            <thead><tr><th>When</th><th>Who</th><th>Action</th><th>Target</th><th>Detail</th></tr></thead>
            <tbody>{audit.map((a) => (
              <tr key={a.id}>
                <td>{new Date(a.at).toLocaleString()}</td><td className="mono">{a.actor}</td><td>{a.action.replace(/_/g, " ").toLowerCase()}</td>
                <td className="mono">{a.target ?? ""}</td>
                <td className="small mono">{Object.keys(a.detail).length ? JSON.stringify(a.detail) : ""}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
        {audit.length === 0 && <div className="empty">No entries yet.</div>}
      </div>
    </div>
  );
}

// ---- API keys (org admin) ---------------------------------------------------------------

function ApiKeys() {
  const [keys, setKeys] = useState<ApiKey[] | null>(null);
  const [missing, setMissing] = useState(false);
  const [label, setLabel] = useState("");
  const [fresh, setFresh] = useState<{ label: string; key: string } | null>(null);
  const [msg, setMsg] = useState("");
  const load = useCallback(() => {
    api.apiKeys().then(setKeys).catch((e) => { if (notFound(e)) setMissing(true); else setMsg((e as Error).message); });
  }, []);
  useEffect(load, [load]);
  if (missing) return <><h4>API keys</h4><p className="muted">This server does not manage per-organisation API keys yet.</p></>;
  return (
    <>
      <h4>API keys <span className="muted">for systems that send payments to this organisation</span></h4>
      {fresh && (
        <div className="notice" role="status">
          Key for <b>{fresh.label}</b>, shown only now: <code className="secret">{fresh.key}</code>{" "}
          <button onClick={() => navigator.clipboard?.writeText(fresh.key).then(() => setMsg("Copied."), () => setMsg("Copy failed."))}>Copy</button>{" "}
          <button className="ghost" onClick={() => setFresh(null)}>Done</button>
        </div>
      )}
      {msg && <p className="note">{msg}</p>}
      <table className="compact">
        <thead><tr><th>Label</th><th>Starts with</th><th>Created</th><th>Last used</th><th></th></tr></thead>
        <tbody>
          {(keys ?? []).map((k) => (
            <tr key={k.id}><td>{k.label}</td><td className="mono">{k.prefix}…</td><td>{new Date(k.created_at).toLocaleDateString()}</td>
              <td>{k.last_used_at ? new Date(k.last_used_at).toLocaleString() : <span className="muted">never</span>}</td>
              <td className="r"><button className="danger" onClick={() => {
                if (window.confirm(`Revoke the key "${k.label}"? Systems using it will be refused at once.`))
                  api.deleteApiKey(k.id).then(() => { setMsg(`Revoked ${k.label}.`); load(); }, (e) => setMsg((e as Error).message));
              }}>Revoke</button></td></tr>
          ))}
          {keys && !keys.length && <tr><td colSpan={5} className="muted">No keys yet.</td></tr>}
        </tbody>
      </table>
      <form className="actions" onSubmit={(e) => {
        e.preventDefault();
        api.createApiKey(label.trim()).then((k) => { setFresh({ label: k.label, key: k.key }); setLabel(""); load(); }, (err) => setMsg((err as Error).message));
      }}>
        <label className="inline">Label<input value={label} placeholder="core banking" onChange={(e) => setLabel(e.target.value)} /></label>
        <button type="submit" disabled={!label.trim()}>Create key</button>
      </form>
    </>
  );
}

// ---- platform (superadmin) ---------------------------------------------------------------

function Platform({ now }: { now: number }) {
  const [orgs, setOrgs] = useState<Org[] | null>(null);
  const [state, setState] = useState<"loading" | "ok" | "missing" | "error">("loading");
  const [msg, setMsg] = useState("");
  const [form, setForm] = useState({ org_id: "", name: "", region: "" });
  const load = useCallback(() => {
    api.orgs().then((o) => { setOrgs(o); setState("ok"); })
      .catch((e) => { setState(notFound(e) ? "missing" : "error"); setMsg((e as Error).message); });
  }, []);
  useEffect(() => { load(); const t = setInterval(load, 30_000); return () => clearInterval(t); }, [load]);
  const run = async (what: string, fn: () => Promise<unknown>) => {
    try { await fn(); setMsg(`${what}: done.`); } catch (e) { setMsg(`${what}: ${(e as Error).message}`); }
    load();
  };
  const slugOk = /^[a-z0-9][a-z0-9-]{1,38}[a-z0-9]$/.test(form.org_id);
  const totals = orgs?.reduce((a, o) => ({ txns: a.txns + o.transactions_24h, alerts: a.alerts + o.open_alerts, users: a.users + o.users,
                                         active: a.active + (o.status === "ACTIVE" ? 1 : 0) }), { txns: 0, alerts: 0, users: 0, active: 0 });
  if (state === "missing") return <div className="panel"><div className="empty">This server does not support organisations yet. Update the API to manage them here.</div></div>;
  return (
    <>
      {totals && (
        <div className="kpis">
          <Kpi label="Organisations" value={num(orgs!.length)} sub={`${totals.active} active`} />
          <Kpi label="Payments, 24 h" value={num(totals.txns)} sub="all organisations" />
          <Kpi label="Open alerts" value={num(totals.alerts)} sub="all organisations" />
          <Kpi label="People" value={num(totals.users)} sub="with a sign-in" />
        </div>
      )}
      <div className="panel">
        <div className="panel-head"><h3>Organisations</h3><span className="muted small grow">refreshed every 30 s</span><button onClick={load}>Refresh</button></div>
        {msg && <div className="notice" role="status">{msg}</div>}
        {state === "loading" && <div className="empty">Loading.</div>}
        {orgs && (
          <div className="table-wrap">
            <table data-testid="orgs">
              <thead><tr><th>Organisation</th><th>Id</th><th>Region</th><th>State</th><th className="r">People</th><th className="r">Payments, 24 h</th>
                <th className="r">Open alerts</th><th>Last payment</th><th></th></tr></thead>
              <tbody>
                {orgs.map((o) => (
                  <tr key={o.org_id}>
                    <td>{o.name}</td><td className="mono small">{o.org_id}</td><td>{o.region ?? <span className="muted">not set</span>}</td>
                    <td><span className={`state ${o.status === "ACTIVE" ? "ok" : "bad"}`}>{o.status.toLowerCase()}</span></td>
                    <td className="r">{num(o.users)}</td><td className="r">{num(o.transactions_24h)}</td><td className="r">{num(o.open_alerts)}</td>
                    <td>{ago(o.last_event_at, now)}</td>
                    <td className="r">
                      {o.status === "ACTIVE"
                        ? <button className="danger" onClick={() => { if (window.confirm(`Suspend ${o.name}? Its people are signed out and its API keys are refused until you reactivate it.`))
                            run(`Suspend ${o.name}`, () => api.updateOrg(o.org_id, { status: "SUSPENDED" })); }}>Suspend</button>
                        : <button onClick={() => run(`Reactivate ${o.name}`, () => api.updateOrg(o.org_id, { status: "ACTIVE" }))}>Reactivate</button>}
                    </td>
                  </tr>
                ))}
                {!orgs.length && <tr><td colSpan={9} className="muted">No organisations yet.</td></tr>}
              </tbody>
            </table>
          </div>
        )}
      </div>
      <div className="panel narrow">
        <div className="panel-head"><h3>Add an organisation</h3></div>
        <form className="form" onSubmit={(e) => { e.preventDefault(); run(`Create ${form.name}`, async () => {
          await api.createOrg(form.org_id, form.name.trim(), form.region.trim()); setForm({ org_id: "", name: "", region: "" }); }); }}>
          <label>Name<input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="Example Cooperative Bank" /></label>
          <label>Id<input value={form.org_id} onChange={(e) => setForm({ ...form, org_id: e.target.value.toLowerCase() })} placeholder="example-coop" /></label>
          <label>Region<input value={form.region} onChange={(e) => setForm({ ...form, region: e.target.value })} placeholder="Maharashtra" /></label>
          <div className="actions"><button className="primary" type="submit" disabled={!slugOk || !form.name.trim()}>Create</button>
            {form.org_id && !slugOk && <span className="muted small">Id: 3 to 40 lowercase letters, digits or hyphens.</span>}</div>
        </form>
        <p className="muted">Then add the organisation's first administrator under People and audit, with this organisation id.</p>
      </div>
    </>
  );
}

// ---- customer portal ----------------------------------------------------------------------

const TXN_KIND: Record<string, string> = {
  TRANSFER: "Transfer", PAYMENT: "Payment", CASH_OUT: "Cash withdrawal", DEBIT: "Debit", CASH_IN: "Deposit",
};
const REPORT_STATUS: Record<string, string> = {
  UNDER_REVIEW: "Being checked by the bank", CONFIRMED_FRAUD: "Confirmed as fraud", CLOSED_NO_FRAUD: "Closed: no fraud found",
};
const TXN_STATUS: Record<CustomerTxn["status"], string> = { OK: "Completed", UNDER_REVIEW: "Being checked", CONFIRMED_FRAUD: "Stopped as fraud" };
const REPORT_KIND: Record<ReportKind, string> = { NOT_ME: "I did not make this payment", SCAM: "I was tricked into paying", OTHER: "Something else" };

function CustomerPortal({ me, onSignOut }: { me: Me; onSignOut: () => void }) {
  const [txns, setTxns] = useState<CustomerTxn[] | null>(null);
  const [reports, setReports] = useState<CustomerReport[]>([]);
  const [state, setState] = useState<"loading" | "ok" | "missing" | "error">("loading");
  const [err, setErr] = useState("");
  const [open, setOpen] = useState<CustomerTxn | null>(null);
  const [kind, setKind] = useState<ReportKind>("NOT_ME");
  const [note, setNote] = useState("");
  const [sent, setSent] = useState("");
  const [tip] = useState(() => BANNER_TIPS[Math.floor(Math.random() * BANNER_TIPS.length)]);
  const load = useCallback(() => {
    Promise.all([api.myTransactions(50), api.myReports().catch(() => [] as CustomerReport[])])
      .then(([t, r]) => { setTxns(t); setReports(r); setState("ok"); })
      .catch((e) => { setState(notFound(e) ? "missing" : "error"); setErr((e as Error).message); });
  }, []);
  useEffect(() => { load(); const t = setInterval(load, 30_000); return () => clearInterval(t); }, [load]);
  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!open) return;
    try { await api.report(open.transaction_id, kind, note); setSent(`Reported. The fraud team at ${me.org_name ?? "your bank"} will look at it.`); setOpen(null); setNote(""); load(); }
    catch (e2) { setSent(`Not sent: ${(e2 as Error).message}`); }
  };
  return (
    <div className="portal">
      <div className="aware-strip"><b>Stay safe.</b> {tip} <a href={`tel:${HELPLINE}`}>Helpline {HELPLINE}</a></div>
      <header className="portal-head glass">
        <a className="signin-brand" href="/" onClick={onNav("/")}><span className="mark" aria-hidden>F</span> FORESENTRY</a>
        <span className="muted">{me.org_name ?? ""}</span>
        <span className="grow" />
        <span className="who-text right"><b>{me.username}</b><span className="muted">account holder</span></span>
        <button className="ghost" onClick={onSignOut}>Sign out</button>
      </header>
      <main className="portal-main">
        <div className="panel">
          <div className="panel-head"><h3>Your recent payments</h3><span className="muted small grow">from the accounts linked to your sign-in</span></div>
          {sent && <div className="notice" role="status">{sent}</div>}
          {state === "loading" && <div className="empty">Loading.</div>}
          {state === "missing" && <div className="empty">This server does not offer the account-holder view yet.</div>}
          {state === "error" && <div className="error">{err}</div>}
          {txns && (
            <div className="table-wrap">
              <table data-testid="my-payments">
                <thead><tr><th>When</th><th>To</th><th>Type</th><th className="r">Amount</th><th>Status</th><th></th></tr></thead>
                <tbody>
                  {txns.map((t) => (
                    <tr key={t.transaction_id}>
                      <td>{new Date(t.event_time).toLocaleString()}</td><td className="mono">{t.counterparty_id}</td><td>{TXN_KIND[t.txn_type] ?? t.txn_type}</td>
                      <td className="r">{inr(t.amount)}</td>
                      <td><span className={`status ${t.status === "OK" ? "s-OPEN" : t.status === "CONFIRMED_FRAUD" ? "s-CONFIRMED_FRAUD" : "s-IN_REVIEW"}`}>{TXN_STATUS[t.status]}</span></td>
                      <td className="r">{t.reported ? <span className="muted small">reported</span>
                        : <button onClick={() => { setOpen(t); setSent(""); }}>Report a problem</button>}</td>
                    </tr>
                  ))}
                  {!txns.length && <tr><td colSpan={6} className="muted">No payments yet.</td></tr>}
                </tbody>
              </table>
            </div>
          )}
          <p className="note">"Being checked" means your bank's fraud team is looking at the payment. Your bank may contact you; it will never ask for your OTP or PIN.</p>
        </div>

        {open && (
          <form className="panel report" onSubmit={submit}>
            <div className="panel-head"><h3>Report a problem with this payment</h3><button type="button" className="ghost" onClick={() => setOpen(null)}>Cancel</button></div>
            <p>{inr(open.amount)} to <span className="mono">{open.counterparty_id}</span> on {new Date(open.event_time).toLocaleString()}</p>
            <fieldset className="choices">
              {(Object.keys(REPORT_KIND) as ReportKind[]).map((k) => (
                <label key={k} className="choice"><input type="radio" name="kind" checked={kind === k} onChange={() => setKind(k)} /> {REPORT_KIND[k]}</label>
              ))}
            </fieldset>
            <label>What happened (optional)<textarea rows={3} value={note} onChange={(e) => setNote(e.target.value)} maxLength={1000} /></label>
            <div className="actions"><button className="primary" type="submit">Send report</button></div>
            <p className="note">If money left your account because of a scam, also call {HELPLINE} now. Reporting quickly gives the best chance to stop the money.</p>
          </form>
        )}

        <div className="grid2">
          <div className="panel">
            <div className="panel-head"><h3>Your reports</h3></div>
            <table className="compact">
              <tbody>
                {reports.map((r, i) => (
                  <tr key={r.report_id ?? i}><td>{new Date(r.created_at).toLocaleString()}</td><td className="mono">{r.transaction_id}</td>
                    <td>{REPORT_KIND[r.kind]}</td><td>{REPORT_STATUS[r.status ?? ""] ?? "Received"}</td></tr>
                ))}
                {!reports.length && <tr><td className="muted">You have not reported anything.</td></tr>}
              </tbody>
            </table>
          </div>
          <div className="panel">
            <div className="panel-head"><h3>Lost money to a scam?</h3></div>
            <ol className="steps">
              <li><b>Call {HELPLINE}</b>, the national cyber-crime helpline.</li>
              <li><b>Call your bank</b> on the number on your card to block the card or UPI.</li>
              <li><b>Report online</b> at <a href={REPORT_URL} target="_blank" rel="noreferrer">cybercrime.gov.in</a>.</li>
            </ol>
            <p className="note"><a href="/learn/after-fraud" onClick={onNav("/learn/after-fraud")}>What to do after a fraud</a> ·{" "}
              <a href="/#learn" onClick={onNav("/")}>Scam awareness guides</a></p>
          </div>
        </div>
      </main>
    </div>
  );
}

// ---- security (own two-step verification) -----------------------------------------------

function Security({ me }: { me: Me }) {
  const [state, setState] = useState<"loading" | "missing" | "off" | "setup" | "codes" | "on">("loading");
  const [setup, setSetup] = useState<{ secret: string; uri: string; qr: string } | null>(null);
  const [codes, setCodes] = useState<string[]>([]);
  const [code, setCode] = useState("");
  const [password, setPassword] = useState("");
  const [msg, setMsg] = useState("");
  const refresh = useCallback(() => {
    api.mfaStatus().then((s) => setState(s.enabled ? "on" : "off")).catch((e) => { setState(notFound(e) ? "missing" : "off"); if (!notFound(e)) setMsg((e as Error).message); });
  }, []);
  useEffect(refresh, [refresh]);
  const begin = async () => {
    setMsg("");
    try {
      const s = await api.mfaSetup();
      const qr = await QRCode.toDataURL(s.otpauth_uri, { margin: 1, width: 192, color: { dark: "#111418", light: "#ffffff" } });
      setSetup({ secret: s.secret, uri: s.otpauth_uri, qr }); setCode(""); setState("setup");
    } catch (e) { setMsg((e as Error).message); }
  };
  const enable = async (e: React.FormEvent) => {
    e.preventDefault(); setMsg("");
    try { const r = await api.mfaEnable(code.trim()); setCodes(r.recovery_codes); setSetup(null); setCode(""); setState("codes"); }
    catch (err) { setMsg((err as Error).message.startsWith("4") ? "That code was not accepted. Check the time on your phone and try the next code." : (err as Error).message); }
  };
  const disable = async (e: React.FormEvent) => {
    e.preventDefault(); setMsg("");
    try { await api.mfaDisable(password, code.trim()); setPassword(""); setCode(""); setState("off"); setMsg("Two-step verification is off."); }
    catch (err) { setMsg((err as Error).message.startsWith("4") ? "Password or code not accepted." : (err as Error).message); }
  };
  return (
    <div className="panel narrow">
      <div className="panel-head"><h3>Two-step verification</h3><span className="muted small">for {me.username}</span></div>
      {state === "loading" && <div className="empty">Loading.</div>}
      {state === "missing" && <div className="empty">This server does not support two-step verification yet. Update the API to enable it.</div>}
      {state === "off" && (
        <>
          <p>Off. Signing in needs only your password.</p>
          <p className="muted">With it on, signing in also needs a 6-digit code from an authenticator app such as Google Authenticator, Microsoft Authenticator or 1Password.</p>
          <div className="actions"><button className="primary" onClick={begin}>Set up</button></div>
        </>
      )}
      {state === "setup" && setup && (
        <form onSubmit={enable}>
          <ol className="steps">
            <li>Scan this code with your authenticator app.
              <div className="qr"><img src={setup.qr} width={192} height={192} alt="QR code for your authenticator app" /></div>
              <span className="muted">Cannot scan it? Enter this key instead: </span><code className="secret">{setup.secret}</code>
            </li>
            <li>Type the 6-digit code the app shows.
              <label className="inline">Code<input autoFocus inputMode="numeric" autoComplete="one-time-code" value={code} onChange={(e) => setCode(e.target.value)} /></label>
            </li>
          </ol>
          <div className="actions">
            <button className="primary" type="submit" disabled={code.trim().length < 6}>Turn on</button>
            <button type="button" onClick={() => { setSetup(null); setState("off"); }}>Cancel</button>
          </div>
        </form>
      )}
      {state === "codes" && (
        <>
          <p><b>On.</b> Save these recovery codes somewhere safe. Each one signs you in once if you lose your phone. They are shown only now.</p>
          <ul className="codes mono">{codes.map((c) => <li key={c}>{c}</li>)}</ul>
          <div className="actions">
            <button onClick={() => { navigator.clipboard?.writeText(codes.join("\n")).then(() => setMsg("Copied."), () => setMsg("Copy failed; select and copy them by hand.")); }}>Copy codes</button>
            <button className="primary" onClick={() => { setCodes([]); setState("on"); }}>I have saved them</button>
          </div>
        </>
      )}
      {state === "on" && (
        <form onSubmit={disable}>
          <p><b>On.</b> Signing in needs your password and a code from your authenticator app.</p>
          <h4>Turn off</h4>
          <div className="form">
            <label>Password<input type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} /></label>
            <label>Current code<input inputMode="numeric" autoComplete="one-time-code" value={code} onChange={(e) => setCode(e.target.value)} /></label>
          </div>
          <div className="actions"><button className="danger" type="submit" disabled={!password || code.trim().length < 6}>Turn off</button></div>
        </form>
      )}
      {msg && <p className="notice" role="status">{msg}</p>}
    </div>
  );
}

// ---- transaction drawer ---------------------------------------------------------------

function TxnDrawer({ id, onClose }: { id: string; onClose: () => void }) {
  const [t, setT] = useState<TxnDetail | null>(null);
  const [err, setErr] = useState("");
  useEffect(() => { setT(null); api.transaction(id).then(setT).catch((e) => setErr(String(e.message))); }, [id]);
  useEffect(() => {
    const k = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [onClose]);
  return (
    <aside className="drawer glass" aria-label="Transaction detail">
      <div className="panel-head"><h3>Transaction</h3><button onClick={onClose}>Close</button></div>
      {err && <div className="error">{err}</div>}
      {t && (
        <>
          <p className="mono">{t.transaction_id}</p>
          <p><SourceBadge source={t.source} /> <LevelBadge level={t.risk_level} /> <span className={`decision d-${t.decision}`}>{t.decision}</span></p>
          <ScoreRing score={t.risk_score} level={t.risk_level} />
          <table className="compact"><tbody>
            <tr><td>Risk score</td><td className="r">{t.risk_score.toFixed(6)}</td></tr>
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
