// REST client for the VYROX API. People sign in and the browser holds a session
// token for the tab's lifetime (sessionStorage). No API key is shipped to the browser.
export const BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

export type Role = "VIEWER" | "ANALYST" | "ADMIN" | "SUPERADMIN" | "CUSTOMER";
/** SUPERADMIN runs the platform and belongs to no organisation; CUSTOMER is an account holder of one organisation. */
export interface Me { username: string; role: Role; org_id?: string | null; org_name?: string | null }

const TOKEN_KEY = "vyrox-session";
let token: string | null = null;
try { token = sessionStorage.getItem(TOKEN_KEY); } catch { /* storage blocked: stay signed out */ }
const expiryListeners = new Set<() => void>();
export const session = {
  has: () => token !== null,
  set(t: string | null) {
    token = t;
    try { if (t) sessionStorage.setItem(TOKEN_KEY, t); else sessionStorage.removeItem(TOKEN_KEY); } catch { /* ignore */ }
  },
  /** Called when the server says the session is no longer valid. */
  expired() { session.set(null); expiryListeners.forEach((f) => f()); },
  onExpired(f: () => void) { expiryListeners.add(f); return () => { expiryListeners.delete(f); }; },
};
export const authHeaders = (): Record<string, string> => (token ? { Authorization: `Bearer ${token}` } : {});

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = { ...authHeaders(), ...((init?.headers as Record<string, string>) ?? {}) };
  if (typeof init?.body === "string") headers["Content-Type"] = "application/json";
  const res = await fetch(`${BASE}${path}`, { ...init, headers });
  if (res.status === 401 && !path.startsWith("/v1/auth/login")) session.expired();
  if (!res.ok) {
    let detail = await res.text();
    try { const d = JSON.parse(detail).detail; detail = typeof d === "string" ? d : JSON.stringify(d); } catch { /* keep text */ }
    throw new Error(`${res.status}: ${detail}`);
  }
  return (res.status === 204 ? undefined : res.json()) as Promise<T>;
}

export type Source = "LIVE_API" | "LIVE_WEBHOOK" | "KAFKA_STREAM" | "HISTORICAL_IMPORT" | "SIMULATED_DEMO";
export const SOURCE_LABEL: Record<Source, string> = {
  LIVE_API: "LIVE API", LIVE_WEBHOOK: "LIVE WEBHOOK", KAFKA_STREAM: "KAFKA STREAM",
  HISTORICAL_IMPORT: "HISTORICAL IMPORT", SIMULATED_DEMO: "SIMULATED DEMO",
};
export type RiskLevel = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
export const LEVELS: RiskLevel[] = ["LOW", "MEDIUM", "HIGH", "CRITICAL"];
export type AlertStatus = "OPEN" | "IN_REVIEW" | "CONFIRMED_FRAUD" | "FALSE_POSITIVE" | "DISMISSED";
export type StreamStatus = "LIVE" | "IDLE" | "DELAYED" | "DISCONNECTED";

export interface Txn {
  transaction_id: string; source: Source; account_id: string; counterparty_id: string; txn_type: string;
  amount: number; region: string | null; event_time: string; ingested_at: string; scored_at: string;
  persisted_at: string; risk_score: number; risk_level: RiskLevel; decision: "ALLOW" | "REVIEW" | "HOLD";
  reason_codes: string[]; model_version: string; inference_ms: number | null; pipeline_ms: number | null;
}
export interface TxnDetail extends Txn {
  features: Record<string, number>; feature_ms: number | null; alert_id: number | null;
  alert_status: AlertStatus | null; label: boolean | null; label_source: string | null;
}
export interface Alert {
  alert_id: number; account_id: string; first_transaction: string; priority: number; status: AlertStatus;
  trigger_types: string[]; reason_codes: string[]; max_risk_score: number; event_count: number;
  total_amount: number; assigned_to: string | null; escalation_level: number; sla_due_at: string;
  created_at: string; updated_at: string; notes: string | null; source?: Source; sla_breached?: boolean;
}
export interface AlertDetail extends Alert {
  transactions: (Pick<Txn, "transaction_id" | "source" | "txn_type" | "amount" | "counterparty_id" | "event_time"
    | "risk_score" | "risk_level" | "reason_codes"> & { trigger_types: string[] })[];
  events: { kind: string; detail: Record<string, unknown>; actor: string; at: string }[];
}
export interface Pct { p50: number; p95: number; p99: number; samples?: number }
export interface Performance {
  window_minutes: number; samples: number; throughput_tps: number | null; pipeline_ms: Pct | null;
  inference_ms: Pct | null; feature_ms: Pct | null; ingest_ms: Pct | null; push_ms: Pct | null;
  consumer_lag: number | null; spark_backlog: number | null; events_received: number | null;
  events_dead_lettered: number | null; error_rate: number | null; measured_at: string;
}
export interface Stats {
  source_filter: Source | null; processed: number; tps_last_minute: number; total_volume: number;
  high_risk: number; critical: number; avg_risk_score: number; last_event_at: string | null;
  last_inference_at: string | null; risk_levels: Partial<Record<RiskLevel, number>>;
  trend: { minute: string; txns: number; high_risk: number; volume: number }[];
  by_source: { source: Source; txns: number; volume: number; high_risk: number; last_event_at: string; last_minute: number }[];
  alerts: { total: number; open: number; in_review: number; confirmed: number; false_positive: number; dismissed?: number; escalated: number; sla_breached: number };
  label_quality: { labelled: number; tp: number; fp: number; fn: number };
  performance: Performance;
}
export interface Status {
  environment: "demo" | "production"; pipeline_mode: "spark" | "direct"; server_time: string;
  stream_status: StreamStatus; reasons: string[]; kafka_ok: boolean; scorer_alive: boolean;
  model_version: string | null; duplicates_skipped: number | null; consumer_lag: number | null;
  spark_backlog: number | null; last_event_at: string | null; last_inference_at: string | null;
  seconds_since_last_event: number | null; active_sources: Source[]; dashboard_clients: number;
  /** Present when the server runs as more than one process; absent on older servers. */
  node?: string;
  nodes?: { id: string; kind: "api" | "scorer"; alive: boolean; last_seen: string | null; partitions?: number | null }[];
}
export interface User {
  username: string; role: Role; active: boolean; created_at?: string; updated_at?: string;
  mfa_enabled?: boolean; sso?: boolean; org_id?: string | null; account_ids?: string[];
}
export interface Connector {
  name: string; kind: string; path: string; enabled: boolean; balance_known: boolean; last_event_at: string | null;
  events_24h: number; last_error: string | null; last_error_at: string | null;
}
export interface Connectors { connectors: Connector[]; alert_webhook_configured: boolean }
export interface ScoreResult {
  transaction_id: string; risk_score: number; risk_level: RiskLevel; decision: "ALLOW" | "REVIEW" | "HOLD";
  reason_codes: string[]; model_version: string; scored_at: string; latency_ms: number | null; disclaimer?: string;
}
/** A sign-in either finishes or asks for a second factor. */
export type LoginResult = { kind: "done"; me: Me } | { kind: "mfa"; mfaToken: string; expiresIn: number };
export interface AuditRow {
  id: number; at: string; actor: string; role: string; action: string; target: string | null;
  detail: Record<string, unknown>; ip: string | null;
}
export interface Policy {
  thresholds: { medium: number; high: number; critical: number };
  alerts: { score_alert_level: RiskLevel; rule_triggers: string[]; rule_min_level: RiskLevel; dedup_minutes: number;
    sla_minutes: Record<string, number>; max_escalations: number; analysts: string[]; supervisor: string };
}

const q = (source: Source | null) => (source ? `source=${source}` : "");
export type DriftStatus = "STABLE" | "WATCH" | "DRIFTED" | "NEEDS_24H" | "NOT_ENOUGH_DATA" | "NO_REFERENCE";
export interface Drift {
  model_version: string; measured_at: string; status: DriftStatus;
  rows?: number; min_rows?: number; span_hours?: number; reference_rows?: number; reference_data?: string;
  watch_above?: number; drifted_above?: number;
  features: { feature: string; psi: number; status: DriftStatus }[];
}

export interface Org {
  org_id: string; name: string; region: string | null; status: "ACTIVE" | "SUSPENDED"; created_at: string;
  users: number; transactions_24h: number; open_alerts: number; last_event_at: string | null;
}
export interface ApiKey { id: string; label: string; prefix: string; created_at: string; last_used_at: string | null; created_by?: string }
interface ServerKey {
  key_id: string; name: string; prefix: string; created_at: string; last_used_at?: string | null;
  created_by?: string; revoked_at?: string | null;
}
const toKey = (k: ServerKey): ApiKey => ({
  id: k.key_id, label: k.name, prefix: k.prefix, created_at: k.created_at,
  last_used_at: k.last_used_at ?? null, created_by: k.created_by,
});
export interface CustomerTxn {
  transaction_id: string; event_time: string; counterparty_id: string; amount: number; txn_type: string;
  status: "OK" | "UNDER_REVIEW" | "CONFIRMED_FRAUD"; reported: boolean;
}
export type ReportKind = "NOT_ME" | "SCAM" | "OTHER";
export interface CustomerReport { report_id?: number; transaction_id: string; kind: ReportKind; note?: string | null; created_at: string; status?: string }
/** Public, no sign-in. `simulated` is true on demonstration servers. */
export interface PublicSummary {
  environment: string; simulated: boolean; transactions_24h: number; flagged_share_24h: number;
  p50_pipeline_ms: number | null; organisations: number; measured_at: string;
}

export const api = {
  publicSummary: () => call<PublicSummary>("/v1/public/summary"),
  orgs: () => call<Org[]>("/v1/orgs"),
  createOrg: (org_id: string, name: string, region: string) =>
    call<Org>("/v1/orgs", { method: "POST", body: JSON.stringify({ org_id, name, region }) }),
  updateOrg: (org_id: string, body: { name?: string; region?: string; status?: "ACTIVE" | "SUSPENDED" }) =>
    call<Org>(`/v1/orgs/${encodeURIComponent(org_id)}`, { method: "PATCH", body: JSON.stringify(body) }),
  // The server names these key_id and name; revoked keys are listed by the server but not shown here.
  apiKeys: () => call<ServerKey[]>("/v1/api-keys").then((ks) => ks.filter((k) => !k.revoked_at).map(toKey)),
  createApiKey: (label: string) =>
    call<ServerKey & { key: string }>("/v1/api-keys", { method: "POST", body: JSON.stringify({ name: label }) })
      .then((k) => ({ ...toKey(k), key: k.key })),
  deleteApiKey: (id: string) => call<void>(`/v1/api-keys/${encodeURIComponent(id)}`, { method: "DELETE" }),
  myTransactions: (limit = 50) => call<CustomerTxn[]>(`/v1/me/transactions?limit=${limit}`),
  myReports: () => call<CustomerReport[]>("/v1/me/reports"),
  report: (transaction_id: string, kind: ReportKind, note: string) =>
    call<CustomerReport>("/v1/me/reports", { method: "POST", body: JSON.stringify({ transaction_id, kind, ...(note.trim() ? { note: note.trim() } : {}) }) }),
  login: async (username: string, password: string): Promise<LoginResult> => {
    const r = await call<(Me & { token: string }) | { mfa_required: true; mfa_token: string; expires_in: number }>(
      "/v1/auth/login", { method: "POST", body: JSON.stringify({ username, password }) });
    if ("mfa_required" in r) return { kind: "mfa", mfaToken: r.mfa_token, expiresIn: r.expires_in };
    session.set(r.token);
    return { kind: "done", me: { username: r.username, role: r.role } };
  },
  loginMfa: async (mfaToken: string, code: string): Promise<Me> => {
    const r = await call<Me & { token: string }>("/v1/auth/login/mfa", { method: "POST", body: JSON.stringify({ mfa_token: mfaToken, code }) });
    session.set(r.token);
    return { username: r.username, role: r.role };
  },
  /** Single sign-on, if the server has an identity provider configured. Older servers answer 404. */
  sso: () => call<{ enabled: boolean; provider_name: string | null }>("/v1/auth/sso"),
  ssoStartUrl: () => `${BASE}/v1/auth/sso/start`,
  mfaStatus: () => call<{ enabled: boolean }>("/v1/auth/mfa"),
  mfaSetup: () => call<{ secret: string; otpauth_uri: string }>("/v1/auth/mfa/setup", { method: "POST" }),
  mfaEnable: (code: string) => call<{ recovery_codes: string[] }>("/v1/auth/mfa/enable", { method: "POST", body: JSON.stringify({ code }) }),
  mfaDisable: (password: string, code: string) =>
    call<void>("/v1/auth/mfa/disable", { method: "POST", body: JSON.stringify({ password, code }) }),
  status: () => call<Status>("/v1/status"),
  connectors: () => call<Connectors>("/v1/connectors"),
  /** Dry run: the server scores the event with current features and stores nothing. */
  score: (event: Record<string, unknown>) => call<ScoreResult>("/v1/score", { method: "POST", body: JSON.stringify(event) }),
  logout: async () => { try { await call<void>("/v1/auth/logout", { method: "POST" }); } finally { session.set(null); } },
  me: () => call<Me>("/v1/auth/me"),
  users: () => call<User[]>("/v1/users"),
  /** org_id only from a SUPERADMIN; account_ids only for CUSTOMER users. */
  createUser: (username: string, password: string, role: Role, extra: { org_id?: string; account_ids?: string[] } = {}) =>
    call<User>("/v1/users", { method: "POST", body: JSON.stringify({ username, password, role, ...extra }) }),
  changeUser: (username: string, body: { role?: Role; active?: boolean; password?: string; reset_mfa?: boolean }) =>
    call<User>(`/v1/users/${encodeURIComponent(username)}`, { method: "PATCH", body: JSON.stringify(body) }),
  audit: () => call<AuditRow[]>("/v1/audit?limit=200"),
  drift: () => call<Drift>("/v1/model/drift"),
  stats: (source: Source | null) => call<Stats>(`/v1/stats?${q(source)}`),
  recent: (limit: number, source: Source | null) => call<Txn[]>(`/v1/transactions/recent?limit=${limit}&${q(source)}`),
  transaction: (id: string) => call<TxnDetail>(`/v1/transactions/${encodeURIComponent(id)}`),
  alerts: () => call<Alert[]>("/v1/alerts?limit=200"),
  alert: (id: number) => call<AlertDetail>(`/v1/alerts/${id}`),
  dismissStale: (older_than_hours: number) =>
    call<{ closed: number }>("/v1/alerts/dismiss-stale", { method: "POST", body: JSON.stringify({ older_than_hours }) }),
  updateAlert: (id: number, body: { status?: AlertStatus; assigned_to?: string }) =>
    call<Alert>(`/v1/alerts/${id}`, { method: "PATCH", body: JSON.stringify(body) }),
  config: () => call<Policy>("/v1/config"),
  saveConfig: (p: Policy) => call<Policy>("/v1/config", { method: "PUT", body: JSON.stringify(p) }),
  scenario: (scenario: string) =>
    call<{ transaction_ids: string[]; account_id: string }>("/v1/demo/scenario", { method: "POST", body: JSON.stringify({ scenario }) }),
  ingest: (event: Record<string, unknown>) =>
    call<{ transaction_id: string; status: string; source: Source }>("/v1/transactions", { method: "POST", body: JSON.stringify(event) }),
  upload: (file: File) => {
    const form = new FormData();
    form.append("file", file);
    return call<{ rows: number; accepted: number; duplicates: number; invalid: number; first_errors: { row: number; field: string; error: string }[] }>(
      "/v1/imports", { method: "POST", body: form });
  },
};
