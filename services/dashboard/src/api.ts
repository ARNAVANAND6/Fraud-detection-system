// REST client for the VYROX API. The API key here is a development key only:
// anything shipped to a browser is public. Production would use analyst
// sign-in (OIDC) and short-lived tokens instead.
export const BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";
export const KEY = import.meta.env.VITE_API_KEY ?? "dev-key";

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = { "X-API-Key": KEY, ...((init?.headers as Record<string, string>) ?? {}) };
  if (typeof init?.body === "string") headers["Content-Type"] = "application/json";
  const res = await fetch(`${BASE}${path}`, { ...init, headers });
  if (!res.ok) {
    let detail = await res.text();
    try { detail = JSON.stringify(JSON.parse(detail).detail); } catch { /* keep text */ }
    throw new Error(`${res.status}: ${detail}`);
  }
  return res.json() as Promise<T>;
}

export type Source = "LIVE_API" | "LIVE_WEBHOOK" | "KAFKA_STREAM" | "HISTORICAL_IMPORT" | "SIMULATED_DEMO";
export const SOURCE_LABEL: Record<Source, string> = {
  LIVE_API: "LIVE API", LIVE_WEBHOOK: "LIVE WEBHOOK", KAFKA_STREAM: "KAFKA STREAM",
  HISTORICAL_IMPORT: "HISTORICAL IMPORT", SIMULATED_DEMO: "SIMULATED DEMO",
};
export type RiskLevel = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
export const LEVELS: RiskLevel[] = ["LOW", "MEDIUM", "HIGH", "CRITICAL"];
export type AlertStatus = "OPEN" | "IN_REVIEW" | "CONFIRMED_FRAUD" | "FALSE_POSITIVE";
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
  alerts: { total: number; open: number; in_review: number; confirmed: number; false_positive: number; escalated: number; sla_breached: number };
  label_quality: { labelled: number; tp: number; fp: number; fn: number };
  performance: Performance;
}
export interface Status {
  environment: "demo" | "production"; pipeline_mode: "spark" | "direct"; server_time: string;
  stream_status: StreamStatus; reasons: string[]; kafka_ok: boolean; scorer_alive: boolean;
  model_version: string | null; duplicates_skipped: number | null; consumer_lag: number | null;
  spark_backlog: number | null; last_event_at: string | null; last_inference_at: string | null;
  seconds_since_last_event: number | null; active_sources: Source[]; dashboard_clients: number;
}
export interface Policy {
  thresholds: { medium: number; high: number; critical: number };
  alerts: { score_alert_level: RiskLevel; rule_triggers: string[]; rule_min_level: RiskLevel; dedup_minutes: number;
    sla_minutes: Record<string, number>; max_escalations: number; analysts: string[]; supervisor: string };
}

const q = (source: Source | null) => (source ? `source=${source}` : "");
export const api = {
  stats: (source: Source | null) => call<Stats>(`/v1/stats?${q(source)}`),
  recent: (limit: number, source: Source | null) => call<Txn[]>(`/v1/transactions/recent?limit=${limit}&${q(source)}`),
  transaction: (id: string) => call<TxnDetail>(`/v1/transactions/${encodeURIComponent(id)}`),
  alerts: () => call<Alert[]>("/v1/alerts?limit=200"),
  alert: (id: number) => call<AlertDetail>(`/v1/alerts/${id}`),
  updateAlert: (id: number, body: { status?: AlertStatus; assigned_to?: string }) =>
    call<Alert>(`/v1/alerts/${id}`, { method: "PATCH", body: JSON.stringify({ ...body, actor: "dashboard-user" }) }),
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
