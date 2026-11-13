// Small SVG charts. Marks follow one rule set: thin bars with rounded data
// ends, 2px lines, recessive grid, text in ink colours (never the series
// colour), and a hover tooltip on every plot.
import { useState } from "react";
import { LEVELS, RiskLevel } from "./api";

export const LEVEL_COLOR: Record<RiskLevel, string> = {
  LOW: "var(--good)", MEDIUM: "var(--warning)", HIGH: "var(--serious)", CRITICAL: "var(--critical)",
};
export const LEVEL_ICON: Record<RiskLevel, string> = { LOW: "●", MEDIUM: "▲", HIGH: "◆", CRITICAL: "■" };
const fmt = (n: number) => n.toLocaleString("en-IN");

/** Share of transactions per risk level. One bar per level, labelled with count and share. */
export function RiskDistribution({ levels }: { levels: Partial<Record<RiskLevel, number>> }) {
  const total = LEVELS.reduce((a, l) => a + (levels[l] ?? 0), 0);
  if (!total) return <div className="empty">No scored transactions yet.</div>;
  // Square-root scale: LOW is usually above 98%, which would flatten the other bars on a linear scale.
  const max = Math.sqrt(Math.max(...LEVELS.map((l) => levels[l] ?? 0)));
  return (
    <div className="dist" role="img" aria-label="Transactions by risk level">
      {LEVELS.map((l) => {
        const n = levels[l] ?? 0;
        return (
          <div className="dist-row" key={l} title={`${l}: ${fmt(n)} transactions (${((100 * n) / total).toFixed(2)}%)`}>
            <span className="dist-label"><i style={{ color: LEVEL_COLOR[l] }}>{LEVEL_ICON[l]}</i> {l}</span>
            <span className="dist-track">
              <span className="dist-bar" style={{ width: `${n ? Math.max(1, (100 * Math.sqrt(n)) / max) : 0}%`, background: LEVEL_COLOR[l] }} />
            </span>
            <span className="dist-value">{fmt(n)} <span className="muted">{((100 * n) / total).toFixed(n && n / total < 0.01 ? 2 : 1)}%</span></span>
          </div>
        );
      })}
      <div className="note">Bar length uses a square-root scale so small classes stay visible. Read the numbers for exact shares.</div>
    </div>
  );
}

interface Point { minute: string; value: number }

/** One measure over time, one axis. Hover shows the minute and the value. */
export function MinuteSeries({ points, label, color, kind }: { points: Point[]; label: string; color: string; kind: "line" | "bars" }) {
  const [hover, setHover] = useState<number | null>(null);
  if (points.length === 0) return <div className="empty">No stored events in the last 30 minutes.</div>;
  const W = 600, H = 120, L = 40, R = 8, T = 8, B = 20;
  const max = Math.max(1, ...points.map((p) => p.value));
  const t0 = Date.parse(points[0].minute), t1 = Math.max(Date.parse(points[points.length - 1].minute), t0 + 60_000);
  const x = (m: string) => L + ((Date.parse(m) - t0) / (t1 - t0)) * (W - L - R);
  const y = (v: number) => T + (1 - v / max) * (H - T - B);
  const hhmm = (m: string) => new Date(m).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  const bw = Math.max(3, Math.min(14, (W - L - R) / Math.max(points.length, 1) - 2));
  const h = hover !== null ? points[hover] : null;
  return (
    <div className="series">
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={label}
           onMouseLeave={() => setHover(null)}
           onMouseMove={(e) => {
             const box = e.currentTarget.getBoundingClientRect();
             const px = ((e.clientX - box.left) / box.width) * W;
             let best = 0;
             points.forEach((p, i) => { if (Math.abs(x(p.minute) - px) < Math.abs(x(points[best].minute) - px)) best = i; });
             setHover(best);
           }}>
        {[0, 0.5, 1].map((f) => (
          <g key={f}>
            <line x1={L} x2={W - R} y1={y(max * f)} y2={y(max * f)} className={f === 0 ? "axis" : "grid"} />
            <text x={L - 6} y={y(max * f) + 3} textAnchor="end" className="tick">{fmt(Math.round(max * f))}</text>
          </g>
        ))}
        <text x={L} y={H - 4} className="tick">{hhmm(points[0].minute)}</text>
        <text x={W - R} y={H - 4} textAnchor="end" className="tick">{hhmm(points[points.length - 1].minute)}</text>
        {kind === "line" ? (
          <>
            <polyline fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round"
                      points={points.map((p) => `${x(p.minute)},${y(p.value)}`).join(" ")} />
            {points.length === 1 && <circle cx={x(points[0].minute)} cy={y(points[0].value)} r="4" fill={color} />}
          </>
        ) : points.map((p) => p.value > 0 && (
          <rect key={p.minute} x={x(p.minute) - bw / 2} width={bw} y={y(p.value)} height={y(0) - y(p.value)} rx="2" fill={color} />
        ))}
        {h && (
          <>
            <line x1={x(h.minute)} x2={x(h.minute)} y1={T} y2={H - B} className="cross" />
            {kind === "line" && <circle cx={x(h.minute)} cy={y(h.value)} r="4" fill={color} stroke="var(--surface)" strokeWidth="2" />}
          </>
        )}
      </svg>
      <div className="series-tip">{h ? <><b>{fmt(h.value)}</b> {label.toLowerCase()} at {hhmm(h.minute)}</> : <span className="muted">Hover for values</span>}</div>
    </div>
  );
}
