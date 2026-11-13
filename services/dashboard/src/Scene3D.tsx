// 3D live view: every point is one stored transaction pushed by the backend.
//
//   left-right  amount (log scale)        up-down  risk score (0 to 1)
//   depth       age: new transactions arrive at the front and recede as they age
//
// Nothing here is simulated. Points only move because they get older, and the
// clock stops when the stream is not live, so a stalled pipeline gives a still,
// dimmed scene instead of something that looks alive.
//
// Kept light on purpose: plain canvas with a hand-written perspective
// projection (no WebGL, no 3D library), capped at 20 frames per second, at most
// 1,200 points, and no drawing at all while the tab or the panel is hidden.
import { useEffect, useRef, useState } from "react";
import { LEVELS, Policy, RiskLevel, Txn } from "./api";
import { LEVEL_ICON } from "./charts";

const WINDOW_S = 20;
const MAX_POINTS = 1200;
const FRAME_MS = 1000 / 20;
const LEVEL_VAR: Record<RiskLevel, string> = { LOW: "--good", MEDIUM: "--warning", HIGH: "--serious", CRITICAL: "--critical" };
const SIZE: Record<RiskLevel, number> = { LOW: 1.6, MEDIUM: 3.2, HIGH: 4.2, CRITICAL: 5.2 };
const AMOUNT_MIN = 1, AMOUNT_MAX = 6; // log10 rupees: 10 to 10,00,000

interface Props {
  feed: Txn[];
  isLive: boolean;
  frozenAt: number | null;            // browser time of the last message, used when not live
  clockOffset: number;                // browser clock minus server clock, ms
  thresholds: Policy["thresholds"] | null;
  onOpen: (id: string) => void;
}
interface Placed { t: Txn; sx: number; sy: number; r: number; depth: number }

export function Scene3D({ feed, isLive, frozenAt, clockOffset, thresholds, onOpen }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const state = useRef({ feed, isLive, frozenAt, clockOffset, thresholds, tiltX: 0, tiltY: 0, hover: null as Placed | null, placed: [] as Placed[], mx: -1, my: -1 });
  const [tip, setTip] = useState<Placed | null>(null);
  Object.assign(state.current, { feed, isLive, frozenAt, clockOffset, thresholds });

  useEffect(() => {
    const canvas = canvasRef.current!;
    const ctx = canvas.getContext("2d")!;
    const css = getComputedStyle(document.documentElement);
    const color = (v: string) => css.getPropertyValue(v).trim();
    const C = { line: color("--line"), axis: color("--axis"), muted: color("--muted"), ink: color("--ink"), surface: color("--surface"),
                level: Object.fromEntries(LEVELS.map((l) => [l, color(LEVEL_VAR[l])])) as Record<RiskLevel, string> };
    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    let raf = 0, last = 0, visible = true, W = 0, H = 0;

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 1.5);
      W = canvas.clientWidth; H = canvas.clientHeight;
      canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };
    resize();
    const ro = new ResizeObserver(resize); ro.observe(canvas);
    const io = new IntersectionObserver(([e]) => { visible = e.isIntersecting; }); io.observe(canvas);

    // World box: x in [-1, 1] (amount), y in [0, 1] (risk), z in [0, 1] (age, 0 = now).
    const project = (x: number, y: number, z: number) => {
      const s = state.current;
      const yaw = -0.42 + s.tiltX * 0.22, pitch = 0.30 + s.tiltY * 0.10;
      x *= 1.7;
      const zc = (z - 0.5) * 3.0, yc = (y - 0.5) * 0.9;
      const x1 = x * Math.cos(yaw) - zc * Math.sin(yaw);
      const z1 = x * Math.sin(yaw) + zc * Math.cos(yaw);
      const y1 = yc * Math.cos(pitch) - z1 * Math.sin(pitch);
      const z2 = yc * Math.sin(pitch) + z1 * Math.cos(pitch) + 3.6;
      const f = Math.min(W * 0.42, H * 1.75) / z2;
      return { sx: W / 2 + x1 * f, sy: H / 2 - y1 * f + 28, scale: f / 200, depth: z2 };
    };
    const seg = (a: [number, number, number], b: [number, number, number], stroke: string, width = 1, dash: number[] = []) => {
      const p = project(...a), q = project(...b);
      ctx.strokeStyle = stroke; ctx.lineWidth = width; ctx.setLineDash(dash);
      ctx.beginPath(); ctx.moveTo(p.sx, p.sy); ctx.lineTo(q.sx, q.sy); ctx.stroke();
    };
    const label = (text: string, at: [number, number, number], align: CanvasTextAlign = "left", dy = 0) => {
      const p = project(...at);
      ctx.fillStyle = C.muted; ctx.font = "10.5px Inter, system-ui, sans-serif"; ctx.textAlign = align;
      ctx.fillText(text, p.sx, p.sy + dy);
    };

    const draw = (now: number) => {
      raf = requestAnimationFrame(draw);
      if (!visible || document.hidden || now - last < (reduced ? 500 : FRAME_MS)) return;
      last = now;
      const s = state.current;
      // "Now" on the server clock. Frozen at the last message when the stream is not live.
      const clock = (s.isLive || s.frozenAt == null ? Date.now() : s.frozenAt) - s.clockOffset;
      ctx.clearRect(0, 0, W, H);
      ctx.globalAlpha = s.isLive ? 1 : 0.45;

      // Floor grid and frame.
      for (let i = 0; i <= 6; i++) seg([-1, 0, i / 6], [1, 0, i / 6], i === 0 ? C.axis : C.line);
      for (let i = 0; i <= 5; i++) seg([-1 + (2 * i) / 5, 0, 0], [-1 + (2 * i) / 5, 0, 1], C.line);
      seg([-1, 0, 1], [-1, 1, 1], C.line); seg([1, 0, 1], [1, 1, 1], C.line); seg([-1, 1, 1], [1, 1, 1], C.line);
      seg([-1, 0, 0], [-1, 1, 0], C.axis); seg([-1, 1, 0], [-1, 1, 1], C.line);
      // Threshold planes, drawn as lines along the left wall and the back wall.
      if (s.thresholds) {
        (["medium", "high", "critical"] as const).forEach((k) => {
          const y = s.thresholds![k], col = C.level[k.toUpperCase() as RiskLevel];
          seg([-1, y, 0], [-1, y, 1], col, 1, [3, 4]); seg([-1, y, 1], [1, y, 1], col, 1, [3, 4]);
          label(`${k.toUpperCase()} ≥ ${y}`, [-1, y, 0], "right", 3);
        });
      }
      ctx.setLineDash([]);
      label("risk 0", [-1, 0, 0], "right", 3); label("risk 1", [-1, 1, 0], "right", 3);
      label("₹10", [-1, 0, 0], "center", 16); label("₹1,000", [-0.2, 0, 0], "center", 16); label("₹10,00,000", [1, 0, 0], "center", 16);
      label("now", [1, 0, 0], "left", 3); label(`${WINDOW_S} s ago`, [1, 0, 1], "left", 3);

      // Points, far to near so nearer ones draw on top.
      const placed: Placed[] = [];
      for (let i = 0; i < s.feed.length && placed.length < MAX_POINTS; i++) {
        const t = s.feed[i];
        const age = (clock - Date.parse(t.persisted_at)) / 1000;
        if (age > WINDOW_S) continue;
        const lx = (Math.min(AMOUNT_MAX, Math.max(AMOUNT_MIN, Math.log10(Math.max(t.amount, 1)))) - AMOUNT_MIN) / (AMOUNT_MAX - AMOUNT_MIN);
        const p = project(lx * 2 - 1, t.risk_score, Math.max(0, age) / WINDOW_S);
        placed.push({ t, sx: p.sx, sy: p.sy, r: Math.max(1.2, SIZE[t.risk_level] * p.scale * 1.6), depth: p.depth });
      }
      placed.sort((a, b) => b.depth - a.depth);
      for (const p of placed) {
        const hot = p.t.risk_level !== "LOW";
        if (hot) { // a stem to the floor makes height readable in 3D
          const lx = (Math.min(AMOUNT_MAX, Math.max(AMOUNT_MIN, Math.log10(Math.max(p.t.amount, 1)))) - AMOUNT_MIN) / (AMOUNT_MAX - AMOUNT_MIN);
          const age = Math.max(0, (clock - Date.parse(p.t.persisted_at)) / 1000) / WINDOW_S;
          const base = project(lx * 2 - 1, 0, age);
          ctx.strokeStyle = C.level[p.t.risk_level]; ctx.globalAlpha = (s.isLive ? 1 : 0.45) * 0.45; ctx.lineWidth = 1;
          ctx.beginPath(); ctx.moveTo(p.sx, p.sy); ctx.lineTo(base.sx, base.sy); ctx.stroke();
        }
        ctx.globalAlpha = (s.isLive ? 1 : 0.45) * (hot ? 1 : 0.75);
        ctx.fillStyle = C.level[p.t.risk_level];
        if (!hot) { ctx.fillRect(p.sx - p.r, p.sy - p.r, p.r * 2, p.r * 2); continue; } // squares are much cheaper than arcs
        ctx.beginPath(); ctx.arc(p.sx, p.sy, p.r, 0, Math.PI * 2); ctx.fill();
        if (hot) { ctx.strokeStyle = C.surface; ctx.lineWidth = 1.5; ctx.stroke(); }
      }
      s.placed = placed;

      // Hover: nearest point within reach of the pointer.
      let best: Placed | null = null, bestD = 14 * 14;
      if (s.mx >= 0) for (const p of placed) {
        const d = (p.sx - s.mx) ** 2 + (p.sy - s.my) ** 2;
        if (d < bestD || (d < 14 * 14 && best && p.t.risk_score > best.t.risk_score && d < bestD * 1.5)) { best = p; bestD = d; }
      }
      if (best) {
        ctx.globalAlpha = 1; ctx.strokeStyle = C.ink; ctx.lineWidth = 2;
        ctx.beginPath(); ctx.arc(best.sx, best.sy, best.r + 3, 0, Math.PI * 2); ctx.stroke();
      }
      if ((best?.t.transaction_id ?? null) !== (s.hover?.t.transaction_id ?? null)) { s.hover = best; setTip(best); }
      ctx.globalAlpha = 1;
    };
    raf = requestAnimationFrame(draw);
    return () => { cancelAnimationFrame(raf); ro.disconnect(); io.disconnect(); };
  }, []);

  const move = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const box = e.currentTarget.getBoundingClientRect(), s = state.current;
    s.mx = e.clientX - box.left; s.my = e.clientY - box.top;
    s.tiltX = (s.mx / box.width - 0.5) * 2; s.tiltY = (s.my / box.height - 0.5) * 2;
  };
  const leave = () => { const s = state.current; s.mx = s.my = -1; s.tiltX = s.tiltY = 0; };
  const inWindow = state.current.placed.length;

  return (
    <div className="scene" data-testid="scene3d">
      <canvas ref={canvasRef} onMouseMove={move} onMouseLeave={leave} onClick={() => tip && onOpen(tip.t.transaction_id)}
              role="img" aria-label="3D view of transactions from the last 20 seconds by amount, risk score and age" />
      <div className="scene-legend">
        {LEVELS.map((l) => <span key={l}><i style={{ color: `var(${LEVEL_VAR[l]})` }}>{LEVEL_ICON[l]}</i> {l}</span>)}
        <span className="muted">move the pointer to tilt · click a point to open it</span>
      </div>
      <div className="scene-tip">
        {tip ? <><b className="mono">{tip.t.transaction_id}</b> · ₹{tip.t.amount.toLocaleString("en-IN")} {tip.t.txn_type} · risk {tip.t.risk_score.toFixed(3)} · {tip.t.risk_level} · {tip.t.source.replace("_", " ")}</>
             : <span className="muted">{isLive ? `${inWindow} transactions from the last ${WINDOW_S} s. The same rows are in the feed table below.` : "Paused: the stream is not live, so the view is frozen at the last message."}</span>}
      </div>
    </div>
  );
}
