import { useEffect, useRef } from "react";

// A slowly turning sphere of points with payments arcing between them. Most arcs
// complete; a few turn red and stop short, the way a held payment would. Plain
// 2D canvas with a perspective projection: no 3D library, a few hundred points.
// It stops drawing when off screen and draws one still frame for reduced motion.

const POINTS = 420;
const ARCS = 14;

type V3 = [number, number, number];

function fibonacciSphere(n: number): V3[] {
  const pts: V3[] = [];
  const golden = Math.PI * (3 - Math.sqrt(5));
  for (let i = 0; i < n; i++) {
    const y = 1 - (i / (n - 1)) * 2;
    const r = Math.sqrt(1 - y * y);
    const t = golden * i;
    pts.push([Math.cos(t) * r, y, Math.sin(t) * r]);
  }
  return pts;
}

/** Point on the great circle between a and b, lifted off the surface in the middle. */
function arcPoint(a: V3, b: V3, t: number): V3 {
  const dot = Math.min(1, Math.max(-1, a[0] * b[0] + a[1] * b[1] + a[2] * b[2]));
  const w = Math.acos(dot);
  const s = Math.sin(w) || 1e-6;
  const k1 = Math.sin((1 - t) * w) / s, k2 = Math.sin(t * w) / s;
  const lift = 1 + 0.28 * Math.sin(Math.PI * t) * (w / Math.PI + 0.3);
  return [(a[0] * k1 + b[0] * k2) * lift, (a[1] * k1 + b[1] * k2) * lift, (a[2] * k1 + b[2] * k2) * lift];
}

interface Arc { a: V3; b: V3; start: number; dur: number; flagged: boolean }

export function Globe() {
  const ref = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const canvas = ref.current!;
    const ctx = canvas.getContext("2d")!;
    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const pts = fibonacciSphere(POINTS);
    let seed = 7;
    const rnd = () => ((seed = (seed * 16807) % 2147483647) / 2147483647);
    const newArc = (now: number): Arc => {
      // Pair points less than about 80 degrees apart: near-opposite points make the great circle unstable.
      const a = pts[Math.floor(rnd() * POINTS)];
      let b = a;
      for (let k = 0; k < 40; k++) {
        const c = pts[Math.floor(rnd() * POINTS)];
        const dot = a[0] * c[0] + a[1] * c[1] + a[2] * c[2];
        if (dot > 0.2 && dot < 0.95) { b = c; break; }
      }
      return { a, b, start: now + rnd() * 2400, dur: 1800 + rnd() * 1400, flagged: rnd() < 0.18 };
    };
    let arcs: Arc[] = Array.from({ length: ARCS }, () => newArc(performance.now()));
    let W = 0, H = 0, raf = 0, visible = true;

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      W = canvas.clientWidth; H = canvas.clientHeight;
      canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };
    resize();
    const ro = new ResizeObserver(resize); ro.observe(canvas);
    const io = new IntersectionObserver(([e]) => { visible = e.isIntersecting; if (visible && !reduced) loop(); });
    io.observe(canvas);

    const tilt = 0.38;
    const project = (p: V3, rot: number): [number, number, number] => {
      const cr = Math.cos(rot), sr = Math.sin(rot);
      const x = p[0] * cr + p[2] * sr, z0 = -p[0] * sr + p[2] * cr;
      const ct = Math.cos(tilt), st = Math.sin(tilt);
      const y = p[1] * ct - z0 * st, z = p[1] * st + z0 * ct;
      const R = Math.min(W, H) * 0.4, f = 3.2;
      const s = f / (f - z);
      return [W / 2 + x * R * s, H / 2 - y * R * s, z];
    };

    const draw = (now: number) => {
      ctx.clearRect(0, 0, W, H);
      const rot = now * 0.00006;
      // Points: the back half fades so the sphere reads as a solid.
      for (const p of pts) {
        const [x, y, z] = project(p, rot);
        // Nearer points are darker and larger, which is most of what makes it read as 3D.
        const depth = (z + 1) / 2;
        ctx.fillStyle = `rgba(17, 20, 24, ${0.1 + 0.62 * depth * depth})`;
        const r = 1 + 1.6 * depth;
        ctx.fillRect(x - r / 2, y - r / 2, r, r);
      }
      // Arcs.
      arcs = arcs.map((arc) => {
        const t = (now - arc.start) / arc.dur;
        if (t > 1.6) return newArc(now);
        if (t <= 0) return arc;
        const end = arc.flagged ? Math.min(t, 0.62) : Math.min(t, 1);
        const from = Math.max(0, Math.min(end, t - 0.6));
        ctx.beginPath();
        for (let i = 0; i <= 24; i++) {
          const [x, y] = project(arcPoint(arc.a, arc.b, from + ((end - from) * i) / 24), rot);
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        }
        const fade = t > 1 ? Math.max(0, 1 - (t - 1) / 0.6) : 1;
        ctx.strokeStyle = arc.flagged && t > 0.62 ? `rgba(196, 43, 43, ${0.9 * fade})` : `rgba(31, 138, 82, ${0.75 * fade})`;
        ctx.lineWidth = 1.7;
        ctx.stroke();
        if (arc.flagged && t > 0.62) {
          const [x, y] = project(arcPoint(arc.a, arc.b, 0.62), rot);
          ctx.strokeStyle = `rgba(196, 43, 43, ${fade})`;
          ctx.lineWidth = 1.5;
          ctx.beginPath(); ctx.arc(x, y, 5 + 3 * Math.min(1, (t - 0.62) * 3), 0, Math.PI * 2); ctx.stroke();
        }
        return arc;
      });
    };

    const loop = () => {
      cancelAnimationFrame(raf);
      const step = (now: number) => {
        if (!visible || document.hidden) return;
        draw(now);
        raf = requestAnimationFrame(step);
      };
      raf = requestAnimationFrame(step);
    };
    const onVis = () => { if (!document.hidden && !reduced) loop(); };
    document.addEventListener("visibilitychange", onVis);
    if (reduced) draw(performance.now() + 1500); else loop();
    return () => { cancelAnimationFrame(raf); ro.disconnect(); io.disconnect(); document.removeEventListener("visibilitychange", onVis); };
  }, []);
  return <canvas ref={ref} className="globe" aria-hidden />;
}
