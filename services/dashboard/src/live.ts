// Live connection to the backend over Server-Sent Events.
//
// Everything the dashboard shows as "live" arrives through this stream, which
// the API feeds from records the scorer has already committed to PostgreSQL.
// Nothing here generates, interpolates or animates data on its own.
//
// fetch() is used instead of EventSource so the API key can travel in a
// header rather than in the URL.
import { useEffect, useRef, useState } from "react";
import { Alert, BASE, KEY, Stats, Status, Txn } from "./api";

export type Conn = "connecting" | "connected" | "disconnected";
const FEED_SIZE = 200;
const SILENCE_MS = 6000; // the server sends a status event every second

export interface Live {
  conn: Conn;
  connectedSince: number | null;
  lastMessageAt: number | null;   // browser clock, last byte received
  status: Status | null;          // server-side pipeline status
  stats: Stats | null;
  feed: Txn[];                    // newest first
  alertTick: number;              // bumps whenever an alert event arrives
  lastAlert: (Alert & { kind: string }) | null;
  delivery: { p50: number; p95: number; p99: number; samples: number } | null; // persisted -> this browser
  received: number;               // transaction events received since the page loaded
  reconnects: number;
}

function percentile(sorted: number[], q: number): number {
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
}

export function useLive(seedFeed: () => Promise<Txn[]>): Live {
  const [live, setLive] = useState<Live>({
    conn: "connecting", connectedSince: null, lastMessageAt: null, status: null, stats: null, feed: [],
    alertTick: 0, lastAlert: null, delivery: null, received: 0, reconnects: 0,
  });
  const seedRef = useRef(seedFeed);
  seedRef.current = seedFeed;

  useEffect(() => {
    let stopped = false;
    let controller: AbortController | null = null;
    let lastByte = 0;
    let pending: Txn[] = [];
    let patch: Partial<Live> = {};
    let alertBumps = 0;
    let receivedNew = 0;
    // Clock offset between this browser and the server. The smallest observed
    // (browser now - server_time) is the best estimate of skew plus the minimum
    // network delay, so subtracting it leaves the delivery delay.
    let offset = Number.POSITIVE_INFINITY;
    const samples: number[] = [];

    const flush = () => {
      if (!pending.length && !Object.keys(patch).length && !alertBumps) return;
      const add = pending.reverse();
      const p = patch, bumps = alertBumps, n = receivedNew;
      pending = []; patch = {}; alertBumps = 0; receivedNew = 0;
      let delivery: Live["delivery"] | undefined;
      if (n && samples.length) {
        const s = [...samples].sort((a, b) => a - b);
        delivery = { p50: percentile(s, 0.5), p95: percentile(s, 0.95), p99: percentile(s, 0.99), samples: s.length };
      }
      setLive((prev) => {
        let feed = prev.feed;
        if (add.length) {
          const seen = new Set(add.map((t) => t.transaction_id));
          // Keep the newest FEED_SIZE per source, so a quiet source is not pushed out by a busy one.
          const kept: Record<string, number> = {};
          feed = [...add, ...prev.feed.filter((t) => !seen.has(t.transaction_id))]
            .filter((t) => (kept[t.source] = (kept[t.source] ?? 0) + 1) <= FEED_SIZE);
        }
        return { ...prev, ...p, feed, received: prev.received + n, alertTick: prev.alertTick + bumps,
                 delivery: delivery ?? prev.delivery };
      });
    };
    const flushTimer = window.setInterval(flush, 250);

    const handle = (event: string, data: string) => {
      const now = Date.now();
      let body: any;
      try { body = JSON.parse(data); } catch { return; }
      if (event === "status") {
        offset = Math.min(offset, now - Date.parse(body.server_time));
        patch.status = body;
      } else if (event === "stats") {
        patch.stats = body;
      } else if (event === "transaction") {
        pending.push(body);
        receivedNew += 1;
        if (Number.isFinite(offset)) {
          samples.push(Math.max(0, now - offset - Date.parse(body.persisted_at)));
          if (samples.length > 1000) samples.shift();
        }
      } else if (event === "alert") {
        alertBumps += 1;
        patch.lastAlert = body;
      }
    };

    const connect = async () => {
      while (!stopped) {
        controller = new AbortController();
        try {
          const res = await fetch(`${BASE}/v1/stream`, { headers: { "X-API-Key": KEY }, signal: controller.signal });
          if (!res.ok || !res.body) throw new Error(`stream ${res.status}`);
          lastByte = Date.now();
          // Stored history first, so the feed never starts from invented rows.
          const seed = await seedRef.current().catch(() => [] as Txn[]);
          setLive((prev) => ({ ...prev, conn: "connected", connectedSince: Date.now(), lastMessageAt: lastByte, feed: seed }));
          const reader = res.body.getReader();
          const decoder = new TextDecoder();
          let buffer = "";
          for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            lastByte = Date.now();
            patch.lastMessageAt = lastByte;
            buffer += decoder.decode(value, { stream: true });
            let cut: number;
            while ((cut = buffer.indexOf("\n\n")) >= 0) {
              const block = buffer.slice(0, cut);
              buffer = buffer.slice(cut + 2);
              let ev = "message", data = "";
              for (const line of block.split("\n")) {
                if (line.startsWith("event:")) ev = line.slice(6).trim();
                else if (line.startsWith("data:")) data += line.slice(5).trim();
              }
              if (data) handle(ev, data);
            }
          }
        } catch { /* fall through to reconnect */ }
        if (stopped) return;
        flush();
        setLive((prev) => ({ ...prev, conn: "disconnected", connectedSince: null, reconnects: prev.reconnects + 1 }));
        await new Promise((r) => setTimeout(r, 2000));
      }
    };
    connect();

    // A connection can die without closing (laptop sleep, dropped Wi-Fi).
    // If the server has been silent for too long, stop trusting it and reconnect.
    const watchdog = window.setInterval(() => {
      if (lastByte && Date.now() - lastByte > SILENCE_MS) {
        lastByte = 0;
        controller?.abort();
      }
    }, 1000);

    return () => {
      stopped = true;
      controller?.abort();
      window.clearInterval(flushTimer);
      window.clearInterval(watchdog);
    };
  }, []);

  return live;
}
