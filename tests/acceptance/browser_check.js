// Browser-level check of acceptance tests 1, 6, 7 and 8: what a person actually sees.
//   node tests/acceptance/browser_check.js <screenshot-dir>
// Env: DASHBOARD_URL, API_URL, API_KEY, SCORER_STOP_CMD, SCORER_START_CMD, API_STOP_CMD, API_START_CMD
const { chromium } = require("playwright");
const { execSync, spawn } = require("child_process");
const DASH = process.env.DASHBOARD_URL || "http://localhost:3000";
const API = process.env.API_URL || "http://localhost:8000";
const KEY = process.env.API_KEY || "dev-key";
const STOP = process.env.SCORER_STOP_CMD || "docker compose stop scorer";
const START = process.env.SCORER_START_CMD || "docker compose start scorer";
const API_STOP = process.env.API_STOP_CMD || "docker compose stop api";
const API_START = process.env.API_START_CMD || "docker compose start api";
const dir = process.argv[2] || ".";
const results = [];
const check = (name, ok, detail = "") => { results.push({ name, ok, detail }); console.log(`${ok ? "PASS" : "FAIL"}  ${name}  ${detail}`); };

async function send(id) {
  const body = { transaction_id: id, event_time: new Date().toISOString(), account_id: `C_BRW_${Date.now().toString(36)}`, counterparty_id: "M_BROWSER",
    txn_type: "PAYMENT", amount: 4321, old_balance_orig: 90000, old_balance_dest: 0 };
  const r = await fetch(`${API}/v1/transactions`, { method: "POST", headers: { "X-API-Key": KEY, "Content-Type": "application/json" }, body: JSON.stringify(body) });
  return r.status;
}

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1500, height: 1000 }, deviceScaleFactor: 2 });
  let navigations = 0;
  page.on("framenavigated", (f) => { if (f === page.mainFrame()) navigations += 1; });
  await page.goto(DASH);
  const pill = page.getByTestId("stream-pill");
  await pill.filter({ hasText: /LIVE|IDLE/ }).waitFor({ timeout: 20000 });
  check("environment badge is shown", await page.locator(".env").innerText().then((t) => /DEMO|PRODUCTION/.test(t)));

  const processed0 = await page.getByTestId("kpi-processed").innerText();
  await page.locator("header select").selectOption("LIVE_API"); // the generator is busy; watch the REST source
  await page.waitForTimeout(1500);
  // 1 + 6: a new transaction shows up in the feed with no reload and no click.
  const before = navigations;
  const id1 = `TXN_BROWSER_${Date.now().toString(36)}`;
  const t0 = Date.now();
  await send(id1);
  await page.locator("table[data-testid=feed]").getByText(id1).waitFor({ timeout: 20000 });
  check("transaction appears in the live feed", true, `${Date.now() - t0} ms after the POST`);
  await page.locator("header select").selectOption("");
  await page.waitForFunction((v) => document.querySelector("[data-testid=kpi-processed]").innerText !== v, processed0, { timeout: 15000 });
  check("KPIs changed with no page reload", navigations === before, `navigations during the test: ${navigations - before}`);
  await page.screenshot({ path: `${dir}/1-live.png` });

  // 7: stop the scoring stream; the page must say so and stop presenting data as live.
  execSync(STOP + " || true", { stdio: "ignore" });
  const t1 = Date.now();
  await pill.filter({ hasText: "STREAM DISCONNECTED" }).waitFor({ timeout: 25000 });
  check("disconnected state is shown", true, `${((Date.now() - t1) / 1000).toFixed(1)} s after the scorer stopped`);
  const banner = await page.getByTestId("banner").innerText();
  check("banner explains the outage", /heartbeat missing/.test(banner) && /stored records/.test(banner));
  check("feed is no longer titled live", await page.getByText("Transaction feed (not live)").isVisible());
  check("content is dimmed while not live", await page.locator("main.stale").count() === 1);
  await page.locator("header select").selectOption("LIVE_API");
  const id2 = `TXN_BROWSER_${Date.now().toString(36)}_held`;
  await send(id2);
  await page.waitForTimeout(4000);
  check("nothing new is shown while disconnected", (await page.locator("table[data-testid=feed]").getByText(id2).count()) === 0);
  await page.screenshot({ path: `${dir}/2-disconnected.png` });

  // 8: bring it back; the held transaction is processed and appears.
  spawn("bash", ["-c", START], { detached: true, stdio: "ignore" }).unref();
  const t2 = Date.now();
  await page.locator("table[data-testid=feed]").getByText(id2).waitFor({ timeout: 90000 });
  check("held transaction appears after reconnect", true, `${((Date.now() - t2) / 1000).toFixed(1)} s after restart`);
  await pill.filter({ hasText: /LIVE|IDLE/ }).waitFor({ timeout: 60000 });
  check("status returns to live", true);
  await page.screenshot({ path: `${dir}/3-recovered.png` });

  // The API itself goes away: the page must say it has no connection, then recover by itself.
  execSync(API_STOP + " || true", { stdio: "ignore" });
  await pill.filter({ hasText: "NO CONNECTION TO SERVER" }).waitFor({ timeout: 20000 });
  check("lost server connection is shown", true);
  check("page says the figures are not live", /not live/.test(await page.getByTestId("banner").innerText()));
  await page.screenshot({ path: `${dir}/4-no-connection.png` });
  spawn("bash", ["-c", API_START], { detached: true, stdio: "ignore" }).unref();
  await pill.filter({ hasText: /LIVE|IDLE/ }).waitFor({ timeout: 60000 });
  check("page reconnects by itself, no reload", navigations === before);
  await browser.close();
  process.exit(results.every((r) => r.ok) ? 0 : 1);
})().catch((e) => { console.error("FAIL", e.message); process.exit(1); });
