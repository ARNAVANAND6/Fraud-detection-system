import { useEffect, useState } from "react";
import { PublicSummary, api } from "../api";
import { go, onNav } from "../router";
import { ARTICLES, Article, BANNER_TIPS, HELPLINE, QUIZ, REPORT_URL } from "./content";
import { Globe } from "./Globe";
import "./home.css";

// The public FORESENTRY site: what the product does, for the teams that buy it, and
// plain-language scam awareness for everyone who pays and gets paid.

const fmt = (n: number) => n.toLocaleString("en-IN");

function AwarenessBar() {
  const [hidden, setHidden] = useState(() => { try { return sessionStorage.getItem("fs-bar") === "off"; } catch { return false; } });
  const [i, setI] = useState(0);
  useEffect(() => {
    if (hidden || window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    const t = window.setInterval(() => setI((v) => (v + 1) % BANNER_TIPS.length), 7000);
    return () => window.clearInterval(t);
  }, [hidden]);
  if (hidden) return null;
  return (
    <div className="aware-bar" role="region" aria-label="Fraud awareness">
      <span className="aware-label">Stay safe</span>
      <span className="aware-tip" aria-live="polite">{BANNER_TIPS[i]}</span>
      <a className="aware-call" href={`tel:${HELPLINE}`}>Helpline {HELPLINE}</a>
      <button className="aware-x" aria-label="Hide this message"
              onClick={() => { setHidden(true); try { sessionStorage.setItem("fs-bar", "off"); } catch { /* not remembered */ } }}>×</button>
    </div>
  );
}

function SiteNav() {
  return (
    <header className="site-nav">
      <a className="logo" href="/" onClick={onNav("/")}><span className="logo-mark" aria-hidden>F</span>FORESENTRY</a>
      <nav aria-label="Site">
        <a href="/#platform" onClick={(e) => { e.preventDefault(); scrollToId("platform"); }}>Platform</a>
        <a href="/#how" onClick={(e) => { e.preventDefault(); scrollToId("how"); }}>How it works</a>
        <a href="/#learn" onClick={(e) => { e.preventDefault(); scrollToId("learn"); }}>Scam awareness</a>
        <a href="/#help" onClick={(e) => { e.preventDefault(); scrollToId("help"); }}>Report fraud</a>
      </nav>
      <a className="btn btn-dark" href="/console" onClick={onNav("/console")}>Sign in</a>
    </header>
  );
}

function scrollToId(id: string) {
  if (window.location.pathname !== "/") { go("/"); window.setTimeout(() => scrollToId(id), 60); return; }
  document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
}

/** A worked example of one decision, as the console shows it. Marked as an example: it is not live. */
function DecisionCard() {
  return (
    <div className="decision-card" aria-label="Example of a scored payment">
      <div className="dc-head"><span className="dc-tag">Example</span><span className="muted">TRANSFER · just now</span></div>
      <div className="dc-amount">₹95,000</div>
      <div className="dc-route mono">C00000001 → M000000001</div>
      <div className="dc-score">
        <div className="dc-meter"><span style={{ width: "96%" }} /></div>
        <b>0.962</b>
      </div>
      <div className="dc-verdict">Hold for investigation</div>
      <div className="dc-reasons">
        <span>NEW_DEVICE</span><span>DRAINS_BALANCE</span><span>HIGH_VELOCITY_1H</span>
      </div>
    </div>
  );
}

function LiveStrip() {
  const [s, setS] = useState<PublicSummary | null>(null);
  useEffect(() => {
    let stop = false;
    const load = () => api.publicSummary().then((r) => { if (!stop) setS(r); }).catch(() => undefined);
    load();
    const t = window.setInterval(load, 30_000);
    return () => { stop = true; window.clearInterval(t); };
  }, []);
  if (!s) return null;
  return (
    <section className="live-strip" aria-label="Measured by this deployment">
      <div><b>{fmt(s.transactions_24h)}</b><span>payments scored, last 24 h</span></div>
      <div><b>{(100 * s.flagged_share_24h).toFixed(2)}%</b><span>flagged for review or hold</span></div>
      <div><b>{s.p50_pipeline_ms != null ? `${Math.round(s.p50_pipeline_ms)} ms` : "n/a"}</b><span>median, received to decided</span></div>
      <div><b>{fmt(s.organisations)}</b><span>organisations on this server</span></div>
      <p className="live-note">
        {s.simulated ? "Simulated traffic on a demonstration server. " : "Measured on this deployment. "}
        Updated {new Date(s.measured_at).toLocaleTimeString()}.
      </p>
    </section>
  );
}

const FEATURES = [
  { title: "Decides before the money moves", text: "Each payment is scored as it arrives: rules and a gradient-boosted model, with the account's recent behaviour read from an in-memory feature store." },
  { title: "Says why", text: "Every decision carries reason codes an analyst can read: new device, drains the balance, many payments to one account in an hour." },
  { title: "One queue for the fraud team", text: "Related payments group into one alert per account, prioritised, assigned and escalated when the response time runs out." },
  { title: "Accounts protected properly", text: "Roles, single sign-on, two-step verification and an append-only audit log of every sign-in, decision and setting change." },
  { title: "Scales out, not up", text: "Payments are partitioned by account across Kafka; add API and scorer processes and the work spreads across them." },
  { title: "Watches its own model", text: "Live inputs are compared with training data every hour, so a shift in behaviour is visible before accuracy quietly drops." },
];

const STEPS = [
  { n: "1", title: "Receive", text: "Payment events arrive by API, signed webhook from a payment gateway, or a Kafka topic." },
  { n: "2", title: "Enrich", text: "Velocity, history and network features for the account are read and updated in milliseconds." },
  { n: "3", title: "Score", text: "Rules and the model produce a risk score, a level and a recommended action." },
  { n: "4", title: "Act", text: "Allow, review or hold. Holds open an alert for an analyst; the bank's systems get the answer by API." },
];

const AUDIENCES = [
  { who: "Banks", what: "Screen UPI, IMPS and card payments in real time and give account holders a way to report 'this wasn't me'." },
  { who: "Payment apps and gateways", what: "Score merchant and wallet traffic with your own thresholds and send flagged payments to review." },
  { who: "Merchants", what: "Catch account takeover and refund abuse before orders ship, without blocking good customers." },
  { who: "Public agencies", what: "Run it on your own servers, per organisation, with the data staying where it is processed." },
];

function Quiz() {
  const [i, setI] = useState(0);
  const [picked, setPicked] = useState<boolean | null>(null);
  const [score, setScore] = useState(0);
  const done = i >= QUIZ.length;
  const q = QUIZ[i];
  const answer = (scam: boolean) => { if (picked !== null) return; setPicked(scam); if (scam === q.scam) setScore((s) => s + 1); };
  if (done) {
    return (
      <div className="quiz" aria-live="polite">
        <div className="quiz-head"><b>Spot the scam</b><span className="muted">finished</span></div>
        <p className="quiz-q">You got {score} of {QUIZ.length}.</p>
        <p className="muted">One rule covers most of them: nobody genuine needs your OTP, PIN or your screen.</p>
        <button className="btn" onClick={() => { setI(0); setPicked(null); setScore(0); }}>Try again</button>
      </div>
    );
  }
  return (
    <div className="quiz" aria-live="polite">
      <div className="quiz-head"><b>Spot the scam</b><span className="muted">{i + 1} of {QUIZ.length}</span></div>
      <p className="quiz-q">{q.scenario}</p>
      <div className="quiz-actions">
        <button className={`btn ${picked === true ? (q.scam ? "right" : "wrong") : ""}`} onClick={() => answer(true)}>Scam</button>
        <button className={`btn ${picked === false ? (!q.scam ? "right" : "wrong") : ""}`} onClick={() => answer(false)}>Looks fine</button>
      </div>
      {picked !== null && (
        <>
          <p className="quiz-why"><b>{picked === q.scam ? "Right." : "Not quite."}</b> {q.why}</p>
          <button className="btn btn-dark" onClick={() => { setI(i + 1); setPicked(null); }}>{i + 1 < QUIZ.length ? "Next" : "See result"}</button>
        </>
      )}
    </div>
  );
}

function ArticleCard({ a }: { a: Article }) {
  return (
    <a className="article-card" href={`/learn/${a.slug}`} onClick={onNav(`/learn/${a.slug}`)}>
      <span className="a-tag">{a.tag}</span>
      <b>{a.title}</b>
      <span className="muted">{a.summary}</span>
      <span className="a-min">{a.minutes} min read</span>
    </a>
  );
}

function HelpPanel() {
  return (
    <section className="help" id="help">
      <div>
        <h2>Lost money to a scam?</h2>
        <p>Report it straight away. The sooner it is reported, the better the chance the money can be held before it is withdrawn.</p>
      </div>
      <ol>
        <li><b>Call {HELPLINE}</b>, the national cyber-crime helpline, with the transaction reference.</li>
        <li><b>Call your bank</b> on the number printed on your card to block the card, UPI or net banking.</li>
        <li><b>Report online</b> at <a href={REPORT_URL} target="_blank" rel="noreferrer">cybercrime.gov.in</a> and keep the acknowledgement.</li>
        <li><b>Keep the evidence</b>: screenshots, the chat, call logs and every transaction ID.</li>
      </ol>
    </section>
  );
}

function SiteFooter() {
  return (
    <footer className="site-foot">
      <div className="logo"><span className="logo-mark" aria-hidden>F</span>FORESENTRY</div>
      <p className="muted">
        Risk scores are estimates for review, not banking or regulatory decisions. Awareness content is general guidance; for your
        own case, contact your bank and the helpline.
      </p>
      <div className="foot-links">
        <a href="/console" onClick={onNav("/console")}>Sign in</a>
        <a href={`tel:${HELPLINE}`}>Helpline {HELPLINE}</a>
        <a href={REPORT_URL} target="_blank" rel="noreferrer">cybercrime.gov.in</a>
      </div>
    </footer>
  );
}

export function ArticlePage({ slug }: { slug: string }) {
  const a = ARTICLES.find((x) => x.slug === slug);
  useEffect(() => { document.title = a ? `${a.title} · FORESENTRY` : "FORESENTRY"; }, [a]);
  return (
    <div className="site">
      <AwarenessBar />
      <SiteNav />
      <main className="article">
        {!a ? (
          <p>That article does not exist. <a href="/" onClick={onNav("/")}>Back to the home page</a>.</p>
        ) : (
          <>
            <a className="back" href="/#learn" onClick={(e) => { e.preventDefault(); scrollToId("learn"); }}>← Scam awareness</a>
            <span className="a-tag">{a.tag}</span>
            <h1>{a.title}</h1>
            <p className="lede">{a.summary}</p>
            {groupList(a.body).map((block, i) => Array.isArray(block)
              ? <ul key={i}>{block.map((li) => <li key={li}>{li}</li>)}</ul>
              : <p key={i}>{block}</p>)}
            <HelpPanel />
            <h3>More to read</h3>
            <div className="articles">{ARTICLES.filter((x) => x.slug !== a.slug).slice(0, 3).map((x) => <ArticleCard key={x.slug} a={x} />)}</div>
          </>
        )}
      </main>
      <SiteFooter />
    </div>
  );
}

/** Consecutive "- " paragraphs become one list. */
function groupList(body: string[]): (string | string[])[] {
  const out: (string | string[])[] = [];
  for (const p of body) {
    if (p.startsWith("- ")) {
      const last = out[out.length - 1];
      if (Array.isArray(last)) last.push(p.slice(2)); else out.push([p.slice(2)]);
    } else out.push(p);
  }
  return out;
}

export function Home() {
  useEffect(() => { document.title = "FORESENTRY · Real-time payment fraud prevention"; }, []);
  return (
    <div className="site">
      <AwarenessBar />
      <SiteNav />

      <section className="hero">
        <div className="hero-text">
          <span className="eyebrow">Real-time payment risk</span>
          <h1>Stop fraud before the money moves.</h1>
          <p>
            FORESENTRY scores every payment the moment it arrives, explains each decision, and puts the risky ones in front of your
            fraud team, with the account holder able to say "this wasn't me".
          </p>
          <div className="hero-cta">
            <a className="btn btn-dark" href="/console" onClick={onNav("/console")}>Open the console</a>
            <a className="btn" href="/#help" onClick={(e) => { e.preventDefault(); scrollToId("help"); }}>Report a fraud</a>
          </div>
        </div>
        <div className="hero-visual">
          <Globe />
          <DecisionCard />
        </div>
      </section>

      <LiveStrip />

      <section className="block" id="platform">
        <div className="block-head">
          <span className="eyebrow">Platform</span>
          <h2>Everything between "payment received" and "payment released"</h2>
        </div>
        <div className="features">
          {FEATURES.map((f) => (
            <div className="feature" key={f.title}>
              <b>{f.title}</b>
              <p>{f.text}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="block" id="how">
        <div className="block-head">
          <span className="eyebrow">How it works</span>
          <h2>Four steps, in about a tenth of a second</h2>
        </div>
        <ol className="flow">
          {STEPS.map((s) => (
            <li key={s.n}><span className="flow-n">{s.n}</span><b>{s.title}</b><p>{s.text}</p></li>
          ))}
        </ol>
      </section>

      <section className="block">
        <div className="block-head">
          <span className="eyebrow">Who it is for</span>
          <h2>One platform, separate organisations</h2>
          <p className="muted">Each organisation sees only its own payments, people and settings. Platform operators manage organisations, not their data.</p>
        </div>
        <div className="audiences">
          {AUDIENCES.map((a) => <div key={a.who}><b>{a.who}</b><p>{a.what}</p></div>)}
        </div>
      </section>

      <section className="block learn" id="learn">
        <div className="block-head">
          <span className="eyebrow">Scam awareness</span>
          <h2>Know the tricks before they reach you</h2>
          <p className="muted">Short guides to the scams people in India report most often, and what to do if one gets through.</p>
        </div>
        <div className="learn-grid">
          <div className="articles">{ARTICLES.map((a) => <ArticleCard key={a.slug} a={a} />)}</div>
          <Quiz />
        </div>
      </section>

      <HelpPanel />

      <section className="block doors">
        <div className="block-head">
          <span className="eyebrow">Sign in</span>
          <h2>One sign-in, the right view for your role</h2>
        </div>
        <div className="door-grid">
          <a className="door" href="/console" onClick={onNav("/console")}>
            <b>Fraud team</b><p>Analysts and administrators: live feed, alert queue, checks, settings and people.</p><span>Open console →</span>
          </a>
          <a className="door" href="/console" onClick={onNav("/console")}>
            <b>Account holders</b><p>See your recent payments and report one that was not you or was a scam.</p><span>Customer sign-in →</span>
          </a>
          <a className="door" href="/console" onClick={onNav("/console")}>
            <b>Platform operators</b><p>Create and suspend organisations and watch every deployment's health.</p><span>Operator sign-in →</span>
          </a>
        </div>
      </section>

      <SiteFooter />
    </div>
  );
}
