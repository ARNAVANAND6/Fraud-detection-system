"""Synthetic payment event generator.

It simulates a PaySim-like population (accounts with their own spending
habits) and injects labelled fraud scenarios plus data-quality "chaos":
duplicates, late events and malformed records at known rates. Known rates are
what let the pipeline tests prove dedup, late handling and the DLQ work.

Everything here is synthetic. Results on this data say nothing about real
Indian UPI fraud.

Modes:
  kafka  stream events to Kafka in (accelerated) real time
  csv    write a fixed number of events to CSV for offline training
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "libs"))
from fraudlib.config import get_settings
from fraudlib.schemas import TransactionEvent, TxnType

log = logging.getLogger("generator")

REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]  # synthetic regions only
NORMAL_TYPE_WEIGHTS = {
    TxnType.PAYMENT: 0.45, TxnType.TRANSFER: 0.15, TxnType.CASH_OUT: 0.2,
    TxnType.CASH_IN: 0.17, TxnType.DEBIT: 0.03,
}
CSV_FIELDS = [f for f in TransactionEvent.model_fields if f not in ("source", "ingested_at")] + ["is_fraud", "scenario"]


@dataclass
class Account:
    account_id: str
    balance: float
    mean_amount: float
    region: str
    device_id: str
    payees: list[str] = field(default_factory=list)


@dataclass
class Config:
    n_accounts: int = 5000
    fraud_rate: float = 0.004       # share of "ticks" that start a fraud scenario
    duplicate_rate: float = 0.01
    late_rate: float = 0.01
    malformed_rate: float = 0.002
    seed: int = 42


class Simulator:
    def __init__(self, cfg: Config, start: datetime):
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.clock = start
        self.accounts = [self._new_account(i) for i in range(cfg.n_accounts)]
        self.mules = [f"M{self.rng.randrange(10**9):09d}" for _ in range(max(5, cfg.n_accounts // 500))]
        self.merchants = [f"MER{i:05d}" for i in range(500)]

    def _new_account(self, i: int) -> Account:
        r = self.rng
        acct = Account(
            account_id=f"C{i:08d}",
            balance=round(r.lognormvariate(10, 1.2), 2),
            mean_amount=round(r.lognormvariate(7.5, 1.0), 2),
            region=r.choice(REGIONS),
            device_id=f"D{r.randrange(10**8):08d}",
        )
        acct.payees = [f"C{r.randrange(self.cfg.n_accounts):08d}" for _ in range(r.randint(2, 8))]
        return acct

    # ---- event construction -------------------------------------------------
    def _event(self, acct: Account, txn_type: TxnType, amount: float, dest: str,
               when: datetime, device: str | None = None, dest_balance: float | None = None) -> dict:
        amount = round(max(1.0, amount), 2)
        e = {
            "schema_version": 1,
            "transaction_id": f"TXN_{uuid.UUID(int=self.rng.getrandbits(128)).hex[:20]}",
            "event_time": when.isoformat(),
            "account_id": acct.account_id,
            "counterparty_id": dest,
            "txn_type": txn_type.value,
            "amount": amount,
            "old_balance_orig": round(acct.balance, 2),
            "old_balance_dest": round(dest_balance if dest_balance is not None
                                      else self.rng.lognormvariate(9, 1.5), 2),
            "channel": self.rng.choice(["APP", "APP", "APP", "WEB", "USSD"]),
            "region": acct.region,
            "device_id": device or acct.device_id,
        }
        if txn_type in (TxnType.CASH_IN,):
            acct.balance += amount
        else:
            acct.balance = max(0.0, acct.balance - amount)
        return e

    def _normal(self) -> list[tuple[dict, bool, str | None]]:
        r = self.rng
        acct = r.choice(self.accounts)
        t = r.choices(list(NORMAL_TYPE_WEIGHTS), weights=list(NORMAL_TYPE_WEIGHTS.values()))[0]
        amount = r.lognormvariate(0, 0.6) * acct.mean_amount
        if t == TxnType.PAYMENT:
            dest = r.choice(self.merchants)
        elif t == TxnType.TRANSFER and r.random() < 0.9:
            dest = r.choice(acct.payees)
        else:
            dest = f"C{r.randrange(self.cfg.n_accounts):08d}"
        if t != TxnType.CASH_IN:
            amount = min(amount, max(acct.balance * 0.8, 1.0))
        # Legitimate look-alikes. Without them every fraud signal is a perfect
        # separator and the model learns the simulator, not fraud.
        device = None
        u = r.random()
        if u < 0.006:                       # rent, fees: a big but honest payment
            amount = acct.mean_amount * r.uniform(8, 25)
            acct.balance = max(acct.balance, amount * 1.2)
        elif u < 0.010 and t in (TxnType.TRANSFER, TxnType.CASH_OUT):
            amount = acct.balance            # moving all savings to one's own account
        elif u < 0.016:
            device = f"D{r.randrange(10**8):08d}"  # new phone
        elif u < 0.020:                     # a quick run of honest payments
            out = []
            for k in range(r.randint(3, 5)):
                when = self.clock + timedelta(seconds=k * r.uniform(3, 15))
                out.append((self._event(acct, TxnType.PAYMENT, r.lognormvariate(0, 0.5) * acct.mean_amount,
                                        r.choice(self.merchants), when), False, None))
            return out
        return [(self._event(acct, t, amount, dest, self.clock, device), False, None)]

    def _fraud(self) -> list[tuple[dict, bool, str]]:
        r = self.rng
        acct = r.choice(self.accounts)
        scenario = r.choice(["ACCOUNT_TAKEOVER", "RAPID_BURST", "MULE_FANOUT", "UNUSUAL_AMOUNT"])
        out = []
        if scenario == "ACCOUNT_TAKEOVER":
            # New device, drain the balance to a mule, mule cashes out.
            # Careful fraudsters reuse the victim's device half the time and do not always drain fully.
            device = f"D{r.randrange(10**8):08d}" if r.random() < 0.5 else None
            mule = r.choice(self.mules + acct.payees[:1])
            amt = acct.balance * r.uniform(0.5, 1.0)
            out.append((self._event(acct, TxnType.TRANSFER, amt, mule, self.clock, device,
                                    0.0 if r.random() < 0.6 else None), True, scenario))
        elif scenario == "RAPID_BURST":
            device = f"D{r.randrange(10**8):08d}"
            for k in range(r.randint(4, 8)):
                when = self.clock + timedelta(seconds=k * r.uniform(2, 8))
                amt = r.uniform(0.05, 0.2) * max(acct.balance, 1000)
                ev = self._event(acct, TxnType.TRANSFER, amt, r.choice(self.mules), when, device)
                out.append((ev, True, scenario))
        elif scenario == "MULE_FANOUT":
            mule = r.choice(self.mules)
            for k in range(r.randint(3, 6)):
                victim = r.choice(self.accounts)
                when = self.clock + timedelta(seconds=k * r.uniform(10, 60))
                out.append((self._event(victim, TxnType.TRANSFER, victim.balance * r.uniform(0.6, 1.0),
                                        mule, when, dest_balance=0.0), True, scenario))
        else:
            amt = acct.mean_amount * r.uniform(10, 30)
            acct.balance = max(acct.balance, amt * 1.1)
            out.append((self._event(acct, TxnType.CASH_OUT, amt, f"C{r.randrange(self.cfg.n_accounts):08d}",
                                    self.clock), True, scenario))
        return out

    def tick(self, seconds: float) -> list[tuple[dict, bool, str | None]]:
        """Advance the simulated clock and return the transactions it produced."""
        self.clock += timedelta(seconds=seconds)
        if self.rng.random() < 0.0005:  # top up some balances so accounts don't all drain
            for a in self.rng.sample(self.accounts, k=min(50, len(self.accounts))):
                a.balance += self.rng.lognormvariate(9, 1)
        if self.rng.random() < self.cfg.fraud_rate:
            return self._fraud()
        return self._normal()

    # ---- chaos -----------------------------------------------------------------
    def chaos(self, event: dict) -> tuple[list[bytes], dict[str, int]]:
        """Return the raw payloads to send for one event, plus chaos counters."""
        r, stats = self.rng, {"duplicates": 0, "late": 0, "malformed": 0}
        if r.random() < self.cfg.late_rate:
            event = {**event, "event_time": (datetime.fromisoformat(event["event_time"])
                                             - timedelta(minutes=r.uniform(2, 30))).isoformat()}
            stats["late"] = 1
        payloads = [json.dumps(event).encode()]
        if r.random() < self.cfg.duplicate_rate:
            payloads.append(payloads[0])
            stats["duplicates"] = 1
        if r.random() < self.cfg.malformed_rate:
            bad = dict(event)
            choice = r.randrange(3)
            if choice == 0:
                bad.pop("amount")
            elif choice == 1:
                bad["amount"] = -abs(bad["amount"])
            else:
                payloads.append(b"{not valid json")
                stats["malformed"] = 1
                return payloads, stats
            bad["transaction_id"] += "_BAD"
            payloads.append(json.dumps(bad).encode())
            stats["malformed"] = 1
        return payloads, stats


def run_csv(sim: Simulator, n: int, out: Path, rate: float) -> None:
    # Ticks are spaced like kafka mode at the same --rate. Training on a
    # different pace than the live stream makes every velocity feature shift
    # (the first end-to-end run showed exactly that), so the two must match.
    out.parent.mkdir(parents=True, exist_ok=True)
    written = frauds = 0
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        while written < n:
            for event, is_fraud, scenario in sim.tick(sim.rng.expovariate(rate)):
                w.writerow({**event, "is_fraud": int(is_fraud), "scenario": scenario or ""})
                written += 1
                frauds += int(is_fraud)
    log.info("wrote %d events (%d fraud, %.3f%%) to %s", written, frauds, 100 * frauds / written, out)


def run_kafka(sim: Simulator, rate: float, max_events: int | None) -> None:
    from confluent_kafka import Producer

    s = get_settings()
    if s.environment != "demo":
        raise SystemExit("The synthetic generator only runs in the demo environment (ENVIRONMENT=demo).")
    producer = Producer({
        "bootstrap.servers": s.kafka_bootstrap,
        "enable.idempotence": True,   # no duplicates from producer retries
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
    })
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    totals = {"events": 0, "sent": 0, "fraud": 0, "duplicates": 0, "late": 0, "malformed": 0}
    interval = 1.0 / rate
    next_t = time.perf_counter()
    last_report = time.time()
    while running and (max_events is None or totals["events"] < max_events):
        # Simulated time follows wall-clock time, so event_time is close to "now".
        sim.clock = datetime.now(UTC)
        for event, is_fraud, scenario in sim.tick(0):
            # Every generated event is labelled as simulated, with its send time.
            event["source"] = "SIMULATED_DEMO"
            event["ingested_at"] = datetime.now(UTC).isoformat()
            payloads, stats = sim.chaos(event)
            for p in payloads:
                producer.produce(s.topic_raw, key=event["account_id"], value=p)
            label = {"transaction_id": event["transaction_id"], "is_fraud": is_fraud,
                     "scenario": scenario, "label_time": datetime.now(UTC).isoformat()}
            producer.produce(s.topic_labels, key=event["transaction_id"], value=json.dumps(label))
            totals["events"] += 1
            totals["sent"] += len(payloads)
            totals["fraud"] += int(is_fraud)
            for k, v in stats.items():
                totals[k] += v
        producer.poll(0)
        next_t += interval
        delay = next_t - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        if time.time() - last_report > 10:
            log.info("progress %s", json.dumps(totals))
            last_report = time.time()
    producer.flush(30)
    # The chaos counts are the ground truth the reconciliation test compares against.
    log.info("final %s", json.dumps(totals))
    Path("reports").mkdir(exist_ok=True)
    Path("reports/generator_last_run.json").write_text(json.dumps(totals, indent=2))


def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["kafka", "csv"], default="kafka")
    ap.add_argument("--rate", type=float, default=50, help="ticks per second; csv mode uses it to space simulated time")
    ap.add_argument("--max-events", type=int, default=None)
    ap.add_argument("--n", type=int, default=200_000, help="events to write (csv mode)")
    ap.add_argument("--out", type=Path, default=Path("data/synthetic/transactions.csv"))
    ap.add_argument("--accounts", type=int, default=5000)
    ap.add_argument("--fraud-rate", type=float, default=0.004)
    ap.add_argument("--duplicate-rate", type=float, default=0.01)
    ap.add_argument("--late-rate", type=float, default=0.01)
    ap.add_argument("--malformed-rate", type=float, default=0.002)
    ap.add_argument("--seed", type=int, default=None,
                    help="default: 42 in csv mode (reproducible training data), random in kafka mode "
                         "(a restarted stream must not re-send the same transaction ids)")
    a = ap.parse_args()
    if a.seed is None:
        a.seed = 42 if a.mode == "csv" else random.SystemRandom().randrange(2**32)
    cfg = Config(a.accounts, a.fraud_rate, a.duplicate_rate, a.late_rate, a.malformed_rate, a.seed)
    sim = Simulator(cfg, start=datetime(2026, 1, 1, tzinfo=UTC))
    if a.mode == "csv":
        run_csv(sim, a.n, a.out, a.rate)
    else:
        run_kafka(sim, a.rate, a.max_events)


if __name__ == "__main__":
    main()
