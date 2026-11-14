"""Feature definitions, written once and used offline and online.

`compute_features` is the single source of truth. Training calls it through
`OfflineFeatureBuilder` (point-in-time replay over history); the scoring
service calls it through `RedisFeatureStore` (history read from Redis). The
parity test in tests/unit/test_features.py runs both on the same events and
asserts identical output.

Point-in-time rule: a transaction's features only use events with a strictly
earlier event time. The transaction itself is never part of its own features.
"""
from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

import numpy as np

from .schemas import TransactionEvent, TxnType

WINDOW_60S = 60_000
WINDOW_1H = 3_600_000
WINDOW_24H = 86_400_000
HISTORY_TTL_S = 2 * 24 * 3600  # keep a little more than the longest window
FAN_IN_TYPES = (TxnType.TRANSFER, TxnType.CASH_OUT)  # payments to merchants are not fan-in
FAN_IN_READ_CAP = 2000  # inbound transfers read per lookup; the count saturates beyond this

FEATURE_NAMES: list[str] = [
    "amount",
    "log_amount",
    "is_transfer",
    "is_cash_out",
    "hour_of_day",
    "amount_to_balance",
    "drains_balance",
    "dest_balance_zero",
    "count_60s",
    "count_1h",
    "count_24h",
    "sum_1h",
    "avg_amount_24h",
    "amount_vs_avg_24h",
    "seconds_since_last",
    "is_new_counterparty",
    "is_new_device",
    "dest_senders_1h",
]


@dataclass(frozen=True)
class HistoryItem:
    ts_ms: int
    amount: float


def compute_features(
    event: TransactionEvent,
    history: Iterable[HistoryItem],
    counterparty_first_seen_ms: int | None,
    device_first_seen_ms: int | None,
    dest_senders_1h: int,
) -> dict[str, float]:
    """Pure function: same inputs give the same features, wherever it runs."""
    now = event.event_ms
    # Strictly earlier events inside the longest window.
    prior = [h for h in history if now - WINDOW_24H <= h.ts_ms < now]

    def in_window(w: int) -> list[HistoryItem]:
        return [h for h in prior if h.ts_ms >= now - w]

    w60, w1h = in_window(WINDOW_60S), in_window(WINDOW_1H)
    count_24h = len(prior)
    avg_24h = sum(h.amount for h in prior) / count_24h if count_24h else 0.0
    last = max((h.ts_ms for h in prior), default=None)

    bal = event.old_balance_orig
    return {
        "amount": event.amount,
        "log_amount": math.log1p(event.amount),
        "is_transfer": float(event.txn_type == TxnType.TRANSFER),
        "is_cash_out": float(event.txn_type == TxnType.CASH_OUT),
        "hour_of_day": float(event.event_time.hour),
        "amount_to_balance": event.amount / (bal + 1.0),
        "drains_balance": float(bal > 0 and event.amount >= 0.95 * bal),
        "dest_balance_zero": float(event.old_balance_dest == 0),
        "count_60s": float(len(w60)),
        "count_1h": float(len(w1h)),
        "count_24h": float(count_24h),
        "sum_1h": sum(h.amount for h in w1h),
        "avg_amount_24h": avg_24h,
        "amount_vs_avg_24h": event.amount / (avg_24h + 1.0) if count_24h else 0.0,
        # -1 means "no earlier transaction in 24h"; trees handle the sentinel fine.
        "seconds_since_last": (now - last) / 1000.0 if last is not None else -1.0,
        "is_new_counterparty": float(
            counterparty_first_seen_ms is None or counterparty_first_seen_ms >= now
        ),
        "is_new_device": float(
            event.device_id is not None
            and (device_first_seen_ms is None or device_first_seen_ms >= now)
        ),
        # Relationship signal: how many different accounts sent a transfer or
        # cash-out to this counterparty in the last hour (money-mule fan-in).
        "dest_senders_1h": float(dest_senders_1h),
    }


class OfflineFeatureBuilder:
    """Point-in-time features for training, replaying events in time order."""

    def build(self, events: list[TransactionEvent]) -> list[dict[str, float]]:
        order = sorted(range(len(events)), key=lambda i: events[i].event_ms)
        history: dict[str, list[HistoryItem]] = defaultdict(list)
        cp_first: dict[tuple[str, str], int] = {}
        dev_first: dict[tuple[str, str], int] = {}
        dest_in: dict[str, list[tuple[int, str]]] = defaultdict(list)  # counterparty -> (ts, sender)
        out: list[dict[str, float] | None] = [None] * len(events)
        for i in order:
            e = events[i]
            now = e.event_ms
            cp_key = (e.account_id, e.counterparty_id)
            dev_key = (e.account_id, e.device_id or "")
            inbound = dest_in[e.counterparty_id]
            while inbound and inbound[0][0] < now - WINDOW_1H:
                inbound.pop(0)  # safe: events are replayed in time order
            out[i] = compute_features(
                e,
                history[e.account_id],
                cp_first.get(cp_key),
                dev_first.get(dev_key) if e.device_id else None,
                len({sender for ts, sender in inbound if ts < now}),
            )
            hist = history[e.account_id]
            hist.append(HistoryItem(now, e.amount))
            if len(hist) > 64 and hist[0].ts_ms < now - WINDOW_24H:
                history[e.account_id] = [h for h in hist if h.ts_ms >= now - WINDOW_24H]
            cp_first.setdefault(cp_key, now)
            if e.device_id:
                dev_first.setdefault(dev_key, now)
            if e.txn_type in FAN_IN_TYPES:
                inbound.append((now, e.account_id))
        return [o for o in out if o is not None]


class _Row:
    """The fields compute_features reads, without the cost of a validated event."""

    __slots__ = ("amount", "txn_type", "event_time", "event_ms", "old_balance_orig", "old_balance_dest", "device_id")


def _first_seen(keys: np.ndarray, ms: np.ndarray) -> np.ndarray:
    """For each row, the earliest event time at which its key appears anywhere."""
    _, inverse = np.unique(keys, return_inverse=True)
    first = np.full(int(inverse.max()) + 1 if len(inverse) else 0, np.iinfo(np.int64).max, dtype=np.int64)
    np.minimum.at(first, inverse, ms)
    return first[inverse]


def build_feature_matrix(
    event_ms: np.ndarray,
    account: np.ndarray,
    counterparty: np.ndarray,
    device: np.ndarray,
    has_device: np.ndarray,
    txn_type: list[TxnType] | np.ndarray,
    amount: np.ndarray,
    old_balance_orig: np.ndarray,
    old_balance_dest: np.ndarray,
    block: int = 100_000,
) -> np.ndarray:
    """Same features as OfflineFeatureBuilder.build, for data too large to hold as event objects.

    Takes one array per column (ids as integers, for example 64-bit hashes of
    the original strings) and returns a float32 matrix, rows in input order,
    columns in FEATURE_NAMES order. Every value still comes from
    compute_features; the difference is only in how history is held:

    - "seen before" for (account, counterparty) and (account, device) is worked
      out up front from the whole column instead of a dict that grows forever;
    - per-account history and per-destination inbound lists are dropped once
      they fall outside the longest window, so memory follows the number of
      accounts active in 24 h, not the size of the dataset.

    tests/unit/test_features.py asserts the output equals OfflineFeatureBuilder.build.
    """
    n = len(event_ms)
    event_ms = np.asarray(event_ms, dtype=np.int64)
    account = np.asarray(account).astype(np.uint64)
    counterparty = np.asarray(counterparty).astype(np.uint64)
    device = np.asarray(device).astype(np.uint64)
    mix = np.uint64(0x9E3779B97F4A7C15)
    with np.errstate(over="ignore"):
        cp_first = _first_seen(account * mix ^ counterparty, event_ms)
        dev_first = _first_seen(account * mix ^ device, event_ms)
    order = np.argsort(event_ms, kind="stable")
    types = list(TxnType)
    type_code = np.asarray([types.index(t) for t in txn_type], dtype=np.int8) if not isinstance(txn_type, np.ndarray) \
        else txn_type.astype(np.int8)
    fan_in = {types.index(t) for t in FAN_IN_TYPES}

    out = np.empty((n, len(FEATURE_NAMES)), dtype=np.float32)
    history: dict[int, list[HistoryItem]] = {}
    dest_in: dict[int, list[tuple[int, int]]] = {}
    row = _Row()
    empty: list = []
    for start in range(0, n, block):
        idx = order[start:start + block]
        cols = zip(idx.tolist(), event_ms[idx].tolist(), account[idx].tolist(), counterparty[idx].tolist(),
                   has_device[idx].tolist(), type_code[idx].tolist(), amount[idx].tolist(),
                   old_balance_orig[idx].tolist(), old_balance_dest[idx].tolist(),
                   cp_first[idx].tolist(), dev_first[idx].tolist(), strict=True)
        for i, now, acct, dest, has_dev, tcode, amt, bal_o, bal_d, cp_seen, dev_seen in cols:
            inbound = dest_in.get(dest)
            if inbound:
                while inbound and inbound[0][0] < now - WINDOW_1H:
                    inbound.pop(0)  # safe: events are replayed in time order
            row.amount, row.txn_type, row.event_ms = amt, types[tcode], now
            row.event_time = datetime.fromtimestamp(now / 1000, UTC)
            row.old_balance_orig, row.old_balance_dest = bal_o, bal_d
            row.device_id = "d" if has_dev else None
            hist = history.get(acct)
            f = compute_features(
                row, hist or empty, cp_seen, dev_seen if has_dev else None,
                len({sender for ts, sender in inbound if ts < now}) if inbound else 0,
            )
            out[i] = [f[name] for name in FEATURE_NAMES]
            if hist is None:
                hist = history[acct] = []
            hist.append(HistoryItem(now, amt))
            if len(hist) > 64 and hist[0].ts_ms < now - WINDOW_24H:
                history[acct] = [h for h in hist if h.ts_ms >= now - WINDOW_24H]
            if tcode in fan_in:
                if inbound is None:
                    inbound = dest_in[dest] = []
                inbound.append((now, acct))
        # Drop state that can no longer affect any later row.
        if len(idx):
            now = int(event_ms[idx[-1]])
            history = {k: v for k, v in history.items() if v[-1].ts_ms >= now - WINDOW_24H}
            dest_in = {k: v for k, v in dest_in.items() if v and v[-1][0] >= now - WINDOW_1H}
    return out


class RedisFeatureStore:
    """Online feature store backed by Redis.

    Keys:
      hist:{acct}   sorted set, member "<txn_id>|<amount>", score = event ms
      cp:{acct}     hash counterparty -> first-seen event ms
      dev:{acct}    hash device -> first-seen event ms
      cpin:{dest}   sorted set, member "<sender>|<txn_id>", score = event ms
                    (transfers and cash-outs only)

    Replaying the same event is idempotent: sorted-set members are the same
    and first-seen times only move earlier. Reads only look at strictly
    earlier event times, so a replayed event gets the features it got the
    first time. Per-account keys are written by one consumer because Kafka is
    keyed by account_id. `cpin` is shared across accounts; with several
    scorer replicas two senders to the same destination can be scored at the
    same moment and each miss the other.
    """

    def __init__(self, redis_client) -> None:
        self.r = redis_client

    def _read(self, event: TransactionEvent):
        now = event.event_ms
        acct = event.account_id
        p = self.r.pipeline(transaction=False)
        p.zrangebyscore(f"hist:{acct}", now - WINDOW_24H, f"({now}", withscores=True)
        p.hget(f"cp:{acct}", event.counterparty_id)
        p.hget(f"dev:{acct}", event.device_id or "-")
        p.zrangebyscore(f"cpin:{event.counterparty_id}", now - WINDOW_1H, f"({now}", start=0, num=FAN_IN_READ_CAP)
        raw_hist, cp_first, dev_first, dest_senders = p.execute()
        history = []
        for member, score in raw_hist:
            m = member.decode() if isinstance(member, bytes) else member
            history.append(HistoryItem(int(score), float(m.rsplit("|", 1)[1])))
        return (
            history,
            int(cp_first) if cp_first is not None else None,
            int(dev_first) if dev_first is not None and event.device_id else None,
            len({(m.decode() if isinstance(m, bytes) else m).split("|", 1)[0] for m in dest_senders}),
        )

    def get_features(self, event: TransactionEvent) -> dict[str, float]:
        return compute_features(event, *self._read(event))

    def get_and_update(self, event: TransactionEvent) -> dict[str, float]:
        now = event.event_ms
        acct = event.account_id
        history, cp_first, dev_first, dest_senders = self._read(event)
        feats = compute_features(event, history, cp_first, dev_first, dest_senders)
        p = self.r.pipeline(transaction=True)
        p.zadd(f"hist:{acct}", {f"{event.transaction_id}|{event.amount}": now})
        # Trim relative to this event; late events never trim newer data.
        p.zremrangebyscore(f"hist:{acct}", "-inf", now - 2 * WINDOW_24H)
        p.expire(f"hist:{acct}", HISTORY_TTL_S)
        if cp_first is None or now < cp_first:
            p.hset(f"cp:{acct}", event.counterparty_id, now)
        if event.device_id and (dev_first is None or now < dev_first):
            p.hset(f"dev:{acct}", event.device_id, now)
        if event.txn_type in FAN_IN_TYPES:
            p.zadd(f"cpin:{event.counterparty_id}", {f"{acct}|{event.transaction_id}": now})
            p.zremrangebyscore(f"cpin:{event.counterparty_id}", "-inf", now - 2 * WINDOW_1H)
            p.expire(f"cpin:{event.counterparty_id}", 3 * 3600)
        p.execute()
        return feats
