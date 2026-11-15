"""Payment-gateway webhooks: Razorpay and Stripe.

Each adapter checks the provider's signature on the raw body, keeps only
completed card/UPI/bank payments, and maps them to a TransactionEvent with
source LIVE_WEBHOOK. Everything after that is the normal pipeline.

What a gateway does not tell us, and what we do about it:

- Who the payer is. We use the provider's customer id, else the UPI address,
  card fingerprint, phone or e-mail, and store only a keyed digest of it
  (HMAC with SECRET_KEY), never the value itself.
- The payer's balance. Gateways do not know it. The event carries none, so
  the three balance features are missing and the balance rules do not fire
  (reading "unknown" as "zero" made every payment over about Rs 25,000
  critical). `balance_known` is false in the connector's status. The model
  was trained with balances; see docs/INTEGRATION.md.
- The device. Not sent by either provider; left empty.

Turning a connector on needs the merchant's own provider account: set
RAZORPAY_WEBHOOK_SECRET or STRIPE_WEBHOOK_SECRET to the secret the provider
shows when the webhook is created. Nothing here was tested against a live
provider account; the tests use the providers' documented signing scheme.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import UTC, datetime

from fraudlib.schemas import Source, TransactionEvent, TxnType

STRIPE_TOLERANCE_S = 300
# Stripe amounts are in the smallest unit; these currencies have none.
_ZERO_DECIMAL = {"BIF", "CLP", "DJF", "GNF", "JPY", "KMF", "KRW", "MGA", "PYG", "RWF", "UGX", "VND", "VUV",
                 "XAF", "XOF", "XPF"}
_CHANNELS = {"upi": "UPI", "card": "CARD", "netbanking": "NETBANKING", "wallet": "WALLET", "emi": "CARD",
             "bank_transfer": "BANK", "paylater": "PAYLATER", "us_bank_account": "BANK", "sepa_debit": "BANK"}

RAZORPAY_EVENTS = {"payment.authorized", "payment.captured"}
STRIPE_EVENTS = {"charge.succeeded", "charge.captured"}


class Rejected(Exception):
    """The request is not a genuine webhook from the provider (HTTP 400)."""


class Ignored(Exception):
    """A genuine webhook we do not score, such as a refund or another currency (HTTP 200)."""


def pseudonym(secret_key: str, provider: str, value: str) -> str:
    digest = hmac.new(secret_key.encode(), f"{provider}|{value}".encode(), hashlib.sha256).hexdigest()
    return f"{provider[:3]}_{digest[:24]}"


def _channel(method: str | None) -> str:
    return _CHANNELS.get((method or "").lower(), (method or "OTHER").upper()[:16])


# ---- Razorpay ------------------------------------------------------------------------
def verify_razorpay(secret: str, body: bytes, signature: str | None) -> None:
    """X-Razorpay-Signature is the hex HMAC-SHA256 of the raw body with the webhook secret."""
    if not signature:
        raise Rejected("missing X-Razorpay-Signature")
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature.strip()):
        raise Rejected("signature mismatch")


def map_razorpay(body: bytes, secret_key: str, currencies: tuple[str, ...]) -> TransactionEvent:
    try:
        event = json.loads(body)
        kind = event["event"]
    except (ValueError, KeyError, TypeError):
        raise Rejected("not a Razorpay event") from None
    if kind not in RAZORPAY_EVENTS:
        raise Ignored(f"{kind} is not scored")
    try:
        p = event["payload"]["payment"]["entity"]
        payment_id, amount, currency, created = p["id"], int(p["amount"]), p["currency"].upper(), int(p["created_at"])
    except (KeyError, TypeError, ValueError, AttributeError):
        raise Rejected("payment entity is missing id, amount, currency or created_at") from None
    if currency not in currencies:
        raise Ignored(f"currency {currency} is not scored (CONNECTOR_CURRENCIES)")
    upi = p.get("upi") or {}
    payer = (p.get("customer_id") or upi.get("vpa") or p.get("vpa") or p.get("card_id")
             or p.get("contact") or p.get("email"))
    if not payer:
        raise Ignored("payment has no payer identifier")
    merchant = event.get("account_id") or "razorpay-merchant"
    return TransactionEvent(
        transaction_id=payment_id[:64], event_time=datetime.fromtimestamp(created, UTC),
        account_id=pseudonym(secret_key, "razorpay", str(payer)), counterparty_id=str(merchant)[:64],
        txn_type=TxnType.PAYMENT, amount=amount / 100, old_balance_orig=None, old_balance_dest=None,
        channel=_channel(p.get("method")), region="UNKNOWN", device_id=None)


# ---- Stripe --------------------------------------------------------------------------
def verify_stripe(secret: str, body: bytes, header: str | None, now: float | None = None) -> None:
    """Stripe-Signature: t=<unix>,v1=<hex HMAC-SHA256 of "<t>.<body>">[,v1=...]. Five-minute window."""
    if not header:
        raise Rejected("missing Stripe-Signature")
    parts: dict[str, list[str]] = {}
    for item in header.split(","):
        k, _, v = item.strip().partition("=")
        parts.setdefault(k, []).append(v)
    try:
        ts = int(parts["t"][0])
    except (KeyError, ValueError):
        raise Rejected("Stripe-Signature has no timestamp") from None
    if abs((time.time() if now is None else now) - ts) > STRIPE_TOLERANCE_S:
        raise Rejected("timestamp outside the allowed window (replay protection)")
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    if not any(hmac.compare_digest(expected, sig) for sig in parts.get("v1", [])):
        raise Rejected("signature mismatch")


def map_stripe(body: bytes, secret_key: str, currencies: tuple[str, ...]) -> TransactionEvent:
    try:
        event = json.loads(body)
        kind = event["type"]
    except (ValueError, KeyError, TypeError):
        raise Rejected("not a Stripe event") from None
    if kind not in STRIPE_EVENTS:
        raise Ignored(f"{kind} is not scored")
    try:
        c = event["data"]["object"]
        charge_id, amount, currency, created = c["id"], int(c["amount"]), c["currency"].upper(), int(c["created"])
    except (KeyError, TypeError, ValueError, AttributeError):
        raise Rejected("charge is missing id, amount, currency or created") from None
    if currency not in currencies:
        raise Ignored(f"currency {currency} is not scored (CONNECTOR_CURRENCIES)")
    details = c.get("payment_method_details") or {}
    method = details.get("type")
    card = details.get("card") or {}
    payer = (c.get("customer") or card.get("fingerprint") or (c.get("billing_details") or {}).get("email")
             or c.get("receipt_email"))
    if not payer:
        raise Ignored("charge has no payer identifier")
    units = 1 if currency in _ZERO_DECIMAL else 100
    return TransactionEvent(
        transaction_id=charge_id[:64], event_time=datetime.fromtimestamp(created, UTC),
        account_id=pseudonym(secret_key, "stripe", str(payer)),
        counterparty_id=str(event.get("account") or "stripe-merchant")[:64],
        txn_type=TxnType.PAYMENT, amount=amount / units, old_balance_orig=None, old_balance_dest=None,
        channel=_channel(method), region="UNKNOWN", device_id=None)


# ---- status, kept in Redis so every API replica reports the same -------------------------
CONNECTORS = {
    "razorpay": {"kind": "webhook", "path": "/v1/connectors/razorpay/webhook", "setting": "razorpay_webhook_secret",
                 "balance_known": False},
    "stripe": {"kind": "webhook", "path": "/v1/connectors/stripe/webhook", "setting": "stripe_webhook_secret",
               "balance_known": False},
    "generic": {"kind": "webhook", "path": "/v1/webhooks/generic", "setting": "webhook_secret",
                "balance_known": True},
}


def record(redis, name: str, error: str | None = None) -> None:
    now = datetime.now(UTC)
    try:
        if error is None:
            hour = int(now.timestamp() // 3600)
            p = redis.pipeline(transaction=False)
            p.set(f"conn:{name}:last_event_at", now.isoformat())
            p.incr(f"conn:{name}:h:{hour}")
            p.expire(f"conn:{name}:h:{hour}", 26 * 3600)
            p.execute()
        else:
            redis.set(f"conn:{name}:last_error", json.dumps({"at": now.isoformat(), "error": error[:300]}))
    except Exception:  # noqa: BLE001 - status is best effort; never fail the webhook for it
        pass


def status(redis, settings) -> list[dict]:
    hour = int(time.time() // 3600)
    out = []
    for name, meta in CONNECTORS.items():
        row = {"name": name, "kind": meta["kind"], "path": meta["path"],
               "enabled": bool(getattr(settings, meta["setting"])), "balance_known": meta["balance_known"],
               "last_event_at": None, "events_24h": 0, "last_error": None, "last_error_at": None}
        try:
            p = redis.pipeline(transaction=False)
            p.get(f"conn:{name}:last_event_at")
            p.get(f"conn:{name}:last_error")
            p.mget([f"conn:{name}:h:{h}" for h in range(hour - 23, hour + 1)])
            last, err, counts = p.execute()
            row["last_event_at"] = last.decode() if isinstance(last, bytes) else last
            if err:
                e = json.loads(err)
                row["last_error"], row["last_error_at"] = e["error"], e["at"]
            row["events_24h"] = sum(int(c) for c in counts if c)
        except Exception:  # noqa: BLE001
            row["last_error"] = "status unavailable (Redis unreachable)"
        out.append(row)
    return out


SOURCE = Source.LIVE_WEBHOOK
