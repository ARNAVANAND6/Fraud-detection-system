"""Razorpay and Stripe webhooks, connector status and operational alerts.

The payloads follow the providers' documented webhook formats and signing
schemes. They were not sent by a live provider account.
"""
import hashlib
import hmac
import json
import time

import fakeredis
import pytest
from test_api import PASSWORD, make_client

from services.scorer.app import connectors, notify
from services.scorer.app.main import app

RZP_SECRET, STRIPE_SECRET = "rzp-webhook-secret", "whsec_test_secret"


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("RAZORPAY_WEBHOOK_SECRET", RZP_SECRET)
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", STRIPE_SECRET)
    with make_client(monkeypatch, tmp_path) as c:
        yield c
    for attr in ("redis", "store", "scorer", "repo", "gateway"):
        delattr(app.state, attr)


def razorpay_body(event="payment.authorized", **payment):
    entity = {"id": "pay_29QQoUBi66xm2f", "entity": "payment", "amount": 250000, "currency": "INR",
              "status": "authorized", "method": "upi", "vpa": "someone@okbank", "email": "a@b.example",
              "contact": "+919000000000", "created_at": int(time.time()) - 2, **payment}
    return json.dumps({"entity": "event", "account_id": "acc_BFQ7uQEaa7j2z7", "event": event,
                       "contains": ["payment"], "payload": {"payment": {"entity": entity}},
                       "created_at": int(time.time())}).encode()


def rzp_sign(body, secret=RZP_SECRET):
    return {"X-Razorpay-Signature": hmac.new(secret.encode(), body, hashlib.sha256).hexdigest(),
            "Content-Type": "application/json"}


def stripe_body(kind="charge.succeeded", **charge):
    obj = {"id": "ch_3MmlLrLkdIwHu7ix0snN0B15", "object": "charge", "amount": 149900, "currency": "inr",
           "created": int(time.time()) - 1, "customer": "cus_NffrFeUfNV2Hib",
           "payment_method_details": {"type": "card", "card": {"fingerprint": "Xt5EWLLDS7FJjR1c"}}, **charge}
    return json.dumps({"id": "evt_1", "object": "event", "type": kind, "data": {"object": obj}}).encode()


def stripe_sign(body, secret=STRIPE_SECRET, ts=None):
    ts = int(time.time()) if ts is None else ts
    sig = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return {"Stripe-Signature": f"t={ts},v1={sig}", "Content-Type": "application/json"}


def produced(c):
    return [m for t, _, m in app.state.gateway.producer.messages]


def test_razorpay_payment_is_scored_with_a_pseudonymous_payer(client):
    body = razorpay_body()
    r = client.post("/v1/connectors/razorpay/webhook", content=body, headers=rzp_sign(body))
    assert r.status_code == 200 and r.json()["status"] == "accepted", r.text
    [event] = produced(client)
    assert event["transaction_id"] == "pay_29QQoUBi66xm2f" and event["amount"] == 2500.0
    assert event["source"] == "LIVE_WEBHOOK" and event["channel"] == "UPI" and event["txn_type"] == "PAYMENT"
    assert event["account_id"].startswith("raz_") and "someone" not in json.dumps(event)   # no raw UPI address
    assert event["counterparty_id"] == "acc_BFQ7uQEaa7j2z7"
    # The later "captured" webhook for the same payment is recognised, not scored twice.
    body2 = razorpay_body("payment.captured", status="captured")
    again = client.post("/v1/connectors/razorpay/webhook", content=body2, headers=rzp_sign(body2))
    assert again.json()["status"] == "duplicate" and len(produced(client)) == 1


@pytest.mark.parametrize("headers", [{}, {"X-Razorpay-Signature": "0" * 64}])
def test_razorpay_refuses_unsigned_or_forged_bodies(client, headers):
    body = razorpay_body()
    assert client.post("/v1/connectors/razorpay/webhook", content=body, headers=headers).status_code == 400
    forged = rzp_sign(body, "someone-elses-secret")
    assert client.post("/v1/connectors/razorpay/webhook", content=body, headers=forged).status_code == 400
    assert produced(client) == []


def test_razorpay_ignores_what_it_does_not_score(client):
    for body in (razorpay_body("refund.created"), razorpay_body(currency="USD")):
        r = client.post("/v1/connectors/razorpay/webhook", content=body, headers=rzp_sign(body))
        assert r.status_code == 200 and r.json()["status"] == "ignored"
    assert produced(client) == []


def test_stripe_charge_is_scored(client):
    body = stripe_body()
    r = client.post("/v1/connectors/stripe/webhook", content=body, headers=stripe_sign(body))
    assert r.status_code == 200 and r.json()["status"] == "accepted", r.text
    [event] = produced(client)
    assert event["amount"] == 1499.0 and event["channel"] == "CARD" and event["account_id"].startswith("str_")


def test_stripe_zero_decimal_currency(client, monkeypatch):
    s = app.state.settings
    app.state.settings = s.__class__(**{**s.__dict__, "connector_currencies": ("JPY",)})
    body = stripe_body(currency="jpy", amount=5000)
    client.post("/v1/connectors/stripe/webhook", content=body, headers=stripe_sign(body))
    assert produced(client)[0]["amount"] == 5000.0


def test_stripe_refuses_old_or_forged_signatures(client):
    body = stripe_body()
    old = stripe_sign(body, ts=int(time.time()) - 600)
    assert client.post("/v1/connectors/stripe/webhook", content=body, headers=old).status_code == 400
    forged = stripe_sign(body, secret="whsec_other")
    assert client.post("/v1/connectors/stripe/webhook", content=body, headers=forged).status_code == 400
    tampered = stripe_sign(body)
    assert client.post("/v1/connectors/stripe/webhook", content=body + b" ", headers=tampered).status_code == 400
    assert produced(client) == []


def test_connector_status_counts_events_and_shows_the_last_error(client):
    token = client.post("/v1/auth/login", json={"username": "vik", "password": PASSWORD}).json()["token"]
    body = razorpay_body()
    client.post("/v1/connectors/razorpay/webhook", content=body, headers=rzp_sign(body))
    client.post("/v1/connectors/stripe/webhook", content=body, headers={"Stripe-Signature": "t=1,v1=00"})
    out = client.get("/v1/connectors", headers={"Authorization": f"Bearer {token}"}).json()
    rows = {r["name"]: r for r in out["connectors"]}
    assert rows["razorpay"]["enabled"] and rows["razorpay"]["events_24h"] == 1 and rows["razorpay"]["last_event_at"]
    assert rows["razorpay"]["balance_known"] is False
    assert rows["stripe"]["events_24h"] == 0 and "window" in rows["stripe"]["last_error"]
    assert out["alert_webhook_configured"] is False
    assert client.get("/v1/connectors").status_code == 401


def test_a_connector_without_a_secret_is_off(client):
    s = app.state.settings
    app.state.settings = s.__class__(**{**s.__dict__, "razorpay_webhook_secret": ""})
    body = razorpay_body()
    assert client.post("/v1/connectors/razorpay/webhook", content=body, headers=rzp_sign(body)).status_code == 503


def test_notifications_are_sent_once_per_window_across_replicas():
    r, sent = fakeredis.FakeRedis(), []
    post = lambda url, text: sent.append((url, text))  # noqa: E731
    assert notify.send(r, "https://hooks.example/x", "down:scorer:s1", "scorer down", post=post)
    assert not notify.send(r, "https://hooks.example/x", "down:scorer:s1", "scorer down", post=post)  # other API
    assert notify.send(r, "https://hooks.example/x", "down:api:a1", "api down", post=post)
    assert not notify.send(r, "", "anything", "no url configured", post=post)
    assert [t for _, t in sent] == ["scorer down", "api down"]


def test_fraud_alert_text_has_no_account_or_amount():
    text = notify.fraud_alert_text({"alert_id": "a1", "priority": 1, "max_risk_score": 0.97,
                                    "reason_codes": ["NEW_DEVICE"], "account_id": "ACC123", "amount": 99000},
                                   "https://risk.example")
    assert "a1" in text and "0.97" in text and "ACC123" not in text and "99000" not in text


def test_pseudonyms_are_stable_and_keyed():
    a = connectors.pseudonym("k1" * 20, "razorpay", "someone@okbank")
    assert a == connectors.pseudonym("k1" * 20, "razorpay", "someone@okbank")
    assert a != connectors.pseudonym("k2" * 20, "razorpay", "someone@okbank")


def test_unknown_balance_is_missing_not_zero():
    from datetime import UTC, datetime

    from fraudlib.features import compute_features
    from fraudlib.schemas import TransactionEvent

    from services.scorer.app.repository import _json_safe
    e = TransactionEvent(transaction_id="t", event_time=datetime.now(UTC), account_id="a", counterparty_id="m",
                         txn_type="PAYMENT", amount=95_000)
    f = compute_features(e, [], None, None, 0)
    assert all(f[k] != f[k] for k in ("amount_to_balance", "drains_balance", "dest_balance_zero"))  # NaN
    assert json.loads(json.dumps(_json_safe(f)))["amount_to_balance"] is None    # stored as null, valid JSON
    from fraudlib.rules import evaluate_rules
    assert "BALANCE_DRAIN" not in evaluate_rules(f)[1]


def test_analysts_can_dry_run_a_score(client):
    token = client.post("/v1/auth/login", json={"username": "asha", "password": PASSWORD}).json()["token"]
    body = {"transaction_id": "dry-1", "event_time": "2026-11-15T10:00:00Z", "account_id": "a1",
            "counterparty_id": "m1", "txn_type": "PAYMENT", "amount": 1200}
    r = client.post("/v1/score", json=body, headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200 and 0 <= r.json()["risk_score"] <= 1
    viewer = client.post("/v1/auth/login", json={"username": "vik", "password": PASSWORD}).json()["token"]
    assert client.post("/v1/score", json=body, headers={"Authorization": f"Bearer {viewer}"}).status_code == 403


def test_fraud_alert_messages_are_capped_per_hour():
    r, sent = fakeredis.FakeRedis(), []
    post = lambda url, text: sent.append(text)  # noqa: E731
    results = [notify.send_capped(r, "https://hooks.example/x", f"alert {i}", limit=3, post=post) for i in range(6)]
    assert results == [True, True, True, True, False, False]
    assert sent[:3] == ["alert 0", "alert 1", "alert 2"] and "no more messages" in sent[3]
