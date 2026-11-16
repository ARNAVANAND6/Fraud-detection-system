"""Organisations: each bank sees only its own people, payments and keys; the superadmin sees all."""
from types import SimpleNamespace

import fakeredis
import pytest
from fastapi import HTTPException
from fraudlib.features import RedisFeatureStore
from fraudlib.schemas import TransactionEvent
from test_api import EVENT, PASSWORD, as_user, client  # noqa: F401  (fixture)

from services.scorer.app import tenancy
from services.scorer.app.main import app


def who(role, org="bank-a"):
    return SimpleNamespace(role=role, org_id=None if role == "SUPERADMIN" else org)


def test_scope_keeps_people_inside_their_organisation():
    assert tenancy.scope(who("ADMIN")) == "bank-a"
    assert tenancy.scope(who("ANALYST"), "bank-a") == "bank-a"
    with pytest.raises(HTTPException) as e:
        tenancy.scope(who("ADMIN"), "bank-b")
    assert e.value.status_code == 403
    assert tenancy.scope(who("SUPERADMIN")) is None            # every organisation
    assert tenancy.scope(who("SUPERADMIN"), "bank-b") == "bank-b"


def test_demo_keeps_its_old_redis_keys_and_others_are_prefixed():
    assert tenancy.ns("demo", "C1") == "C1" and tenancy.ns(None, "C1") == "C1"
    assert tenancy.ns("bank-a", "C1") == "bank-a|C1"


def test_the_same_account_id_at_two_banks_has_separate_history():
    store = RedisFeatureStore(fakeredis.FakeRedis())

    def ev(org, i):
        return TransactionEvent(**{**EVENT, "transaction_id": f"{org}-{i}", "org_id": org,
                                   "event_time": f"2026-01-01T00:00:{i:02d}+00:00"})
    for i in range(4):
        store.get_and_update(ev("bank-a", i))
    later_a, later_b = store.get_features(ev("bank-a", 30)), store.get_features(ev("bank-b", 30))
    assert later_a != later_b                       # bank-b's C1 has none of bank-a's C1 history
    assert later_b == store.get_features(ev("bank-c", 30))


def test_organisation_and_customer_endpoints_check_the_role(client):  # noqa: F811
    admin, root = as_user(client), as_user(client, "root", PASSWORD)
    assert client.get("/v1/orgs", headers=admin).status_code == 403
    assert client.post("/v1/orgs", json={"org_id": "bank-b", "name": "B"}, headers=admin).status_code == 403
    # Staff are not account holders, and customers cannot reach staff pages.
    assert client.get("/v1/me/transactions", headers=admin).status_code == 403
    assert client.get("/v1/me/transactions", headers=root).status_code == 403
    # An admin cannot list another bank's keys.
    assert client.get("/v1/api-keys?org_id=bank-b", headers=admin).status_code == 403
    # Nobody is promoted to superadmin, and an admin cannot create one.
    new = {"username": "eve", "password": "long-enough-pass", "role": "SUPERADMIN"}
    assert client.post("/v1/users", json=new, headers=admin).status_code == 403
    assert client.patch("/v1/users/vik", json={"role": "SUPERADMIN"}, headers=root).status_code in (403, 422)
    # A customer needs at least one account to see.
    cust = {"username": "ravi", "password": "long-enough-pass", "role": "CUSTOMER"}
    assert client.post("/v1/users", json=cust, headers=admin).status_code == 422
    r = client.post("/v1/users", json={**cust, "account_ids": ["C1"]}, headers=admin)
    assert r.status_code == 201, r.text
    ravi = as_user(client, "ravi", "long-enough-pass")
    me = client.get("/v1/auth/me", headers=ravi).json()
    assert me["role"] == "CUSTOMER" and me["account_ids"] == ["C1"] and me["org_id"] == "demo"
    for path in ("/v1/alerts", "/v1/transactions/recent", "/v1/users", "/v1/stats"):
        assert client.get(path, headers=ravi).status_code == 403, path


def test_a_bank_key_stamps_its_bank_on_payments_and_stops_when_revoked_or_suspended(client):  # noqa: F811
    root, admin = as_user(client, "root", PASSWORD), as_user(client)
    assert client.post("/v1/orgs", json={"org_id": "bank-b", "name": "Bank B"}, headers=root).status_code == 201
    assert client.post("/v1/orgs", json={"org_id": "bank-b", "name": "Again"}, headers=root).status_code == 409
    assert client.post("/v1/orgs", json={"org_id": "Bad Id!", "name": "X"}, headers=root).status_code == 422
    # The demo admin cannot issue a key for bank-b; the superadmin can.
    assert client.post("/v1/api-keys", json={"name": "x", "org_id": "bank-b"}, headers=admin).status_code == 403
    made = client.post("/v1/api-keys", json={"name": "core banking", "org_id": "bank-b"}, headers=root).json()
    assert made["key"].startswith("fsk_") and made["org_id"] == "bank-b"
    assert all("key" not in k for k in client.get("/v1/api-keys", headers=root).json())   # shown once
    assert client.get("/v1/api-keys", headers=admin).json() == []                         # demo sees none of B's
    bank_b = {"X-API-Key": made["key"]}
    r = client.post("/v1/transactions", json={**EVENT, "transaction_id": "B-1", "org_id": "demo"}, headers=bank_b)
    assert r.status_code == 202, r.text
    assert app.state.gateway.producer.messages[-1][2]["org_id"] == "bank-b"   # the key decides, not the body
    # Suspending the bank stops its key at once; reactivating restores it.
    assert client.patch("/v1/orgs/bank-b", json={"status": "SUSPENDED"}, headers=root).status_code == 200
    assert client.post("/v1/transactions", json={**EVENT, "transaction_id": "B-2"}, headers=bank_b).status_code == 403
    client.patch("/v1/orgs/bank-b", json={"status": "ACTIVE"}, headers=root)
    assert client.delete(f"/v1/api-keys/{made['key_id']}", headers=admin).status_code == 404   # not demo's key
    assert client.delete(f"/v1/api-keys/{made['key_id']}", headers=root).status_code == 204
    assert client.post("/v1/transactions", json={**EVENT, "transaction_id": "B-3"}, headers=bank_b).status_code == 401
