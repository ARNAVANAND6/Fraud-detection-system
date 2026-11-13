import random
from datetime import UTC, datetime, timedelta

import fakeredis
import pytest
from fraudlib.features import FEATURE_NAMES, OfflineFeatureBuilder, RedisFeatureStore
from fraudlib.schemas import TransactionEvent

T0 = datetime(2026, 1, 1, 12, tzinfo=UTC)


def ev(i, acct="C1", secs=0, amount=100.0, cp="C2", device="D1", bal=10_000.0, txn_type="TRANSFER"):
    return TransactionEvent(transaction_id=f"T{i}", event_time=T0 + timedelta(seconds=secs), account_id=acct,
                            counterparty_id=cp, txn_type=txn_type, amount=amount, old_balance_orig=bal,
                            old_balance_dest=0, device_id=device)


def test_features_exclude_the_event_itself_and_future_events():
    events = [ev(1, secs=0), ev(2, secs=10), ev(3, secs=20)]
    f = OfflineFeatureBuilder().build(events)
    assert [x["count_60s"] for x in f] == [0, 1, 2]
    assert f[0]["is_new_counterparty"] == 1 and f[1]["is_new_counterparty"] == 0


def test_windows_expire():
    events = [ev(1, secs=0), ev(2, secs=61), ev(3, secs=3601)]
    f = OfflineFeatureBuilder().build(events)
    assert f[1]["count_60s"] == 0 and f[1]["count_1h"] == 1
    assert f[2]["count_1h"] == 1 and f[2]["count_24h"] == 2


def test_offline_order_does_not_depend_on_input_order():
    events = [ev(1, secs=0), ev(2, secs=10), ev(3, secs=20)]
    shuffled = [events[2], events[0], events[1]]
    a = OfflineFeatureBuilder().build(events)
    b = OfflineFeatureBuilder().build(shuffled)
    assert a[2] == b[0] and a[0] == b[1]


def test_all_feature_names_present():
    f = OfflineFeatureBuilder().build([ev(1)])[0]
    assert list(f) == FEATURE_NAMES


def random_events(n=600, seed=7):
    rng = random.Random(seed)
    out, t = [], 0.0
    for i in range(n):
        t += rng.expovariate(1 / 20)
        out.append(ev(i, acct=f"C{rng.randrange(8)}", secs=t, amount=round(rng.uniform(1, 5000), 2),
                      cp=f"P{rng.randrange(15)}", device=rng.choice(["D1", "D2", None])))
    return out


def test_offline_online_parity():
    """The training path and the serving path must produce identical features."""
    events = random_events()
    offline = OfflineFeatureBuilder().build(events)
    store = RedisFeatureStore(fakeredis.FakeRedis())
    online = [store.get_and_update(e) for e in events]  # in time order, as the stream delivers
    for off, on in zip(offline, online, strict=True):
        assert off == pytest.approx(on)


def test_replaying_an_event_is_idempotent():
    events = random_events(100)
    store = RedisFeatureStore(fakeredis.FakeRedis())
    first = [store.get_and_update(e) for e in events]
    replay = [store.get_and_update(e) for e in events]  # consumer restart re-delivers everything
    assert first == replay


def test_dry_run_read_does_not_change_state():
    events = random_events(50)
    store = RedisFeatureStore(fakeredis.FakeRedis())
    for e in events[:-1]:
        store.get_and_update(e)
    a = store.get_features(events[-1])
    assert a == store.get_features(events[-1]) == store.get_and_update(events[-1])


def test_fan_in_counts_distinct_senders_in_the_last_hour():
    events = [ev(i, acct=f"S{i % 5}", secs=i * 10, cp="MULE") for i in range(12)]
    events.append(ev(99, acct="S9", secs=4000, cp="MULE"))  # senders older than 1h have dropped out
    f = OfflineFeatureBuilder().build(events)
    assert [x["dest_senders_1h"] for x in f[:6]] == [0, 1, 2, 3, 4, 5]
    assert f[11]["dest_senders_1h"] == 5
    assert f[12]["dest_senders_1h"] == 0


def test_fan_in_parity_offline_online():
    events = [ev(i, acct=f"S{i % 7}", secs=i * 400, cp=f"M{i % 2}") for i in range(60)]
    store = RedisFeatureStore(fakeredis.FakeRedis())
    online = [store.get_and_update(e)["dest_senders_1h"] for e in events]
    assert online == [x["dest_senders_1h"] for x in OfflineFeatureBuilder().build(events)]
