# Integration guide

How a payment system sends transactions to VYROX and reads the result. The
interactive reference for every endpoint is at `/docs` on the API.

VYROX ships no adapter for any bank, UPI switch, card network or payment
gateway. The sender must map its own records to the schema below, and must be
authorised to share that data.

## 1. The transaction

```json
{
  "transaction_id": "TXN-2026-000123",
  "event_time": "2026-11-15T09:30:00+05:30",
  "account_id": "ACC-1001",
  "counterparty_id": "ACC-2002",
  "txn_type": "TRANSFER",
  "amount": 48000.0,
  "old_balance_orig": 49000.0,
  "old_balance_dest": 0.0,
  "channel": "APP",
  "region": "SOUTH",
  "device_id": "DEV-77"
}
```

| Field | Rule |
|---|---|
| `transaction_id` | Required, up to 64 characters. It is the idempotency key: sending it twice stores one record |
| `event_time` | Required, with a time zone |
| `account_id`, `counterparty_id` | Required, up to 64 characters. Send stable tokens, not raw account numbers |
| `txn_type` | `PAYMENT`, `TRANSFER`, `CASH_OUT`, `CASH_IN` or `DEBIT` |
| `amount` | Greater than 0 |
| `old_balance_orig`, `old_balance_dest` | Balances before the transaction, 0 or more |
| `channel`, `region`, `device_id` | Optional |

Unknown fields are rejected with 422, so a mapping mistake fails loudly.

## 2. Three ways in

| Route | Use it for | Authentication |
|---|---|---|
| `POST /v1/transactions` | One transaction per request, from your own service | `X-API-Key` |
| `POST /v1/webhooks/generic` | Push from a system that signs its calls | HMAC signature |
| `POST /v1/imports` | A CSV or Parquet file of past transactions | Administrator sign-in |

All three answer `202`: the transaction is durably in Kafka and will be scored. They do not return the score.
If Kafka is unavailable they answer `503` and nothing is accepted, so the caller should retry with the same
`transaction_id`.

```bash
curl -X POST https://risk.example.com/v1/transactions \
  -H "X-API-Key: $VYROX_API_KEY" -H "Content-Type: application/json" -d @transaction.json
```

### Signed webhook

Body: `{"event_id": "...", "type": "transaction.created", "data": { the transaction }}`.
Headers: `X-Vyrox-Timestamp` (Unix seconds) and `X-Vyrox-Signature`, the hex HMAC-SHA256 of
`<timestamp>.<raw body>` with the shared webhook secret. Calls more than 5 minutes old are rejected.

```python
import hashlib, hmac, json, time, urllib.request
body = json.dumps({"event_id": "evt-1", "type": "transaction.created", "data": txn}).encode()
ts = str(int(time.time()))
sig = hmac.new(SECRET.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()
urllib.request.urlopen(urllib.request.Request(
    "https://risk.example.com/v1/webhooks/generic", data=body,
    headers={"Content-Type": "application/json", "X-Vyrox-Timestamp": ts, "X-Vyrox-Signature": sig}))
```

## 3. Getting the result

| Need | Call |
|---|---|
| The stored score for one transaction | `GET /v1/transactions/{transaction_id}` (404 until it is scored) |
| A score without storing anything | `POST /v1/score` with the transaction. It does not update history, so it is for testing, not for decisions |
| A live feed of every scored transaction and alert | `GET /v1/stream` (Server-Sent Events), or consume the scored Kafka topic |
| Open alerts | `GET /v1/alerts` |

A result carries `risk_score` (0 to 1), `risk_level`, `decision` (the configured action for that level),
`reason_codes` and `model_version`.

**Scoring is asynchronous.** Measured in the test environment, a transaction is stored about 30 ms after
ingestion in direct mode and about 630 ms in Spark mode (median, at 51 events per second). VYROX is built to
raise alerts for review after a payment is submitted. It has no synchronous approve-or-decline call, so it
cannot sit inside a payment authorisation as shipped.

The risk score is a model estimate for a reviewer. It is not a banking or regulatory decision.

## 4. Feeding outcomes back

Confirmed and dismissed alerts become labels (`labels` table) with who decided and when. These are the data for
retraining and for measuring real precision. No separate chargeback feed is built.

## 5. Limits to plan around

- Measured maximum: 251.7 transactions per second on a 4-core test machine with one scorer process
  (`reports/loadtest.json`). Your hardware will differ; run `tools/loadtest.py` on it.
- The API rate limit is `RATE_LIMIT_PER_MINUTE` per caller.
- Delivery is at-least-once with idempotent storage. It is not exactly-once.
