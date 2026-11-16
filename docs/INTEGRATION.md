# Integration guide

How a payment system sends transactions to Foresentry and reads the result. The
interactive reference for every endpoint is at `/docs` on the API.

Foresentry has webhook adapters for two payment gateways, Razorpay and Stripe (section 2a). It has none for a
bank, UPI switch or card network: those senders map their own records to the schema below. Every sender must
be authorised to share the data.

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
| `old_balance_orig`, `old_balance_dest` | Balances before the transaction, 0 or more. Leave them out if you do not know them; do not send 0 for "unknown" (see 2a) |
| `channel`, `region`, `device_id` | Optional |

Unknown fields are rejected with 422, so a mapping mistake fails loudly.

## 2. Three ways in

| Route | Use it for | Authentication |
|---|---|---|
| `POST /v1/transactions` | One transaction per request, from your own service | `X-API-Key` |
| `POST /v1/webhooks/generic` | Push from a system that signs its calls | HMAC signature |
| `POST /v1/imports` | A CSV or Parquet file of past transactions | Administrator sign-in |

Each organisation gets its own API key from its administrator (console, API keys, or
`POST /v1/api-keys`). The key decides which organisation a payment belongs to; an `org_id` in the body is
ignored. The same `transaction_id` may be used by two organisations without clashing.

All three answer `202`: the transaction is durably in Kafka and will be scored. They do not return the score.
If Kafka is unavailable they answer `503` and nothing is accepted, so the caller should retry with the same
`transaction_id`.

```bash
curl -X POST https://risk.example.com/v1/transactions \
  -H "X-API-Key: $FORESENTRY_API_KEY" -H "Content-Type: application/json" -d @transaction.json
```

### Signed webhook

Body: `{"event_id": "...", "type": "transaction.created", "data": { the transaction }}`.
Headers: `X-Foresentry-Timestamp` (Unix seconds) and `X-Foresentry-Signature`, the hex HMAC-SHA256 of
`<timestamp>.<raw body>` with the shared webhook secret. Calls more than 5 minutes old are rejected.

```python
import hashlib, hmac, json, time, urllib.request
body = json.dumps({"event_id": "evt-1", "type": "transaction.created", "data": txn}).encode()
ts = str(int(time.time()))
sig = hmac.new(SECRET.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()
urllib.request.urlopen(urllib.request.Request(
    "https://risk.example.com/v1/webhooks/generic", data=body,
    headers={"Content-Type": "application/json", "X-Foresentry-Timestamp": ts, "X-Foresentry-Signature": sig}))
```

## 2a. Payment-gateway connectors

| Gateway | Webhook URL to register | Secret setting | Events scored |
|---|---|---|---|
| Razorpay | `https://<your host>/v1/connectors/razorpay/webhook` | `RAZORPAY_WEBHOOK_SECRET` | `payment.authorized`, `payment.captured` |
| Stripe | `https://<your host>/v1/connectors/stripe/webhook` | `STRIPE_WEBHOOK_SECRET` (`whsec_...`) | `charge.succeeded`, `charge.captured` |

1. In the gateway's dashboard, create a webhook to the URL above for those events. It shows a signing secret.
2. Put that secret in `.env` and restart the API. The connector is off while its secret is empty.
3. `GET /v1/connectors` (and the dashboard's Integrations page) shows each connector: on or off, last event,
   events in the last 24 hours and the last error, such as a bad signature.

What the adapters do:

- Check the signature on the raw body: Razorpay's `X-Razorpay-Signature` (HMAC-SHA256 of the body), Stripe's
  `Stripe-Signature` (HMAC-SHA256 of `<timestamp>.<body>`, refused outside 5 minutes). A bad signature is `400`.
- Score only payments in `CONNECTOR_CURRENCIES` (default `INR`; amounts are not converted). Other events,
  refunds and other currencies answer `200 ignored`, so the gateway does not retry them.
- Use the payment id as `transaction_id`. Razorpay sends `authorized` and then `captured` for one payment; the
  second is recognised as a duplicate.
- Payer: the gateway's customer id, else UPI address, card, phone or e-mail, stored only as a keyed digest
  (HMAC with `SECRET_KEY`). Changing `SECRET_KEY` changes every payer's digest and so resets their history.
- Merchant (`counterparty_id`): the Razorpay `account_id` or Stripe connected account.
- No balances and no device: gateways do not send them.

**What that means for the score.** The deployed model was trained with balances and leans on them. Sending 0
for an unknown balance made every payment over about Rs 25,000 score as critical in a check on the deployed
model. Unknown balances are therefore left out: the three balance features are missing, the balance rules do
not fire, and because the model without balances scores almost everything near zero, the rules set the floor
for these events. Measured on the running stack: a 5th payment of Rs 9,000 from one UPI address within
7 seconds scored 0.35 (MEDIUM, rapid transactions); a first Rs 95,000 payment scored 0.20 (LOW, new
counterparty, high amount). In effect gateway payments are scored by the rules plus velocity. A useful model
for them needs training on the merchant's own gateway history with confirmed fraud labels.

Razorpay's `created_at` is in whole seconds, so payments from one payer in the same second do not count
towards each other's velocity.

Tested with payloads in each gateway's documented format and signing scheme, through the running stack. Not
tested against a live Razorpay or Stripe account: that needs the merchant's own account.

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
ingestion in direct mode and about 630 ms in Spark mode (median, at 51 events per second). Foresentry is built to
raise alerts for review after a payment is submitted. It has no synchronous approve-or-decline call, so it
cannot sit inside a payment authorisation as shipped.

The risk score is a model estimate for a reviewer. It is not a banking or regulatory decision.

## 3a. Account holders

A person with the `CUSTOMER` role is linked to one or more `account_id`s at one organisation.
`GET /v1/me/transactions` lists their payments with a plain status (`OK`, `UNDER_REVIEW`,
`CONFIRMED_FRAUD`) and no score. `POST /v1/me/reports` with `{"transaction_id", "kind": "NOT_ME" | "SCAM" |
"OTHER", "note"}` opens or joins an alert (`REPORTED_BY_CUSTOMER`) for the organisation's analysts.
Reporting the same payment again returns the first report. A payment that is not on their accounts is 404.

`GET /v1/public/summary` needs no sign-in and gives platform-wide totals for the last 24 hours (payments,
share flagged, median pipeline time, number of organisations), cached for 30 seconds, with
`"simulated": true` in the demo environment. Nothing in it identifies an organisation, account or amount.

## 4. Feeding outcomes back

Confirmed and dismissed alerts become labels (`labels` table) with who decided and when. These are the data for
retraining and for measuring real precision. No separate chargeback feed is built.

## 4a. Operational alerts

Set `ALERT_WEBHOOK_URL` to a Slack (or Teams, or any) incoming-webhook URL. Foresentry posts `{"text": ...}` for:

- a new priority-1 fraud alert: its id, score and reason codes, never the account or amount. At most 10 an
  hour; the 11th message says the rest are in the queue.
- an API or scorer node that stopped reporting, and the stream disconnecting: once per 30 minutes per problem,
  however many API replicas notice it.
- a failed backup (the `backup` service).

## 5. Limits to plan around

- Measured maximum: 251.7 transactions per second on a 4-core test machine with one scorer process
  (`reports/loadtest.json`). Your hardware will differ; run `tools/loadtest.py` on it.
- The API rate limit is `RATE_LIMIT_PER_MINUTE` per caller.
- Delivery is at-least-once with idempotent storage. It is not exactly-once.
