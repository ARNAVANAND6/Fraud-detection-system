# Deployment and operations

How to run Foresentry for a pilot, and how to look after it. The application processes (API and scorer) can
run two or more of each, so losing one does not stop the service (see "Several servers"). Kafka, Redis and
PostgreSQL as shipped are one container each: if one of those stops, scoring stops until it is back
(nothing accepted is lost, see Recovery).

## What you need

- A Linux server with Docker. 4 cores and 8 GB of RAM are enough for direct mode at tens of events per second;
  Spark mode needs about 8 GB free for itself.
- A host name pointing at the server, with ports 80 and 443 reachable, for a public certificate.
- An authorised source of transactions. Foresentry has none built in.

## First start

```bash
cp .env.production.example .env
# In .env set long random values for POSTGRES_PASSWORD (also inside DATABASE_URL), API_KEYS,
# WEBHOOK_SECRET, ADMIN_PASSWORD and SUPERADMIN_PASSWORD, then add:
#   SITE_ADDRESS=risk.example.com
#   DASHBOARD_API_URL=
#   PIPELINE_MODE=direct            (or spark, if the lake is wanted in the scoring path)
docker compose --profile app --profile tls --profile backup up -d --build
```

Then:

1. Stop the simulator container: `docker compose stop generator`. It refuses to run in production and would
   otherwise restart in a loop.
2. Open `https://risk.example.com`, sign in as the administrator, create a named account for each person and
   change the first administrator's password.
3. Remove the `ports:` lines of `kafka`, `redis`, `postgres`, `api`, `dashboard` and `scorer` in
   `docker-compose.yml`, or firewall those ports. Only 80 and 443 should be reachable from outside.
4. Check `https://risk.example.com/health/ready`: it reports Redis, PostgreSQL, Kafka and the scorer.

The services refuse to start in production while any demo credential is still set.

## The model

`models/current` in this repository is trained on simulated data and is only good for a demonstration. Before
real use, train on the pilot's own labelled history (`ml/train.py`), put the result in `models/current` and
restart the scorer and API. Training writes `reference.json` beside the model; the Performance tab uses it to
show drift. See [MODEL_CARD.md](MODEL_CARD.md).

## Backups

The `backup` profile writes a checksummed PostgreSQL archive to `./backups` every `BACKUP_INTERVAL_HOURS`
(default 24) and removes archives older than `BACKUP_KEEP_DAYS` (default 14).

- Copy the archives to another machine or object store. A backup on the same disk is not a backup.
- The archives are not encrypted. Encrypt them where they are stored.
- Restore into a new database, check it, then point `DATABASE_URL` at it:

```bash
docker compose run --rm --entrypoint sh backup /tools/restore.sh /backups/<file>.dump vyrox_restored
```

Kafka (72 hours of events) and Redis (feature history) are not backed up. After losing Redis, velocity features
start from empty history and rebuild as traffic flows; replaying recent events with `tools/replay.py` refills it.

## Recovery

| Failure | What happens | What to do |
|---|---|---|
| Scorer stops | Ingestion keeps accepting; the dashboard shows STREAM DISCONNECTED within seconds | It restarts by itself and continues from its last committed offset |
| Kafka stops | Ingestion answers 503; nothing is accepted or lost | Senders retry with the same `transaction_id` |
| PostgreSQL or Redis stops | The scorer waits and retries; events queue in Kafka | Restore the service; the backlog drains |
| Server sleeps or pauses | The scorer rejoins its consumer group and replays uncommitted events; duplicates are skipped | Nothing |
| An event fails three times | It goes to the dead-letter topic with the error | `tools/replay.py dlq list`, fix, `dlq replay` |

Delivery is at-least-once with idempotent storage. It is not exactly-once.

## Capacity

Measured with `tools/loadtest.py` on one 4-core, 16 GB test machine on 16 Nov 2026, direct mode, two APIs.
Kafka, Redis, PostgreSQL and every scorer shared that machine. The tool pushes a burst into Kafka and times how
fast the backlog drains (`reports/loadtest_*.json`):

| Scorer processes | Payments per second |
|---|---|
| 1 | 298 |
| 2 | 590 |
| 4 | 701 |

From two to four scorers the machine's four cores were the limit, not the design. Redis had been emptied before
these runs, so accounts had short histories. With the simulator's 5,000 accounts each holding about 700 payments
of history, the same machine managed 160 payments per second with one scorer and 436 with four.

What changed to get there (16 Nov 2026):

- **Dashboard totals are running counts** (`stats_total`, `stats_minute`, kept by the scorer in the same
  transaction as the payment). Before, every dashboard refresh recounted the whole `predictions` table, and
  in the first load test that took about half the database's CPU. A recount on 816,427 stored payments matched
  the running counts exactly.
- **One database transaction per batch** of up to 100 payments, not one per payment. If any payment in a batch
  fails, the batch is undone and each payment is retried on its own.
- **Time-range counts use the index.** `clock_timestamp()` in a filter stopped PostgreSQL using the index on
  `persisted_at`: 420 ms per count, 19 ms with `now()`.
- **Redis history is compact and bounded.** History is kept for 25 hours, not 48. Entries are short, and sorted
  sets of up to 1,024 entries use Redis's compact encoding. Counterparty and device keys now expire after 90 days
  without activity; before, they never expired. Measured: one history entry went from 115 bytes to 31 bytes
  (800 payments in one account). Across all keys it came to 115 bytes per stored payment, with 5,000 accounts
  holding 10 payments each.
- **A full Redis pauses scoring instead of dropping payments.** Out-of-memory is now retried like an outage, so
  payments wait in Kafka. Before, every payment was dead-lettered until someone noticed.

Sizing Redis: allow about 120 bytes for every payment in the last 25 hours, plus headroom. At 1,000 payments a
second that is about 11 GB. Set it with `REDIS_MAXMEMORY` (default `1gb`). Eviction stays off on purpose:
quietly dropping an account's history would give wrong features.

Run the load test on your own servers before committing to a volume. It only runs in the demo environment,
because it writes simulated transactions.

### What a national-scale test still needs

None of the following has been built or measured here:

- Three or more machines for Kafka (replicated), a PostgreSQL primary with a standby, and Redis Cluster so
  that feature memory is split across machines. At 10,000 payments a second, history alone is about 110 GB.
- Partitioning `predictions` by month, with a retention policy. It grows by about 1 KB per payment.
- Enough Kafka partitions for the number of scorers planned (6 as shipped; a scorer with no partition is idle).
- A load test from separate client machines at the target rate, sustained for hours, with a failure injected.

## Several servers

`docker-compose.ha.yml` runs two APIs and two scorers:

```bash
docker compose -f docker-compose.yml -f docker-compose.ha.yml --profile app --profile tls up -d
```

What makes that safe:

- Sessions, two-factor challenges and SSO sign-ins are in PostgreSQL; lock-outs and API-key rate limits are in
  Redis. Any API can serve any request, and a person stays signed in when one API stops.
- The proxy spreads requests over the APIs, checks each one's `/health/live` every 2 seconds, and retries a
  failed request on another API for up to 5 seconds.
- Every API reads the scored-events topic with its own consumer group, so each one can feed live dashboards.
  A dashboard's live stream reconnects by itself to whichever API answers.
- The scorers share one consumer group. Kafka moves a stopped scorer's partitions to the others within
  `SCORER_SESSION_TIMEOUT_MS` (10 seconds); they resume from the last committed offset. Events in flight
  may be scored twice; the second copy is recognised and skipped.
- Each process reports itself in Redis every second. `GET /v1/status` lists them under `nodes` (alive or not,
  last seen, and for scorers how many partitions they hold), and a node that stopped in the last 10 minutes is
  named in `reasons`.
- Schema changes at start-up take a database lock, so replicas starting together do not collide.

Measured on the test machine, 15 Nov 2026, Spark mode, simulator at its default rate, `/v1/status` polled
through the proxy twice a second for 140 seconds (271 requests), `docker kill` (no clean shutdown):

| Step | What happened |
|---|---|
| Kill `api-1` at 20 s | 0 failed requests. `api-2` answered all 145 requests until `api-1` was back. |
| Kill `scorer-1` at 45 s | `scorer-2` took all 9 partitions 14.5 s later. Lag peaked at 336 events and was back to 0 within 2 s of the hand-over. The stream status stayed LIVE throughout, because `scorer-2` kept scoring its own partitions. |
| Start both again at 95 s | Both rejoined within 5 s; the proxy went back to alternating (41 and 46 requests) and the partitions split 5 and 4. |

So, during a scorer failure, accounts on the failed scorer's partitions are scored up to about 15 seconds late.
Nothing was lost.

Still one of each, and what to use instead for a pilot that must survive losing a machine:

| Store | As shipped | For several machines |
|---|---|---|
| Kafka | 1 broker, replication factor 1 | 3 brokers, `--replication-factor 3`, `min.insync.replicas=2` (or a managed Kafka) |
| PostgreSQL | 1 container | A managed database with a standby (RDS, Cloud SQL, Azure) or Patroni |
| Redis | 1 container | Redis Sentinel or a managed Redis. Redis holds feature history; losing it degrades the velocity features until history rebuilds, it does not lose transactions |

These were not built or tested here. The application only needs their addresses (`KAFKA_BOOTSTRAP`,
`DATABASE_URL`, `REDIS_URL`).

The fan-in feature (distinct senders to one destination in the last hour) is shared across accounts. With
several scorers, two senders paying the same destination at the same instant can each miss the other.

## Monitoring

- `/health/live` and `/health/ready` for probes.
- Prometheus metrics on the API (`/metrics`, internal only) and the scorer (port 9101);
  `docker compose --profile obs up -d` starts Prometheus and Grafana.
- JSON logs on stdout of every service.
- The dashboard's Performance tab: measured latency, consumer lag, error rate and model drift.
- `ALERT_WEBHOOK_URL` posts to Slack, Teams or similar when a node stops, the stream disconnects, a backup
  fails or a priority-1 fraud alert is raised (docs/INTEGRATION.md, 4a). Tested against a local receiver, not a
  real Slack workspace. No e-mail or paging integration; consumer lag has no alert yet.

## Upgrades

Database changes in `db/migrations` are applied when PostgreSQL first initialises; the services create the
sign-in tables at start-up. There is no migration tool yet, so before an upgrade take a backup and read the
release notes for any SQL to run by hand.
