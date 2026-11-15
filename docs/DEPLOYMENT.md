# Deployment and operations

How to run VYROX on one server for a pilot, and how to look after it. This is
a single-node deployment. It has no high availability: if the server stops,
scoring stops until it is back (nothing accepted is lost, see Recovery).

## What you need

- A Linux server with Docker. 4 cores and 8 GB of RAM are enough for direct mode at tens of events per second;
  Spark mode needs about 8 GB free for itself.
- A host name pointing at the server, with ports 80 and 443 reachable, for a public certificate.
- An authorised source of transactions. VYROX has none built in.

## First start

```bash
cp .env.production.example .env
# In .env set long random values for POSTGRES_PASSWORD (also inside DATABASE_URL), API_KEYS,
# WEBHOOK_SECRET and ADMIN_PASSWORD, then add:
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

Measured with `tools/loadtest.py` on a 4-core test machine, direct mode, one scorer process: 251.7 transactions
per second (`reports/loadtest.json`). The limit is the scorer: it handles one event at a time with one database
transaction each. Run the load test on your own server before committing to a volume; it only runs in the demo
environment because it writes simulated transactions.

Not built: more than one API replica (the live stream is held in one process), and tested scale-out of the
scorer (the topic has partitions for it, but the fan-in feature can miss simultaneous events across replicas).

## Monitoring

- `/health/live` and `/health/ready` for probes.
- Prometheus metrics on the API (`/metrics`, internal only) and the scorer (port 9101);
  `docker compose --profile obs up -d` starts Prometheus and Grafana.
- JSON logs on stdout of every service.
- The dashboard's Performance tab: measured latency, consumer lag, error rate and model drift.
- No alerting to e-mail or chat is configured. Add Prometheus alert rules for consumer lag, a missing scorer
  heartbeat and failed backups ("BACKUP FAILED" in the backup container's log).

## Upgrades

Database changes in `db/migrations` are applied when PostgreSQL first initialises; the services create the
sign-in tables at start-up. There is no migration tool yet, so before an upgrade take a backup and read the
release notes for any SQL to run by hand.
