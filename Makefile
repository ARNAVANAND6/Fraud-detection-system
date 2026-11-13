PY ?= .venv/bin/python
HOSTENV = KAFKA_BOOTSTRAP=localhost:29092 REDIS_URL=redis://localhost:6379/0 \
          DATABASE_URL=postgresql://fraud:fraud@localhost:5432/vyrox_demo \
          ENVIRONMENT=demo WEBHOOK_SECRET=demo-webhook-secret PYTHONPATH=libs:.

.PHONY: venv infra up down spark data train test lint reconcile run-local acceptance replay-dlq

venv:
	python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

infra:            ## Kafka, Redis, PostgreSQL
	docker compose up -d

up:               ## + Spark, generator, scorer, API, dashboard (PIPELINE_MODE in .env)
	docker compose --profile spark --profile app up -d --build

spark:            ## + Spark streaming job
	docker compose --profile spark up -d --build

down:             ## stop everything (volumes are kept)
	docker compose --profile app --profile spark --profile obs down

data:             ## synthetic training data, same pace as the live stream
	$(PY) services/generator/generator.py --mode csv --n 200000 --rate 50

train:
	$(PY) ml/train.py --version v0.2-synthetic
	mkdir -p models/current && cp models/v0.2-synthetic/* models/current/

test:
	$(PY) -m pytest -q

lint:
	.venv/bin/ruff check libs services ml tests tools

run-local:        ## scorer + API + generator on the host, against `make infra`
	$(HOSTENV) $(PY) -m services.scorer.app.consumer & \
	$(HOSTENV) $(PY) -m uvicorn services.scorer.app.main:app --port 8000 --workers 1 --timeout-graceful-shutdown 3 & \
	$(HOSTENV) $(PY) services/generator/generator.py --mode kafka --rate 50

reconcile:        ## Bronze vs Silver vs quarantine counts
	docker compose exec spark sh -c '/opt/spark/bin/spark-submit --conf spark.jars.ivy=/home/spark/.ivy2 --packages $$SPARK_PACKAGES reconcile.py'

acceptance:       ## the ten acceptance tests against the running compose stack
	VYROX_ACCEPTANCE=1 $(HOSTENV) $(PY) -m pytest tests/acceptance -v

replay-dlq:       ## summarise the dead-letter topic (see tools/replay.py for replay)
	$(HOSTENV) $(PY) tools/replay.py dlq list
