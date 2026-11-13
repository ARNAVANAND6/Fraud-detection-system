from prometheus_client import Counter, Gauge, Histogram

LATENCY_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

EVENTS = Counter("scorer_events_total", "Events consumed by outcome", ["outcome"])
RETRIES = Counter("scorer_retries_total", "Processing retries", ["kind"])
FEATURE_SECONDS = Histogram("scorer_feature_seconds", "Redis feature read + update", buckets=LATENCY_BUCKETS)
INFERENCE_SECONDS = Histogram("scorer_inference_seconds", "Rules + model inference", buckets=LATENCY_BUCKETS)
PIPELINE_SECONDS = Histogram("scorer_pipeline_seconds", "Ingested to persisted", buckets=LATENCY_BUCKETS)
ALERTS = Counter("scorer_alerts_total", "Alert engine outcomes", ["kind"])
INGEST_SECONDS = Histogram("gateway_ingest_seconds", "Request received to Kafka ack", ["source"],
                           buckets=LATENCY_BUCKETS)
INGESTED = Counter("gateway_events_total", "Gateway events by source and outcome", ["source", "outcome"])
API_LATENCY = Histogram("api_request_seconds", "API request time", ["route"], buckets=LATENCY_BUCKETS)
SSE_CLIENTS = Gauge("api_sse_clients", "Connected dashboard streams")
