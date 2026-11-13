"""Environment-based settings.

ENVIRONMENT is `demo` or `production`. The two never share topics, database
or Redis keys: topic names are prefixed with the environment and each env
file points at its own database and Redis DB.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

DEMO_WEBHOOK_SECRET = "demo-webhook-secret"
DEMO_API_KEY = "dev-key"


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    environment: str = field(default_factory=lambda: _env("ENVIRONMENT", "demo").lower())
    # spark  = scorer reads the topic Spark validated and forwarded (full workflow)
    # direct = scorer reads the raw topic (lower latency, Spark only builds the lake)
    pipeline_mode: str = field(default_factory=lambda: _env("PIPELINE_MODE", "spark").lower())
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:29092"))
    redis_url: str = field(default_factory=lambda: _env("REDIS_URL", "redis://localhost:6379/0"))
    database_url: str = field(
        default_factory=lambda: _env("DATABASE_URL", "postgresql://fraud:fraud@localhost:5432/vyrox_demo")
    )
    model_dir: str = field(default_factory=lambda: _env("MODEL_DIR", "models/current"))
    api_keys: tuple[str, ...] = field(
        default_factory=lambda: tuple(k for k in _env("API_KEYS", DEMO_API_KEY).split(",") if k)
    )
    webhook_secret: str = field(default_factory=lambda: _env("WEBHOOK_SECRET", ""))
    rate_limit_per_minute: int = field(default_factory=lambda: int(_env("RATE_LIMIT_PER_MINUTE", "6000")))
    cors_origins: tuple[str, ...] = field(
        default_factory=lambda: tuple(_env("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000").split(","))
    )
    lag_delayed_threshold: int = field(default_factory=lambda: int(_env("LAG_DELAYED_THRESHOLD", "500")))
    idle_after_seconds: int = field(default_factory=lambda: int(_env("IDLE_AFTER_SECONDS", "15")))

    def __post_init__(self) -> None:
        if self.environment not in ("demo", "production"):
            raise ValueError("ENVIRONMENT must be 'demo' or 'production'")
        if self.pipeline_mode not in ("spark", "direct"):
            raise ValueError("PIPELINE_MODE must be 'spark' or 'direct'")

    def topic(self, name: str) -> str:
        return f"{self.environment}.payments.{name}.v1"

    @property
    def topic_raw(self) -> str:
        return self.topic("transactions")

    @property
    def topic_validated(self) -> str:
        return self.topic("transactions.validated")

    @property
    def topic_scorer_input(self) -> str:
        return self.topic_validated if self.pipeline_mode == "spark" else self.topic_raw

    @property
    def topic_labels(self) -> str:
        return self.topic("labels")

    @property
    def topic_scored(self) -> str:
        return self.topic("scored")

    @property
    def topic_dlq(self) -> str:
        return self.topic("transactions.dlq")

    @property
    def scorer_group(self) -> str:
        return f"vyrox-scorer-{self.environment}"

    def assert_safe_for_production(self) -> None:
        """Refuse to run production with demo credentials."""
        if self.environment != "production":
            return
        problems = []
        if DEMO_API_KEY in self.api_keys:
            problems.append("API_KEYS still contains the demo key")
        if self.webhook_secret in ("", DEMO_WEBHOOK_SECRET):
            problems.append("WEBHOOK_SECRET is unset or the demo secret")
        if problems:
            raise RuntimeError("Unsafe production config: " + "; ".join(problems))


def get_settings() -> Settings:
    return Settings()
