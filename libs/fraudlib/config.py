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
DEMO_ADMIN_USERNAME = "admin"
DEMO_ADMIN_PASSWORD = "vyrox-demo-admin"  # demo only; production refuses to start with it
DEMO_SUPERADMIN_PASSWORD = "foresentry-demo-super"  # demo only; production refuses to start with it
DEMO_SECRET_KEY = "vyrox-demo-secret-key"  # demo only; encrypts 2FA secrets, signs SSO state


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_list(name: str, default: str = "") -> tuple[str, ...]:
    return tuple(item.strip() for item in _env(name, default).split(",") if item.strip())


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
    # Payment-gateway webhooks (connectors.py). Empty = that connector is off.
    razorpay_webhook_secret: str = field(default_factory=lambda: _env("RAZORPAY_WEBHOOK_SECRET", ""))
    stripe_webhook_secret: str = field(default_factory=lambda: _env("STRIPE_WEBHOOK_SECRET", ""))
    # Only payments in these currencies are scored; amounts are not converted.
    connector_currencies: tuple[str, ...] = field(
        default_factory=lambda: tuple(c.upper() for c in _env_list("CONNECTOR_CURRENCIES", "INR")))
    # Where operational alerts go (notify.py): a Slack, Teams or generic incoming-webhook URL. Empty = off.
    alert_webhook_url: str = field(default_factory=lambda: _env("ALERT_WEBHOOK_URL", ""))
    # First administrator, created at start-up if no such user exists yet.
    admin_username: str = field(default_factory=lambda: _env("ADMIN_USERNAME", DEMO_ADMIN_USERNAME).lower())
    admin_password: str = field(default_factory=lambda: _env("ADMIN_PASSWORD", DEMO_ADMIN_PASSWORD))
    # The platform operator, who manages organisations. Created on first start.
    superadmin_username: str = field(default_factory=lambda: _env("SUPERADMIN_USERNAME", "superadmin").lower())
    superadmin_password: str = field(default_factory=lambda: _env("SUPERADMIN_PASSWORD", DEMO_SUPERADMIN_PASSWORD))
    # Which organisation the Razorpay, Stripe and signed-webhook connectors deliver to.
    connector_org: str = field(default_factory=lambda: _env("CONNECTOR_ORG", "demo"))
    # Encrypts two-factor secrets at rest. Changing it invalidates every enrolled authenticator.
    secret_key: str = field(default_factory=lambda: _env("SECRET_KEY", DEMO_SECRET_KEY))
    # Roles that must use two-factor sign-in; a password alone is refused once they have enrolled,
    # and they are told to enrol when they have not. Empty = optional for everyone.
    mfa_required_roles: tuple[str, ...] = field(
        default_factory=lambda: tuple(r.upper() for r in _env_list("MFA_REQUIRED_ROLES")))
    # Single sign-on (OpenID Connect). Off unless OIDC_ISSUER and OIDC_CLIENT_ID are set.
    oidc_issuer: str = field(default_factory=lambda: _env("OIDC_ISSUER", "").rstrip("/"))
    oidc_client_id: str = field(default_factory=lambda: _env("OIDC_CLIENT_ID", ""))
    oidc_client_secret: str = field(default_factory=lambda: _env("OIDC_CLIENT_SECRET", ""))
    oidc_provider_name: str = field(default_factory=lambda: _env("OIDC_PROVIDER_NAME", "single sign-on"))
    # Public URL of /v1/auth/sso/callback as the browser reaches it; registered with the provider.
    oidc_redirect_url: str = field(default_factory=lambda: _env("OIDC_REDIRECT_URL", ""))
    # Where to send the browser after sign-in (the dashboard).
    dashboard_url: str = field(default_factory=lambda: _env("DASHBOARD_URL", "http://localhost:3000").rstrip("/"))
    # People who sign in with SSO but have no account yet get one with this role, if their e-mail
    # domain is listed. Empty list = only people an administrator added (by e-mail) can sign in.
    oidc_allowed_domains: tuple[str, ...] = field(
        default_factory=lambda: tuple(d.lower() for d in _env_list("OIDC_ALLOWED_DOMAINS")))
    oidc_default_role: str = field(default_factory=lambda: _env("OIDC_DEFAULT_ROLE", "VIEWER").upper())
    rate_limit_per_minute: int = field(default_factory=lambda: int(_env("RATE_LIMIT_PER_MINUTE", "6000")))
    cors_origins: tuple[str, ...] = field(
        default_factory=lambda: tuple(_env("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000").split(","))
    )
    lag_delayed_threshold: int = field(default_factory=lambda: int(_env("LAG_DELAYED_THRESHOLD", "500")))
    scorer_session_timeout_ms: int = field(default_factory=lambda: int(_env("SCORER_SESSION_TIMEOUT_MS", "10000")))
    idle_after_seconds: int = field(default_factory=lambda: int(_env("IDLE_AFTER_SECONDS", "15")))

    def __post_init__(self) -> None:
        if self.environment not in ("demo", "production"):
            raise ValueError("ENVIRONMENT must be 'demo' or 'production'")
        if self.pipeline_mode not in ("spark", "direct"):
            raise ValueError("PIPELINE_MODE must be 'spark' or 'direct'")

    @property
    def sso_enabled(self) -> bool:
        return bool(self.oidc_issuer and self.oidc_client_id and self.oidc_redirect_url)

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
        if self.superadmin_password == DEMO_SUPERADMIN_PASSWORD:
            problems.append("SUPERADMIN_PASSWORD is unset or the demo password")
        if self.admin_password == DEMO_ADMIN_PASSWORD:
            problems.append("ADMIN_PASSWORD is unset or the demo password")
        if self.secret_key == DEMO_SECRET_KEY or len(self.secret_key) < 32:
            problems.append("SECRET_KEY is unset, the demo key, or shorter than 32 characters")
        if problems:
            raise RuntimeError("Unsafe production config: " + "; ".join(problems))


def get_settings() -> Settings:
    return Settings()
