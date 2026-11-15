# Security overview

What VYROX does today to protect data and access, and what it does not do.
Every "does" below is implemented in this repository and covered by a test or
a recorded check. Nothing here is a certification.

## Access

| Caller | Authenticates with | May do |
|---|---|---|
| Viewer | username and password | read |
| Analyst | username and password | read, work alerts |
| Admin | username and password | everything, including settings, imports, people and the audit log |
| Machine | `X-API-Key` header | send transactions, dry-run scores, read. Nothing else |

- Passwords: salted scrypt hashes (n = 2^14), minimum 10 characters. Plain passwords are never stored or logged.
- Lock-out: five failed sign-ins for one username from one address block that pair for 15 minutes.
- Sessions: a random token, valid 8 hours. Only its SHA-256 digest is stored. Changing a person's role or
  password, or deactivating them, ends their sessions immediately.
- The last active administrator cannot be demoted or deactivated.
- **Two-factor sign-in** with any authenticator app (TOTP, RFC 6238). Secrets are encrypted at rest with a key
  derived from `SECRET_KEY`; a code is accepted once, within one 30-second step of drift; five wrong codes end
  the sign-in attempt. Ten single-use recovery codes are shown once and stored as SHA-256 digests. An
  administrator can reset a lost authenticator. `MFA_REQUIRED_ROLES=ADMIN,ANALYST` makes it compulsory: a
  person in those roles who has not enrolled can only enrol until they do.
- **Single sign-on** with OpenID Connect (Google Workspace, Microsoft Entra ID, Okta, Keycloak and others):
  authorization code flow with PKCE, state and nonce single use, ID token signature checked against the
  provider's published keys (asymmetric algorithms only), issuer, audience and expiry checked, verified e-mail
  required. People are matched by the provider's subject id, or by an e-mail an administrator set on their
  account. Unknown people are refused unless their e-mail domain is in `OIDC_ALLOWED_DOMAINS`. Two-factor
  sign-in for SSO users is enforced at the identity provider.
- Production refuses to start with the demo API key, demo webhook secret or demo administrator password.

## Audit

`audit_log` records every sign-in, failed sign-in, refused request, alert change, settings change (before and
after values), import, demo scenario and change to a user, with the actor, time and address. The API can only
append to it. An alert change is always recorded under the signed-in person; a request cannot choose the actor.

## In transit

`docker compose --profile tls` puts a Caddy proxy in front of the dashboard and API on one origin:

- HTTP redirects to HTTPS; HSTS for one year.
- Content-Security-Policy limited to the site itself, `X-Frame-Options: DENY`, `nosniff`, no referrer.
- `/metrics` is not published through the proxy.
- With a public host name Caddy obtains and renews the certificate. With `localhost` it uses its own local
  authority, which browsers do not trust until it is installed.

Kafka, Redis and PostgreSQL talk plain text inside the Docker network. They must not be exposed to other networks
as shipped; the compose file publishes their ports for development and those mappings should be removed in production.

## Data

- Webhooks are verified with HMAC-SHA256 over `<timestamp>.<raw body>` and rejected outside a 5-minute window.
- Ingestion validates every field and rejects unknown ones. `source` and `ingested_at` are set by the gateway and
  never trusted from the caller.
- Demo and production use separate Kafka topics, databases and Redis databases. Production rejects simulated events
  and the simulator refuses to run there. Every stored record carries its source.
- Backups: `tools/backup.sh` writes a checksummed PostgreSQL archive; `tools/restore.sh` verifies the checksum and
  restores only into a new database. A restore was tested and row counts matched the live database.

## Not in place

These are the items a bank's security review will ask for and will not find:

- SAML single sign-on (OpenID Connect is supported), and hardware security keys (WebAuthn).
- Encryption at rest. Use encrypted disks or a managed database; backups are not encrypted by the script.
- Tamper-evident audit log. A database administrator can still change `audit_log`.
- Field-level masking or tokenisation of account identifiers.
- TLS and authentication between the internal services (Kafka, Redis, PostgreSQL).
- Lock-out and rate limits are kept in the API process: they reset on restart and are per replica.
- An independent penetration test, and any certification or attestation: ISO 27001, SOC 2, PCI DSS.
- A data-protection assessment under India's DPDP Act, and RBI outsourcing and data-localisation requirements.
  These depend on the deploying organisation and need legal and compliance work, not code.

## Reporting a problem

Open a private security advisory on the GitHub repository. Do not put secrets or real customer data in an issue.
