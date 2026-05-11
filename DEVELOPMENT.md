# Project Horizon — Local Development Setup

This guide gets a new engineer from a clean clone to a running Beta-10-equivalent Horizon instance in roughly thirty minutes.

It is the **dev environment** companion to `README.md`. Production deployment (Railway) is handled separately and requires no setup beyond pushing to `main`.

## What this document is — and isn't

- It **is** the canonical local-development setup path for Horizon engineers.
- It **is** the Phase 0 / Step 0.1 baseline per ADR-002 (development environment parity).
- It **is not** a production runbook — Railway deployment is the production path.
- It **is not** a Postgres operations guide — local Postgres exists to support Phase 0.2+ work; production Postgres is gated behind ADR-001 cost/backup/restore validation.

## Versions pinned for this repository

`.tool-versions` declares the expected toolchain versions:

| Tool | Version | Notes |
|---|---|---|
| Python | 3.10.13 | Matches Railway production runtime |
| Postgres | 16.4 | Required for Phase 0.2 onwards; not required to run Beta 10 locally in legacy mode |

If you use `asdf`, `mise`, or `rtx` as a version manager, running `asdf install` (or `mise install`) in the repo root reads `.tool-versions` automatically. Other version managers (`pyenv`, `pgenv`) require manual installation; the version numbers above are authoritative.

## 1. Clone and enter the repository

```bash
git clone git@github.com:doc291/project-horizon.git
cd project-horizon
```

## 2. Install Python 3.10

Recommended: use a version manager so toolchain pins are honoured automatically.

**Option A — `mise` (or `rtx`, drop-in compatible):**
```bash
brew install mise        # macOS, one-time
mise install             # reads .tool-versions
```

**Option B — `asdf`:**
```bash
brew install asdf        # macOS, one-time
asdf plugin add python
asdf install             # reads .tool-versions
```

**Option C — `pyenv` (Python only):**
```bash
brew install pyenv       # macOS, one-time
pyenv install 3.10.13
pyenv local 3.10.13
```

Verify:
```bash
python3 --version        # → Python 3.10.13
```

## 3. Install Postgres 16 (required from Phase 0.2 onwards)

Beta 10 in its original form requires no database. Phase 0.2 introduces an optional Postgres connection that is silently skipped when `DATABASE_URL` is unset, so Postgres is only strictly required once you begin Phase 0.2 development. Install it now to avoid a second setup step later.

**macOS — Homebrew:**
```bash
brew install postgresql@16
brew services start postgresql@16
createdb horizon_dev
```

**macOS — Postgres.app (GUI):** Download Postgres.app v16 from https://postgresapp.com — it bundles its own server.

**Linux — apt:**
```bash
sudo apt install postgresql-16
sudo systemctl start postgresql
sudo -u postgres createdb horizon_dev
```

**Docker (cross-platform alternative):**
```bash
docker run --name horizon-pg -e POSTGRES_PASSWORD=devpw -e POSTGRES_DB=horizon_dev \
  -p 5432:5432 -d postgres:16.4
```

Verify:
```bash
psql -d horizon_dev -c "SELECT version();"
```

## 4. Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-dev.txt
```

`requirements-dev.txt` transitively installs `requirements.txt`, so a single command covers runtime and development needs.

## 5. Set environment variables

Create a `.env` file in the repo root (it is git-ignored automatically via `.gitignore` patterns; if not, add `.env` to your local ignore). Minimum content for a Beta-10-equivalent local instance:

```bash
# Auth — matches Beta 10 dev defaults; never reuse these for production
HORIZON_USER=horizon
HORIZON_PASS=ams2026

# Port profile to load on start
HORIZON_PORT=BRISBANE

# Local server port
PORT=8000

# Do NOT set DATABASE_URL in this file unless you are explicitly developing
# Phase 0.2+ Postgres-backed features. Beta 10 runs in-memory when unset.
# DATABASE_URL=postgresql://localhost:5432/horizon_dev
```

To load the file into your shell each session:

```bash
set -a; source .env; set +a
```

(Or use a tool like `direnv` to auto-load.)

## 6. Run the server

```bash
python3 server.py
```

Expected output: a banner with the active port profile, AISStream / MST connector state, and the URL `http://localhost:8000`.

Open the URL, log in with the credentials above, and you should see the standard Horizon dashboard with simulated vessel data.

## 7. Verify Beta-10-equivalent behaviour

Smoke test (run in a separate terminal):

```bash
# Login page renders
curl -s -o /dev/null -w "Login: %{http_code}\n" http://localhost:8000/login

# Health endpoint responds (public, no auth)
curl -s -o /dev/null -w "Health: %{http_code}\n" http://localhost:8000/health

# Protected route redirects to login when unauthenticated
curl -s -o /dev/null -w "Root: %{http_code} → %{redirect_url}\n" http://localhost:8000/

# Security headers (PR 1 + PR 2 outputs)
curl -sI http://localhost:8000/login | grep -iE "x-content-type|x-frame|referrer|permissions|cross-origin|strict-transport"
```

Expected:
- `Login: 200`
- `Health: 200`
- `Root: 302 → http://localhost:8000/login?next=/`
- Six security headers present (HSTS may be omitted on `http://` — that's expected in dev)

## 8. Switching between Beta-10-equivalent and Postgres-backed modes

Phase 0 ships in two operational modes that share the same codebase:

| Mode | `DATABASE_URL` | Behaviour |
|---|---|---|
| **Beta-10-equivalent** (default) | unset | Original Beta 10 behaviour, in-memory state, no audit ledger. This is what Railway production runs today. |
| **Phase 0 Postgres-backed** (development only until v1 readiness) | set | Same UX as Beta 10 from the user's perspective. Adds tenant configuration tables, audit ledger emission, hash-chained immutability per ADR-002. |

In Phase 0, **DATABASE_URL stays unset in Railway production** until cost, backup, and restore posture are confirmed (per ADR-002 amendment recorded at acceptance). Development can freely use the Postgres-backed mode.

### Startup verification (when DATABASE_URL is set)

When `DATABASE_URL` is set, the application verifies Postgres connectivity at startup before serving requests. Expected log line on a healthy connection:

```
INFO  [horizon.db] DATABASE_URL verified — Postgres connectivity OK
```

If the connection fails, the application exits with a clear error. This is intentional: misconfiguration should surface immediately, not at first query.

If `DATABASE_URL` is unset, expected log line:

```
INFO  [horizon.db] DATABASE_URL not set — running in legacy in-memory mode
```

### Running Alembic migrations (development only)

Alembic manages the database schema. Migrations live in `migrations/versions/`. Each Phase 0 step adds new migrations; this section is the operating procedure.

```bash
# Ensure DATABASE_URL is set in your shell (.env file loaded)
echo $DATABASE_URL   # → postgresql://localhost:5432/horizon_dev

# Apply all pending migrations
alembic upgrade head

# Show the current revision
alembic current

# Show migration history
alembic history --verbose

# Roll back one migration (development only — production migrations are
# forward-only in v1)
alembic downgrade -1
```

The Phase 0.2 baseline is `0001_initial_marker`, an empty marker that creates only the `alembic_version` table. Subsequent migrations build on it.

**Important:** Alembic is a development tool in Phase 0. Production migrations will be a separate, governed process once any tenant deployment uses Postgres operationally. Do not run `alembic` against any production database without explicit authorisation.

## 9. Common issues

| Symptom | Likely cause | Fix |
|---|---|---|
| `python3: command not found` | Version manager not initialised in shell | Source the version manager init in `~/.zshrc` / `~/.bashrc` |
| `ModuleNotFoundError` on startup | Virtualenv not activated | `source .venv/bin/activate` |
| Login fails with set credentials | `.env` not loaded into the shell | `set -a; source .env; set +a` and restart server |
| `connection refused` on `psql` | Postgres service not running | `brew services start postgresql@16` |
| `pip install` errors for `psycopg[binary]` | Building from source on unsupported platform | Try `pip install psycopg-binary` directly |
| Port 8000 already in use | Another process holding the port | `PORT=8001 python3 server.py` or kill the other process |

## 10. Where to look in the code

- `server.py` — the entire backend (HTTP handler, auth, all API endpoints, conflict detection, guidance generation, decision support). Currently ~4,100 lines.
- `index.html` — the entire operational dashboard frontend. Currently ~3,100 lines.
- `deploy/` — static marketing site for `horizon.ams.group`.
- `port_profiles.py` — port-specific operational parameters (lat/lon, berths, tidal params, wind limits).
- `weather.py`, `bom_tides.py` — weather and tide ingestion.
- `aisstream_scraper.py`, `mst_scraper.py` — AIS connectors.
- `requirements.txt` — runtime dependencies (4 packages).
- `requirements-dev.txt` — development dependencies (this file's companion).
- `db.py` — Postgres connection module (Phase 0.2). Gated behind `DATABASE_URL`.
- `alembic.ini`, `migrations/` — schema migration tooling. Used only when `DATABASE_URL` is set.
- `CLAUDE.md` — architectural overview, conventions, and operational notes.

Once Phase 0.2 lands, additional modules and migration directories will appear; this section will be updated with each Phase 0 PR.

## 11. Out of scope for Phase 0

The following are explicitly out of scope and should not be added to this guide during Phase 0:

- AWS migration / cloud destination setup
- Customer-specific tenant provisioning
- Production Postgres operational runbooks
- Container builds / Dockerfile additions
- Native mobile development tooling
- CI configuration changes

These belong to Phase 1 or later and will have their own documentation.

## 12. Reporting issues

If this guide does not get you to a running Horizon in 30 minutes, that is itself a bug worth reporting. Open a PR amending this document with the missing step; the next engineer benefits.
