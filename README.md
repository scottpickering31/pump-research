# Pump Research

Pump Research will build a long-running, research-grade dataset for newly appearing Pump.fun-originated Solana tokens and their later market behaviour on DEX Screener. The application is strictly for discovery, data collection, storage, lifecycle tracking, archival, and research. It is **not** a trading bot and will never manage wallets, keys, transactions, or trades.

## Architecture at a glance

```text
Provider adapters                       Collection control plane
┌──────────────────┐                    ┌──────────────────────────┐
│ token discovery  │──source events────>│ durable schedule/leases  │
└──────────────────┘                    │ batch planner/API budget │
                                        └────────────┬─────────────┘
┌──────────────────┐                                 │ requests
│ DEX Screener     │<────────────────────────────────┘
│ market-data API  │────responses/attempt outcomes─────────┐
└──────────────────┘                                       v
                                               ┌──────────────────────┐
                                               │ PostgreSQL           │
                                               │ raw/source facts     │
                                               │ normalized facts     │
                                               │ attempts & schedules │
                                               │ derived lifecycle    │
                                               └──────────┬───────────┘
                                                          v
                                             archival / reports / DQ
```

Discovery is provider-agnostic and supplies source events about chain/address identities. DEX Screener is a replaceable market-data adapter used after addresses are known. Raw/source facts, normalized facts, operational collection evidence, and derived lifecycle state have separate persistence boundaries.

See [docs/architecture.md](docs/architecture.md) for temporal semantics, recovery, API budgeting, 100M-row planning, and archival requirements.

## Intended stack

- Python 3.12+ with `asyncio`
- PostgreSQL with SQLAlchemy 2.x async, `asyncpg`, and Alembic
- `httpx`, Pydantic v2, `pydantic-settings`, and tenacity where appropriate
- `pytest`, Ruff, mypy, and structured logging
- Docker Compose for local PostgreSQL development

Dependencies and infrastructure will be introduced only in approved phases.

## Planned phases

0. Project charter, architectural review, documentation, and directory scaffold.
1. Tooling baseline, configuration model, testing, and local PostgreSQL setup.
2. Persistence design and migrations for identities, immutable source facts, attempts, scheduling state, lifecycle history, and configuration history.
3. Replaceable discovery contract and the PumpPortal live adapter, including explicit gap semantics.
4. DEX Screener client with safe batching, shared rate limiting, response provenance, and tests.
5. Restart-safe scheduling, leases, lifecycle classification, and adaptive polling.
6. Operational metrics, collection-gap detection, capacity validation, and 24-hour data-quality reports.
7. Verified Parquet archival with manifests, integrity checks, and cross-tier reporting.

Each phase requires approval. The implemented cadences and lifecycle-classification thresholds are configurable and versioned; all remain provisional until measured API and storage budgets exist.

## Current status

The DEX Screener market-data client and provider-neutral discovery boundary are implemented. The collector now runs fixed supervised loops that persist discovery, reconcile `PENDING_DEX`, claim adaptive scheduler batches, persist immutable DEX observations, derive lifecycle transitions, and schedule subsequent work. It reconstructs all work from PostgreSQL after restart, records run/component heartbeats, finalizes abandoned runs, and handles SIGINT/SIGTERM without an in-memory token queue.

The DEX Screener client follows the current official [`/tokens/v1` API reference](https://docs.dexscreener.com/api/reference): 30-address batches, a 300-RPM documented endpoint limit, and a 240-RPM default client budget. See [docs/dexscreener.md](docs/dexscreener.md).

The first database-backed 24-hour collection/data-quality report is implemented. It produces hourly counts, request/latency/rate-limit measures, cadence and gap measures, lifecycle activity, null rates, duplicate-delivery rates, and a database-size snapshot. See [docs/reporting.md](docs/reporting.md).

See [docs/discovery.md](docs/discovery.md) for the provider-neutral discovery contract,
PumpPortal credential setup, and the live stream's important no-replay coverage limitation.

See [docs/dex_availability.md](docs/dex_availability.md) for the initial `PENDING_DEX → NEW` workflow, batch limit, and restart recovery behavior.

See [docs/scheduler.md](docs/scheduler.md) for adaptive intervals, priority/fairness, leases, batching, request-capacity reservation, restart behavior, and lateness measurements.

See [docs/lifecycle.md](docs/lifecycle.md) for configured lifecycle transitions, their evidence, temporal safeguards, and the intentionally limited current scope.

See [docs/reliability.md](docs/reliability.md) for fault-injection coverage and the physical collector stop/restart test.

See [docs/commands.md](docs/commands.md) for a compact application, database,
reporting, and development command reference.

## Local development setup

Prerequisites: Python 3.12+ and Docker Desktop (including Docker Compose).

```bash
cp .env.example .env
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
docker compose up -d
docker compose ps
python -m alembic upgrade head
python -m pump_research database health
python -m pump_research epoch list
docker compose exec -T postgres createdb -U pump_research pump_research_capacity_test
PUMP_RESEARCH_ENVIRONMENT=test PUMP_RESEARCH_TEST_DATABASE_URL='postgresql+asyncpg://pump_research:pump_research@localhost:5433/pump_research_capacity_test' python -m pytest
python -m ruff check .
python -m mypy
```

`docker compose ps` reports `healthy` after PostgreSQL passes its health check.
The collector database uses the named `postgres_data` Docker volume. Integration
tests require an explicit test URL and `PUMP_RESEARCH_ENVIRONMENT=test`; the
fixture checks PostgreSQL's actual connected database name immediately before
destructive SQL. Apply collector database migrations with `make migrate`.

Copy `.env.example` to `.env` for local overrides. `.env` is ignored by Git; never commit
credentials or connection strings for shared environments. Before a live collector run, obtain a
PumpPortal API key using its official [Getting Started](https://pumpportal.fun/trading-api/setup/)
page and set `PUMP_RESEARCH_PUMPPORTAL_API_KEY`. The application neither creates nor stores the
linked wallet or its private key.

`python -m pump_research report 24h --epoch 1` writes Markdown, JSON, and hourly
CSV. Use `--end-at 2026-08-14T12:00:00Z` to reproduce a
past UTC-aligned window and `--output-directory path/to/output` to choose a
different destination.

Start a live collector only after setting the PumpPortal API key and reviewing
the live WebSocket source's no-replay coverage risk:

```bash
python -m pump_research epoch create --number 1 --purpose 'first valid 24-hour research collection'
python -m pump_research collector run --epoch 1
python -m pump_research collector status
python -m pump_research report 24h --epoch 1
```

`collector run` continues until SIGINT or SIGTERM. `collector status` is a
read-only JSON snapshot of durable run health, discovery connectivity, DEX
request use, token/state totals, schedule lateness, and database size.
PostgreSQL permits only one live collector process for a database, ensuring
reconciliation and observation requests share the configured DEX Screener
budget; a second invocation exits rather than running split-brain.

The default host port is `5433` because many local PostgreSQL installations already use `5432`; PostgreSQL remains on `5432` inside the Compose network. Update both `PUMP_RESEARCH_DATABASE_URL` and `compose.yaml` if a different host port is required.

## Proposed directory layout

```text
.
├── AGENTS.md
├── README.md
├── .gitignore
├── docs/
│   ├── architecture.md      # durability and research-integrity design
│   ├── database.md          # persistence schema, indexes, and scale notes
│   ├── dexscreener.md       # official API contract and client policy
│   ├── dex_availability.md  # durable initial DEX-admission workflow
│   ├── discovery.md         # discovery contract and coverage semantics
│   ├── lifecycle.md         # configured transition evidence and safeguards
│   ├── reporting.md         # 24-hour collection/data-quality report semantics
│   ├── reliability.md       # fault injection and physical restart coverage
│   └── scheduler.md         # adaptive cadence, priority, and recovery
├── src/
│   └── pump_research/
│       ├── domain/          # provider-neutral identities and contracts
│       ├── config.py        # Pydantic settings
│       ├── logging.py       # structured logging setup
│       ├── database.py      # async engine and health check only
│       ├── cli.py           # `database health` command
│       ├── persistence/     # SQLAlchemy models and repository abstractions
│       ├── market_data/     # DEX Screener client, parsing, rate limiting
│       ├── discovery/       # provider-neutral contract and replaceable adapters
│       ├── collection/      # initial DEX admission and later orchestration
│       ├── scheduling/      # durable adaptive due-work and lease coordination
│       ├── lifecycle/       # derived state and versioned transitions
│       ├── archival/        # verified Parquet archival and manifests
│       ├── reporting/       # as-of data-quality/collection reports
│       └── monitoring/      # health, metrics, and gap detection
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── alembic/                 # migration environment and revision history
├── scripts/                 # explicit operational commands only
├── data/                    # untracked local development data
├── archives/                # untracked generated Parquet output
└── logs/                    # untracked local logs
```

Directories without source files remain intentional placeholders until their corresponding phase is approved.
