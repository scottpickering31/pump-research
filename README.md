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
3. Replaceable discovery contract and an initial discovery adapter, including checkpoint and gap semantics.
4. DEX Screener client with safe batching, shared rate limiting, response provenance, and tests.
5. Restart-safe scheduling, leases, lifecycle classification, and adaptive polling.
6. Operational metrics, collection-gap detection, capacity validation, and 24-hour data-quality reports.
7. Verified Parquet archival with manifests, integrity checks, and cross-tier reporting.

Each phase requires approval. Cadence and retention decisions remain provisional until measured API and storage budgets exist.

## Current status

Phase 2 persistence is complete, and the DEX Screener market-data client has been implemented ahead of discovery. It provides the Python/PostgreSQL foundation, migrations, repository abstractions, an official-contract async DEX Screener batch client, and tests. Discovery implementation, lifecycle heuristics, polling scheduler, and collector orchestration remain intentionally absent.

The DEX Screener client follows the current official [`/tokens/v1` API reference](https://docs.dexscreener.com/api/reference): 30-address batches, a 300-RPM documented endpoint limit, and a 240-RPM default client budget. See [docs/dexscreener.md](docs/dexscreener.md).

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
python -m pytest
python -m ruff check .
python -m mypy
```

`docker compose ps` reports `healthy` after PostgreSQL passes its health check. The database uses the named `postgres_data` Docker volume and therefore persists across container recreation. Apply database migrations with `make migrate`. For convenience, equivalent commands are available through `make db-up`, `make db-status`, `make db-health`, and `make check` once the virtual environment exists.

Copy `.env.example` to `.env` for local overrides. `.env` is ignored by Git; never commit credentials or connection strings for shared environments.

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
│   └── dexscreener.md       # official API contract and client policy
├── src/
│   └── pump_research/
│       ├── domain/          # provider-neutral identities and contracts
│       ├── config.py        # Pydantic settings
│       ├── logging.py       # structured logging setup
│       ├── database.py      # async engine and health check only
│       ├── cli.py           # `database health` command
│       ├── persistence/     # SQLAlchemy models and repository abstractions
│       ├── market_data/     # DEX Screener client, parsing, rate limiting
│       ├── discovery/       # replaceable token-discovery adapters
│       ├── collection/      # batching, rate budgets, attempt orchestration
│       ├── scheduling/      # durable due-work and lease coordination
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
