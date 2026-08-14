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

Phase 0 is complete: this repository contains project guidance, an architecture note, a proposed directory layout, and ignore rules only. It intentionally contains no application implementation, dependency lockfiles, API integrations, database models, migrations, Docker configuration, or collector.

## Proposed directory layout

```text
.
├── AGENTS.md
├── README.md
├── .gitignore
├── docs/
│   └── architecture.md      # durability and research-integrity design
├── src/
│   └── pump_research/
│       ├── domain/          # provider-neutral identities and contracts
│       ├── discovery/       # replaceable token-discovery adapters
│       ├── market_data/     # provider-specific market-data clients
│       ├── collection/      # batching, rate budgets, attempt orchestration
│       ├── scheduling/      # durable due-work and lease coordination
│       ├── lifecycle/       # derived state and versioned transitions
│       ├── persistence/     # repositories and transaction boundaries
│       ├── archival/        # verified Parquet archival and manifests
│       ├── reporting/       # as-of data-quality/collection reports
│       └── monitoring/      # health, metrics, and gap detection
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── alembic/                 # future schema migrations only
├── scripts/                 # explicit operational commands only
├── data/                    # untracked local development data
├── archives/                # untracked generated Parquet output
└── logs/                    # untracked local logs
```

The placeholder directories are intentionally empty until their corresponding phase is approved.
