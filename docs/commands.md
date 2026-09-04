# Command reference

Run these commands from the repository root. Activate the virtual environment
first when opening a new terminal:

```bash
. .venv/bin/activate
```

Use `python -m pump_research --help` to see the application commands.

## Normal operation

Start PostgreSQL and confirm it is healthy:

```bash
docker compose up -d
docker compose ps
python -m pump_research database health
```

Apply outstanding migrations while the collector is stopped:

```bash
python -m alembic upgrade head
```

Declare the epoch, then start the collector explicitly inside it. It runs until
you press `Ctrl+C` or send it SIGTERM:

```bash
python -m pump_research epoch create --number 1 --purpose 'first valid 24-hour research collection'
python -m pump_research collector run --epoch 1
```

In another terminal, view its durable status:

```bash
. .venv/bin/activate
python -m pump_research collector status
```

Only one collector should run against a database. Its state is reconstructed
from PostgreSQL when it restarts.

## Reports

Generate the current trailing 24-hour report:

```bash
python -m pump_research report 24h --epoch 1
```

This writes Markdown, JSON, and hourly CSV artifacts.

Generate a reproducible historical window or choose another output directory:

```bash
python -m pump_research report 24h --epoch 1 --end-at 2026-08-15T12:00:00Z
python -m pump_research report 24h --epoch 1 --output-directory reports/example
```

## PostgreSQL and Docker

```bash
docker compose ps                 # Container and health status
docker compose logs -f postgres   # Follow PostgreSQL logs; Ctrl+C exits logs
docker compose stop postgres      # Stop PostgreSQL but preserve its volume
docker compose up -d postgres     # Start it again
docker compose down               # Remove containers; named data volume remains
```

Do not run `docker compose down -v`: `-v` removes the persistent database
volume.

Check migration state:

```bash
python -m alembic current
python -m alembic history
python -m alembic check
```

Open an interactive PostgreSQL prompt:

```bash
docker compose exec postgres psql -U pump_research -d pump_research
```

Useful commands inside `psql`:

```sql
\dt
\d tokens
\d observations
\q
```

Useful read-only queries:

```sql
SELECT count(*) AS tokens FROM tokens;
SELECT count(*) AS pairs FROM pairs;
SELECT count(*) AS observations FROM observations;
SELECT count(*) AS pending_dex
FROM dex_availability_tasks
WHERE state = 'PENDING_DEX';

SELECT lifecycle_state, count(*)
FROM poll_schedules
GROUP BY lifecycle_state
ORDER BY lifecycle_state;

SELECT received_at, pair_id, price_usd, liquidity_usd, volume_m5_usd
FROM observations
ORDER BY received_at DESC
LIMIT 20;

SELECT id, started_at, collection_started_at, finished_at, status, last_heartbeat_at
FROM collector_runs
ORDER BY started_at DESC
LIMIT 10;

SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;
```

Always use a time filter or `LIMIT` when inspecting high-volume tables. Do not
manually `DELETE`, `UPDATE`, or `TRUNCATE` research tables.

## Development checks

```bash
PUMP_RESEARCH_TEST_DATABASE_URL='postgresql+asyncpg://pump_research:pump_research@localhost:5433/pump_research_capacity_test' \
  PUMP_RESEARCH_ENVIRONMENT=test \
  python -m pytest
python -m ruff check .
python -m mypy
```

Integration collection refuses to run without the explicit test URL and test
environment. Immediately before destructive SQL it checks PostgreSQL's actual
`current_database()` for an approved structural test marker. Never point
`PUMP_RESEARCH_TEST_DATABASE_URL` at a collector database.

Epoch, archive, backup, and safety commands are documented with the controlled
run procedure in [epoch1-readiness.md](epoch1-readiness.md).

Equivalent Makefile shortcuts are available:

```bash
make db-up
make db-status
make migrate
make db-health
make check
make db-down
```

Configuration comes from `.env`. Never place real credentials in
`.env.example` or commit `.env`.

## S3-compatible archive copies

Configure all six provider-neutral settings before using object storage:

```text
PUMP_RESEARCH_ARCHIVE_S3_ENDPOINT_URL
PUMP_RESEARCH_ARCHIVE_S3_BUCKET
PUMP_RESEARCH_ARCHIVE_S3_PREFIX
PUMP_RESEARCH_ARCHIVE_S3_ACCESS_KEY_ID
PUMP_RESEARCH_ARCHIVE_S3_SECRET_ACCESS_KEY
PUMP_RESEARCH_ARCHIVE_S3_REGION
```

The endpoint must be HTTPS and cannot contain user information, a path, query
data, or a fragment. Credentials are passed explicitly to a SigV4 Boto3 client; the SDK
cannot fall back to ambient credentials. The access key ID and secret key are
secret settings and are excluded from configuration snapshots and output.

Run the non-destructive readiness probe first. It uploads a small deterministic
object below `_readiness/pump-research/archive-storage/v1/`, checks its length,
fully reads it back, and verifies SHA-256. It prints `PASS` or `FAIL`. The probe
is intentionally retained because the archive writer has no delete operation;
repeated runs reuse the same content-addressed key.

```bash
python -m pump_research archive s3-readiness
```

Copy an already verified canonical archive, publishing the manifest last, then
independently re-read an existing copy:

```bash
python -m pump_research archive copy-s3 /path/to/manifest.json \
  --role secondary \
  --independent-copy \
  --independence-detail 'separate storage provider and physical failure domain'

python -m pump_research archive verify-s3-copy /path/to/manifest.json \
  --role secondary \
  --independent-copy \
  --independence-detail 'separate storage provider and physical failure domain'
```

The assertion is mandatory for a secondary copy. Every object is checked by
content length and full SHA-256 readback before the catalog records it. These
commands provide no delete action.
