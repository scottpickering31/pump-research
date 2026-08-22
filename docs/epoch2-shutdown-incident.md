# Epoch 2 shutdown reconciliation

## Incident and data disposition

Epoch 2 collection was deliberately stopped with Ctrl+C after its long-duration
run. The process and DEX traffic stopped, but collector run
`4e91d74b-1027-4b24-8a5f-ebc35b13229e` remained durably `running`. Its last
heartbeat was `2026-08-17T18:25:48.652929Z`; the scheduled-observation health
projection then recorded `BrokenPipeError` at
`2026-08-17T18:25:50.797352Z`. The final verified backup already exists.

Epoch 2 remains valid. Its approximately 9.2 million observations are not
deleted, rewritten, invalidated, or reassigned. Reconciliation changes only the
mutable run/component operational projections and appends immutable terminal
evidence. Epoch closure remains a separate explicit action.

## Root cause

The documented launch pipeline runs the collector and `tee` in one foreground
process group:

```bash
caffeinate ... .venv/bin/python -m pump_research collector run --epoch 2 \
  2>&1 | tee -a logs/epoch2/collector.log
```

Ctrl+C sends SIGINT to both processes. Python's installed signal handler starts
a bounded graceful drain, while `tee` may terminate immediately and close the
collector's stdout pipe. A structured log emitted by an in-flight scheduled
observation then raised `BrokenPipeError`. The worker correctly surfaced what it
believed was an unexpected exception, but the runtime had placed run
finalization after worker drain and component-stop persistence. The exception
therefore bypassed finalization. The outer process-lock cleanup still ran, which
explains the observed combination: no collector process or advisory lock, but a
stale `running` row.

## Fix and invariants

- Structured collector logging uses a closed-pipe-safe writer. Losing a log
  consumer cannot enter collection control flow.
- SIGINT and SIGTERM stop new loop iterations, wait for bounded in-flight work,
  cancel after the configured grace period if necessary, mark component
  projections stopped, and finalize the run as `stopped` with `finished_at`.
- Run finalization is in the runtime's guaranteed cleanup path. Unexpected
  worker termination still finalizes as `failed` and is re-raised.
- Each terminal transition appends one immutable `collector_run_events` row in
  the same transaction as the mutable run update.
- A hard process death remains a `running` row with a stale heartbeat until it
  is classified as failed by restart recovery or explicitly reconciled.
- The epoch is never closed automatically.

## Safe stale-run reconciliation

`collector reconcile-stale` is not a generic status override. It refuses unless:

1. the requested epoch exists and is still running;
2. its latest run is still `running`;
3. the heartbeat exceeds the configured stale threshold; and
4. it can acquire the exact database session advisory lock required by every
   collector process.

The lock check prevents repair while a genuine collector is active. On success,
one transaction preserves the original start and heartbeat, records the
operator reason and prior component state in immutable terminal evidence, sets
`finished_at` to the reconciliation time (the exact exit instant is unknown),
marks the run `stopped`, and changes current component projections to `stopped`.
Re-running the command while the epoch remains open is idempotent and cannot
create a second event. Once the epoch is closed, reconciliation is correctly
refused because there is no longer a running epoch to repair.

## Verified command sequence for Epoch 2

Set the live URL in the shell without printing it, then apply the additive
migration. Do not start a collector:

```bash
export PUMP_RESEARCH_DATABASE_URL='postgresql+asyncpg://pump_research:pump_research@localhost:5433/pump_research'
.venv/bin/python -m alembic upgrade head
```

Reconcile only the stale Epoch 2 run:

```bash
.venv/bin/python -m pump_research collector reconcile-stale \
  --epoch 2 \
  --reason "Operator-requested Ctrl+C shutdown after completed Epoch 2 validation and verified final backup; stale run reconciled after confirming no collector advisory lock was active."
```

Verify it is stopped while Epoch 2 remains open:

```bash
.venv/bin/python -m pump_research collector status
```

Expected fields include `operational_state=STOPPED`,
`run_lifecycle=GRACEFULLY_STOPPED`, `actively_collecting=false`, a non-null
`collector_run.finished_at`, and `collection_epoch.status=running`.

Close Epoch 2 explicitly without invalidating it:

```bash
.venv/bin/python -m pump_research epoch close \
  --number 2 \
  --status completed \
  --reason "Completed successful long-duration collector validation; final verified backup captured before shutdown."
```

Verify the terminal epoch projection:

```bash
.venv/bin/python -m pump_research epoch status --number 2
```

Expected: `status=completed`, `data_valid=true`, and a non-null `ended_at`.
Do not create or start Epoch 3 as part of this procedure.

## Applied live result

The verified migration and recovery were applied on 2026-08-17. The advisory
lock was available, so reconciliation finalized run
`4e91d74b-1027-4b24-8a5f-ebc35b13229e` as `stopped` at
`2026-08-17T18:43:48.257101Z` and appended exactly one terminal event. Epoch 2
was then closed `completed` and valid at `2026-08-17T18:44:15.386089Z`.
Observation count was 9,202,662 immediately before and after reconciliation and
closure. No collector was started and no Epoch 3 was created.
