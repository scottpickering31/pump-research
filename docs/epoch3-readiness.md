# Epoch 3 readiness and controlled deployment

Status: **NO-GO as of 2026-08-22**. This is an unexecuted runbook. It must not be interpreted as
authorization to migrate live, create Epoch 3, or start collection.

## Gate decision

| Action | Decision | Blocking conditions |
|---|---|---|
| Apply V2 migrations live | NO-GO | no full 20 GB clone lock/timing rehearsal; host free disk 2.35%; final dump begins at `e4b7a9c1d203` |
| Create Epoch 3 | NO-GO | migration and provider gates not complete; target host/backup paths not approved |
| Start Epoch 3 | NO-GO | no production-ready Solana holder RPC; full integrated resource measurements absent |

## Numeric 24–72 hour validation thresholds

| Signal | GO/healthy | Warning | Immediate STOP / investigation |
|---|---:|---:|---:|
| heartbeat age | <=30 s | 30–60 s | >60 s or process/status disagreement |
| continuity | no unexplained gap | one interval late with durable reason | unexplained gap >2 effective intervals for protected work or >5 min global gap |
| DEX rolling requests | <=175/min normally | 175–191/min | >=192 scheduled-safe for 5 min or >=240 at any point |
| DEX HTTP 429 | 0 | 1–2/min transient | >=3/min for 3 min or any retry-budget bypass |
| DEX HTTP 5xx | <1%/5 min | 1–5% | >5% for 5 min |
| ACTIVE/RESURRECTED p95 lateness | <=10 s | 10–30 s | >30 s for 5 min |
| INITIAL NEW p95 lateness | <=30 s | 30–90 s | >90 s for 10 min |
| scheduler mode | NORMAL | DEGRADED with honest intervals | CRITICAL >5 min or protected-only overload |
| legacy unmapped schedules | 0 | n/a | any nonzero |
| candidate oldest task | <10 min | 10–30 min | >30 min with core healthy |
| Tier 2 oldest task | <15 min | 15–60 min | >60 min outside declared provider outage |
| Tier 3 oldest task | <60 min | 1–6 h | unbounded growth or budget bypass |
| basic Solana RPC | <=2/min, <1% failures | isolated retry/429 | sustained 429/error >10 min; disable evidence path, never core |
| indexed provider | disabled in Mode A | unavailable is explicit | any unavailable/failed fact represented as zero/safe |
| DB growth | <=16 GiB/day extrapolated | 16–20 | >20 GiB/day for 2 h or unexplained jump |
| archive lag | <2 h after closed range | 2–6 h | >6 h, checksum failure, or gap/overlap |
| verified archive | all closed scopes | verification pending | corrupt/mismatched scope |
| free disk | >=35% | 20–35% | <20% or <3 days projected runway |
| RSS | <75% host RAM | 75–85% | >85% for 10 min/OOM |
| CPU | <70% sustained | 70–90% | >90% for 15 min with growing core lateness |
| backup age | <24 h | 24–36 h | >36 h or verification fails |

Any immutable provenance mismatch, future-data leakage, duplicate semantic fact with different
content, or unaccounted observation gap is an immediate stop and makes the validation invalid until
explicit review. A provider-only outage may leave optional evidence unavailable without invalidating
core market collection if the outage is durable, bounded, and declared in the epoch report.

## Future live deployment procedure — do not execute yet

Replace bracketed paths and record every command/output in the change ticket. Use the exact reviewed
commit. Do not combine steps into an unattended script.

1. Verify no collector process or `caffeinate` wrapper:

   ```bash
   pgrep -ifl 'python.*pump_research.*collector|python.*pump-research.*collector|caffeinate.*pump-research'
   ```

   Expected: no output.

2. With the current live code/schema, verify Epoch 2 is completed/valid, latest run stopped,
   observations = 9,202,662, epochs = 0/1/2, and advisory locks = 0. Because the current V2 status
   reader expects unapplied tables, use the documented read-only pre-migration SQL check for this
   one step and save its output.

3. Verify the existing final backup catalog and file:

   ```bash
   .venv/bin/python -m pump_research backup status --epoch 2
   .venv/bin/python -m pump_research backup verify \
     /absolute/independent/path/epoch2-final.dump --epoch 2 --independent-copy
   ```

   Confirm SHA-256 `21acd1e0e46421250cb4ce6c302e15066e66d52c12f285b92f7acd3491584846`.

4. Create a fresh custom-format pre-migration dump to independent storage:

   ```bash
   pg_dump --format=custom --file=/absolute/independent/path/epoch2-premigration-YYYYMMDD.dump pump_research
   pg_restore --list /absolute/independent/path/epoch2-premigration-YYYYMMDD.dump
   shasum -a 256 /absolute/independent/path/epoch2-premigration-YYYYMMDD.dump
   ```

5. Copy that dump and checksum to a second physical/provider destination, re-read it there, and
   record independent verification. Two directories on one machine do not qualify.

6. Require at least 35% free target disk and at least 45 GiB temporary rehearsal headroom. Confirm
   the target can hold live DB + fresh dump/restore + 15 GiB WAL/staging without crossing 65% use.

7. Restore the fresh dump into a new isolated database on the target host and repeat the entire
   Phase 7 migration/reconstruction rehearsal. The dump's embedded Alembic revision is authoritative;
   do not assume it equals live.

8. During the approved live maintenance window, apply and verify one step at a time:

   ```bash
   .venv/bin/python -m alembic upgrade 7c31a8e4d5f2
   .venv/bin/python -m alembic current
   .venv/bin/python -m alembic upgrade b184a7d2e903
   .venv/bin/python -m alembic current
   .venv/bin/python -m alembic upgrade c61e29d841af
   .venv/bin/python -m alembic current
   .venv/bin/python -m alembic upgrade e52a1c9d704f
   .venv/bin/python -m alembic current
   .venv/bin/python -m alembic upgrade f63b7d9a20ce
   .venv/bin/python -m alembic current
   .venv/bin/python -m alembic check
   ```

   If the fresh dump restores below `f2c8d4a6197e`, first upgrade to `f2c8d4a6197e` and verify the
   graceful-stop state against authoritative final live facts.

9. Verify observation/token/epoch counts, Epoch 2 validity, no running collector, and no advisory
   lock. Run `collector status`; it must now show `legacy_unmapped_population` equal to the legacy
   schedule count because reconstruction is intentionally epoch-start work.

10. Configure archive root, primary object store, physically independent secondary store, and
    credential scopes. Verify upload/readback without delete permission.

11. Configure accepted provider endpoints. For the validation recommendation use Mode A and ensure
    advanced fields explicitly report unavailable. A dedicated/private Solana endpoint must pass
    the provider acceptance suite; never use the failing public holder path as a safe fact.

12. Explicitly create the planned validation epoch only after human approval:

    ```bash
    .venv/bin/python -m pump_research epoch create \
      --number 3 --name 'Epoch 3' \
      --purpose '24-72h integrated Collector V2 validation'
    .venv/bin/python -m pump_research epoch status --number 3
    ```

13. Create and independently verify a fresh Epoch 3 prestart dump after the planned epoch exists.

14. Start explicitly. On macOS validation use the tested pipeline; on the VPS prefer systemd with
    direct stdout/stderr capture and `KillSignal=SIGTERM`:

    ```bash
    mkdir -p logs/epoch3
    caffeinate -dimsu .venv/bin/python -m pump_research collector run --epoch 3 \
      2>&1 | tee -a logs/epoch3/collector.log
    ```

    Epoch start, scheduler reconstruction, and run creation are one transaction. After it commits,
    the runtime commits the run's `collection_started_at` in a separate short transaction before it
    creates any provider worker. Allow 15 minutes for the one-time 58.9k-schedule audit write. If it
    fails, no partial epoch-start projection or live boundary should commit.

15. In another terminal, capture `collector status` at 1, 10, and 30 minutes, 2 hours, and 24 hours.
    At the first sample require zero legacy-unmapped schedules, no leases left from initialization,
    demand below safe capacity, and no mass overdue avalanche. Apply the numeric thresholds above.

16. Stop with one Ctrl+C/SIGTERM, wait for `GRACEFULLY_STOPPED`, verify locks = 0, export/verify the
    final closed scope, take and verify the final backup, then explicitly close Epoch 3. Do not close
    while genuinely active.

## Rollback and migration-failure recovery

If a migration fails:

1. keep the collector stopped and do not create/start Epoch 3;
2. record `alembic current`, PostgreSQL error, active locks, disk, and WAL state;
3. if the failed revision transaction rolled back, verify the prior head and counts before deciding
   whether to retry;
4. do not automatically downgrade a database that has accepted V2 facts;
5. restore the verified pre-migration dump into a **new** database, verify schema/counts/checksums,
   then atomically repoint only after human approval;
6. preserve the failed database for diagnosis until the replacement is verified.

Failures after Phase 1, 2, 3, 5, or 6 use the same restore-first rule. Isolated downgrades passed,
but they drop later evidence families and are not a substitute for a verified backup. If collector
startup fails during epoch initialization, the transaction must leave the planned epoch and legacy
projection unchanged; verify before retry. If it fails after providers start, stop, preserve facts,
and explicitly abort/invalid the validation only when continuity/provenance requires it.

## Final blockers

- Move the rehearsal to a host with >=45 GiB free temporary headroom.
- Restore all 9,202,662 observations and measure every migration lock on the real partition tree.
- Measure integrated CPU, RSS, DB writes/sec, WAL, IOPS, and p95 commit latency under replay load.
- Acceptance-test a private Solana RPC for basic security and top-holder calls.
- Establish and verify a physically/provider-independent archive target.
- Repeat backup restore and hot/cold market/security checksum equivalence on the full scope.
