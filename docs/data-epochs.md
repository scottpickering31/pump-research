# Collection data epochs

Collection epochs are explicit research boundaries. Facts from different epochs
must not be silently combined. Each valid research fact is traceable through its
collector run to exactly one epoch, and reports select an epoch before applying a
time window.

## Epoch 0 — permanently lost engineering burn-in

- Purpose: engineering burn-in only.
- Began: 2026-08-15.
- Status: invalid; data lost.
- Research validity: **NONE**.
- Incident: an integration-test fixture executed `TRUNCATE ... CASCADE` against
  the live `pump_research` database.
- PostgreSQL `archive_mode` was off and no local logical or physical dump existed.
- The dataset is unrecoverable. No recovery will be attempted.
- Partial remnants, if discovered later, must not be spliced into Epoch 1 or used
  in research, backtesting, calibration, or completeness claims.

The incident demonstrated that a test URL string is not a sufficient safety
boundary. Resulting fixes require an explicit test URL, an explicit `test`
environment, and an immediate pre-operation check of PostgreSQL's actual
`current_database()` value. Epoch declarations and transitions are durable,
collector runs reference an epoch, storage is sampled, archive exports are
verified, and backup verification is reported independently.

## Epoch 1 — first valid research epoch

- Purpose: first valid 24-hour research collection and measurement run.
- Initial intended duration: 24 hours.
- Initial status: planned; it must be created explicitly.
- It starts only after the Epoch 1 readiness safeguards and quality gates pass.
- Its exact UTC lifecycle start is the durable `running` epoch event written in the same
  startup transaction as the first collector run. This is not proof of live collection.
- Effective live-work intervals are derived from each run's separately committed
  `collection_started_at`; restart gaps remain gaps and source-specific coverage requires source
  evidence.
- Restarts remain inside Epoch 1 and do not change its original start time.
- It ends only through an explicit epoch close command after the collector has
  stopped.

No automatic archive deletion, partition removal, compaction, or retention
cleanup is authorized during Epoch 1. The run exists to measure actual database
growth, archive compression, API demand, achieved scheduler cadence, and dataset
completeness before a longer-term retention decision is made.
