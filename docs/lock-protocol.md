# PostgreSQL lock protocol

This matrix records the collection lock protocol after the Epoch 7 concurrency
audit. UUID ordering means PostgreSQL UUID ordering in an `ORDER BY`, not Python
input order or an unordered SQL `IN` result.

| Transaction/workflow | Resources and acquisition order | PostgreSQL row/advisory mode and implicit FK dependencies | Why required |
|---|---|---|---|
| Scheduler lifecycle update; lifecycle classification; ordinary candidate projection | `Token` -> `PollSchedule` (one token; request-wide classification visits token UUID order) | Token `NO KEY UPDATE`; schedule `UPDATE`; inserted lifecycle/schedule/candidate evidence takes implicit Token `KEY SHARE` | Serializes projection creation/update while remaining compatible with immutable child evidence FK checks. Token identity keys are immutable. |
| Poll claim and coverage transition | schedule/token-FK coordination advisory, exclusive -> due schedules -> new `PollBatch`/members | Transaction advisory exclusive; schedule `UPDATE SKIP LOCKED`; member Token and Batch FKs take `KEY SHARE` | The advisory lock is also the global claim/request-budget lock. It prevents a Phase 6 Token fence from overlapping a schedule-first child insert. Schedule `UPDATE` owns leases. |
| Scheduled poll fact persistence and completion | coordination advisory, shared, before any facts -> implicit Token FK locks -> per-token projection locks -> `PollBatch` -> all member schedules in token UUID order | Transaction advisory shared; Token FK `KEY SHARE`; projection Token `NO KEY UPDATE`; batch and schedules `UPDATE` | Shared holders retain completion concurrency. Early gate entry prevents advisory/Token inversion. Batch and schedule updates validate and release the durable lease. UUID order covers expired/reclaimed overlapping batches. |
| Epoch schedule reconstruction | coordination advisory, shared -> all schedules in token UUID order -> coverage/schedule evidence | Transaction advisory shared; schedules `UPDATE`; evidence FKs `KEY SHARE` | Rebuilds every projection atomically without overlapping the Phase 6 Token fence. |
| Phase 6 immutable evidence writes | optional per-identity advisory -> append-only evidence insert | Per-identity transaction advisory where used; implicit Token `KEY SHARE`; no schedule lock | FK integrity keeps evidence attached to the immutable token. Evidence commits finish before that worker starts evaluation. Concurrent workers may hold compatible `KEY SHARE` locks. |
| Phase 6 evidence evaluation fence | coordination advisory, exclusive -> Token -> candidate current state -> schedule | Transaction advisory exclusive; Token `UPDATE`; candidate state and schedule `UPDATE` | Token `UPDATE` waits behind every already-granted evidence FK `KEY SHARE`, forming the complete immutable-evidence commit barrier. The advisory gate drains schedule-first writers first, eliminating `Schedule -> Token` / `Token -> Schedule` cycles without retry. |
| Phase 2 token-security persistence | all Tokens in UUID order -> immutable snapshot inserts -> leased security tasks | Token `NO KEY UPDATE`; snapshot FK `KEY SHARE`; task `UPDATE` | Parent-first coordination is compatible with child FK checks. UUID ordering serializes overlapping reverse batches before task locks. |
| DEX availability claim | due availability tasks ordered by due time/token UUID; joined Token is read only | task relation only: `UPDATE SKIP LOCKED OF dex_availability_tasks`; Token has no row lock | Exactly-once ownership and lease mutation live entirely on the task row. The task FK preserves parent integrity; locking Token caused false `SKIP LOCKED` misses and supplied no required serialization. |
| DEX availability completion | owned availability tasks selected by lease | task `UPDATE`; Token FK remains durable | Lease predicate rejects stale owners. Present/absent sets are disjoint, and a reclaimed lease no longer matches the stale completion. |
| Candidate task claim/completion | candidate budget advisory -> due task rows; completion locks one owned task | Transaction advisory exclusive for the global rate budget; task `UPDATE SKIP LOCKED` on claim and `UPDATE` on completion | Enforces the global budget and exactly-once lease ownership. Completion is single-task. |
| Collector/epoch recovery | collector session advisory -> current epoch -> latest run -> component rows by component name | Session advisory exclusive; row `UPDATE` | Proves no live collector exists and makes stale-state reconciliation atomic. This lock domain is isolated from token/schedule workflows. |
| Archive catalog transitions | one deterministic `ArchiveScope` | scope `UPDATE`; child archive-event FKs `KEY SHARE` | Serializes one archive state machine. No token/schedule resources are acquired. |

## Inversion audit

- The formerly reachable pair was scheduler `PollSchedule UPDATE -> Token KEY SHARE`
  versus Phase 6 `Token UPDATE -> PollSchedule UPDATE`. Every schedule-first path
  now enters the shared side of the coordination advisory before either resource;
  Phase 6 enters its exclusive side before the Token fence.
- Projection coordination remains `Token NO KEY UPDATE -> PollSchedule UPDATE`.
  `NO KEY UPDATE` is compatible with the `KEY SHARE` taken by token FKs.
- Multi-token parent and completion schedule locks use token UUID order. Poll claim
  selection has a different fairness order but is globally serialized by the
  exclusive advisory and cannot overlap shared completion/reconstruction paths.
- Joined task claims name only the child/task relation in `FOR UPDATE OF ...`.
  Joined parent rows are MVCC reads unless a separate semantic parent lock is
  explicitly listed above.
- Budget and identity advisory locks use distinct fixed/hash namespaces and are not
  acquired by a workflow that later acquires another workflow's advisory lock.

No remaining `A -> B` / `B -> A` acquisition pair was found in the audited
collection, candidate, lifecycle, scheduler, DEX availability, epoch, or archive
transactions.
