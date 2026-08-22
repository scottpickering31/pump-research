# Storage-efficiency audit before the 24-hour run

## Scope and safety

This audit is diagnostic only. It made no schema changes, deleted no rows, and
did not change observation fields, lifecycle behaviour, or polling policy. All
database queries were read-only while the live collector continued writing.

## Approved follow-up implementation

Migration `6a71c2d90e4b` implements only the two subsequently approved changes:

- it creates immutable `lifecycle_policies`, verifies that each historical
  digest maps to exactly one JSON document, backfills the distinct documents,
  adds a restricted foreign key, and makes the old inline snapshot nullable;
- it leaves every historical inline snapshot untouched while repository writes
  store `NULL` in that compatibility column and resolve the exact policy by
  digest;
- its downgrade rehydrates normalized-only rows before restoring `NOT NULL`, so
  the migration is losslessly reversible;
- it removes `ix_poll_batch_members_token_due_at` while retaining the `due_at`
  evidence column and the `(token_id, claimed_at)` reporting index.

Read-only live diagnostics before implementation recorded zero scans of every
due-time partition index. The scheduler completion plan used the partition
primary key, and the cadence-report plan used `(token_id, claimed_at)`. The
live policy document measured exactly 441 bytes on every inspected row, so the
new write path saves approximately **44.1 MB (42.1 MiB) per 100,000 future
evaluations**, plus the avoided due-time index growth. The migration was tested
on disposable databases and was not applied to the live database during this
implementation task.

The user-provided baseline represents approximately the first 30 minutes:
1,050 tokens, 1,038 pairs, 87,637 observations, 87,817 lifecycle-evidence
evaluations, and 88,087 poll-batch members. A
second catalog snapshot was taken between 2026-08-15 12:34 and 12:40 UTC after
the collector had continued running during the audit. Counts therefore vary
slightly between individual diagnostic queries. This is expected and is stated
explicitly rather than treating a moving database as one transactional
snapshot.

## Executive findings

1. `lifecycle_evidence_evaluations` is the clear normalization priority. Every
   inspected evaluation used the same 441-byte policy JSON. Its `reason_detail`
   averaged another 531 bytes and, in this run, duplicated evidence reachable
   through the selected observation, pair, token, and API-request references.
   Together these JSON values were about 80% of the logical row and about 61%
   of total relation bytes per row.
2. `api_request_log` is not unexpectedly bloated. The apparent missing space is
   its PostgreSQL TOAST relation, which holds compressed raw DEX responses. At
   the later snapshot the base heap was about 12.0 MiB, ordinary indexes 1.6
   MiB, and TOAST plus auxiliary storage about 39.5 MiB. Raw responses are
   required research evidence and should not be discarded.
3. `poll_batch_members` has three B-tree indexes. Their combined size is larger
   than the heap because each narrow 112-byte heap row has roughly 172 bytes of
   index entries. The primary key and `(token_id, claimed_at)` index have
   defensible operational/reporting uses. No application query uses
   `(token_id, due_at)`; it is a candidate for later removal after approval.
4. The direct 30-minute database-growth extrapolation is approximately 11.5
   GiB/day, 0.34 TiB/30 days, 1.01 TiB/90 days, and 4.09 TiB/365 days. This is a
   short-window point estimate, not a forecast: lifecycle downshifts may lower
   rates, while continuing discovery grows the monitored population.
5. A lossless lifecycle-policy normalization is safe. A cautious staged design
   can retain every existing inline snapshot, store each policy once, and stop
   duplicating it on future evaluations. Compacting `reason_detail` also looks
   promising, but should wait until multi-pair and failed-selection live cases
   have been verified.

## Measurements

### User-provided 30-minute baseline

| Relation | Rows | Heap/table | Indexes | Total |
|---|---:|---:|---:|---:|
| `lifecycle_evidence_evaluations_2026_08` | 87,817 | 116 MiB | 20 MiB | 136 MiB |
| `observations_2026_08` | 87,637 | 23 MiB | 13 MiB | 36 MiB |
| `api_request_log` | not supplied | 7.6 MiB | about 1 MiB | 32 MiB |
| `poll_batch_members_2026_08` | 88,087 | 9.7 MiB | 14 MiB | 24 MiB |
| Entire database | — | — | — | at least 245 MiB |

### Later live snapshot

At 12:37 UTC, `pg_database_size` was 422,591,511 bytes (403 MiB), with
approximately 1,504 tokens, 142,057 observations/evaluations/members, and 6,925
API-request rows. A later exact per-row query saw 149,047 rows in each of the
three partitioned fact tables and 7,215 API-request rows:

| Relation | Heap bytes/row | Index bytes/row | TOAST/aux bytes/row | Total bytes/row |
|---|---:|---:|---:|---:|
| `lifecycle_evidence_evaluations_2026_08` | 1,365 | 233 | less than 1 | **1,599** |
| `observations_2026_08` | 269 | 155 | less than 1 | **424** |
| `api_request_log` | 1,746 | 232 | 5,735 | **7,713** |
| `poll_batch_members_2026_08` | 112 | 172 | less than 1 | **284** |

The later physical totals were approximately 227 MiB for lifecycle evidence,
60 MiB for observations, 53 MiB for API requests, and 40 MiB for poll members.
Append-only tables had no material dead-row count; the sizes are data and index
content, not update/delete bloat.

### Fields dominating storage

#### Lifecycle evidence

Across approximately 142,000 live evaluations:

- distinct `policy_sha256` values: **1**;
- distinct `policy_snapshot` documents: **1**;
- stored size of every policy snapshot: **441 bytes**;
- average `reason_detail`: **531.39 bytes** (range 523–561);
- average logical composite row: **1,209.69 bytes**;
- every outcome was `selected / only_candidate_pair`;
- every reason document had exactly one candidate.

`policy_snapshot` and `reason_detail` therefore represented approximately 36%
and 44% of the logical row respectively. At the original 87,817-row baseline,
the inline policy copies occupied about 36.9 MiB and the reason documents about
44.5 MiB before page-level overhead.

Every `reason_detail` document was byte-distinct because it embeds observation
UUIDs and timestamps; placing those whole documents in a hash-deduplicated JSON
table would not help. The contents are nevertheless mostly redundant:

- candidate `observation_id`, receipt time, pair ID, liquidity, and volume values
  are available through the immutable observation;
- chain, pair address, and DEX identifier are available through the pair;
- the selected observation and pair are already typed columns on the evaluation;
- the evaluation already references the token and exact API request;
- selected pair address and selected liquidity are repeated again outside the
  candidate array;
- `liquidity_tie_count` is reconstructable by applying the versioned policy to
  the candidate observations.

A reconstruction diagnostic joined every evaluation to observations from its
`(token_id, api_request_log_id)`. Across 151,087 evaluations it found **zero
missing candidate sets and zero candidate-count mismatches**. This supports
normalization, but the current live sample contains no multi-candidate or failed
selection, so it is not yet sufficient evidence to compact every reason shape.

#### API request log and TOAST

At about 7,000 request rows:

- average complete logical row: **6,868 bytes**;
- average request payload: **1,064 bytes**;
- average stored response payload: **5,513 bytes**;
- average JSON text before TOAST compression: **16,237 bytes**;
- stored-to-text ratio: **0.3395** (about 66% smaller);
- 4,887 responses exceeded 2,000 stored bytes;
- the TOAST relation contained about 5,120 values in about 21,092 chunks;
- PostgreSQL used its default `pglz` TOAST compression.

The later physical breakdown was approximately:

| Component | Size |
|---|---:|
| Base heap | 12.0 MiB |
| Normal indexes | 1.6 MiB |
| TOAST heap and TOAST index | 39.5 MiB |
| Total | 53.1 MiB |

This explains why `pg_total_relation_size` is much larger than
`pg_relation_size + pg_indexes_size`: the former includes the separate TOAST
table, its index, free-space maps, and visibility maps.

Exact response hashes were not overwhelmingly repetitive. Of 6,953 rows in one
snapshot, 5,782 response hashes were distinct. Exact response-document
normalization would have avoided only about 1.27 MiB at that point. Empty
responses accounted for 796 duplicate rows but only 19.9 KiB; repeated nonempty
responses accounted for about 1.28 MiB. Repeated request documents represented
another 294 KiB. Static pair metadata does repeat across *different* raw
responses, but factoring fields out of those responses would make the original
provider document harder to reproduce and is not recommended.

#### Poll batch members and indexes

The average poll-member logical row was 106.6 bytes and physical heap use was
about 111.8 bytes. Index storage was about 172.2 bytes/row (61% of total bytes):

| Index | Snapshot size | Observed scans | Assessment |
|---|---:|---:|---|
| Primary key `(claimed_at, batch_id, token_id)` | 8.3 MiB | 7,357 | Keep: idempotency, batch completion, and restart evidence. |
| `(token_id, claimed_at)` | 7.9 MiB | 0 | Keep: explicitly used by 24-hour cadence/largest-gap queries; the report had not run during this statistics window. |
| `(token_id, due_at)` | 7.9 MiB | 0 | Candidate to remove: no repository, status, scheduler, or report query uses it. |

`pg_stat_statements` is not installed, so code-path inspection and
`pg_stat_user_indexes` were both used. Zero scans alone is not proof of
uselessness; this is why `(token_id, claimed_at)` must remain despite its zero
count. Removing `(token_id, due_at)` would not remove due-time evidence and is
not needed for lease recovery.

#### Observations

Observations are comparatively efficient. The average logical row was 260.9
bytes: about 138.7 bytes for identity/provenance/timestamps and 86.2 bytes for
all market-value columns combined. Its three indexes were all used and serve
primary-key, request/pair idempotency, and pair time-series access. No
observation field or index is recommended for removal.

## Current-rate capacity projection

The first-30-minute counts imply these point rates:

| Relation | Rows/day | Measured total bytes/row | Estimated GiB/day |
|---|---:|---:|---:|
| Lifecycle evidence | 4,215,216 | 1,599 | 6.28 |
| Observations | 4,206,576 | 424 | 1.66 |
| Poll members | 4,228,176 | 284 | 1.12 |
| API requests | about 203,600 | 7,713 | 1.46 |
| **Four-table subtotal** | — | — | **10.52** |

The API request rate is derived from the observed request-to-observation ratio;
the original 32 MiB/30-minute API relation measurement independently implies
about 1.5 GiB/day.

Using the more conservative whole-database baseline of 245 MiB per 30 minutes:

| Horizon | Linear raw PostgreSQL projection |
|---|---:|
| 1 day | **11.48 GiB** |
| 30 days | **344.5 GiB (0.34 TiB)** |
| 90 days | **1,033.6 GiB (1.01 TiB)** |
| 365 days | **4,191.8 GiB (4.09 TiB)** |

These figures cover PostgreSQL relation storage only. Filesystem capacity must
also allow for WAL, backups, temporary query space, migration working space,
and normal free-space variation. Conversely, this simple linear projection does
not model future `FADING`/`DORMANT` cadence reductions. The estimate should be
recomputed from hourly deltas after 6 and 24 hours.

## Proposed changes (no changes made in this audit)

### 1. Normalize lifecycle policies — recommended first

Use the proposed immutable table:

```text
lifecycle_policies
    policy_sha256    PRIMARY KEY
    policy_snapshot  JSONB NOT NULL
    created_at       TIMESTAMPTZ NOT NULL
```

`lifecycle_evidence_evaluations.policy_sha256` already exists, is non-null, and
participates in the idempotency key. Add an `ON DELETE RESTRICT` foreign key to
`lifecycle_policies`; prevent policy updates/deletes with the existing immutable
table trigger pattern. The collector should insert the policy document once
with `ON CONFLICT DO NOTHING`, verify an existing row has identical content,
then insert evaluations containing only the hash reference.

Safe, no-deletion rollout:

1. Create and protect `lifecycle_policies`.
2. Backfill every distinct `(policy_sha256, policy_snapshot)` pair. Abort if one
   hash maps to multiple documents or if the canonical application hash does
   not equal the stored key.
3. Add the foreign key as `NOT VALID`, validate it, and test historical joins.
4. Make the old inline snapshot nullable and dual-read from the normalized
   policy table. Leave every existing snapshot untouched.
5. Deploy new writes without an inline copy. Existing rows remain byte-for-byte
   intact while future evaluations become smaller.
6. Only under a later explicit retention/migration approval, consider removing
   old duplicate column copies. Copying them to the immutable policy table is
   lossless, but PostgreSQL will not return old heap space merely from a
   metadata-only column drop; a partition rewrite would require maintenance
   time and additional temporary disk.

Historical reproducibility remains complete: an evaluation joins by its stored
hash to the exact immutable policy JSON. At the initial rate, stopping the
441-byte copy saves about **1.73 GiB/day**, 51.9 GiB/30 days, 155.8 GiB/90 days,
and roughly 632 GiB/year. The normalized policy row and its primary-key index
are negligible while only one policy exists.

### 2. Compact lifecycle selection reason evidence — promising but conditional

A hash/document table will not help because all reason documents are unique.
Instead, retain the typed `outcome`, `reason_code`, selected observation/pair,
token, request, watermark, and normalized policy reference, then reconstruct
candidate values from immutable observations and the raw request. A small
typed `candidate_count`/`tie_count` may be retained if fast audits need it.

Before changing new writes:

- prove reconstruction for multi-pair, response-order reversal, missing
  liquidity, ties, selected-pair changes, and explicit failure outcomes;
- decide whether pair identity fields need database-level immutability or
  whether a narrow candidate-to-observation reference table is safer;
- compare old `reason_detail` against reconstructed canonical detail for every
  historical row and require zero mismatches;
- preserve old inline reason documents during the first rollout, as with policy
  snapshots.

If a future compact representation averages 32–64 bytes rather than 531 bytes,
it would save approximately **1.83–1.96 GiB/day** at the initial rate. This is
not yet classified as safe for production because the live sample contains
only one-candidate successful selections.

### 3. Remove the unused poll-member due-time index — recommended after approval

Drop only `ix_poll_batch_members_token_due_at` (and its partition indexes) after
an `EXPLAIN` regression suite confirms current query plans. Keep the `due_at`
column and all rows. Expected future saving is about 57 bytes/member, or **0.22
GiB/day** at the observed rate. Migration planning must account for PostgreSQL's
partitioned-index DDL locking; do it during a controlled maintenance window,
not casually during the burn-in.

### 4. API response-document normalization — safe but low priority

A possible immutable table is:

```text
api_response_documents
    response_payload_sha256  PRIMARY KEY
    response_payload         JSONB NOT NULL
    created_at               TIMESTAMPTZ NOT NULL
```

Each request would retain its timestamp, outcome, membership/request payload,
and response hash while referencing the exact shared document. This preserves
unchanged repeated responses as separate observations and requests. However,
the measured exact-duplicate saving was only about 1.27 MiB and a new document
primary-key index would offset part of it. Do not implement this before the
24-hour report provides a more representative duplicate distribution.

Do not factor static-looking fields out of nonidentical raw responses. TOAST is
already compressing response JSON to about 34% of its text size, and the raw
source document must remain losslessly reproducible.

## Expected savings summary

| Candidate | Confidence | Estimated future saving at current rate |
|---|---|---:|
| Normalize policy snapshot | High | 1.73 GiB/day |
| Compact reason evidence | Conditional | 1.83–1.96 GiB/day |
| Remove unused poll-member index | Medium/high after query-plan tests | 0.22 GiB/day |
| Normalize exact API response documents | Low benefit | roughly 0.02–0.03 GiB/day before new-index overhead |

The high-confidence policy normalization plus candidate index removal would
reduce projected growth by about 1.95 GiB/day without changing facts or
behaviour. Including reason compaction after its stronger proof would bring the
potential reduction to roughly 3.8–3.9 GiB/day, about one third of the initial
whole-database growth rate.

## Migration risks and preservation assessment

- **No current data must be deleted.** An additive policy table and dual-read
  rollout can preserve every existing row exactly and shrink only future rows.
- **Hash collisions/content mismatch must fail closed.** Never use
  `ON CONFLICT DO NOTHING` without comparing the existing JSON document to the
  supplied canonical snapshot.
- **DDL locks matter.** Parent-table/partition constraint and index changes can
  briefly block collector writes. Use `NOT VALID` foreign keys, later
  validation, explicit lock/statement timeouts, and a supervised maintenance
  window.
- **Space reclamation is separate from logical normalization.** Rewriting the
  active August partition or running `VACUUM FULL` would require blocking and
  substantial temporary disk. It is unnecessary to gain savings on future
  writes and is not recommended during burn-in.
- **Reason compaction has incomplete live coverage.** Current reconstruction is
  exact for the observed one-pair success case, but live multi-pair/failure
  evidence must be validated before changing its representation.
- **Index removal is reversible but should be measured.** The index can be
  recreated without changing data, but building it on a much larger live
  partition later consumes I/O and temporary space.
- **Raw API response normalization is semantically safe only by exact hash.**
  Never collapse request timestamps, unchanged observations, or partially
  similar provider documents.

Conclusion: policy normalization can safely be introduced later as an additive,
lossless migration while retaining all existing data. Poll-index removal is
also safe for research integrity after query-plan verification. Reason-detail
compaction is likely the next-largest saving but remains gated on representative
multi-pair/failure reconstruction tests. No change should be applied until the
24-hour run or explicit approval, and none was applied by this audit.
