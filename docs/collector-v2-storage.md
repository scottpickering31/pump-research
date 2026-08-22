# Collector V2 hot/cold storage design

Status: Phase 3 archive/eligibility pipeline implemented; retention execution remains
disabled. No partition detach, deletion, compaction, or Epoch 1/2 rewrite is authorized.

## Measured baseline

Epoch 2 finished at about 21.78 GB decimal (20.28 GiB) for 9,202,662 observations and associated operational/audit data. Between the first and last Epoch 2 storage samples, the database grew 17,929,469,952 bytes over about 35.17 hours: 12.24 GB/day or 11.40 GiB/day.

| Family | Approx. final total | Approx. total bytes/unit | Observation |
|---|---:|---:|---|
| lifecycle evidence partition | 9.86 GB | 1,072/observation | largest family; one wide row per observation |
| observations partition | 3.98 GB | 433/observation | normalized facts plus indexes |
| API request log | 3.69 GB | 9,035/request | 80% is TOAST/raw response payload |
| poll batch members partition | 2.50 GB | 259/member | complete per-token poll ledger |
| poll batches + outcomes | 1.02 GB | about 2.5 KB/request/batch combined | configuration and completion JSON repeat |
| lifecycle events | 0.23 GB | variable | scientifically valuable but not dominant |
| schedule projection + decisions | 0.35 GB | variable | projection churn plus immutable decisions |

The following growth projections are measured-baseline extrapolations, excluding WAL, temporary query space, dumps and replicas:

| Horizon | Hot PostgreSQL growth at 11.40 GiB/day |
|---|---:|
| 30 days | 342 GiB |
| 90 days | 1.00 TiB |
| 365 days | 4.06 TiB |

That is not an acceptable all-hot annual design.

## Measured Parquet behavior

Phase 3 added a read-only benchmark of the busiest Epoch 2 hour (08:00–09:00 UTC):
329,583 observation rows occupied 84,676,584 logical heap bytes and 21,742,062
Parquet/ZSTD bytes, a 3.895:1 ratio. Export throughput was 42,733 rows/s, full readback
3.48m rows/s, and the DuckDB aggregate took 13.176 ms. Maximum process RSS after the run
was about 388 MiB. Only 5.84 GB decimal disk was free, so a complete Epoch 2 export was
correctly rejected as unsafe.

A non-destructive export of Epoch 2 from 2026-08-17 17:00 to 17:10 UTC used the existing streaming Zstandard exporter:

| Metric | Value |
|---|---:|
| source/exported rows | 115,750 / 115,750 |
| logical bytes | 85.1 MB |
| Parquet bytes | 15,241,303 |
| logical-to-Parquet ratio | 5.586:1 |
| bytes/exported row | 131.67 |
| naive same-load extrapolation | 2.044 GiB/day |

Per-family Parquet bytes were observations 2.89 MB, lifecycle evidence 4.37 MB, API request log 4.97 MB, pairs 1.57 MB, poll members 0.63 MB, and all other exported families 0.82 MB. The short range repeats range-level pair/token dimensions more often than a daily export would and is not a substitute for a full Epoch 2 export. It supports a planning envelope of roughly 1.8–2.1 GiB/day for the current exported families at that load.

## V2 ingestion estimate

The conservative V2 scheduling scenario in `collector-v2-scheduling.md` is 4,082 token observations/min or 5.88m/day at one million represented tokens. Applying current physical bytes/unit without schema improvements gives approximately:

- observations: 2.37 GiB/day;
- lifecycle evidence: 5.87 GiB/day;
- poll membership: 1.42 GiB/day;
- API request log at roughly 150 requests/min: 1.82 GiB/day;
- batches, outcomes, events and indexes: another 1–2 GiB/day.

Thus the V2 scheduler alone does **not** solve storage; the naive result remains roughly 12–13 GiB/day. Before a long-term epoch, narrow lifecycle evidence, content-address repeated policy/configuration, and move verified raw/audit partitions out of the hot tier on schedule. A practical design target is 6–8 GiB/day of new PostgreSQL relations before archival and 2–3 GiB/day of compressed archive including new low-frequency/candidate facts. These are engineering targets, not measured V2 results.

| Horizon | V2 PostgreSQL ingest target, 6–8 GiB/day | Archive planning range, 2–3 GiB/day |
|---|---:|---:|
| 30 days | 180–240 GiB | 60–90 GiB |
| 90 days | 540–720 GiB | 180–270 GiB |
| 365 days | 2.14–2.85 TiB | 730 GiB–1.07 TiB |

With verified 14-day hot retention, steady ingest occupies about 84–112 GiB plus dimensions, indexes, WAL and safety headroom; PostgreSQL should still have at least 250 GB usable and preferably 500 GB–1 TB NVMe. Archive size continues to grow, so object storage—not a larger unbounded Postgres—is the annual system of record.

## Hot PostgreSQL contract

Recommended initial hot window: 14 days for high-volume immutable facts, subject to a full V2 load test and verified archive restore. Keep longer only when measured query needs justify it.

Always hot:

- tokens, pairs and current source-attributed metadata projection;
- current DEX-availability and poll schedules/leases;
- current lifecycle and coverage projection;
- immutable policy/configuration documents addressed by digest;
- epochs, runs, active component health and recent connectivity;
- recent high-value observations, candidates, holder/security and quote facts;
- archive/backup manifests and verification state;
- enough recent request/batch evidence for incident diagnosis.

Partition daily, or at most weekly after measurement, for observations, lifecycle evidence, poll members, raw API bodies and candidate transaction facts. Partition keys, primary keys and idempotency constraints must be designed together. Mutable operational projections remain separate and aggressively vacuumed/analyzed; they are never treated as historical truth.

## Warm/cold archive contract

Use Parquet/Zstandard under immutable paths such as:

```text
archive/schema=v2/family=observations/year=YYYY/month=MM/day=DD/epoch=N/scope=.../part-....parquet
archive/schema=v2/family=api_request_log/year=YYYY/month=MM/day=DD/epoch=N/scope=.../part-....parquet
```

Partition by UTC fact/receipt date and epoch/data family. Wallet or transaction facts may additionally bucket by stable token hash only when files become too large; never create one tiny partition per token.

Archive units are closed UTC ranges with a safety lag. Daily export is recommended; hourly staging is optional but publication should avoid tiny files. A manifest records:

- epoch and validity, data family and schema/normalizer version;
- inclusive/exclusive time range and source query/watermark;
- row count, null/empty/outcome counts and min/max keys/times;
- canonical full-content hash or documented partition hash method;
- every file path, byte count and SHA-256;
- logical bytes, Parquet bytes, compression and exporter revision;
- source policy/configuration digests needed to interpret rows;
- verification time/method and analytical-read evidence;
- primary object URI plus independent-copy URI/hash;
- overlap/gap status and explicit `deletion_permitted=false` by default.

Publish to a temporary object key, read back, verify, then atomically expose the manifest/committed prefix. Re-running an identity must verify and return the same manifest; conflicting content is an integrity error.

## Family-specific retention

| Family | Long-term fidelity | Hot treatment | Cold treatment |
|---|---|---|---|
| observations | every row, including repeats | 14 days initially | full Parquet forever per approved research horizon |
| raw discovery/API evidence | every request/event and explicit empty/failure | envelope 30–90 days; body 3–14 days after archive | request envelope + canonical raw body/hash; exact duplicates may share a content-addressed blob, but request occurrences remain distinct |
| lifecycle events | every transition | keep hot longer; low relative volume | full copy |
| lifecycle evidence | every selected/failed evaluation and watermark | narrow V2 row; bulky diagnostics recent | full narrow evidence; exception detail side table |
| poll batch members/outcomes | enough to classify every obligation and gap | 7–14 days | full ledger or a proven lossless normalized obligation/outcome representation |
| scheduler capacity/coverage decisions | every decision bucket/policy | months; low volume | full copy |
| schedule decisions | every lifecycle/coverage/epoch rebase | 30–90 days | full copy |
| mutable poll schedules/DEX tasks/health | latest projection only | hot | epoch-start/end recovery snapshots optional; immutable decisions/outcomes are authoritative |
| tokens/pairs/metadata/security dimensions | full version history | current + recent versions | all immutable versions |
| holder/trader/wallet/liquidity facts | candidate facts and coverage | recent candidates | full immutable facts, possibly separate high-volume family |
| storage samples | all compact samples | 90 days | full or lossless hourly rollup after preserving daily extrema; keep original for validation epochs |
| backup/archive verification | all | always | independent manifest copy |

Do not replace request/member evidence with hourly counts until a formal proof shows that every expected token poll can still be classified as succeeded, empty, failed, partial, late or never attempted. Summaries accelerate reports; they do not replace authoritative ledgers.

## Lifecycle evidence width redesign

Retain per evaluation:

- token/request/policy identity;
- input watermark and outcome;
- selected pair and observation composite key;
- stable reason code;
- compact ambiguity/candidate count fields;
- persisted time.

Move large candidate lists/ranking detail to an exception table written only for multi-pair ambiguity, failure or configured diagnostic sampling. Policy JSON already has a normalized digest table and should never repeat on new facts. This retains reproducibility while targeting roughly 200–300 physical bytes/evaluation instead of the measured 1,072. Validate with `pg_column_size`, index size and real multi-pair distributions before migration. Epoch 1/2 rows remain untouched.

The same rule applies to poll batches/outcomes: reference immutable scheduler/configuration documents by digest; retain only dynamic facts inline.

## Raw payload placement

Raw DEX bodies are authoritative, but PostgreSQL JSONB/TOAST is an expensive long-term blob store. V2 may split:

- hot `api_request_log`: provider, endpoint, times, status/outcome, request identity, response hash, byte count and archive/raw-body state;
- immutable raw body: initially transactional PostgreSQL staging, then verified compressed object/Parquet content addressed by hash;
- normalized facts: committed atomically with or explicitly linked to the raw staging record.

Removing a staged body is still destructive and is forbidden until the archive manifest, independent second copy, analytical read and explicit human approval gates all pass. A raw-body state transition is append-only and auditable. The request row never disappears.

## Querying hot plus cold

Provide a dataset catalog that lists coverage ranges per family/epoch/schema and rejects overlaps/gaps by default. DuckDB/Polars reads object-store Parquet and PostgreSQL exports through explicit cutoffs. A combined query:

1. selects cold rows ending before a manifest cutoff;
2. selects hot rows at or after that cutoff;
3. deduplicates only by durable idempotency/composite key;
4. fails on unapproved overlap, gap, checksum or schema mismatch;
5. applies valid-epoch and as-of filters before features.

Researchers should never glob every file and hope partitions do not overlap.

## Deletion eligibility — design only

Phase 3 now persists this calculation as immutable evidence and exposes status only.
It still performs no deletion.

A PostgreSQL partition can become *eligible for a future human-approved operation* only when:

1. its range is closed and beyond the configured hot window;
2. all families and foreign-key dependencies in the range are exported;
3. source/export row counts, key/time bounds and content checks pass;
4. Parquet analytical queries pass;
5. the primary object was read back after upload;
6. a byte-identical independent second copy exists and is verified;
7. the manifest is durable in PostgreSQL and independently copied;
8. a restore/query drill succeeds;
9. current backups and WAL/disk headroom are healthy;
10. explicit human approval names the exact partition/range.

No automatic deletion command should be scheduled. The current fail-closed gate remains, and any future detach/drop operation needs a separate, narrowly scoped CLI with dry-run output, lock/process checks and immutable approval evidence. Rollback is restore to a new table/database from verified archive or dump, verify, then reattach through an approved procedure.

## Phase 3 hot-window and annual sizing

The realized Epoch 2 rate was 11.40 GiB/day before Phase 2. Until a post-migration V2
run measures otherwise, use 12–14 GiB/day for hot facts including Phase 2 rather than the
unproven 6–8 GiB/day optimization target.

| Hot window | Fact growth | Fact growth + current 20 GiB | Recommended usable NVMe including WAL/temp/migration/staging margin |
|---|---:|---:|---:|
| 7 days | 84–98 GiB | 104–118 GiB | 500 GB minimum; 1 TB comfortable |
| 14 days | 168–196 GiB | 188–216 GiB | 1 TB recommended |
| 30 days | 360–420 GiB | 380–440 GiB | 2 TB recommended |

Fourteen days remains the initial recommendation: it preserves operational incident and
recent-research access while fitting a 1 TB NVMe with meaningful restore/staging space.
Seven days is the fallback for a 500 GB validation host. Thirty days materially raises
NVMe and backup cost without established query value.

The conservative full-archive range is 1.8–3.0 GiB/day, combining the measured all-family
legacy export, the high-load observation benchmark, and Phase 2's 0.08–0.20 GiB/day
estimate:

| Horizon | Primary archive | Secondary archive | Both archive copies |
|---|---:|---:|---:|
| 30 days | 54–90 GiB | 54–90 GiB | 108–180 GiB |
| 90 days | 162–270 GiB | 162–270 GiB | 324–540 GiB |
| 365 days | 657 GiB–1.07 TiB | 657 GiB–1.07 TiB | 1.28–2.14 TiB |

Manifests/catalog metadata should remain below 1 GiB/year. A rolling two-full-dump policy
for a 14-day hot database needs approximately 100–240 GiB depending on pg_dump
compression; retaining one monthly full dump instead would add roughly 50–120 GiB at 30
days, 150–360 GiB at 90 days, and 600 GiB–1.41 TiB at one year. Monthly full dumps are
therefore not the recommended research-history store: keep two recent verified recovery
dumps plus epoch-final/major-migration dumps, and use the two verified Parquet copies as
the cumulative history.

## Backup design

- Daily PostgreSQL logical/custom-format backup for metadata and operational schema; full-database cadence based on measured duration and I/O, initially at least daily for a validation epoch.
- Continuous/daily archive publication to object storage with versioning/object lock where available.
- Independent copy of dumps, Parquet and manifests to a separate provider or external disk.
- Read verification, catalog listing and periodic isolated restore—not checksum-only claims.
- Retain at least two recent recoverable DB backups plus the epoch-final artifact; size capacity for database, WAL/staging and restore workspace simultaneously.

## Storage acceptance gates before a long-term epoch

- Full-day V2 synthetic or short validation export measures every new family.
- PostgreSQL bytes/observation, evidence, request and candidate are within the 6–8 GiB/day target or a revised provisioned budget.
- Full Epoch 3 validation archive verifies and runs as-of analytical queries.
- Object and independent-copy hashes match; restore drill succeeds.
- Hot/cold catalog detects injected overlap and gap fixtures.
- Disk alert thresholds include WAL, temp, archive staging and backup headroom.
- Retention remains disabled until a later explicit approval.
