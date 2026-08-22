# Collector V2 infrastructure target

Status: revised after the Phase 7 partial-clone rehearsal. Nothing is provisioned and no collector
is started. The numbers below remain conservative because a full integrated replay was blocked by
disk safety.

## Design principles

- One maintainable Python application with separable collector, scheduler, archive and reporting commands.
- PostgreSQL remains the authoritative hot operational store.
- S3-compatible object storage holds immutable Parquet/Zstandard, verified dumps and manifests.
- A physically/provider-independent copy protects against account, VPS and object-store loss.
- All external service budgets are shared and persisted; scaling workers must not multiply API limits.
- Architecture stays vendor-neutral: standard Linux, PostgreSQL, filesystem staging and S3-compatible APIs.
- Trading, wallet, signing and execution services are outside this deployment.

## Sizing basis

The measured Epoch 2 database grew 11.40 GiB/day. Until a post-migration V2 run proves
otherwise, provision hot storage at a conservative 12–14 GiB/day and cold archive at
1.8–3.0 GiB/day. The conservative scheduler projects about 4,082 token observations/min
and about 150 DEX requests/min including reserves, with batches near 30 addresses.
Candidate-triggered on-chain streams are not yet measured, so recommended capacity includes
headroom rather than pretending an exact number.

Phase 7 added measured operational facts: the 58,589-schedule one-time reconstruction took 409.886
seconds, grew the clone by about 303 MiB, and contributed roughly 496 MiB of WAL including nearby
archive/catalog work. The fully migrated operational clone was about 450 MiB. Its steady schedule
demand was 1,329.768 observations/minute and 44.326 core DEX requests/minute because most Epoch 2
tokens had cooled or retired. The rehearsal archive covered 175,776 audit-heavy rows at 85.11
bytes/row and 11.408:1 logical compression; the broader observation benchmark remains the more
representative 3.895:1. CPU, RSS, IOPS, and full-partition migration locks remain unmeasured.

## Minimum validation VPS

Suitable for a 24–72 hour Collector V2 validation, not the final annual platform:

| Resource | Minimum | Rationale |
|---|---:|---|
| CPU | 8 modern preferably dedicated vCPU | collector parsing, async workers, PostgreSQL, compression and reports without starving ingestion |
| RAM | 32 GiB | approximately 8 GiB PostgreSQL cache plus OS, Python, archive and query headroom |
| NVMe/SSD | 500 GB usable, target <65% at start | current 20 GB, 72h growth, WAL, indexes, full restore/dump and archive staging |
| Network | reliable 1 Gbps port, at least several TB/month allowance | API ingress is modest; object uploads and verified dumps dominate |
| I/O | sustained low-latency NVMe; thousands of random IOPS | high insert/index/WAL rate matters more than peak bandwidth |
| Backup/object storage | 250 GB initial allocation | multiple validation dumps plus Parquet and independent manifests |

Do not run validation on a disk that cannot retain at least two recent backups plus current DB growth and one restore/staging copy.

## Recommended long-term starting VPS

| Resource | Recommended | Scale trigger |
|---|---:|---|
| CPU | 12–16 dedicated threads/vCPU | retain until the missing replay proves a smaller host; scale on sustained CPU >60% |
| RAM | 64 GiB | cache miss/I/O pressure or analytical queries affecting ingestion |
| NVMe | 1 TB usable for 14-day hot; 2 TB for 30-day hot | free space <35%, WAL/archive staging headroom breach |
| Network | 1 Gbps symmetric with generous transfer | backup/upload window misses or restore SLA requires faster link |
| Object storage | 1.5 TB primary allowance in year one | measured V2 archive exceeds 3 GiB/day or backup retention expands |
| Independent copy | separate provider/device with 1.5 TB allowance | maintain one complete checksum-identical cold history |

At the conservative pre-validation 12–14 GiB/day, 14 hot days add 168–196 GiB before
the current database, WAL, bloat, dumps, migration, archive staging, and emergency
restore space. The 1 TB recommendation is operational headroom, not a claim that all
data should stay hot. A 500 GB host should use a seven-day validation window; 30 days
requires 2 TB.

The sizing was revisited but cannot responsibly be reduced from measured scheduler throughput
alone. A 64 GiB, mirrored 1 TiB NVMe dedicated host is the recommended starting long-term shape;
an 8-vCPU/32-GiB/500-GB instance is validation-only. As one current market reference, a Hetzner
EX44 advertises 64 GiB and two 512 GB NVMe drives from about EUR 42–47/month plus setup/IPv4, but
provider, region, support, backup, and disk durability must be evaluated independently:
<https://www.hetzner.com/dedicated-rootserver/ex44/configurator/>. This is a price reference, not a
vendor selection.

## PostgreSQL layout and tuning envelope

Start with one PostgreSQL instance on local NVMe. Do not put its live data directory on an object/FUSE mount.

- Daily partitions for the highest-volume V2 facts; precreate and monitor them.
- Dedicated PostgreSQL volume or directory with filesystem snapshots only as supplemental backup.
- `shared_buffers` initially 8 GiB on 32 GiB or 12–16 GiB on 64 GiB; tune from workload, not folklore.
- Preserve OS page cache; do not allocate most RAM to PostgreSQL.
- Conservative per-session `work_mem`; archive/report queries must not multiply into OOM.
- WAL/checkpoint settings sized to smooth sustained inserts, with alerts on WAL growth and checkpoint pressure.
- Autovacuum/analyze tuned separately for mutable scheduling projections and append-only partitions.
- Connection pool bounded; four scheduler workers do not require hundreds of database connections.
- Statement timeouts and resource limits for reports so ingestion wins.
- Monitor commits/rollbacks, locks, replication/backup state, WAL bytes, temp bytes, table/index/TOAST growth, vacuum age, cache hit and disk latency.

A later read replica can serve reports, but it is not required before evidence shows contention. A replica is not an independent backup.

## Conceptual deployment

```text
external discovery + DEX/RPC/indexed providers
                    |
             one Collector V2 app
       discovery | scheduler | enrichment
                    |
              local PostgreSQL
        hot facts + ledgers + manifests
                    |
        closed-range archive worker
                    |
          primary object storage
                    |
        independently verified copy
```

Run the collector under a real service supervisor on Linux (for example systemd), not a `tee` pipeline as the control plane. Logging can still stream to journald and rotated files, but process health comes from durable heartbeats and supervisor state. Graceful SIGINT/SIGTERM behavior already exists and remains required.

Use separate service commands/processes if useful, but keep one codebase and schema:

- collector runtime: discovery, availability, market polling and lightweight universal enrichment;
- archive job: low-priority closed-range export/verify/upload;
- backup job: dump, checksum, read/restore verification and independent copy;
- reporting job: bounded queries, preferably against closed partitions/archive;
- later candidate enrichment worker: only after explicit budget and provenance design.

Archive/backup jobs should pause or throttle when database latency, WAL, disk or API health crosses a guard threshold.

## Object-storage contract

Required capabilities:

- immutable/versioned object keys and multipart upload;
- read-after-write behavior that is verified by the application;
- checksums stored in the application manifest, not trusted solely from provider ETags;
- lifecycle rules disabled for authoritative files until explicitly approved;
- server-side encryption plus TLS;
- credentials scoped to archive prefixes; collector need not have delete permission;
- separate credentials/account/location for the independent copy.

The application should depend on a small object-store interface: put temporary, head/read, copy/publish, list exact prefix and verify SHA-256. Provider-specific storage classes can be selected later without changing manifests or Parquet layout.

Planning capacity at the measured/conservative 1.8–3.0 GiB/day:

| Horizon | Primary archive | Primary + one complete independent copy |
|---|---:|---:|
| 30 days | 54–90 GiB | 108–180 GiB |
| 90 days | 162–270 GiB | 324–540 GiB |
| 1 year | 657 GiB–1.07 TiB | 1.28–2.14 TiB |

Verified PostgreSQL dumps are additional. A full dump at the same frequency as Parquet may double requirements; retain them by recovery objective rather than indefinitely duplicating every daily database image.

At current Backblaze B2 published pricing of USD 6.95/TB-month, one 0.66–1.07 TiB first-year
primary archive is roughly USD 5–8/month at year end and two full copies roughly USD 10–16/month,
before dump retention and provider-specific transaction/egress details:
<https://www.backblaze.com/cloud-storage/pricing>. Plan 3–4 TB of object capacity, including two
archive copies, verified dumps, manifests, and growth uncertainty. A realistic initial monthly
infrastructure envelope is about EUR 45–150 for the host class and USD 10–35 for object/archive
storage as it grows, plus a private Solana/indexer plan whose cost is not yet accepted. Re-price at
procurement; these are planning bounds, not quotes.

## Backup and recovery shape

For validation:

- pre-start verified custom-format dump;
- daily verified dump during a multi-day run and a final dump after controlled stop;
- daily Parquet publication for closed ranges;
- manifests/checksums copied immediately to the independent destination;
- restore/list/read verification recorded durably.

For long-term operation, add PostgreSQL physical base backup plus WAL archiving only after testing restore and operational cost. Logical dumps remain useful for metadata/schema portability. Recovery objectives should be explicit:

- PostgreSQL operational recovery restores latest base/dump plus applicable WAL;
- research history is validated against immutable archive manifests;
- new DB is restored alongside the damaged instance, verified, then promoted;
- never overwrite the only damaged copy during recovery.

## Security and operations

- Non-root service user; PostgreSQL not publicly exposed.
- Firewall allow only administration/VPN and required outbound providers.
- Separate test/database credentials and database names; tests cannot reach production.
- Secrets via protected environment/service credential store, never logs or manifests.
- Object-storage archive writer lacks object delete; a separate human role controls retention.
- SSH keys, automatic security updates with controlled reboot windows, time synchronization and disk-health monitoring.
- Alerts for stale collector/run, failed component, capacity mode, request ceiling, API error, discovery gaps, DB latency, disk/WAL growth, archive lag, backup age and checksum failure.

## Migration from Mac concept

1. Stop with a graceful durable run event; keep Epoch 2 completed and untouched.
2. Provision and harden the VPS without creating an epoch.
3. Install the exact code revision and migrate an isolated restored copy of the verified Epoch 2 backup.
4. Verify schema parity, row counts, epoch validity, hashes/sample queries and archive/backup commands.
5. Load-test V2 on synthetic/isolated data only.
6. Establish primary and independent object-store paths and restore drills.
7. Only after roadmap gates pass, explicitly create the short validation epoch and start under the service supervisor.

No live epoch should be created merely to test provisioning.

## When to split infrastructure

Stay on one VPS until measurements show a bottleneck. Split PostgreSQL first if archive compression/reporting cannot be isolated from write latency, or if recovery objectives require a managed/dedicated database. Split archival/reporting next. Do not introduce Kubernetes, Kafka, Redis, Celery or microservices as speculative solutions.

## Infrastructure readiness gates

- Sustained synthetic V2 load keeps CPU, memory, disk latency and DB commit p95 within recorded limits.
- At least seven days of projected hot growth plus two backup/staging copies fit below 65% disk use.
- Object upload, readback, checksum and independent-copy verification pass.
- An isolated restore reaches a queryable consistent epoch.
- Service SIGINT/SIGTERM records graceful stop and supervisor status agrees with durable status.
- Test DB isolation is proven on the VPS.
- API budgets remain one shared service budget regardless of process count.
