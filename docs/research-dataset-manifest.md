# Research dataset manifest

Research datasets are immutable, rebuildable Parquet/ZSTD artifacts. The canonical
identity hashes all semantic inputs: feature/label contracts, code revision, source
descriptors and archive-manifest hashes, valid epoch scope, time/token cohort, candidate
policy, explicit split boundaries, purge/embargo policy, and output schema.

The manifest contains:

- dataset identity and schema version;
- feature/label names, versions, contract hashes, required/optional inputs, and code
  revision;
- database/archive schema revisions and exact source manifest SHA-256 values;
- epoch and cohort rules, reference timestamp policy, split boundaries, purge and embargo;
- row and unique-token counts by split, label distributions, null rates, time bounds,
  duplicates and anomaly diagnostics;
- Parquet file path/bytes/SHA-256, canonical row-content hash, schema hash, generation
  timestamp, and verification result.
- source observations scanned, candidates generated/emitted, build seconds/rate, DuckDB
  validation time, and peak process RSS;
- token coverage, horizon availability, source cadence, extreme-value summaries,
  duplicate identities, impossible values, and timestamp anomalies.

Layout is `dataset=<identity>/data.parquet`, `manifest.json`, and `manifest.sha256`.
Publication is staging plus atomic promotion, with the manifest last. If that identity
already exists, every checksum and semantic contract is verified and reused. Different
bytes or semantics under one identity are an integrity error.

## Candidate timestamps

Canonical v1 uses fixed offsets from valid DEX admission: +30s, +1m, +2m, +5m, +10m,
+30m, and +1h. Candidate existence depends only on admission and source-scope time, never
future outcome. Observation-driven and lifecycle-event modes are supported as explicitly
different policies; they are not mixed into fixed-age v1.

## Splits and purge

Train, validation, and locked-test ranges use explicit UTC boundaries. Token assignment
uses admission time, and a candidate must remain inside that assigned split, preventing
the same token appearing in multiple canonical splits. Gaps between split boundaries are
embargo regions. A supervised row is purged when `T + maximum requested label horizon`
reaches beyond its split's label boundary.

## Retention

Raw archives remain canonical. Derived datasets are cheap relative to raw data and should
usually be rebuildable/ephemeral. Persist only named, verified benchmark/publication
datasets and their manifests; retain small manifests indefinitely. Never delete raw
archive because a derived dataset exists.

The code revision combines committed Git HEAD with a content digest of the complete
research package. This prevents uncommitted validation code from masquerading as the same
derivation. Rebuilds with identical semantic inputs reuse and verify one identity;
changing feature/label semantics changes their contract digest and identity.

## Planning size

The seven-row Epoch 2 benchmark is too small for direct bytes/row scaling because its
124 KiB file is schema/footer dominated. Until a safe large build is available, use
0.8–2.0 KiB/candidate: 0.8–2.0 GiB/million candidates, about 7–17 GiB/30d,
20–51 GiB/90d, and 82–206 GiB/year at seven snapshots and 28 admissions/minute.
Generated datasets remain selectively retained/rebuildable; raw verified archives remain
canonical.
