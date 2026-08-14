# Lifecycle classification

Lifecycle classification is derived application state, not a property of a raw
DEX Screener observation. `LifecycleClassifier` converts the permitted source
fields into an immutable `RawObservationEvidence` value, evaluates the token's
current scheduled state, appends a `lifecycle_events` row, and updates only the
future-poll `poll_schedules` projection in the same PostgreSQL transaction. It
never deletes or modifies a token, pair, request record, or observation.

`observations` contains source/normalized market facts and intentionally has no
lifecycle-state, score, recommendation, or trading-decision column.
`lifecycle_events` is append-only derived history; `poll_schedules` is only its
mutable current operational projection. No trading or opportunity score exists
in either boundary.

## Configured transitions

All thresholds are `PUMP_RESEARCH_LIFECYCLE_*` settings. Defaults in
`.env.example` are provisional collection-policy settings, not claims about
future token quality. They are persisted in full with every transition.

| Current state | New state | Required current observation values | Configuration settings |
| --- | --- | --- | --- |
| `NEW` | `ACTIVE` | `volume_m5_usd >= minimum` | `NEW_TO_ACTIVE_MIN_VOLUME_M5_USD` |
| `NEW` | `WATCH` | `volume_m5_usd < ACTIVE minimum` and `liquidity_usd >= minimum` | `NEW_TO_ACTIVE_MIN_VOLUME_M5_USD`, `NEW_TO_WATCH_MIN_LIQUIDITY_USD` |
| `ACTIVE` | `FADING` | `volume_m5_usd <= maximum` | `ACTIVE_TO_FADING_MAX_VOLUME_M5_USD` |
| `WATCH` | `FADING` | `volume_m5_usd <= maximum` | `WATCH_TO_FADING_MAX_VOLUME_M5_USD` |
| `FADING` | `DORMANT` | `volume_h1_usd <= maximum` and `liquidity_usd <= maximum` | `FADING_TO_DORMANT_MAX_VOLUME_H1_USD`, `FADING_TO_DORMANT_MAX_LIQUIDITY_USD` |
| `DORMANT` | `RESURRECTED` | `volume_m5_usd >= minimum` and `liquidity_usd >= minimum` | `DORMANT_TO_RESURRECTED_MIN_VOLUME_M5_USD`, `DORMANT_TO_RESURRECTED_MIN_LIQUIDITY_USD` |

Missing values never satisfy a rule. This deliberately avoids treating an
incomplete provider response as evidence of inactivity. Rules are evaluated in
the order stored in the configuration snapshot; in particular, `NEW → ACTIVE`
has precedence over `NEW → WATCH`.

## Audit and temporal semantics

Each actual transition records all of the following in `lifecycle_events`:

- `previous_state`, `new_state`, `decided_at`, and `input_watermark`;
- a rule-specific `reason_code`;
- `reason_detail` containing what happened, the normalized input values, the
  thresholds responsible, and the exact observation/request identifiers;
- a full versioned policy snapshot and its SHA-256 digest.

`input_watermark` is the observation's collector `received_at`, not an inferred
source timestamp. The classifier rejects an observation known before the
current state decision, preventing old evidence from creating a retrospective
transition. The partition key (`received_at`) is required when selecting an
observation, so an observation identity is unambiguous at high volume.

The event and the schedule projection are written atomically. A crash rolls
back both, and re-delivery cannot create a duplicate transition because the
event has a deterministic idempotency key. The current implementation calls the
classifier after observation persistence; collection-loop orchestration and
continuous lifecycle evaluation remain separate future work.
