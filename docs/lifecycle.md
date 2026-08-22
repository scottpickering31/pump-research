# Lifecycle classification

Lifecycle classification is derived application state, not a property of a raw
DEX Screener observation. Every relevant pair remains an immutable
`observations` row. A separate immutable `lifecycle_evidence_evaluations` row
records which observation, if any, was selected to represent the token for that
one API response. Only that selected observation is converted into
`RawObservationEvidence` and evaluated against the token's scheduled state.

`observations` contains source/normalized market facts and intentionally has no
lifecycle-state, score, recommendation, or trading-decision column.
`lifecycle_events` is append-only derived history; `poll_schedules` is only its
mutable current operational projection. No trading or opportunity score exists
in either boundary.

## V1 pair-evidence policy

The fixed policy is named `highest_reported_liquidity_usd`, schema version 1.
Its candidate scope is all normalized pair observations for one token from one
DEX Screener API response.

1. With exactly one candidate pair, select it. Liquidity is not required merely
   to establish identity; individual lifecycle rules still refuse to use any
   source field they require when that field is null.
2. With multiple candidates, every candidate must report `liquidity_usd`. If
   any candidate lacks it, record a failed evidence evaluation and perform no
   lifecycle transition.
3. Otherwise select greatest `liquidity_usd`.
4. For an exact liquidity tie, select the lexicographically smallest canonical
   `(chain, pair_address)`. This tie-break is stable and does not use API array
   order.

The policy does not prefer PumpSwap, Raydium, or any other DEX. Pump.fun source
payloads and DEX pair identifiers are retained, but the current discovery
contract does not establish a verified migration-stage semantic suitable for a
venue allowlist. Highest contemporaneous reported USD liquidity is used only as
a simple V1 scheduling/lifecycle evidence choice, not as a quality or trading
score.

Every evaluation stores the complete policy snapshot and SHA-256 digest, the
input watermark, outcome, all candidate observation/pair identifiers and
ranking values, the selected observation/pair when successful, and the reason.
Candidates are written in canonical pair order. All underlying request payloads
and pair observations remain sufficient to recompute selection under a future
policy.

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

Each actual transition references its evidence-evaluation row and records all
of the following in `lifecycle_events`:

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

The evidence evaluation, event, and schedule projection are written in the
polling transaction. Expected selection failures are durable `failed`
evaluations and make the poll outcome `partial` without deleting raw facts. An
unexpected classifier exception rolls derived writes back to a savepoint while
the API response and observations commit; the poll records explicit failure
detail and supervision terminates loudly. Idempotency is enforced by database
constraints and deterministic keys.

V1 limitations: DEX-reported liquidity may be stale, erroneous, or
manipulated; liquidity can move selection between pairs from one poll to the
next; no cross-pair volume aggregation is attempted; and a missing liquidity
value on any multi-pair candidate deliberately prevents derivation for that
response.
