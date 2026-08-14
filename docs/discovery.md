# Token discovery

The discovery package is the only boundary that knows how a source identifies,
authenticates to, or parses a provider. Its public contract is
`TokenDiscoverySource`, which yields `DiscoveryBatch` values containing
canonical `chain` and `address` identities, source evidence, timestamps,
idempotency keys, and explicit coverage semantics. Persistence and future
collection code must depend only on that contract; provider payload fields and
cursor formats are opaque outside this package.

## Checkpoints and durable hand-off

`DiscoveryCheckpoint` is source-owned opaque text. The provider-neutral
`discovery_checkpoint_states` projection associates it with the contract's
`source_name`. `DiscoveryCoordinator` persists discovered events and the new
checkpoint in one transaction and only then advances the source. The adapter
itself still does not persist or interpret a cursor.

The returned `received_at` is collector receipt time. `source_event_at` is
only set when the source supplies a valid timestamp. The full source JSON is
retained as opaque evidence with a canonical SHA-256 digest; no application
lifecycle state is added to it.

## Initial Pump.fun adapter

`PumpFunDiscoverySource` is configured for the currently observed Frontend API
v3 `GET /coins/latest` contract. It sends `Accept: application/json`, an
`Origin` header, and, when configured, a bearer token. Its `ETag` is returned
as an opaque checkpoint and sent back as `If-None-Match`; HTTP 304 is
represented explicitly as a not-modified batch rather than an absent attempt.

The endpoint and authorization flow must be revalidated against Pump.fun's
authorized documentation before an unattended deployment. The implementation
does not conceal an authentication or contract error: it returns an explicit
HTTP or parsing failure instead.

This endpoint returns only the latest coin. It has **best-effort** coverage,
does not support replay, and can miss tokens created between polls. Every
batch therefore carries `BEST_EFFORT`, `supports_replay=False`, and an explicit
coverage note. A future production discovery source must offer an authorized,
cursorable or replayable contract (or independently document and report its
gaps) before the research dataset can claim complete Pump.fun coverage.

The adapter raises explicit transport, HTTP, and parse errors. It keeps small
in-process request metrics only; checkpoint commits are coordinated outside the
adapter. Durable discovery request attempts, gap reporting, retries, and
scheduling remain deferred.
