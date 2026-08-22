# Token discovery

The discovery package is the only boundary that knows how a source identifies,
authenticates to, or parses a provider. Its public `TokenDiscoverySource`
contract yields canonical chain/address identities, source and collector
timestamps, opaque source evidence, durable idempotency keys, and explicit
coverage semantics. Persistence, DEX reconciliation, scheduling, and lifecycle
code do not import or interpret PumpPortal payload types.

## Live provider: PumpPortal

The configured live implementation is `PumpPortalDiscoverySource`. PumpPortal
is a third-party provider, not Pump.fun itself. It connects to PumpPortal's
documented `wss://pumpportal.fun/api/data?api-key=...` endpoint and sends one
`{"method":"subscribeNewToken"}` message on each successful connection. It
uses one WebSocket for the whole creation stream; it never opens a connection
per token.

Official references:

- [Real-time Data API](https://pumpportal.fun/data-api/real-time/)
- [Data API fees](https://pumpportal.fun/fees/)
- [PumpPortal FAQ](https://pumpportal.fun/FAQ/)
- [API-key setup](https://pumpportal.fun/trading-api/setup/)

`subscribeMigration` is not enabled in V1. The current discovery meaning is
"new token created"; migrations are later source events for already-known
mints and are not needed for admission to DEX reconciliation. Adding them in a
future phase would require a distinct internal event type and explicit research
question, not a silent expansion of creation semantics.

## Authentication and configuration

As documented by PumpPortal effective 1 May 2026, the live data connection
requires a PumpPortal API key. Obtain it outside this application from the
official Getting Started page's **Create Wallet & API Key** flow. That provider
flow also creates a linked Lightning wallet and exposes its private key. Pump
Research does not create, fund, import, persist, or use that wallet or private
key; the operator supplies only the API key.

Required for a live run:

```dotenv
PUMP_RESEARCH_PUMPPORTAL_API_KEY=your-pumpportal-api-key
PUMP_RESEARCH_PUMPPORTAL_WEBSOCKET_URL=wss://pumpportal.fun/api/data
```

The URL defaults to the documented value. Keep the API key separate: embedding
`api-key` in the URL is rejected, and a missing or blank key fails before any
connection attempt. The adapter adds the non-empty key as the documented query
parameter and never constructs a bearer header. The runtime configuration
snapshot excludes the key.

PumpPortal currently documents `subscribeNewToken` and `subscribeMigration` as
free. Its current real-time page also states that subscription access uses an
API key linked to a wallet funded with at least 0.02 SOL. This project does not
automate that provider-account prerequisite or any wallet operation.

## Event and provenance mapping

A message must be a JSON object with a non-empty `mint`. `signature`, when
present, is retained as the provider event identifier; otherwise the mint is
the fallback event identifier. A timestamp is normalized only when the message
actually supplies `timestamp`, `created_timestamp`, or `createdTimestamp`.
No source timestamp is fabricated. The collector receipt time is recorded at
WebSocket receipt.

The complete JSON object is retained as opaque source evidence with its
canonical SHA-256 digest. The provider identity is `pumpportal`, the internal
event type is `token_created`, and the durable idempotency key is derived from
the provider event identifier. Repeated delivery is therefore retained as
duplicate-delivery evidence without creating another token or source event.
Malformed or unknown messages are surfaced as discovery errors rather than
being treated as valid tokens. A narrowly recognized successful-subscription
acknowledgement is the only non-token control message ignored.

## Reconnection, checkpoints, and coverage gaps

PumpPortal documents a live stream, not a historical cursor or replay API. The
adapter therefore returns no `DiscoveryCheckpoint`: PostgreSQL still performs
the same atomic batch hand-off, but there is no upstream position it can resume
after a crash. Within a running process, a fetched live batch remains buffered
and is redelivered until `DiscoveryCoordinator` acknowledges it after the
PostgreSQL transaction commits. This prevents a transient database failure
between `fetch` and commit from silently discarding live events. Every batch is
explicitly `BEST_EFFORT` with `supports_replay=False`.

The adapter uses bounded exponential retry with configurable jitter and
resends `subscribeNewToken` after every reconnect. A bounded application queue
and bounded WebSocket receive queue apply backpressure rather than silently
dropping token events. Disconnect and successful-resubscription boundaries are
persisted atomically as provider-neutral `discovery_connectivity_events` with a
shared gap ID. An unmatched disconnect remains an explicit open gap.

Coverage still has unavoidable limitations:

- Token events emitted during a provider/network disconnect cannot be replayed
  through PumpPortal and may be permanently missing.
- A hard process crash cannot write a disconnect boundary at the instant it
  happens. Collector-run heartbeats and the next run/connection delimit the
  possible downtime, but do not recover missed events.
- PumpPortal does not document a historical token-data endpoint, so a fresh
  installation cannot backfill pre-start creations through this adapter.
- A provider-side omission or schema change may be detectable as malformed
  data or reduced volume, but cannot be proven complete from this stream alone.
- The source is third-party and PumpPortal states that its WebSocket can
  disconnect during network instability or server load rebalancing.

Consequently, the dataset must never claim exhaustive Pump.fun creation
coverage solely from this source. Coverage reports should combine connectivity
events, collector-run heartbeat gaps, malformed-event errors, and discovery
volume anomalies.
