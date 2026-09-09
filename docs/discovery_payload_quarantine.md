# Discovery payload quarantine

The 2026-09-07 08:45:18 UTC collector failure was triggered by a PumpPortal
creation message containing `"name": "WORLD BTC \u0000..."`. Python's JSON
decoder accepts that escape and creates a string containing U+0000. The adapter
previously passed the decoded object directly to `discovery_events.source_payload`.
PostgreSQL JSONB rejects the decoded character with SQLSTATE `22P05`:
`unsupported Unicode escape sequence`, `\u0000 cannot be converted to text`.
`ensure_ascii=True` in the canonical hash function does not change that fact:
PostgreSQL interprets the JSON escapes when storing JSONB. PostgreSQL text fields
also cannot store NUL.

The database exception escaped admission and the discovery coordinator.
`CollectorWorker._loop` correctly treats unexpected database failures as fatal;
its TaskGroup then cancels sibling tasks. `CollectorRuntime` reports the resulting
ExceptionGroup as `collector_worker_failed`. The subsequent cancellations in the
supplied evidence are consistent with that shutdown, not a separate root cause.

## Decision and evidence semantics

Reject the **individual message** before normalization when any decoded JSON
string contains NUL or an unpaired UTF-16 surrogate. Inspect all object keys,
values, lists, nested objects, and unknown extras, before extracting identifiers,
timestamps, subscription acknowledgments, or metadata. Literal backslash-u text
such as `"\\u0000"` and valid Unicode remain ordinary data.

Quarantine is preferable here to silently deleting/replacing characters or
embedding a new evidence envelope in an existing JSONB payload column. It keeps
existing JSONB evidence shapes and hashes stable, avoids key collisions from
escaping object keys, and protects normalized text fields without inventing
cleaned identities or display values.

`discovery_rejected_messages` is append-only evidence, with:

- `raw_message` (`BYTEA`): the exact binary websocket message, or UTF-8 with
  `surrogatepass` of the original text websocket message. This preserves the
  message contents, whitespace, JSON escaping, and duplicate object keys. It
  does not include websocket framing or transport compression.
- `message_encoding`: `binary` or `utf8-surrogatepass`, so the original application
  message is reversibly recoverable. No JSON serialization or sanitization is
  applied to these bytes.
- `raw_message_sha256`: SHA-256 over exactly `raw_message`, **not** over parsed or
  canonicalized JSON. This is intentionally a differently named digest from
  `discovery_events.source_payload_sha256`.
- `provider`, `endpoint`, format `schema_version=1`, `reason_code`, UTC
  `received_at`, database `persisted_at`, and `collector_run_id`. The run supplies
  collector/configuration provenance. There is no manufactured source-event or
  request timestamp; these are stream receipt records.
- A durable unique `idempotency_key`: SHA-256 of
  `pumpportal:rejected:{received_at.isoformat()}:{message_encoding}:{raw_message_sha256}`.
  Redelivery of the same pending receipt is idempotent. Separate receipt times
  retain repeated identical messages as separate evidence.

Ordinary events are unchanged: `source_payload` remains the parsed provider
object; `source_payload_sha256` remains SHA-256 of ASCII canonical JSON produced
with sorted keys, compact separators and `ensure_ascii=True`. It does not claim
to hash original wire bytes. The existing signature-or-mint discovery idempotency
key is unchanged.

No canonical token, metadata, schedule, or lifecycle rows are created from a
quarantined message. Thus NUL in `name`, `symbol`, URI/image/social URLs, mint,
signature, or any extra field cannot reach a second PostgreSQL column. The token
and all other source claims remain recoverable from quarantine, but these tokens
are **not automatically polled**. Research coverage analysis must count these
rejections; existing discovery reports do not include them. Automatic reprocessing
would require a separately defined normalization/evidence policy, and must not
backdate observations. No reprocessing or retrospective data rewrite is included.

## Transaction and failure boundaries

The adapter handles only this specific unsafe-string condition locally and
queues a rejection alongside ordinary events and connectivity evidence. A batch
containing only rejections is also retained until acknowledged. The coordinator
commits quarantine, accepted events, and any checkpoint in one transaction before
acknowledging the batch. Rollback keeps the pending batch available; replay after
commit does not duplicate quarantine rows. A later malformed message does not
displace earlier queued rejections.

No SQLAlchemy/DBAPI catch was added. An unrelated database failure still
propagates and rolls back the batch. Worker/TaskGroup supervision is unchanged.
The websocket message size remains bounded at 1 MiB (now explicit, matching its
previous default), and the existing bounded queue/batch provides backpressure.
Like existing live discovery, uncommitted process memory can be lost on a process
crash; PumpPortal provides no replay. This change does not promise otherwise.

The quarantine table requires additive Alembic revision `7d9b3e5f0a12` after
`2b6f0d8e4a91`. Upgrade must precede running this collector version. It does not
modify existing data. Quarantine has no automatic cleanup or archival policy;
retain it in PostgreSQL. Its index supports provider/time coverage checks:

```sql
SELECT provider, reason_code, count(*), min(received_at), max(received_at)
FROM discovery_rejected_messages
WHERE received_at >= :start_utc AND received_at < :end_utc
  AND persisted_at <= :as_of_utc
GROUP BY provider, reason_code;
```

The fixture and migration validation use a separate disposable PostgreSQL 16
container/database. Production data, epochs, collector processes, and secrets
are outside this work. Migration downgrade deletes the quarantine table and must
not be used against retained production evidence.

## Other external JSONB paths audited

These paths remain vulnerable; this patch does **not** provide application-wide
PostgreSQL string safety:

| Path | Externally sourced JSONB / secondary text exposure |
| --- | --- |
| `collection/dex_availability.py`, `collection/polling.py` → `ApiRequestLogRepository` | DEX Screener `api_request_log.response_payload`, including unknown nested pair fields; subsequent pair/token identifiers, metadata and pair facts |
| `collection/boosts.py` → `ApiRequestLogRepository` | Complete DEX boost response records, URLs/descriptions/links; metadata `other_links` |
| `collection/security.py` → `ApiRequestLogRepository` | Full Solana `getMultipleAccounts` response, including unknown response/account fields |
| `security_enrichment/repository.py` | `security_provider_requests.response_payload` and request/cursor/failure evidence; `_jsonable` preserves unsafe strings. Standard holder provider embeds largest-account and parsed-owner RPC responses. Provider-defined feature values and derived lists also have no general string guard |
| `persistence/enrichment.py` | Provider-derived `pair_fact_events.labels`, `token_metadata_events.other_links`, and normalized text fields have no general guard. Built-in mint extension names are generated from numeric extension IDs, rather than arbitrary provider text |
| Discovery connectivity and operational failure writers | PumpPortal disconnect exception messages enter `discovery_connectivity_events.detail`; exception text can enter API/security/task/worker health failure/detail JSONB |
| Direct `DiscoveryEventRepository` / non-PumpPortal admission callers | Raw provider payloads and normalized metadata remain the caller's responsibility; the PumpPortal guard is deliberately not a global repository coercion |

Configuration snapshots, policy JSONB, generated IDs/counts, and internal lifecycle
decisions are not independent raw provider-payload entry points. Archive replay
copies already persisted evidence and has no new live-provider boundary.

A global serializer that replaces NUL would alter evidence/hash semantics and
leave normalized text exposed. Reusing the detection predicate at other provider
boundaries requires durable raw-response rejection handling and explicit partial
batch/attempt outcomes for each workflow. Those changes are not hidden in this
discovery crash fix. Other malformed numeric, oversized, or schema-invalid input
handling remains as before; this is a fix for PostgreSQL-incompatible strings.

## Regression coverage and validation setup

`tests/integration/test_discovery_nul.py` reproduces the original INSERT failure
against PostgreSQL, then exercises real quarantine persistence, subsequent token
admission inside a TaskGroup, exact-byte digest verification, commit-before-ack
redelivery, and rollback/retry after an unrelated SQL error. Unit cases cover all
currently extracted metadata fields and identifiers, nested values and keys,
text/binary messages, literal escape text, valid Unicode, unpaired surrogates,
unchanged ordinary hashes, and repeated receipt/malformed-message ordering.

Validation used PostgreSQL 16 in the separate `pump-research-nul-test` container
on `127.0.0.1:55434`, with disposable databases `pump_research_nul_test` (tests)
and `pump_research_nul_schema_test` (migration cycle/schema comparison).

The first full run from the project directory had 296 passes and one failure:
`test_archive_s3_missing_configuration_fails_closed`. `Settings` automatically
loads `.env`; the test assumes no archive configuration. No configuration/secret
file was inspected or modified. The unchanged test passed from an empty working
directory (1 passed in 0.62s). The full-suite rerun likewise uses an empty working
directory with the absolute test path, so ordinary Settings calls do not read
the project's `.env`. Integration subprocesses explicitly target the guarded
disposable database. No production collector is launched.

Final results:

| Check | Result |
| --- | --- |
| Original PostgreSQL failure reproducer, before implementation | 1 passed in 4.82s; actual INSERT rejected with SQLSTATE `22P05` |
| Focused discovery, safety, persistence/recovery, availability and continuous-collector tests | 74 passed in 32.35s |
| Initial full run from project directory | 296 passed, 1 failed in 271.36s; configuration isolation failure described above |
| Full suite from empty working directory | 297 passed in 271.78s |
| `ruff check .` | All checks passed |
| `mypy --strict` | Success: no issues found in 115 source files |
| Fresh database `alembic upgrade head` | Passed through `7d9b3e5f0a12` |
| New migration downgrade to `2b6f0d8e4a91`, then upgrade to head | Both passed in the disposable schema database |
| `alembic check`, before and after the migration cycle | No new upgrade operations detected |
| `alembic heads` | One head: `7d9b3e5f0a12` |
| `git diff --check` | Passed |

The focused pytest selection was:

```text
tests/unit/test_pumpportal_discovery.py
tests/unit/test_discovery_payload_safety.py
tests/integration/test_discovery_nul.py
tests/integration/test_discovery_connectivity.py
tests/integration/test_failure_recovery.py
tests/integration/test_dex_availability.py
tests/integration/test_continuous_collector.py
```

The successful full command was run from the empty directory
`/tmp/pump-research-nul-validation.eQHDg2`:

```sh
PUMP_RESEARCH_ENVIRONMENT=test \
PUMP_RESEARCH_TEST_DATABASE_URL=postgresql+asyncpg://nul_test:disposable_test_only@127.0.0.1:55434/pump_research_nul_test \
/home/scott/projects/pump-research/.venv/bin/python -m pytest /home/scott/projects/pump-research/tests -q
```

The displayed database credentials were created solely for the disposable local
test container. Reproduction requires a fresh disposable test database; that
container is removed after validation.
