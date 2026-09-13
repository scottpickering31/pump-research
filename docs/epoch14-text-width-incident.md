# Epoch 14: DEX Screener normalized text widths

Production evidence supplied for this investigation reports a collector failure at
2026-09-11 18:56:00 UTC. A sentence-like `baseToken.symbol` exceeded
`pair_fact_events.base_token_symbol VARCHAR(128)`, causing asyncpg
`StringDataRightTruncationError` (SQLSTATE `22001`). The provider models accept
unbounded strings and the enrichment repositories previously passed them directly
to bounded columns. The exception escaped scheduled observation persistence and
stopped the worker TaskGroup. The supplied evidence says the complete response
was already retained in `api_request_log.response_payload`; production was not
queried or modified during this investigation. The exact offending sentence was
not supplied, so the regression fixture uses a long synthetic sentence.

The same mismatch existed for quote-token metadata, token names, DEX identifiers,
and metadata URLs/social fields. Increasing just the symbol width would leave
those paths exposed to another sufficiently long provider value.

## Normalization and provenance contract

`bounded_normalized_text` retains the first N Unicode code points in a normalized
convenience column, where N is the existing column width. PostgreSQL VARCHAR
limits characters, not UTF-8 bytes. There is no ellipsis, stripping, Unicode
normalization, or encoding conversion. None stays NULL; empty and ordinary values
are unchanged. A prefix at exactly the width is not proof that the original was
truncated; inspect its raw source when completeness matters.

Only display/convenience fields use this helper. Prefix URLs/handles need not be
valid or complete destinations; prefix DEX identifiers are not unique identities.
Research that requires complete strings must use the source evidence. Pair and
token identities remain their complete chain/address values.

Before scheduled pair writes, an oversized chain (>32), pair address (>128), base
token address (>128), or quote token address (>128) excludes that entire source
pair from normalized observations/enrichment. The request and batch completion
record a `partial` outcome with `normalization_issues`: kind
`oversized_pair_identity`, field, original length, width, and source locator.
Other valid pairs still produce observations. This also applies when no pair can
be normalized. The complete invalid record remains in the raw response; no
shortened address is ever inserted. Future scheduled work continues through the
existing partial-outcome policy, without rewriting historical schedules.

There are no new exception catches. DBAPI/SQLAlchemy failures unrelated to the
handled widths continue to propagate. The worker's supervision is unchanged.
This is width handling, not a general defense against every malformed provider
payload (for example, numeric overflow or PostgreSQL-incompatible JSON strings).

Raw response dictionaries, unknown fields, JSON arrays, payload digest functions,
source-record digest functions, locators, and idempotency keys are unchanged.
`content_sha256` continues to hash the complete **extracted source-attributed
content** (`asdict(PairFactCreate)` / `asdict(MetadataCreate)`), before applying
column bounds. It is not a hash of the whole raw record, nor of the stored prefix
columns. This preserves existing valid-value hashes and ensures changes only
beyond a stored prefix append a new version. Unchanged enrichment content still
deduplicates; separate requests and market observations remain recorded even for
unchanged responses. A conflicting replay under the same idempotency identity
still fails.

The existing `schema_version=1` and SQL schema remain unchanged: field types,
provenance layout, and digest recipes did not change, and previously storable
values normalize identically. These width semantics are implemented by the
collector code version; no configuration thresholds or migration are introduced.
No historical rows need rewriting.

To recover a full pair value, join `pair_fact_events.api_request_log_id` to
`api_request_log.id` and resolve `source_record_locator` (for example `pairs[0]`):

```sql
SELECT pf.id, pf.base_token_symbol AS symbol_prefix,
       r.response_payload -> 'pairs' ->
         (substring(pf.source_record_locator FROM '^pairs\[([0-9]+)\]$'))::integer
         -> 'baseToken' ->> 'symbol' AS full_symbol
FROM pair_fact_events pf
JOIN api_request_log r ON r.id = pf.api_request_log_id;
```

For metadata, follow `api_request_log_id` and `pairs[i]` or `records[i]` (pair
responses / boost feeds). Resolve the tracked token side using its complete
address for pair metadata. Discovery metadata instead follows
`discovery_event_id` to `discovery_events.source_payload`, with locator
`discovery_event`. Verify the record/payload using the existing canonical JSON
SHA-256 recipes, not PostgreSQL `jsonb::text` formatting. Raw evidence must remain
accessible when using normalized rows from either PostgreSQL or archives; the
existing archival retention/provenance rules still apply.

## Bounded external-string audit

The audit searched all 263 `String(...)` definitions across 58 tables in
`persistence/models.py`,
compared the migrations, and traced collection, discovery, RPC, and security
repository assignments. No DEX response body string is copied into a bounded
column outside the paths below.

| Destination / source | Width | Handling after this fix |
| --- | ---: | --- |
| `pairs.dex_identifier`, `pair_fact_events.dex_identifier` / `dexId` | 128 | Deterministic convenience prefix |
| Pair facts `base_token_name`, `quote_token_name` | 512 | Prefix |
| Pair facts `base_token_symbol`, `quote_token_symbol` | 128 | Prefix |
| `pairs.address`, pair facts base/quote addresses | 128 | Oversized source pair excluded with durable issue; never truncated |
| `pairs.chain` / `chainId` | 32 | Oversized source pair excluded; inserted chain comes from durable claim |
| `tokens.chain`, `tokens.address` / boost-feed chain/address | 32 / 128 | DEX never admits tokens; boost records match existing identities only. Oversized strings remain in raw feed and cannot match a stored token |
| `token_metadata_events.name`, `.symbol` | 512 / 128 | Prefix, shared by pair responses and discovery |
| Metadata `metadata_uri`, `image_url`, `header_url`, `website_url` | 4096 | Prefix; URI currently comes from discovery, image/header/website also from DEX pair/feed |
| Metadata `twitter`, `telegram` | 2048 | Prefix, shared by all metadata producers |
| Pair facts / metadata / boost observations / observations `source_record_locator` | 256 | Generated `pairs[index]` / `records[index]`; bounded in practice by the response list length, never provider text |
| Request `provider`, `endpoint`, `outcome`; enrichment `provider`, `source_kind` | 64 / 256 / 16 / 32 as applicable | Application constants, fixed route templates, or enumerated values; no provider URL copied here |
| SHA-256 columns and idempotency keys | 64 / 128 | Generated fixed-size digests; recipe unchanged |
| Pair URL; `info.openGraph`; feed URL/description | No VARCHAR destination | Retained in raw JSON only |
| Pair labels; website labels; social platforms; feed link type/label/URL | No VARCHAR destination for the full collections | Full raw JSON and, where mapped, `labels` / `other_links` JSONB; no width truncation |
| Unrecognized fields, time-window keys, HTTP error text | No VARCHAR destination | Raw response / JSON detail, or typed numeric mappings; not normalized text columns |
| Availability, market context, lifecycle, candidates, scheduling and boost events | Various | DEX-derived facts are numeric, UUID references, JSON evidence, or application-defined enums/reasons/hashes; no additional external text assignments |

Remaining width exposure **outside DEX-derived collection**, deliberately reported
rather than changing unrelated provider contracts in this incident fix:

- PumpPortal `mint` has only a minimum-length check before `tokens.address(128)`;
  `signature` (or fallback mint) reaches `discovery_events.provider_event_id(256)`
  without an upper bound. Those malformed identities can still fail discovery.
  PumpPortal name/symbol/URI/image/website/social metadata now benefits from the
  shared repository fix, but this does not fix its identity fields.
- Generic discovery adapters can supply unbounded `chain(32)`, `source_name` /
  `provider(64)`, `event_type(64)`, source event ID(256), checkpoint value(2048),
  coverage note(2048), or connectivity reason(128). Current PumpPortal uses fixed
  chain/provider/event/reason values and no replay cursor; these generic adapter
  contracts do not enforce all database widths.
- Universal Solana mint snapshots copy external account owner strings into
  `token_security_snapshots.account_owner(128)`, including malformed-owner paths.
  Authority addresses are base58 encodings of fixed 32-byte values and fit.
  Current decode-error messages are fixed parser messages, not unbounded external
  text; `decode_error(2048)` remains a generic repository contract boundary.
- Standard RPC holder enrichment accepts external token-account and owner strings
  into `holder_balance_facts.token_account(128)` and `.owner_wallet(128)` without
  upper bounds. `source_fact_identity(256)` is request UUID plus account address;
  normal valid addresses fit, but its constituent account is unvalidated.
- Replaceable security-provider contracts also leave widths unenforced for
  `security_provider_requests.page_cursor/next_cursor(256)`, provider(64),
  method(64), provider schema version(32); holder exclusion reason(256);
  creator wallet(128), relationship type(64), source fact identity(256);
  creator-history wallet(128); liquidity signature/LP wallet(128);
  wallet-edge endpoints(128); funding wallet/source/signature(128). The current
  standard RPC adapter returns advanced analyses unavailable and no cursors;
  these remain potential exposures for other implementations, not active DEX
  inputs. Their enum fields and computed hashes fit by construction.
- Generic repositories and direct ORM/SQL writes remain responsible for their
  identity/provenance contracts; this helper must never be applied indiscriminately
  to addresses or locators. Operator configuration, artifact/manifest paths,
  archival locations and verification descriptions have bounded columns too;
  they are not provider response fields and are outside this DEX fix.

No remaining DEX-derived VARCHAR width failure path was found in the supported
scheduled-pair, availability, and boost-feed workflows after this change.

## Added regression tests

`tests/unit/test_normalized_text.py`:

- `test_bounded_normalized_text_preserves_ordinary_values`: five cases (None,
  empty, whitespace, Unicode, exact width).
- `test_bounded_normalized_text_uses_code_points_without_unicode_normalization`:
  multibyte/combining text, deterministic prefix and repeated normalization.
- `test_bounded_normalized_text_rejects_invalid_limit`: zero and negative widths.

`tests/integration/test_dex_text_width.py`:

- `test_scheduled_text_width_preserves_raw_and_completes_task_group`: ordinary,
  exact-width multibyte, oversized base symbol, and all oversized display fields;
  complete scheduled PostgreSQL persistence, schema-width assertions, raw/source
  and content hashes, observation/batch completion, plus deliberate failing
  unbounded `pair_fact_events` INSERT (SQLSTATE 22001).
- `test_oversized_pair_identity_is_partial_without_truncating_addresses`: each of
  chain, pair, base, quote (four fields), with and without a valid companion pair
  (eight cases); explicit request/batch issues and complete raw records.
- `test_full_value_tail_changes_preserve_content_hashing_and_idempotency`:
  suffix-only changes create two pair/metadata versions sharing identical stored
  prefixes; an unchanged third response still produces its request/observation;
  duplicate pair fact and conflicting same-identity replay semantics persist.
- `test_boost_metadata_width_preserves_full_links_and_evidence`: boost-feed image,
  header, website, social links, unbounded labels/descriptions/URLs and raw hashes.
- `test_metadata_uri_width_and_database_errors_remain_visible`: URI convenience
  bound with full linked evidence; unrelated foreign-key SQLSTATE 23503 propagates.
- `test_discovery_metadata_width_preserves_payload_and_metadata_idempotency`:
  shared repository protection for discovery name/symbol/URI/image/links, complete
  discovery evidence/digests, duplicate metadata and conflicting replay semantics.

Total: 24 new parameterized test cases (8 unit, 16 PostgreSQL integration).

## Validation environment and commands

Validation used an isolated copy of the tracked source plus this patch (no `.env`)
and a fresh `postgres:16-alpine` container named
`pump-research-epoch14-width-test`, bound only to `127.0.0.1:55434`. The test database
was `pump_research_epoch14_test`; independent schema checks used
`pump_research_epoch14_schema_test`. Both were disposable; the test container and its volume were removed after
validation. Commands used the
project's Python virtualenv, a cleared environment (`env -i`), explicit test
PostgreSQL URLs, and `PYTHONPATH` pointing to the isolated source. No production
connection was made or production data/epoch state changed. No secrets were
accessed, collector CLI started, commit created, or push performed. In-process
test workflows and synthetic epoch fixtures only used the disposable database.

- Original code plus new `[symbol]` regression: **1 failed in 6.38s**, as expected,
  with `asyncpg.exceptions.StringDataRightTruncationError`, VARCHAR(128), and a
  TaskGroup exception. This check used original tracked code in the isolated copy;
  the working-tree fix was not reverted.
- `python -m pytest tests/unit/test_normalized_text.py tests/integration/test_dex_text_width.py -q`:
  **24 passed in 79.54s (0:01:19)**. The preliminary 19-case run also passed
  (58.25s); the final run includes discovery and invalid-only batch coverage.
- `python -m mypy --strict`: **Success: no issues found in 118 source files**.
- `ruff check .`: **All checks passed!**
- `ruff format --check src/pump_research/persistence/normalized_text.py tests/integration/test_dex_text_width.py tests/unit/test_normalized_text.py`:
  **3 files already formatted**.
- `git diff --check`: passed (no output).
- Fresh database `python -m alembic upgrade head`: passed.
- `python -m alembic heads`: **7d9b3e5f0a12 (head)**.
- Fresh database `python -m alembic check`: **No new upgrade operations detected.**

The full-suite command is:

```sh
python -m pytest --deselect=tests/integration/test_collector_process_recovery.py::test_physical_stop_restart_and_sigterm_reconstruct_postgres_state -q
```

The one deselection is required by the instruction not to start a collector: that
existing test launches a real `python -m pump_research collector run` subprocess.

Full-suite result: **1 failed, 319 passed, 1 deselected in 805.31s (0:13:25)**.
The failing existing test is
`tests/integration/test_phase5_candidates.py::test_concurrent_exact_holder_retry_is_idempotent`.
It raises `LostCandidateTaskLeaseError: candidate enrichment task lease is no
longer owned` from `candidates/repository.py:440`, through security enrichment task
completion. All 24 new width cases passed within this run.

An isolated rerun of that failing test against the original tracked code at
`0a2397e` also failed identically: **1 failed in 6.35s**. Thus the full suite is not
green, but this failure predates the width fix. Candidate/security lease behavior
was left unchanged. Validation logs are retained locally at
`/tmp/epoch14-width-suite.log`, `/tmp/epoch14-width-baseline-holder.log`,
`/tmp/epoch14-width-focused.log`, `/tmp/epoch14-width-red.log`,
`/tmp/epoch14-width-schema-upgrade.log`, and `/tmp/epoch14-width-schema-check.log`.

## Files changed and proposed commit

- `src/pump_research/persistence/normalized_text.py`: shared convenience-prefix helper.
- `src/pump_research/persistence/enrichment.py`: pair/metadata display bounds after hashing.
- `src/pump_research/persistence/repositories.py`: bound the canonical pair's DEX label.
- `src/pump_research/collection/polling.py`: explicit oversized-identity partial outcomes.
- `tests/unit/test_normalized_text.py`: helper boundary semantics.
- `tests/integration/test_dex_text_width.py`: PostgreSQL regressions and provenance assertions.
- `docs/collector-v2-phase2-enrichment.md`: link current normalization semantics.
- `docs/epoch14-text-width-incident.md`: incident, field audit, semantics, and validation report.

No migration is required. Proposed commit message (not committed):

```text
fix: bound DEX normalized text while preserving raw evidence
```
