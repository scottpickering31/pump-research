"""Finite, batched universal Solana mint-security snapshot collection."""

from __future__ import annotations

import base64
import binascii
import hashlib
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.market_data.solana_rpc import (
    MAX_MULTIPLE_ACCOUNTS,
    SOLANA_RPC_PROVIDER,
    SolanaAccountResult,
    SolanaMultipleAccountsResult,
)
from pump_research.persistence.enrichment import (
    TokenSecuritySnapshotRepository,
    TokenSecurityTaskRepository,
    canonical_digest,
)
from pump_research.persistence.repositories import ApiRequestLogRepository

SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
_EXTENSION_NAMES = {
    0: "uninitialized",
    1: "transfer_fee_config",
    2: "transfer_fee_amount",
    3: "mint_close_authority",
    4: "confidential_transfer_mint",
    5: "confidential_transfer_account",
    6: "default_account_state",
    7: "immutable_owner",
    8: "memo_transfer",
    9: "non_transferable",
    10: "interest_bearing_config",
    11: "cpi_guard",
    12: "permanent_delegate",
    13: "non_transferable_account",
    14: "transfer_hook",
    15: "transfer_hook_account",
    16: "confidential_transfer_fee_config",
    17: "confidential_transfer_fee_amount",
    18: "metadata_pointer",
    19: "token_metadata",
    20: "group_pointer",
    21: "token_group",
    22: "group_member_pointer",
    23: "token_group_member",
    24: "confidential_mint_burn",
    25: "scaled_ui_amount",
    26: "pausable",
    27: "pausable_account",
}
_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


class SolanaMintSource(Protocol):
    """Mockable read-only RPC boundary."""

    async def get_multiple_accounts(self, *, addresses: list[str]) -> SolanaMultipleAccountsResult:
        """Return account objects in request order."""


@dataclass(frozen=True, slots=True)
class DecodedMint:
    """Safe normalized interpretation of one source account outcome."""

    status: str
    account_owner: str | None
    token_program: str
    mint_authority: str | None
    freeze_authority: str | None
    raw_supply: Decimal | None
    decimals: int | None
    is_initialized: bool | None
    extension_types: list[object] | None
    raw_account_sha256: str | None
    decode_error: str | None


@dataclass(frozen=True, slots=True)
class SecurityCollectionResult:
    """One bounded security-task pass."""

    claimed: int
    available: int
    unavailable: int
    malformed: int


class TokenSecurityWorkflow:
    """Collect four finite mint snapshots per newly DEX-admitted token."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        source: SolanaMintSource,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._source = source
        self._lease_duration = timedelta(seconds=settings.token_security_lease_seconds)
        self._tasks = TokenSecurityTaskRepository()
        self._requests = ApiRequestLogRepository()
        self._snapshots = TokenSecuritySnapshotRepository()

    async def collect_due(
        self,
        *,
        collector_run_id: uuid.UUID,
        now: datetime | None = None,
    ) -> SecurityCollectionResult:
        """Claim one <=100-address batch, persist outcomes, and advance finite work."""
        requested_at = now or datetime.now(UTC)
        async with self._session_factory() as session, session.begin():
            claims = await self._tasks.claim_due(
                session,
                now=requested_at,
                limit=MAX_MULTIPLE_ACCOUNTS,
                lease_duration=self._lease_duration,
            )
        if not claims:
            return SecurityCollectionResult(0, 0, 0, 0)
        request_identity = uuid.uuid4()
        try:
            result = await self._source.get_multiple_accounts(
                addresses=[claim.address for claim in claims]
            )
            if result.addresses != tuple(claim.address for claim in claims):
                raise ValueError("Solana response does not match the security claims")
        except Exception as error:
            failed_at = datetime.now(UTC)
            async with self._session_factory() as session, session.begin():
                await self._requests.record(
                    session,
                    collector_run_id=collector_run_id,
                    idempotency_key=_key("security-failed", request_identity),
                    provider=SOLANA_RPC_PROVIDER,
                    endpoint="getMultipleAccounts",
                    requested_at=requested_at,
                    received_at=failed_at,
                    outcome="failed",
                    http_status_code=None,
                    request_payload={"addresses": [claim.address for claim in claims]},
                    response_payload=None,
                    response_payload_sha256=None,
                    failure_detail={"error_type": type(error).__name__, "message": str(error)},
                )
                await self._tasks.retry(
                    session,
                    claims=claims,
                    checked_at=failed_at,
                    retry_at=failed_at + timedelta(minutes=1),
                )
            raise

        decoded = tuple(decode_mint_account(account) for account in result.accounts)
        available = sum(item.status == "available" for item in decoded)
        unavailable = sum(item.status == "unavailable" for item in decoded)
        malformed = sum(item.status == "malformed" for item in decoded)
        outcome = (
            "empty" if unavailable == len(decoded) else "partial" if malformed else "succeeded"
        )
        async with self._session_factory() as session, session.begin():
            request = await self._requests.record(
                session,
                collector_run_id=collector_run_id,
                idempotency_key=_key("security", request_identity),
                provider=SOLANA_RPC_PROVIDER,
                endpoint="getMultipleAccounts",
                requested_at=requested_at,
                received_at=result.received_at,
                outcome=outcome,
                http_status_code=200,
                request_payload={
                    "addresses": [claim.address for claim in claims],
                    "encoding": "base64",
                    "commitment": "finalized",
                },
                response_payload=result.raw_response,
                response_payload_sha256=canonical_digest(result.raw_response),
                failure_detail=(
                    {"malformed_accounts": malformed, "unavailable_accounts": unavailable}
                    if outcome == "partial"
                    else None
                ),
            )
            for claim, mint in zip(claims, decoded, strict=True):
                await self._snapshots.record(
                    session,
                    token_id=claim.token_id,
                    collector_run_id=collector_run_id,
                    api_request_log_id=request.id,
                    idempotency_key=_key(
                        "security-snapshot", claim.token_id, claim.phase, request.id
                    ),
                    source_observed_at=None,
                    received_at=result.received_at,
                    rpc_slot=result.slot,
                    **asdict(mint),
                )
            await self._tasks.complete(session, claims=claims, checked_at=result.received_at)
        return SecurityCollectionResult(len(claims), available, unavailable, malformed)


def decode_mint_account(account_result: SolanaAccountResult) -> DecodedMint:
    """Decode classic/Token-2022 mint base state and extension type identifiers."""
    account = account_result.account
    if account is None:
        return DecodedMint(
            status="unavailable",
            account_owner=None,
            token_program="unknown",
            mint_authority=None,
            freeze_authority=None,
            raw_supply=None,
            decimals=None,
            is_initialized=None,
            extension_types=None,
            raw_account_sha256=None,
            decode_error=None,
        )
    owner = account.get("owner")
    data_field = account.get("data")
    if not isinstance(owner, str):
        return _malformed(None, "account owner is missing")
    if (
        not isinstance(data_field, list)
        or len(data_field) != 2
        or not isinstance(data_field[0], str)
        or data_field[1] != "base64"
    ):
        return _malformed(owner, "account data is not [base64, 'base64']")
    try:
        raw = base64.b64decode(data_field[0], validate=True)
    except (ValueError, binascii.Error):
        return _malformed(owner, "account data is invalid base64")
    raw_sha256 = hashlib.sha256(raw).hexdigest()
    program = (
        "spl_token"
        if owner == SPL_TOKEN_PROGRAM
        else "token_2022"
        if owner == TOKEN_2022_PROGRAM
        else "unknown"
    )
    if program == "unknown":
        return _malformed(owner, "account is not owned by a supported token program", raw_sha256)
    if len(raw) < 82:
        return _malformed(owner, "mint account is shorter than 82 bytes", raw_sha256, program)
    try:
        mint_authority = _coption_pubkey(raw[0:36])
        supply = int.from_bytes(raw[36:44], "little")
        decimals = raw[44]
        initialized = raw[45] != 0
        freeze_authority = _coption_pubkey(raw[46:82])
        extensions = _token_2022_extensions(raw) if program == "token_2022" else []
    except ValueError as error:
        return _malformed(owner, str(error), raw_sha256, program)
    return DecodedMint(
        status="available",
        account_owner=owner,
        token_program=program,
        mint_authority=mint_authority,
        freeze_authority=freeze_authority,
        raw_supply=Decimal(supply),
        decimals=decimals,
        is_initialized=initialized,
        extension_types=extensions,
        raw_account_sha256=raw_sha256,
        decode_error=None,
    )


def _coption_pubkey(value: bytes) -> str | None:
    tag = int.from_bytes(value[:4], "little")
    if tag == 0:
        return None
    if tag != 1:
        raise ValueError("mint authority option tag is invalid")
    return _base58_encode(value[4:36])


def _token_2022_extensions(raw: bytes) -> list[object]:
    if len(raw) == 82:
        return []
    if len(raw) < 166:
        raise ValueError("Token-2022 mint extension header is truncated")
    if raw[165] != 1:
        raise ValueError("Token-2022 account type is not Mint")
    offset = 166
    types: list[object] = []
    while offset < len(raw):
        if all(byte == 0 for byte in raw[offset:]):
            break
        if offset + 4 > len(raw):
            raise ValueError("Token-2022 extension header is truncated")
        extension_type = int.from_bytes(raw[offset : offset + 2], "little")
        length = int.from_bytes(raw[offset + 2 : offset + 4], "little")
        offset += 4
        if offset + length > len(raw):
            raise ValueError("Token-2022 extension body is truncated")
        types.append(
            {
                "type_id": extension_type,
                "name": _EXTENSION_NAMES.get(extension_type, "unknown"),
                "length": length,
                "body_sha256": hashlib.sha256(raw[offset : offset + length]).hexdigest(),
            }
        )
        offset += length
    return types


def _malformed(
    owner: str | None,
    error: str,
    raw_sha256: str | None = None,
    program: str = "unknown",
) -> DecodedMint:
    return DecodedMint(
        status="malformed",
        account_owner=owner,
        token_program=program,
        mint_authority=None,
        freeze_authority=None,
        raw_supply=None,
        decimals=None,
        is_initialized=None,
        extension_types=None,
        raw_account_sha256=raw_sha256,
        decode_error=error,
    )


def _base58_encode(value: bytes) -> str:
    leading_zeroes = len(value) - len(value.lstrip(b"\0"))
    number = int.from_bytes(value, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = _B58_ALPHABET[remainder] + encoded
    return "1" * leading_zeroes + encoded


def _key(*parts: object) -> str:
    return hashlib.sha256(":".join(map(str, parts)).encode()).hexdigest()
