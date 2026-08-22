from __future__ import annotations

import base64
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from pump_research.collection.market_context import _returns
from pump_research.collection.polling import _observation_create
from pump_research.collection.security import (
    SPL_TOKEN_PROGRAM,
    TOKEN_2022_PROGRAM,
    decode_mint_account,
)
from pump_research.config import Settings
from pump_research.market_data.dexscreener_models import DexScreenerPair
from pump_research.market_data.solana_rpc import SolanaAccountResult


def _mint_bytes(
    *,
    mint_authority: bytes | None,
    freeze_authority: bytes | None,
    supply: int = 1_000_000,
    decimals: int = 6,
) -> bytearray:
    raw = bytearray(82)
    if mint_authority is not None:
        raw[0:4] = (1).to_bytes(4, "little")
        raw[4:36] = mint_authority
    raw[36:44] = supply.to_bytes(8, "little")
    raw[44] = decimals
    raw[45] = 1
    if freeze_authority is not None:
        raw[46:50] = (1).to_bytes(4, "little")
        raw[50:82] = freeze_authority
    return raw


def _account(owner: str, raw: bytes) -> SolanaAccountResult:
    return SolanaAccountResult(
        address="mint",
        account={
            "owner": owner,
            "data": [base64.b64encode(raw).decode(), "base64"],
            "executable": False,
        },
    )


def test_normalized_market_fields_preserve_missing_vs_zero() -> None:
    pair = DexScreenerPair.model_validate(
        {
            "chainId": "solana",
            "pairAddress": "pair",
            "baseToken": {"address": "token"},
            "quoteToken": {"address": "SOL"},
            "txns": {
                "h6": {"buys": 0, "sells": None},
                "h24": {"buys": 44, "sells": 21},
            },
            "liquidity": {"usd": "10", "base": "0", "quote": None},
            "pairCreatedAt": 1_700_000_000_000,
        }
    )

    observation = _observation_create(uuid.uuid4(), pair, 0)

    assert observation.buys_h6 == 0
    assert observation.sells_h6 is None
    assert observation.buys_h24 == 44
    assert observation.sells_h24 == 21
    assert observation.liquidity_base == Decimal("0")
    assert observation.liquidity_quote is None


def test_classic_spl_mint_authorities_and_supply_decode() -> None:
    authority = bytes(range(1, 33))
    decoded = decode_mint_account(
        _account(
            SPL_TOKEN_PROGRAM,
            bytes(_mint_bytes(mint_authority=authority, freeze_authority=None)),
        )
    )

    assert decoded.status == "available"
    assert decoded.token_program == "spl_token"
    assert decoded.mint_authority is not None
    assert decoded.freeze_authority is None
    assert decoded.raw_supply == Decimal("1000000")
    assert decoded.decimals == 6
    assert decoded.is_initialized is True
    assert decoded.extension_types == []


def test_token_2022_extension_types_are_preserved_without_opaque_score() -> None:
    raw = _mint_bytes(mint_authority=None, freeze_authority=None)
    raw.extend(b"\0" * 83)
    raw.append(1)  # AccountType::Mint
    raw.extend((12).to_bytes(2, "little"))  # permanent delegate
    raw.extend((32).to_bytes(2, "little"))
    raw.extend(bytes(range(32)))

    decoded = decode_mint_account(_account(TOKEN_2022_PROGRAM, bytes(raw)))

    assert decoded.status == "available"
    assert decoded.token_program == "token_2022"
    assert decoded.extension_types is not None
    extension = cast(dict[str, object], decoded.extension_types[0])
    assert extension == {
        "type_id": 12,
        "name": "permanent_delegate",
        "length": 32,
        "body_sha256": extension["body_sha256"],
    }


def test_unavailable_and_wrong_program_are_explicit() -> None:
    unavailable = decode_mint_account(SolanaAccountResult("mint", None))
    wrong_program = decode_mint_account(
        _account("11111111111111111111111111111111", bytes(_mint_bytes(
            mint_authority=None, freeze_authority=None
        )))
    )

    assert unavailable.status == "unavailable"
    assert unavailable.raw_supply is None
    assert wrong_program.status == "malformed"
    assert wrong_program.token_program == "unknown"
    assert wrong_program.decode_error is not None


def test_source_creation_time_is_not_used_as_received_time() -> None:
    pair = DexScreenerPair.model_validate({"pairCreatedAt": 1_700_000_000_000})
    expected = datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)
    assert pair.pair_created_at == int(expected.timestamp() * 1000)


def test_shared_context_returns_use_only_prior_prices() -> None:
    latest, volatility = _returns(
        Decimal("110"),
        [Decimal("100"), Decimal("105")],
    )
    assert latest == Decimal("110") / Decimal("105") - 1
    assert volatility is not None and volatility > 0


def test_phase2_dex_feed_budget_preserves_phase1_reserve() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research"
    )
    feed_requests_per_minute = (
        60 / settings.boost_latest_poll_seconds + 60 / settings.boost_top_poll_seconds
    )
    safe_requests = settings.dex_screener_requests_per_minute * (
        1 - settings.scheduler_capacity_headroom_ratio
    )
    assert feed_requests_per_minute == 1.2
    assert settings.scheduler_reserved_requests_per_minute == 14
    assert feed_requests_per_minute < settings.scheduler_reserved_requests_per_minute
    assert safe_requests == 192
