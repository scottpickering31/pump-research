"""Mockable provider boundary and the conservative standard-RPC holder adapter."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

from pump_research.market_data.solana_rpc import SolanaRpcClient, SolanaRpcError
from pump_research.security_enrichment.contracts import (
    AcquisitionMode,
    CreatorEvidencePage,
    EvidenceAvailability,
    EvidenceCompleteness,
    EvidenceEnvelope,
    FundingEvidencePage,
    HolderAccountFact,
    HolderEvidencePage,
    LiquidityEvidencePage,
    ProviderPageRequest,
    TraderEvidencePage,
    WalletEdgeEvidencePage,
)


class SecurityProviderError(RuntimeError):
    """Transport or provider-contract error, distinct from a valid unavailable result."""


class SecurityEnrichmentProvider(Protocol):
    name: str

    async def fetch_holders(self, request: ProviderPageRequest) -> HolderEvidencePage: ...

    async def fetch_traders(self, request: ProviderPageRequest) -> TraderEvidencePage: ...

    async def fetch_creator(self, request: ProviderPageRequest) -> CreatorEvidencePage: ...

    async def fetch_liquidity(self, request: ProviderPageRequest) -> LiquidityEvidencePage: ...

    async def fetch_wallet_edges(self, request: ProviderPageRequest) -> WalletEdgeEvidencePage: ...

    async def fetch_funding(self, request: ProviderPageRequest) -> FundingEvidencePage: ...


class StandardSolanaHolderProvider:
    """Top-20-only provider; advanced analyses remain explicitly unavailable."""

    name = "solana_rpc"

    def __init__(self, client: SolanaRpcClient) -> None:
        self._client = client

    async def fetch_holders(self, request: ProviderPageRequest) -> HolderEvidencePage:
        try:
            return await self._fetch_holders(request)
        except SolanaRpcError as error:
            raise SecurityProviderError("Solana RPC holder request failed") from error

    async def _fetch_holders(self, request: ProviderPageRequest) -> HolderEvidencePage:
        largest = await self._client.get_token_largest_accounts(mint_address=request.token_address)
        addresses = [item.address for item in largest.accounts]
        owners = (
            await self._client.get_parsed_token_account_owners(addresses=addresses)
            if addresses
            else None
        )
        by_account = (
            {item.token_account: item.owner_wallet for item in owners.owners} if owners else {}
        )
        received_at = max(
            largest.received_at,
            owners.received_at if owners is not None else largest.received_at,
        )
        missing_owner = any(by_account.get(item.address) is None for item in largest.accounts)
        envelope = EvidenceEnvelope(
            provider=self.name,
            provider_schema_version="solana-json-rpc-finalized-v1",
            source_observed_at=None,
            received_at=received_at,
            availability=(
                EvidenceAvailability.PARTIAL if missing_owner else EvidenceAvailability.AVAILABLE
            ),
            completeness=EvidenceCompleteness.TOP_20_TOKEN_ACCOUNTS,
            acquisition_mode=AcquisitionMode.HISTORICALLY_AVAILABLE,
            source_slot=min(
                largest.slot,
                owners.slot if owners is not None else largest.slot,
            ),
            raw_payload={
                "getTokenLargestAccounts": largest.raw_response,
                "getMultipleAccounts": owners.raw_response if owners else None,
            },
        )
        known_pools = set(request.known_pool_accounts)
        return HolderEvidencePage(
            envelope=envelope,
            mint_supply_raw=request.mint_supply_raw,
            holder_count=None,
            accounts=tuple(
                HolderAccountFact(
                    token_account=item.address,
                    owner_wallet=by_account.get(item.address),
                    raw_balance=Decimal(item.raw_amount),
                    is_known_pool=item.address in known_pools,
                    is_creator=(
                        request.creator_wallet is not None
                        and by_account.get(item.address) == request.creator_wallet
                    ),
                    exclusion_reason=(
                        "known_pool_account" if item.address in known_pools else None
                    ),
                )
                for item in largest.accounts
            ),
        )

    async def fetch_traders(self, request: ProviderPageRequest) -> TraderEvidencePage:
        envelope = _unavailable(self.name, "indexed_trader_history_not_configured")
        window_end = request.window_end or request.input_watermark
        return TraderEvidencePage(
            envelope=envelope,
            window_start=request.window_start or window_end - timedelta(hours=1),
            window_end=window_end,
            trades=(),
        )

    async def fetch_creator(self, request: ProviderPageRequest) -> CreatorEvidencePage:
        return CreatorEvidencePage(
            envelope=_unavailable(self.name, "creator_history_source_not_configured"),
            relationships=(),
            history=None,
        )

    async def fetch_liquidity(self, request: ProviderPageRequest) -> LiquidityEvidencePage:
        return LiquidityEvidencePage(
            envelope=_unavailable(self.name, "pool_program_decoder_not_configured"),
            events=(),
        )

    async def fetch_wallet_edges(self, request: ProviderPageRequest) -> WalletEdgeEvidencePage:
        return WalletEdgeEvidencePage(
            envelope=_unavailable(self.name, "wallet_history_source_not_configured"),
            edges=(),
        )

    async def fetch_funding(self, request: ProviderPageRequest) -> FundingEvidencePage:
        return FundingEvidencePage(
            envelope=_unavailable(self.name, "funding_history_source_not_configured"),
            relationships=(),
        )


def failed_envelope(*, provider: str, received_at: datetime, failure_code: str) -> EvidenceEnvelope:
    return EvidenceEnvelope(
        provider=provider,
        provider_schema_version="unknown",
        source_observed_at=None,
        received_at=received_at.astimezone(UTC),
        availability=EvidenceAvailability.FAILED,
        completeness=EvidenceCompleteness.UNKNOWN,
        acquisition_mode=AcquisitionMode.HISTORICALLY_AVAILABLE,
        failure_code=failure_code,
    )


def _unavailable(provider: str, reason: str) -> EvidenceEnvelope:
    return EvidenceEnvelope(
        provider=provider,
        provider_schema_version="not-configured-v1",
        source_observed_at=None,
        received_at=datetime.now(UTC),
        availability=EvidenceAvailability.UNAVAILABLE,
        completeness=EvidenceCompleteness.UNKNOWN,
        acquisition_mode=AcquisitionMode.HISTORICALLY_AVAILABLE,
        failure_code=reason,
        raw_payload={"reason": reason},
    )
