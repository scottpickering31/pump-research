"""Minimal read-only Solana JSON-RPC client for batched mint account facts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from pump_research.config import Settings
from pump_research.market_data.rate_limiter import AsyncRateLimiter

SOLANA_RPC_PROVIDER = "solana_rpc"
MAX_MULTIPLE_ACCOUNTS = 100


class SolanaRpcError(RuntimeError):
    """A transport, HTTP, JSON-RPC, or response-contract failure."""


@dataclass(frozen=True, slots=True)
class SolanaAccountResult:
    """One raw account object retained in request order."""

    address: str
    account: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class SolanaMultipleAccountsResult:
    """Typed envelope and full raw evidence for getMultipleAccounts."""

    addresses: tuple[str, ...]
    slot: int
    accounts: tuple[SolanaAccountResult, ...]
    received_at: datetime
    raw_response: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SolanaLargestTokenAccount:
    address: str
    raw_amount: int
    decimals: int


@dataclass(frozen=True, slots=True)
class SolanaLargestTokenAccountsResult:
    mint_address: str
    slot: int
    accounts: tuple[SolanaLargestTokenAccount, ...]
    received_at: datetime
    raw_response: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SolanaParsedTokenAccountOwner:
    token_account: str
    owner_wallet: str | None


@dataclass(frozen=True, slots=True)
class SolanaParsedTokenOwnersResult:
    slot: int
    owners: tuple[SolanaParsedTokenAccountOwner, ...]
    received_at: datetime
    raw_response: dict[str, Any]


class SolanaRpcClient:
    """Read-only client with an explicit provider budget separate from DEX capacity."""

    def __init__(
        self,
        settings: Settings,
        *,
        http_client: httpx.AsyncClient | None = None,
        rate_limiter: AsyncRateLimiter | None = None,
    ) -> None:
        self._http_client = http_client or httpx.AsyncClient(
            base_url=settings.solana_rpc_url,
            timeout=httpx.Timeout(settings.solana_rpc_timeout_seconds),
        )
        self._owns_http_client = http_client is None
        self._rate_limiter = rate_limiter or AsyncRateLimiter(
            settings.solana_rpc_requests_per_minute
        )

    async def __aenter__(self) -> SolanaRpcClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http_client:
            await self._http_client.aclose()

    async def get_multiple_accounts(self, *, addresses: list[str]) -> SolanaMultipleAccountsResult:
        """Fetch no more than 100 accounts with finalized base64 evidence."""
        if not addresses or len(addresses) > MAX_MULTIPLE_ACCOUNTS:
            raise ValueError("getMultipleAccounts requires between 1 and 100 addresses")
        if len(set(addresses)) != len(addresses) or any(not item.strip() for item in addresses):
            raise ValueError("getMultipleAccounts addresses must be unique and non-empty")
        await self._rate_limiter.acquire()
        request_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [addresses, {"encoding": "base64", "commitment": "finalized"}],
        }
        try:
            response = await self._http_client.post("", json=request_payload)
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise SolanaRpcError("Solana getMultipleAccounts request failed") from error
        if not isinstance(payload, dict):
            raise SolanaRpcError("Solana JSON-RPC response must be an object")
        if payload.get("error") is not None:
            raise SolanaRpcError(f"Solana JSON-RPC error: {payload['error']!r}")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise SolanaRpcError("Solana JSON-RPC result is missing")
        context = result.get("context")
        values = result.get("value")
        if (
            not isinstance(context, dict)
            or not isinstance(context.get("slot"), int)
            or not isinstance(values, list)
            or len(values) != len(addresses)
            or any(value is not None and not isinstance(value, dict) for value in values)
        ):
            raise SolanaRpcError("Solana getMultipleAccounts response shape is invalid")
        received_at = datetime.now(UTC)
        return SolanaMultipleAccountsResult(
            addresses=tuple(addresses),
            slot=int(context["slot"]),
            accounts=tuple(
                SolanaAccountResult(address, value)
                for address, value in zip(addresses, values, strict=True)
            ),
            received_at=received_at,
            raw_response=payload,
        )

    async def get_token_largest_accounts(
        self, *, mint_address: str
    ) -> SolanaLargestTokenAccountsResult:
        """Return the RPC-defined top 20 token accounts at finalized commitment."""
        payload = await self._rpc(
            "getTokenLargestAccounts",
            [mint_address, {"commitment": "finalized"}],
        )
        result = payload.get("result")
        if not isinstance(result, dict):
            raise SolanaRpcError("getTokenLargestAccounts result is missing")
        context, values = result.get("context"), result.get("value")
        if not isinstance(context, dict) or not isinstance(context.get("slot"), int):
            raise SolanaRpcError("getTokenLargestAccounts context is invalid")
        if not isinstance(values, list) or len(values) > 20:
            raise SolanaRpcError("getTokenLargestAccounts value is invalid")
        accounts: list[SolanaLargestTokenAccount] = []
        for value in values:
            if not isinstance(value, dict):
                raise SolanaRpcError("getTokenLargestAccounts record is invalid")
            address, amount, decimals = (
                value.get("address"),
                value.get("amount"),
                value.get("decimals"),
            )
            if (
                not isinstance(address, str)
                or not isinstance(amount, str)
                or not amount.isdigit()
                or not isinstance(decimals, int)
            ):
                raise SolanaRpcError("getTokenLargestAccounts record fields are invalid")
            accounts.append(SolanaLargestTokenAccount(address, int(amount), decimals))
        return SolanaLargestTokenAccountsResult(
            mint_address=mint_address,
            slot=int(context["slot"]),
            accounts=tuple(accounts),
            received_at=datetime.now(UTC),
            raw_response=payload,
        )

    async def get_parsed_token_account_owners(
        self, *, addresses: list[str]
    ) -> SolanaParsedTokenOwnersResult:
        """Resolve owner wallets for no more than 100 token accounts."""
        if not addresses or len(addresses) > MAX_MULTIPLE_ACCOUNTS:
            raise ValueError("parsed token owners require between 1 and 100 addresses")
        payload = await self._rpc(
            "getMultipleAccounts",
            [addresses, {"encoding": "jsonParsed", "commitment": "finalized"}],
        )
        result = payload.get("result")
        if not isinstance(result, dict):
            raise SolanaRpcError("parsed getMultipleAccounts result is missing")
        context, values = result.get("context"), result.get("value")
        if (
            not isinstance(context, dict)
            or not isinstance(context.get("slot"), int)
            or not isinstance(values, list)
            or len(values) != len(addresses)
        ):
            raise SolanaRpcError("parsed getMultipleAccounts response shape is invalid")
        owners: list[SolanaParsedTokenAccountOwner] = []
        for address, value in zip(addresses, values, strict=True):
            owner: str | None = None
            if value is not None:
                if not isinstance(value, dict):
                    raise SolanaRpcError("parsed token account must be an object or null")
                data = value.get("data")
                if isinstance(data, dict):
                    parsed = data.get("parsed")
                    if isinstance(parsed, dict):
                        info = parsed.get("info")
                        if isinstance(info, dict) and isinstance(info.get("owner"), str):
                            owner = str(info["owner"])
            owners.append(SolanaParsedTokenAccountOwner(address, owner))
        return SolanaParsedTokenOwnersResult(
            slot=int(context["slot"]),
            owners=tuple(owners),
            received_at=datetime.now(UTC),
            raw_response=payload,
        )

    async def _rpc(self, method: str, params: list[object]) -> dict[str, Any]:
        await self._rate_limiter.acquire()
        try:
            response = await self._http_client.post(
                "", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise SolanaRpcError(f"Solana {method} request failed") from error
        if not isinstance(payload, dict):
            raise SolanaRpcError(f"Solana {method} response must be an object")
        if payload.get("error") is not None:
            raise SolanaRpcError(f"Solana {method} JSON-RPC error: {payload['error']!r}")
        return payload
