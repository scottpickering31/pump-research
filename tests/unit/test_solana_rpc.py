from __future__ import annotations

import logging
import traceback

import httpx
import pytest

from pump_research.config import Settings
from pump_research.logging import configure_logging, get_logger
from pump_research.market_data.solana_rpc import SolanaRpcClient, SolanaRpcError

DATABASE_URL = "postgresql+asyncpg://unused:unused@localhost/unused"
RPC_SECRET = "helius-secret-value"
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={RPC_SECRET}"


def _settings() -> Settings:
    return Settings(
        database_url=DATABASE_URL,
        solana_rpc_url=RPC_URL,
        solana_rpc_requests_per_minute=30,
    )


@pytest.mark.asyncio
async def test_get_multiple_accounts_uses_exact_query_bearing_endpoint() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "result": {"context": {"slot": 10}, "value": [None]},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        rpc = SolanaRpcClient(_settings(), http_client=http_client)
        result = await rpc.get_multiple_accounts(addresses=["account-a"])

    assert result.slot == 10
    assert [str(request.url) for request in requests] == [RPC_URL]
    assert requests[0].url.params["api-key"] == RPC_SECRET


@pytest.mark.asyncio
async def test_generic_rpc_methods_use_exact_query_bearing_endpoint() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        method = request.read().decode()
        if "getTokenLargestAccounts" in method:
            value: list[object] = [
                {"address": "token-account-a", "amount": "100", "decimals": 6}
            ]
        else:
            value = [{"data": {"parsed": {"info": {"owner": "wallet-a"}}}}]
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "result": {"context": {"slot": 11}, "value": value},
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://injected-client-base.invalid",
    ) as http_client:
        rpc = SolanaRpcClient(_settings(), http_client=http_client)
        largest = await rpc.get_token_largest_accounts(mint_address="mint-a")
        owners = await rpc.get_parsed_token_account_owners(addresses=["token-account-a"])

    assert largest.accounts[0].address == "token-account-a"
    assert owners.owners[0].owner_wallet == "wallet-a"
    assert [str(request.url) for request in requests] == [RPC_URL, RPC_URL]
    assert all(request.url.params["api-key"] == RPC_SECRET for request in requests)


@pytest.mark.asyncio
async def test_rpc_authentication_failure_does_not_leak_query_credentials(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="unauthorized")

    settings = _settings()
    configure_logging(settings)
    caplog.set_level(logging.INFO)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        rpc = SolanaRpcClient(settings, http_client=http_client)
        with pytest.raises(SolanaRpcError) as captured:
            await rpc.get_token_largest_accounts(mint_address="mint-a")

    try:
        raise captured.value
    except SolanaRpcError:
        get_logger(component="solana_rpc_test").exception("solana_rpc_request_failed")

    rendered_error = "".join(
        traceback.format_exception(
            type(captured.value), captured.value, captured.value.__traceback__
        )
    )
    rendered_logs = caplog.text + capsys.readouterr().out
    assert str(captured.value) == "Solana getTokenLargestAccounts request failed"
    assert RPC_SECRET not in rendered_error
    assert RPC_SECRET not in rendered_logs
    assert RPC_URL not in rendered_error
    assert RPC_URL not in rendered_logs


@pytest.mark.asyncio
async def test_json_rpc_error_does_not_echo_untrusted_provider_details() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": -32000, "message": f"rejected endpoint {RPC_URL}"},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        rpc = SolanaRpcClient(_settings(), http_client=http_client)
        with pytest.raises(SolanaRpcError) as captured:
            await rpc.get_token_largest_accounts(mint_address="mint-a")

    assert str(captured.value) == "Solana getTokenLargestAccounts JSON-RPC error"
    assert RPC_SECRET not in str(captured.value)
