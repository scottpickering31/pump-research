"""Typed representations of the current DEX Screener token-pairs response."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class DexScreenerToken(BaseModel):
    """Token identity and display metadata returned within a pair."""

    model_config = ConfigDict(extra="ignore")

    address: str
    name: str | None = None
    symbol: str | None = None


class DexScreenerTransactions(BaseModel):
    """Buy/sell counts for one provider-defined time window."""

    model_config = ConfigDict(extra="ignore")

    buys: int | None = None
    sells: int | None = None


class DexScreenerLiquidity(BaseModel):
    """Liquidity values associated with a pair."""

    model_config = ConfigDict(extra="ignore")

    usd: Decimal | None = None
    base: Decimal | None = None
    quote: Decimal | None = None


class DexScreenerWebsite(BaseModel):
    """One source-provided website link."""

    model_config = ConfigDict(extra="ignore")

    label: str | None = None
    url: str | None = None


class DexScreenerSocial(BaseModel):
    """One source-provided social handle/link."""

    model_config = ConfigDict(extra="ignore")

    platform: str | None = None
    handle: str | None = None


class DexScreenerInfo(BaseModel):
    """Optional display metadata embedded in a pair response."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    image_url: str | None = Field(default=None, validation_alias="imageUrl")
    header: str | None = None
    open_graph: str | None = Field(default=None, validation_alias="openGraph")
    websites: list[DexScreenerWebsite] | None = None
    socials: list[DexScreenerSocial] | None = None


class DexScreenerBoosts(BaseModel):
    """Pair-response boost state; absence is distinct from an explicit zero."""

    model_config = ConfigDict(extra="ignore")

    active: int | None = Field(default=None, ge=0)


class DexScreenerBoostFeedLink(BaseModel):
    """One metadata link included with a global boost feed record."""

    model_config = ConfigDict(extra="ignore")

    type: str | None = None
    label: str | None = None
    url: str | None = None


class DexScreenerBoostFeedRecord(BaseModel):
    """Numeric facts returned by the global latest/top boost feeds."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    url: str | None = None
    chain_id: str | None = Field(default=None, validation_alias="chainId")
    token_address: str | None = Field(default=None, validation_alias="tokenAddress")
    amount: Decimal | None = None
    total_amount: Decimal | None = Field(default=None, validation_alias="totalAmount")
    icon: str | None = None
    header: str | None = None
    description: str | None = None
    links: list[DexScreenerBoostFeedLink] | None = None


class DexScreenerPair(BaseModel):
    """A typed DEX Screener pair record without application-derived state."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    chain_id: str | None = Field(default=None, validation_alias="chainId")
    dex_id: str | None = Field(default=None, validation_alias="dexId")
    url: str | None = None
    pair_address: str | None = Field(default=None, validation_alias="pairAddress")
    labels: list[str] | None = None
    base_token: DexScreenerToken | None = Field(default=None, validation_alias="baseToken")
    quote_token: DexScreenerToken | None = Field(default=None, validation_alias="quoteToken")
    price_native: Decimal | None = Field(default=None, validation_alias="priceNative")
    price_usd: Decimal | None = Field(default=None, validation_alias="priceUsd")
    txns: dict[str, DexScreenerTransactions] = Field(default_factory=dict)
    volume: dict[str, Decimal] = Field(default_factory=dict)
    price_change: dict[str, Decimal] = Field(default_factory=dict, validation_alias="priceChange")
    liquidity: DexScreenerLiquidity | None = None
    fdv: Decimal | None = None
    market_cap: Decimal | None = Field(default=None, validation_alias="marketCap")
    pair_created_at: int | None = Field(default=None, validation_alias="pairCreatedAt")
    info: DexScreenerInfo | None = None
    boosts: DexScreenerBoosts | None = None
