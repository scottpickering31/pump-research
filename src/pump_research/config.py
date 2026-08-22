"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import logging
from decimal import Decimal
from functools import lru_cache
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url


class Settings(BaseSettings):
    """Application settings for the currently supported local environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PUMP_RESEARCH_",
        extra="ignore",
    )

    environment: str = "development"
    database_url: str = Field(min_length=1)
    database_connect_timeout_seconds: float = Field(default=5.0, gt=0)
    log_level: str = "INFO"
    log_json: bool = False
    dex_screener_base_url: str = "https://api.dexscreener.com"
    dex_screener_requests_per_minute: int = Field(default=240, gt=0, le=300)
    dex_screener_timeout_seconds: float = Field(default=10.0, gt=0)
    dex_screener_max_attempts: int = Field(default=3, ge=1, le=5)
    dex_screener_retry_backoff_seconds: float = Field(default=0.5, ge=0, le=30)
    dex_screener_boost_route_requests_per_minute: int = Field(default=60, ge=1, le=60)
    boost_latest_poll_seconds: int = Field(default=60, ge=30, le=3600)
    boost_top_poll_seconds: int = Field(default=300, ge=60, le=3600)
    solana_rpc_url: str = "https://api.mainnet-beta.solana.com"
    solana_rpc_timeout_seconds: float = Field(default=15.0, gt=0, le=60)
    solana_rpc_requests_per_minute: int = Field(default=2, ge=1, le=30)
    token_security_poll_seconds: int = Field(default=30, ge=5, le=300)
    token_security_lease_seconds: int = Field(default=180, ge=30, le=900)
    market_context_interval_seconds: int = Field(default=300, ge=60, le=3600)
    pumpportal_websocket_url: str = "wss://pumpportal.fun/api/data"
    pumpportal_api_key: SecretStr | None = None
    pumpportal_connect_timeout_seconds: float = Field(default=10.0, gt=0, le=60)
    pumpportal_fetch_wait_seconds: float = Field(default=1.0, gt=0, le=30)
    pumpportal_batch_size: int = Field(default=500, ge=1, le=10_000)
    pumpportal_queue_capacity: int = Field(default=10_000, ge=100, le=100_000)
    pumpportal_reconnect_initial_seconds: float = Field(default=1.0, gt=0, le=60)
    pumpportal_reconnect_max_seconds: float = Field(default=30.0, gt=0, le=300)
    pumpportal_reconnect_jitter_ratio: float = Field(default=0.2, ge=0, le=1)
    collector_discovery_poll_seconds: float = Field(default=2.0, gt=0, le=300)
    collector_reconciliation_poll_seconds: float = Field(default=1.0, gt=0, le=300)
    collector_scheduler_poll_seconds: float = Field(default=0.25, gt=0, le=60)
    collector_heartbeat_seconds: float = Field(default=10.0, gt=0, le=300)
    storage_telemetry_interval_seconds: int = Field(default=600, ge=60, le=86_400)
    collector_shutdown_grace_seconds: float = Field(default=30.0, gt=0, le=300)
    dex_availability_retry_seconds: int = Field(default=60, ge=1)
    dex_availability_lease_seconds: int = Field(default=120, ge=1)
    scheduler_new_initial_interval_seconds: int = Field(default=15, ge=1)
    scheduler_new_initial_duration_seconds: int = Field(default=120, ge=1)
    scheduler_new_interval_seconds: int = Field(default=30, ge=1)
    scheduler_active_interval_seconds: int = Field(default=5, ge=1)
    scheduler_watch_interval_seconds: int = Field(default=15, ge=1)
    scheduler_fading_interval_seconds: int = Field(default=120, ge=1)
    scheduler_dormant_interval_seconds: int = Field(default=900, ge=1)
    scheduler_resurrected_interval_seconds: int = Field(default=5, ge=1)
    scheduler_mature_interval_seconds: int = Field(default=300, ge=1)
    scheduler_cooled_interval_seconds: int = Field(default=1800, ge=1)
    scheduler_long_tail_day_interval_seconds: int = Field(default=7200, ge=1)
    scheduler_long_tail_week_interval_seconds: int = Field(default=43200, ge=1)
    scheduler_early_until_seconds: int = Field(default=600, ge=1)
    scheduler_mature_until_seconds: int = Field(default=3600, ge=1)
    scheduler_cooled_until_seconds: int = Field(default=21600, ge=1)
    scheduler_long_tail_day_until_seconds: int = Field(default=86400, ge=1)
    scheduler_retire_after_seconds: int = Field(default=604800, ge=1)
    scheduler_fading_tail_fast_duration_seconds: int = Field(default=1800, ge=1)
    scheduler_fading_tail_total_duration_seconds: int = Field(default=21600, ge=1)
    scheduler_fading_tail_cool_interval_seconds: int = Field(default=1800, ge=1)
    scheduler_control_scan_tokens_per_minute: int = Field(default=2, ge=1, le=30)
    scheduler_reserved_requests_per_minute: int = Field(default=14, ge=0, le=120)
    scheduler_capacity_headroom_ratio: float = Field(default=0.20, ge=0, lt=1)
    scheduler_capacity_refresh_seconds: int = Field(default=30, ge=1, le=300)
    scheduler_batch_size: int = Field(default=30, ge=1, le=30)
    scheduler_lease_seconds: int = Field(default=120, ge=1)
    scheduler_max_in_flight_batches: int = Field(default=4, ge=1, le=100)
    candidate_evaluation_interval_seconds: int = Field(default=5, ge=1, le=300)
    candidate_tier1_coverage_seconds: int = Field(default=1800, ge=60, le=86400)
    candidate_tier2_coverage_seconds: int = Field(default=3600, ge=60, le=172800)
    candidate_coverage_interval_seconds: int = Field(default=15, ge=5, le=300)
    candidate_security_freshness_seconds: int = Field(default=21600, ge=60, le=604800)
    candidate_tasks_per_minute: int = Field(default=12, ge=1, le=120)
    candidate_expensive_slots_per_minute: int = Field(default=2, ge=0, le=30)
    candidate_boost_wakeups_per_minute: int = Field(default=5, ge=1, le=60)
    candidate_max_active_coverage: int = Field(default=100, ge=1, le=10000)
    candidate_task_lease_seconds: int = Field(default=300, ge=30, le=3600)
    candidate_task_max_attempts: int = Field(default=4, ge=1, le=20)
    candidate_min_liquidity_usd: Decimal = Field(default=Decimal("10000"), ge=0)
    candidate_min_transactions_m5: int = Field(default=20, ge=0)
    candidate_min_volume_liquidity_ratio: Decimal = Field(default=Decimal("0.05"), ge=0)
    candidate_tier2_min_liquidity_usd: Decimal = Field(default=Decimal("50000"), ge=0)
    candidate_tier2_min_transactions_m5: int = Field(default=50, ge=0)
    candidate_tier2_min_volume_liquidity_ratio: Decimal = Field(default=Decimal("0.10"), ge=0)
    security_indexer_url: str | None = None
    security_indexer_api_key: SecretStr | None = None
    security_indexer_requests_per_minute: int = Field(default=6, ge=1, le=120)
    security_enrichment_poll_seconds: float = Field(default=1.0, ge=0.1, le=60)
    security_enrichment_workers: int = Field(default=4, ge=1, le=16)
    security_transaction_history_requests_per_minute: int = Field(default=4, ge=1, le=60)
    # A standard-RPC holder snapshot performs two calls: largest accounts and
    # parsed owner lookup. One logical snapshot/min therefore respects the
    # existing two-raw-RPC/min service ceiling.
    security_holder_requests_per_minute: int = Field(default=1, ge=1, le=30)
    security_wallet_graph_requests_per_minute: int = Field(default=1, ge=1, le=30)
    security_max_pages_per_task: int = Field(default=4, ge=1, le=20)
    security_page_size: int = Field(default=250, ge=1, le=1000)
    security_max_wallets_per_candidate: int = Field(default=40, ge=2, le=500)
    security_max_edges_per_candidate: int = Field(default=500, ge=1, le=10_000)
    security_max_funding_hops: int = Field(default=2, ge=1, le=2)
    security_holder_ttl_seconds: int = Field(default=600, ge=60, le=86_400)
    security_trader_ttl_seconds: int = Field(default=300, ge=60, le=86_400)
    security_creator_ttl_seconds: int = Field(default=86_400, ge=300, le=2_592_000)
    security_liquidity_ttl_seconds: int = Field(default=300, ge=30, le=86_400)
    security_wallet_graph_ttl_seconds: int = Field(default=3600, ge=300, le=604_800)
    security_funding_ttl_seconds: int = Field(default=86_400, ge=300, le=2_592_000)
    security_tier3_ttl_seconds: int = Field(default=3600, ge=300, le=172_800)
    security_max_tier3_candidates: int = Field(default=20, ge=1, le=1000)
    security_tier3_holder_top10_pct: Decimal = Field(default=Decimal("60"), ge=0, le=100)
    security_tier3_max_unique_traders: int = Field(default=20, ge=1)
    security_tier3_min_total_trades: int = Field(default=100, ge=1)
    security_tier3_common_funder_share: Decimal = Field(default=Decimal("25"), ge=0, le=100)
    security_tier3_liquidity_removal_pct: Decimal = Field(default=Decimal("30"), ge=0, le=100)
    archive_export_chunk_rows: int = Field(default=25_000, ge=100, le=250_000)
    archive_max_file_rows: int = Field(default=1_000_000, ge=25_000, le=10_000_000)
    archive_minimum_free_bytes: int = Field(default=2 * 1024**3, ge=256 * 1024**2)
    archive_minimum_hot_retention_days: int = Field(default=14, ge=1, le=365)
    lifecycle_new_to_active_min_volume_m5_usd: Decimal = Field(default=Decimal("100"), ge=0)
    lifecycle_new_to_watch_min_liquidity_usd: Decimal = Field(default=Decimal("1000"), ge=0)
    lifecycle_active_to_fading_max_volume_m5_usd: Decimal = Field(default=Decimal("25"), ge=0)
    lifecycle_watch_to_fading_max_volume_m5_usd: Decimal = Field(default=Decimal("10"), ge=0)
    lifecycle_fading_to_dormant_max_volume_h1_usd: Decimal = Field(default=Decimal("10"), ge=0)
    lifecycle_fading_to_dormant_max_liquidity_usd: Decimal = Field(default=Decimal("100"), ge=0)
    lifecycle_dormant_to_resurrected_min_volume_m5_usd: Decimal = Field(
        default=Decimal("100"), ge=0
    )
    lifecycle_dormant_to_resurrected_min_liquidity_usd: Decimal = Field(
        default=Decimal("500"), ge=0
    )

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        """Require the async PostgreSQL driver used by this application."""
        try:
            url = make_url(value)
        except Exception as error:
            msg = "PUMP_RESEARCH_DATABASE_URL must be a valid SQLAlchemy URL"
            raise ValueError(msg) from error

        if url.drivername != "postgresql+asyncpg":
            msg = "PUMP_RESEARCH_DATABASE_URL must use the postgresql+asyncpg driver"
            raise ValueError(msg)
        return value

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        """Normalize log-level spelling without accepting an empty value."""
        normalized = value.upper()
        if normalized not in logging.getLevelNamesMapping():
            msg = "PUMP_RESEARCH_LOG_LEVEL must be a standard Python log level"
            raise ValueError(msg)
        return normalized

    @field_validator("pumpportal_websocket_url")
    @classmethod
    def validate_pumpportal_websocket_url(cls, value: str) -> str:
        """Require a secure URL without an embedded API credential."""
        parsed = urlsplit(value)
        if parsed.scheme != "wss" or not parsed.netloc or parsed.fragment:
            msg = "PUMP_RESEARCH_PUMPPORTAL_WEBSOCKET_URL must be a valid wss:// URL"
            raise ValueError(msg)
        if parsed.username is not None or parsed.password is not None:
            msg = "PUMP_RESEARCH_PUMPPORTAL_WEBSOCKET_URL must not contain credentials"
            raise ValueError(msg)
        if any(partition.split("=", 1)[0] == "api-key" for partition in parsed.query.split("&")):
            msg = "Set PUMP_RESEARCH_PUMPPORTAL_API_KEY separately from the WebSocket URL"
            raise ValueError(msg)
        return value

    @field_validator("solana_rpc_url")
    @classmethod
    def validate_solana_rpc_url(cls, value: str) -> str:
        """Require an HTTP(S) RPC endpoint without embedded credentials."""
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.fragment:
            msg = "PUMP_RESEARCH_SOLANA_RPC_URL must be a valid http(s) URL"
            raise ValueError(msg)
        if parsed.username is not None or parsed.password is not None:
            msg = "PUMP_RESEARCH_SOLANA_RPC_URL must not contain embedded credentials"
            raise ValueError(msg)
        return value

    @field_validator("security_indexer_url")
    @classmethod
    def validate_security_indexer_url(cls, value: str | None) -> str | None:
        """An optional provider endpoint must not embed its credential."""
        if value is None:
            return None
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.fragment:
            raise ValueError("PUMP_RESEARCH_SECURITY_INDEXER_URL must be a valid http(s) URL")
        if parsed.username is not None or parsed.password is not None or parsed.query:
            raise ValueError(
                "PUMP_RESEARCH_SECURITY_INDEXER_URL must not contain credentials or query data"
            )
        return value.rstrip("/")

    @field_validator("pumpportal_api_key", mode="before")
    @classmethod
    def reject_blank_pumpportal_api_key(cls, value: object) -> object:
        """Never turn an empty environment assignment into an auth query value."""
        if value is None:
            return None
        secret = value.get_secret_value() if isinstance(value, SecretStr) else str(value)
        if not secret.strip():
            msg = "PUMP_RESEARCH_PUMPPORTAL_API_KEY must not be blank"
            raise ValueError(msg)
        return secret.strip()

    @field_validator("security_indexer_api_key", mode="before")
    @classmethod
    def reject_blank_security_indexer_api_key(cls, value: object) -> object:
        if value is None:
            return None
        secret = value.get_secret_value() if isinstance(value, SecretStr) else str(value)
        if not secret.strip():
            raise ValueError("PUMP_RESEARCH_SECURITY_INDEXER_API_KEY must not be blank")
        return secret.strip()


@lru_cache
def get_settings() -> Settings:
    """Return the cached process configuration."""
    return Settings()
