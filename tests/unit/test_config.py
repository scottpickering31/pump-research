from __future__ import annotations

import pytest
from pydantic import ValidationError

from pump_research.config import ArchiveS3ConfigurationError, Settings
from pump_research.epochs import epoch_configuration


def test_settings_accept_asyncpg_postgres_url() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5432/pump_research"
    )

    assert settings.database_connect_timeout_seconds == 5.0
    assert settings.log_level == "INFO"
    assert settings.scheduler_new_initial_interval_seconds == 15
    assert settings.scheduler_new_initial_duration_seconds == 120
    assert settings.scheduler_new_interval_seconds == 30
    assert settings.scheduler_fading_interval_seconds == 120
    assert settings.scheduler_capacity_headroom_ratio == 0.20


def test_settings_reject_non_asyncpg_url() -> None:
    with pytest.raises(ValidationError, match="postgresql\\+asyncpg"):
        Settings(database_url="postgresql://researcher:password@localhost:5432/pump_research")


def test_archive_s3_configuration_is_complete_validated_and_secret() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5432/pump_research",
        archive_s3_endpoint_url="https://s3.eu-central-003.example.invalid",
        archive_s3_bucket="research-archive",
        archive_s3_prefix="pump-research/archives/v2",
        archive_s3_access_key_id="private-key-id",
        archive_s3_secret_access_key="private-application-key",
        archive_s3_region="eu-central-003",
    )

    configuration = settings.require_archive_s3_configuration()
    assert configuration.endpoint_url == "https://s3.eu-central-003.example.invalid"
    assert configuration.bucket == "research-archive"
    assert configuration.prefix == "pump-research/archives/v2"
    assert str(configuration.access_key_id) == "**********"
    assert str(configuration.secret_access_key) == "**********"


def test_archive_s3_missing_configuration_fails_closed() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5432/pump_research"
    )
    with pytest.raises(ArchiveS3ConfigurationError) as captured:
        settings.require_archive_s3_configuration()
    message = str(captured.value)
    assert "PUMP_RESEARCH_ARCHIVE_S3_ENDPOINT_URL" in message
    assert "PUMP_RESEARCH_ARCHIVE_S3_SECRET_ACCESS_KEY" in message


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (
            "archive_s3_endpoint_url",
            "https://s3.example.invalid?applicationKey=query-secret-value",
            "path, query, or fragment",
        ),
        ("archive_s3_endpoint_url", "http://s3.example.invalid", "https://"),
        (
            "archive_s3_endpoint_url",
            "https://s3.example.invalid/credential-like-path",
            "path, query, or fragment",
        ),
        ("archive_s3_bucket", "unsafe/bucket", "bucket name"),
        ("archive_s3_prefix", "../escape", "relative object prefix"),
        ("archive_s3_prefix", "archive//v2", "relative object prefix"),
    ],
)
def test_archive_s3_endpoint_bucket_and_prefix_validation_hides_input(
    field: str, value: str, message: str
) -> None:
    with pytest.raises(ValidationError, match=message) as captured:
        Settings.model_validate(
            {
                "database_url": (
                    "postgresql+asyncpg://researcher:password@localhost:5432/pump_research"
                ),
                field: value,
            }
        )
    assert "query-secret-value" not in str(captured.value)


def test_archive_s3_secrets_never_enter_epoch_configuration_snapshot() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5432/pump_research",
        archive_s3_endpoint_url="https://s3.example.invalid",
        archive_s3_bucket="research-archive",
        archive_s3_prefix="archives/v2",
        archive_s3_access_key_id="snapshot-key-id",
        archive_s3_secret_access_key="snapshot-secret-key",
        archive_s3_region="region-1",
    )
    _, snapshot = epoch_configuration(settings, 3)
    rendered = str(snapshot)
    assert "snapshot-key-id" not in rendered
    assert "snapshot-secret-key" not in rendered
    assert "archive_s3_access_key_id" not in snapshot["settings"]  # type: ignore[operator]
    assert "archive_s3_secret_access_key" not in snapshot["settings"]  # type: ignore[operator]
