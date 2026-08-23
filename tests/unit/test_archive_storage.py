from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import uuid
from pathlib import Path
from typing import Any, BinaryIO, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.archival import ArchiveConflictError
from pump_research.archive_catalog import ArchiveCatalogError
from pump_research.archive_storage import (
    READINESS_CONTENT,
    READINESS_OBJECT_KEY,
    S3ClientProtocol,
    S3CompatibleObjectStore,
    S3CompatibleStorageError,
    check_s3_compatible_readiness,
    copy_archive,
    create_s3_compatible_object_store,
    verify_object_store_copy,
)
from pump_research.config import Settings


class MissingObjectError(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str]]] = {}
        self.put_calls = 0
        self.put_requests: list[dict[str, Any]] = []

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls += 1
        self.put_requests.append({key: value for key, value in kwargs.items() if key != "Body"})
        body = cast(BinaryIO, kwargs["Body"])
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = (
            body.read(),
            cast(dict[str, str], kwargs["Metadata"]),
        )
        return {}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        try:
            body, metadata = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        except KeyError as error:
            raise MissingObjectError from error
        return {"ContentLength": len(body), "Metadata": metadata}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        try:
            body, _ = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        except KeyError as error:
            raise MissingObjectError from error
        return {"Body": io.BytesIO(body)}


@pytest.mark.asyncio
async def test_s3_compatible_upload_is_idempotent_and_read_verified(tmp_path: Path) -> None:
    source = tmp_path / "part.parquet"
    source.write_bytes(b"immutable archive object")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    client = FakeS3Client()
    store = S3CompatibleObjectStore(
        client=client,
        bucket="research",
        prefix="archive/v2",
    )
    first = await store.put_file(
        key="family=observations/part.parquet", source=source, sha256=digest
    )
    second = await store.put_file(
        key="family=observations/part.parquet", source=source, sha256=digest
    )
    assert first == second
    assert client.put_calls == 1
    assert await store.sha256_readback("family=observations/part.parquet") == digest


@pytest.mark.asyncio
async def test_s3_compatible_conflict_and_missing_object_fail_closed(tmp_path: Path) -> None:
    source = tmp_path / "part.parquet"
    source.write_bytes(b"canonical")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    client = FakeS3Client()
    store = S3CompatibleObjectStore(client=client, bucket="research")
    assert await store.stat("missing") is None
    await store.put_file(key="part", source=source, sha256=digest)
    client.objects[("research", "part")] = (b"corrupt remote", {"sha256": digest})
    with pytest.raises(ArchiveConflictError, match="different content"):
        await store.put_file(key="part", source=source, sha256=digest)


def test_s3_compatible_keys_cannot_escape_prefix(tmp_path: Path) -> None:
    store = S3CompatibleObjectStore(client=FakeS3Client(), bucket="research")
    source = tmp_path / "x"
    source.write_bytes(b"x")
    with pytest.raises(ValueError, match="cannot traverse"):
        asyncio.run(store.put_file(key="../escape", source=source, sha256="0" * 64))


def _s3_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://unused:unused@localhost/unused",
        "archive_s3_endpoint_url": "https://s3.eu-central-003.backblazeb2.invalid",
        "archive_s3_bucket": "private-research-archive",
        "archive_s3_prefix": "pump-research/archives/v2",
        "archive_s3_access_key_id": "key-id-must-stay-secret",
        "archive_s3_secret_access_key": "application-key-must-stay-secret",
        "archive_s3_region": "eu-central-003",
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def test_standard_sdk_receives_exact_endpoint_and_explicit_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    client = FakeS3Client()

    def fake_client(service: str, **kwargs: object) -> S3ClientProtocol:
        calls.append((service, kwargs))
        return client

    monkeypatch.setattr("boto3.client", fake_client)
    store = create_s3_compatible_object_store(_s3_settings())

    service, kwargs = calls[0]
    assert service == "s3"
    assert kwargs["endpoint_url"] == "https://s3.eu-central-003.backblazeb2.invalid"
    assert kwargs["aws_access_key_id"] == "key-id-must-stay-secret"
    assert kwargs["aws_secret_access_key"] == "application-key-must-stay-secret"
    assert kwargs["region_name"] == "eu-central-003"
    assert cast(Any, kwargs["config"]).signature_version == "s3v4"
    assert store.bucket == "private-research-archive"
    assert store.prefix == "pump-research/archives/v2"


@pytest.mark.asyncio
async def test_b2_compatible_put_headers_and_readiness_are_idempotent() -> None:
    client = FakeS3Client()
    store = S3CompatibleObjectStore(
        client=client,
        bucket="private-research-archive",
        prefix="pump-research/archives/v2",
    )
    first = await check_s3_compatible_readiness(store)
    second = await check_s3_compatible_readiness(store)

    assert first == second
    assert first["status"] == "PASS"
    assert first["delete_attempted"] is False
    assert client.put_calls == 1
    body, metadata = client.objects[
        ("private-research-archive", f"pump-research/archives/v2/{READINESS_OBJECT_KEY}")
    ]
    assert body == READINESS_CONTENT
    digest = hashlib.sha256(READINESS_CONTENT).hexdigest()
    assert metadata == {"sha256": digest}
    assert client.put_requests[0]["ContentLength"] == len(READINESS_CONTENT)
    assert client.put_requests[0]["ChecksumSHA256"] == base64.b64encode(
        bytes.fromhex(digest)
    ).decode("ascii")


@pytest.mark.asyncio
async def test_readiness_conflicting_existing_object_fails_closed() -> None:
    client = FakeS3Client()
    client.objects[("research", READINESS_OBJECT_KEY)] = (
        b"conflicting readiness evidence",
        {"sha256": hashlib.sha256(READINESS_CONTENT).hexdigest()},
    )
    store = S3CompatibleObjectStore(client=client, bucket="research")
    with pytest.raises(ArchiveConflictError, match="different content"):
        await check_s3_compatible_readiness(store)


@pytest.mark.asyncio
async def test_wrong_credentials_are_reported_without_exception_secret() -> None:
    class RejectedClient(FakeS3Client):
        def head_object(self, **kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise RuntimeError("provider rejected application-key-must-stay-secret")

    store = S3CompatibleObjectStore(client=RejectedClient(), bucket="research")
    with pytest.raises(S3CompatibleStorageError) as captured:
        await store.stat("probe")
    assert "application-key-must-stay-secret" not in str(captured.value)
    assert str(captured.value) == "S3-compatible HEAD failed for object key probe"


def test_sdk_initialization_error_cannot_echo_credentials() -> None:
    def rejected_factory(service: str, **kwargs: object) -> S3ClientProtocol:
        del service
        raise RuntimeError(
            f"bad {kwargs['aws_access_key_id']} {kwargs['aws_secret_access_key']}"
        )

    with pytest.raises(S3CompatibleStorageError) as captured:
        create_s3_compatible_object_store(_s3_settings(), client_factory=rejected_factory)
    assert "key-id-must-stay-secret" not in str(captured.value)
    assert "application-key-must-stay-secret" not in str(captured.value)


@pytest.mark.asyncio
async def test_existing_s3_copy_is_fully_read_and_independence_is_required(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "canonical"
    data = root / "schema=v2/family=observations/part-00000.parquet"
    manifest = (
        root
        / "schema=v2/manifests/epoch=2"
        / f"scope={'a' * 64}"
        / "manifest.json"
    )
    data.parent.mkdir(parents=True)
    manifest.parent.mkdir(parents=True)
    data.write_bytes(b"parquet evidence")
    scope_id = uuid.uuid4()
    manifest.write_text(
        json.dumps(
            {
                "archive_schema_version": 2,
                "archive_scope_id": str(scope_id),
                "entries": [{"file": data.relative_to(root).as_posix()}],
            }
        ),
        encoding="utf-8",
    )
    manifest.with_name("manifest.sha256").write_text("manifest sidecar", encoding="ascii")
    verification: dict[str, object] = {
        "manifest_sha256": "b" * 64,
        "aggregate_file_sha256": "c" * 64,
    }
    recorded: list[dict[str, object]] = []

    async def fake_verify(path: Path) -> dict[str, object]:
        assert path == manifest
        return verification

    async def fake_record(
        session_factory: async_sessionmaker[AsyncSession], **kwargs: object
    ) -> None:
        del session_factory
        recorded.append(kwargs)

    monkeypatch.setattr("pump_research.archive_storage.verify_archive", fake_verify)
    monkeypatch.setattr("pump_research.archive_storage.record_copy_verification", fake_record)
    session_factory = cast(async_sessionmaker[AsyncSession], object())
    client = FakeS3Client()
    store = S3CompatibleObjectStore(client=client, bucket="research", prefix="archive/v2")

    with pytest.raises(ArchiveCatalogError, match="independence assertion"):
        await copy_archive(
            session_factory,
            manifest_path=manifest,
            store=store,
            copy_role="secondary",
            independence_asserted=False,
            independence_detail=None,
        )
    await copy_archive(
        session_factory,
        manifest_path=manifest,
        store=store,
        copy_role="secondary",
        independence_asserted=True,
        independence_detail="separate provider and physical device",
    )
    result = await verify_object_store_copy(
        session_factory,
        source_manifest=manifest,
        store=store,
        copy_role="secondary",
        independence_asserted=True,
        independence_detail="separate provider and physical device",
    )
    assert result["verified"] is True
    assert client.put_calls == 3
    assert len(recorded) == 2
    assert recorded[-1]["independence_asserted"] is True


def test_archive_object_store_contract_has_no_delete_path() -> None:
    assert "delete_object" not in S3ClientProtocol.__dict__
    assert not hasattr(S3CompatibleObjectStore, "delete")
    assert not hasattr(S3CompatibleObjectStore, "delete_object")
