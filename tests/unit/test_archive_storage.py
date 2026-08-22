from __future__ import annotations

import asyncio
import hashlib
import io
from pathlib import Path
from typing import Any, BinaryIO, cast

import pytest

from pump_research.archival import ArchiveConflictError
from pump_research.archive_storage import S3CompatibleObjectStore


class MissingObjectError(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, dict[str, str]]] = {}
        self.put_calls = 0

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls += 1
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
