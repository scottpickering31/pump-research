"""Provider-neutral archive object storage and independently verified copying."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Protocol, cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.archival import ArchiveConflictError, verify_archive
from pump_research.archive_catalog import ArchiveCatalogError, record_copy_verification


@dataclass(frozen=True, slots=True)
class StoredObject:
    key: str
    content_length: int
    sha256: str | None


class ArchiveObjectStore(Protocol):
    """Small provider-neutral contract needed by the archive publisher."""

    provider_kind: str
    location: str

    async def put_file(self, *, key: str, source: Path, sha256: str) -> StoredObject: ...

    async def stat(self, key: str) -> StoredObject | None: ...

    async def sha256_readback(self, key: str) -> str: ...


class FilesystemObjectStore:
    """Local/mounted storage implementation with atomic, idempotent writes."""

    provider_kind = "filesystem"

    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()
        self.location = str(self.root)

    async def put_file(self, *, key: str, source: Path, sha256: str) -> StoredObject:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            existing = _sha256_file(target)
            if existing != sha256:
                raise ArchiveConflictError(f"object key already contains different content: {key}")
            return StoredObject(key, target.stat().st_size, existing)
        temporary = target.with_name(f".{target.name}.upload-{uuid.uuid4().hex}")
        try:
            with source.open("rb") as source_handle, temporary.open("xb") as target_handle:
                for block in iter(lambda: source_handle.read(1024 * 1024), b""):
                    target_handle.write(block)
                target_handle.flush()
                os.fsync(target_handle.fileno())
            if _sha256_file(temporary) != sha256:
                raise ArchiveConflictError(f"staged filesystem object checksum failed: {key}")
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink()
        return StoredObject(key, target.stat().st_size, sha256)

    async def stat(self, key: str) -> StoredObject | None:
        target = self._path(key)
        if not target.is_file():
            return None
        return StoredObject(key, target.stat().st_size, _sha256_file(target))

    async def sha256_readback(self, key: str) -> str:
        target = self._path(key)
        if not target.is_file():
            raise FileNotFoundError(f"archive object is missing: {key}")
        return _sha256_file(target)

    def _path(self, key: str) -> Path:
        pure = PurePosixPath(key)
        if pure.is_absolute() or ".." in pure.parts:
            raise ValueError("archive object key must be relative and cannot traverse")
        target = (self.root / Path(*pure.parts)).resolve()
        if not target.is_relative_to(self.root):
            raise ValueError("archive object key escapes storage root")
        return target


class S3ClientProtocol(Protocol):
    """Structural subset implemented by common S3-compatible SDK clients."""

    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


class S3CompatibleObjectStore:
    """SDK-neutral S3-compatible adapter; client credentials stay outside this object."""

    provider_kind = "s3_compatible"

    def __init__(self, *, client: S3ClientProtocol, bucket: str, prefix: str = "") -> None:
        if not bucket:
            raise ValueError("S3-compatible bucket is required")
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.location = f"s3://{bucket}/{self.prefix}".rstrip("/")

    async def put_file(self, *, key: str, source: Path, sha256: str) -> StoredObject:
        object_key = self._key(key)
        existing = await self.stat(key)
        if existing is not None:
            readback = await self.sha256_readback(key)
            if existing.content_length != source.stat().st_size or readback != sha256:
                raise ArchiveConflictError(
                    f"S3-compatible object key already contains different content: {object_key}"
                )
            return StoredObject(key, existing.content_length, readback)

        def upload() -> None:
            with source.open("rb") as handle:
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=object_key,
                    Body=handle,
                    ContentLength=source.stat().st_size,
                    ChecksumSHA256=base64.b64encode(bytes.fromhex(sha256)).decode("ascii"),
                    Metadata={"sha256": sha256},
                )

        await asyncio.to_thread(upload)
        stored = await self.stat(key)
        if stored is None or stored.content_length != source.stat().st_size:
            raise ArchiveConflictError(f"S3-compatible upload length verification failed: {key}")
        if await self.sha256_readback(key) != sha256:
            raise ArchiveConflictError(f"S3-compatible upload checksum verification failed: {key}")
        return StoredObject(key, stored.content_length, sha256)

    async def stat(self, key: str) -> StoredObject | None:
        object_key = self._key(key)

        def head() -> dict[str, Any] | None:
            try:
                return self.client.head_object(Bucket=self.bucket, Key=object_key)
            except Exception as error:
                response = getattr(error, "response", {})
                code = str(response.get("Error", {}).get("Code", ""))
                if code in {"404", "NoSuchKey", "NotFound"}:
                    return None
                raise

        response = await asyncio.to_thread(head)
        if response is None:
            return None
        metadata = cast(dict[str, str], response.get("Metadata", {}))
        return StoredObject(key, int(response["ContentLength"]), metadata.get("sha256"))

    async def sha256_readback(self, key: str) -> str:
        object_key = self._key(key)

        def download_hash() -> str:
            response = self.client.get_object(Bucket=self.bucket, Key=object_key)
            body = cast(BinaryIO, response["Body"])
            digest = hashlib.sha256()
            try:
                for block in iter(lambda: body.read(1024 * 1024), b""):
                    digest.update(block)
            finally:
                close = getattr(body, "close", None)
                if close is not None:
                    close()
            return digest.hexdigest()

        return await asyncio.to_thread(download_hash)

    def _key(self, key: str) -> str:
        pure = PurePosixPath(key)
        if pure.is_absolute() or ".." in pure.parts:
            raise ValueError("archive object key must be relative and cannot traverse")
        return "/".join(part for part in (self.prefix, pure.as_posix()) if part)


async def copy_archive(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    manifest_path: Path,
    store: ArchiveObjectStore,
    copy_role: str,
    independence_asserted: bool,
    independence_detail: str | None,
    fail_after_objects: int | None = None,
) -> dict[str, object]:
    """Idempotently copy and read-verify every archive object, publishing manifest last."""
    if copy_role == "secondary" and not independence_asserted:
        raise ArchiveCatalogError(
            "secondary archive copy requires an explicit independence assertion"
        )
    verification = await verify_archive(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if int(manifest.get("archive_schema_version", 0)) != 2:
        raise ValueError("production archive copy requires archive schema version 2")
    root = manifest_path.resolve().parents[4]
    data_paths = [root / entry["file"] for entry in manifest["entries"]]
    sidecar = manifest_path.with_name("manifest.sha256")
    ordered = [*data_paths, sidecar, manifest_path]
    objects: list[dict[str, object]] = []
    for index, source in enumerate(ordered, start=1):
        key = source.resolve().relative_to(root).as_posix()
        digest = _sha256_file(source)
        stored = await store.put_file(key=key, source=source, sha256=digest)
        if stored.content_length != source.stat().st_size:
            raise ArchiveConflictError(f"archive copy content length differs: {key}")
        if await store.sha256_readback(key) != digest:
            raise ArchiveConflictError(f"archive copy readback checksum differs: {key}")
        objects.append({"key": key, "bytes": stored.content_length, "sha256": digest})
        if fail_after_objects is not None and index >= fail_after_objects:
            raise RuntimeError("injected archive-copy interruption")
    if isinstance(store, FilesystemObjectStore):
        copied_manifest = store.root / manifest_path.resolve().relative_to(root)
        copied_verification = await verify_archive(copied_manifest)
        if copied_verification["manifest_sha256"] != verification["manifest_sha256"]:
            raise ArchiveConflictError("copied archive manifest identity differs after readback")
    await record_copy_verification(
        session_factory,
        scope_id=uuid.UUID(manifest["archive_scope_id"]),
        copy_role=copy_role,
        provider_kind=store.provider_kind,
        location=store.location,
        manifest_sha256=cast(str, verification["manifest_sha256"]),
        aggregate_file_sha256=cast(str, verification["aggregate_file_sha256"]),
        total_bytes=sum(cast(int, item["bytes"]) for item in objects),
        object_count=len(objects),
        independence_asserted=independence_asserted,
        independence_detail=independence_detail,
        verification_method="content-length + full SHA256 read-after-upload",
        detail={"objects": objects, "manifest_published_last": True},
    )
    return {
        "verified": True,
        "copy_role": copy_role,
        "provider_kind": store.provider_kind,
        "location": store.location,
        "object_count": len(objects),
        "bytes": sum(cast(int, item["bytes"]) for item in objects),
        "manifest_sha256": verification["manifest_sha256"],
        "aggregate_file_sha256": verification["aggregate_file_sha256"],
        "independence_asserted": independence_asserted,
    }


async def verify_filesystem_copy(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    source_manifest: Path,
    destination: Path,
    copy_role: str,
    independence_asserted: bool,
    independence_detail: str | None,
) -> dict[str, object]:
    """Re-read an existing filesystem copy and durably record only exact equality."""
    source_verification = await verify_archive(source_manifest)
    root = source_manifest.resolve().parents[4]
    copied_manifest = destination.expanduser().resolve() / source_manifest.resolve().relative_to(
        root
    )
    copied_verification = await verify_archive(copied_manifest)
    for key in ("manifest_sha256", "aggregate_file_sha256", "archive_identity_sha256"):
        if copied_verification[key] != source_verification[key]:
            raise ArchiveConflictError(f"filesystem copy {key} differs from canonical archive")
    manifest = json.loads(source_manifest.read_text(encoding="utf-8"))
    total_bytes = (
        copied_manifest.stat().st_size + copied_manifest.with_name("manifest.sha256").stat().st_size
    )
    total_bytes += sum(
        (destination.expanduser().resolve() / entry["file"]).stat().st_size
        for entry in manifest["entries"]
    )
    await record_copy_verification(
        session_factory,
        scope_id=uuid.UUID(manifest["archive_scope_id"]),
        copy_role=copy_role,
        provider_kind="filesystem",
        location=str(destination.expanduser().resolve()),
        manifest_sha256=cast(str, source_verification["manifest_sha256"]),
        aggregate_file_sha256=cast(str, source_verification["aggregate_file_sha256"]),
        total_bytes=total_bytes,
        object_count=len(manifest["entries"]) + 2,
        independence_asserted=independence_asserted,
        independence_detail=independence_detail,
        verification_method="full independent filesystem Parquet/checksum/DuckDB readback",
        detail=copied_verification,
    )
    return {
        "verified": True,
        "copy_role": copy_role,
        "provider_kind": "filesystem",
        "location": str(destination.expanduser().resolve()),
        "object_count": len(manifest["entries"]) + 2,
        "bytes": total_bytes,
        "manifest_sha256": source_verification["manifest_sha256"],
        "aggregate_file_sha256": source_verification["aggregate_file_sha256"],
        "independence_asserted": independence_asserted,
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
