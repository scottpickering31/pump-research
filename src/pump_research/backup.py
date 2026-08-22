"""Verification and durable status for independently stored backup artifacts."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.archival import verify_archive
from pump_research.epochs import get_epoch_status
from pump_research.persistence.models import BackupVerification, CollectionEpoch


async def verify_backup(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    epoch_number: int,
    path: Path,
    independent_copy: bool,
    project_root: Path,
) -> dict[str, object]:
    """Read and validate an artifact before recording immutable backup evidence."""
    resolved = path.expanduser().resolve(strict=True)
    project = project_root.resolve()
    if independent_copy and resolved.is_relative_to(project):
        raise ValueError("an independent backup copy must be outside the project workspace")
    if resolved.name.endswith(".manifest.json"):
        detail = await verify_archive(resolved)
        kind = "verified_archive_manifest"
        method = "manifest + Parquet SHA256/content/readback"
    elif resolved.suffix == ".dump":
        pg_restore = shutil.which("pg_restore")
        if pg_restore is not None:
            text_result = subprocess.run(
                [pg_restore, "--list", str(resolved)],
                check=False,
                capture_output=True,
                text=True,
            )
            method = "pg_restore --list"
            catalog_output = text_result.stdout
            return_code = text_result.returncode
        else:
            docker = shutil.which("docker")
            compose_file = project_root / "compose.yaml"
            if docker is None or not compose_file.is_file():
                raise RuntimeError(
                    "pg_restore or Docker Compose is required to verify a custom PostgreSQL dump"
                )
            with resolved.open("rb") as dump_handle:
                byte_result = subprocess.run(
                    [
                        docker,
                        "compose",
                        "-f",
                        str(compose_file),
                        "exec",
                        "-T",
                        "postgres",
                        "pg_restore",
                        "--list",
                    ],
                    cwd=project_root,
                    stdin=dump_handle,
                    check=False,
                    capture_output=True,
                    text=False,
                )
            catalog_output = byte_result.stdout.decode(errors="replace")
            return_code = byte_result.returncode
            method = "Docker Compose pg_restore --list"
        if return_code != 0 or not catalog_output.strip():
            raise ValueError("pg_restore could not read the backup catalog")
        detail = {"catalog_lines": len(catalog_output.splitlines())}
        kind = "postgres_custom_dump"
    elif resolved.suffix == ".sql":
        size = resolved.stat().st_size
        if size == 0:
            raise ValueError("SQL backup is empty")
        with resolved.open("rb") as file_handle:
            header = file_handle.read(4096)
            file_handle.seek(max(0, size - 4096))
            trailer = file_handle.read()
        if b"PostgreSQL database dump" not in header:
            raise ValueError("SQL file does not contain a PostgreSQL dump header")
        if b"PostgreSQL database dump complete" not in trailer:
            raise ValueError("SQL file does not contain a completed pg_dump trailer")
        header.decode("utf-8")
        trailer.decode("utf-8")
        detail = {"header_and_completion_marker_read": True}
        kind = "postgres_plain_sql_dump"
        method = "pg_dump header/trailer UTF-8 readback"
    else:
        raise ValueError("backup must be a .dump, .sql, or verified archive manifest")

    digest = _sha256_file(resolved)
    size = resolved.stat().st_size
    verified_at = datetime.now(UTC)
    async with session_factory() as session, session.begin():
        epoch = await get_epoch_status(session, epoch_number)
        await session.execute(
            insert(BackupVerification)
            .values(
                collection_epoch_id=epoch.id,
                artifact_path=str(resolved),
                artifact_kind=kind,
                artifact_bytes=size,
                artifact_sha256=digest,
                verification_method=method,
                independent_copy=independent_copy,
                verified_at=verified_at,
                detail=detail,
            )
            .on_conflict_do_nothing(constraint="uq_backup_verifications_artifact")
        )
    return {
        "verified": True,
        "epoch": epoch_number,
        "path": str(resolved),
        "kind": kind,
        "bytes": size,
        "sha256": digest,
        "verification_method": method,
        "independent_copy": independent_copy,
        "verified_at": verified_at.isoformat(),
        "detail": detail,
    }


async def backup_status(
    session_factory: async_sessionmaker[AsyncSession], *, epoch_number: int | None = None
) -> dict[str, object]:
    """Report only artifacts whose bytes were actually read and verified."""
    async with session_factory() as session:
        if epoch_number is None:
            epoch_number = await session.scalar(
                select(CollectionEpoch.epoch_number)
                .order_by(CollectionEpoch.epoch_number.desc())
                .limit(1)
            )
        if epoch_number is None:
            return {"epoch": None, "verified_backups": [], "independent_backup_present": False}
        epoch = await get_epoch_status(session, int(epoch_number))
        rows = list(
            (
                await session.execute(
                    select(BackupVerification)
                    .where(BackupVerification.collection_epoch_id == epoch.id)
                    .order_by(BackupVerification.verified_at.desc())
                )
            ).scalars()
        )
    return {
        "epoch": epoch.epoch_number,
        "epoch_id": str(epoch.id),
        "independent_backup_present": any(row.independent_copy for row in rows),
        "verified_backups": [
            {
                "path": row.artifact_path,
                "kind": row.artifact_kind,
                "bytes": row.artifact_bytes,
                "sha256": row.artifact_sha256,
                "verification_method": row.verification_method,
                "independent_copy": row.independent_copy,
                "verified_at": row.verified_at.isoformat(),
            }
            for row in rows
        ],
        "claim": (
            "A backup is reported only after read verification; filesystem/device "
            "independence remains an operator assertion."
        ),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for block in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
