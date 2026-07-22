from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, fields, is_dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from .models import utc_now


class EvidenceError(RuntimeError):
    """Evidence could not be written or verified without ambiguity."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        default=str,
    ) + "\n"


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {
            field.name: _jsonable(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Path):
        return value.as_posix()
    return value


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe_relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise EvidenceError(f"unsafe evidence path: {value!r}")
    return path.as_posix()


@dataclass(frozen=True)
class EvidenceFile:
    path: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EvidenceManifest:
    migration_id: str
    generated_at: str
    files: tuple[EvidenceFile, ...]
    aggregate_sha256: str
    schema_version: int = 1
    manifest_self_hash_excluded: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "aggregate_sha256": self.aggregate_sha256,
            "files": [item.to_dict() for item in self.files],
            "generated_at": self.generated_at,
            "manifest_self_hash_excluded": True,
            "migration_id": self.migration_id,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> EvidenceManifest:
        return cls(
            migration_id=str(value["migration_id"]),
            generated_at=str(value["generated_at"]),
            files=tuple(EvidenceFile(**item) for item in value["files"]),
            aggregate_sha256=str(value["aggregate_sha256"]),
            schema_version=int(value.get("schema_version", 1)),
            manifest_self_hash_excluded=bool(
                value.get("manifest_self_hash_excluded", False)
            ),
        )


class EvidenceRecorder:
    """Writes immutable-style evidence artifacts and hashes every artifact file."""

    MANIFEST_NAME = "manifest.json"

    def __init__(self, root: str | Path, migration_id: str) -> None:
        if not migration_id.strip():
            raise ValueError("migration_id is required")
        self.root = Path(root).resolve()
        self.migration_id = migration_id
        self.root.mkdir(parents=True, exist_ok=True)

    def _target(self, relative_path: str) -> Path:
        normalized = _safe_relative_path(relative_path)
        if normalized == self.MANIFEST_NAME:
            raise EvidenceError("manifest.json is managed by build_manifest")
        candidate = self.root / normalized
        parents_to_check: list[Path] = []
        current = candidate
        while current != self.root:
            parents_to_check.append(current)
            current = current.parent
        if any(path.is_symlink() for path in parents_to_check):
            raise EvidenceError("evidence paths may not traverse symbolic links")
        target = candidate.resolve()
        if not target.is_relative_to(self.root):
            raise EvidenceError("evidence path escaped the bundle root")
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    @staticmethod
    def _atomic_write(target: Path, payload: bytes) -> None:
        temporary = target.with_name(f".{target.name}.lineagetx.tmp")
        if temporary.exists():
            temporary.unlink()
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(target)

    def write_json(self, relative_path: str, value: Any) -> EvidenceFile:
        target = self._target(relative_path)
        payload = _canonical_json(_jsonable(value)).encode("utf-8")
        self._atomic_write(target, payload)
        return EvidenceFile(
            path=target.relative_to(self.root).as_posix(),
            sha256=_sha256_bytes(payload),
            size_bytes=len(payload),
        )

    def write_text(self, relative_path: str, value: str) -> EvidenceFile:
        target = self._target(relative_path)
        payload = value.encode("utf-8")
        self._atomic_write(target, payload)
        return EvidenceFile(
            path=target.relative_to(self.root).as_posix(),
            sha256=_sha256_bytes(payload),
            size_bytes=len(payload),
        )

    def capture_state(self, store: Any, stage: str) -> EvidenceFile:
        migration = store.get_migration(self.migration_id)
        return self.write_json(
            f"state/{_safe_relative_path(stage)}.json",
            {
                "migration": migration.to_dict(),
                "participants": [
                    item.to_dict() for item in store.list_participants(self.migration_id)
                ],
                "approvals": [
                    item.to_dict() for item in store.list_approvals(self.migration_id)
                ],
                "events": [
                    item.to_dict() for item in store.list_events(self.migration_id)
                ],
            },
        )

    def _artifact_files(self) -> tuple[EvidenceFile, ...]:
        artifacts: list[EvidenceFile] = []
        manifest = self.root / self.MANIFEST_NAME
        for path in sorted(self.root.rglob("*")):
            if path == manifest:
                continue
            if path.is_symlink():
                raise EvidenceError(f"evidence bundle contains a symlink: {path}")
            if path.is_dir():
                continue
            if not path.is_file():
                raise EvidenceError(f"evidence bundle contains a non-file: {path}")
            relative = path.relative_to(self.root).as_posix()
            if relative.startswith(".") and relative.endswith(".lineagetx.tmp"):
                raise EvidenceError("evidence bundle contains an incomplete atomic write")
            payload = path.read_bytes()
            artifacts.append(
                EvidenceFile(
                    path=relative,
                    sha256=_sha256_bytes(payload),
                    size_bytes=len(payload),
                )
            )
        return tuple(sorted(artifacts, key=lambda artifact: artifact.path))

    @staticmethod
    def _aggregate(files: tuple[EvidenceFile, ...]) -> str:
        serialized = json.dumps(
            [item.to_dict() for item in files],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return _sha256_bytes(serialized)

    def build_manifest(self, *, generated_at: str | None = None) -> EvidenceManifest:
        files = self._artifact_files()
        manifest = EvidenceManifest(
            migration_id=self.migration_id,
            generated_at=generated_at or utc_now(),
            files=files,
            aggregate_sha256=self._aggregate(files),
        )
        self._atomic_write(
            self.root / self.MANIFEST_NAME,
            _canonical_json(manifest.to_dict()).encode("utf-8"),
        )
        return manifest

    def verify_manifest(self) -> EvidenceManifest:
        manifest_path = self.root / self.MANIFEST_NAME
        if not manifest_path.is_file() or manifest_path.is_symlink():
            raise EvidenceError("evidence manifest is missing or unsafe")
        try:
            value = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest = EvidenceManifest.from_dict(value)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            raise EvidenceError(f"invalid evidence manifest: {error}") from error
        if manifest.migration_id != self.migration_id:
            raise EvidenceError("manifest belongs to another migration")
        if not manifest.manifest_self_hash_excluded:
            raise EvidenceError("manifest must explicitly document its self-hash exclusion")
        actual = self._artifact_files()
        if actual != manifest.files:
            expected_by_path = {item.path: item for item in manifest.files}
            actual_by_path = {item.path: item for item in actual}
            if set(expected_by_path) != set(actual_by_path):
                raise EvidenceError("manifest file set does not match the evidence bundle")
            raise EvidenceError("at least one evidence SHA-256 or size does not match")
        if self._aggregate(actual) != manifest.aggregate_sha256:
            raise EvidenceError("aggregate evidence SHA-256 does not match")
        return manifest
