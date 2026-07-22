from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .models import canonical_json, require_utc_timestamp


class ResumeSnapshotError(RuntimeError):
    """A persisted NEEDS_APPROVAL snapshot is missing, forged, or stale."""


@dataclass(frozen=True)
class ApprovalPauseSnapshot:
    migration_id: str
    participant_id: str
    owner_urn: str
    old_field: str
    new_field: str
    impact_fingerprint: str
    repository_base_shas: Mapping[str, str]
    paused_at: str
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported approval pause snapshot schema")
        if not self.owner_urn.startswith("urn:li:"):
            raise ValueError("approval pause owner must be a DataHub URN")
        if not self.migration_id or not self.participant_id:
            raise ValueError("approval pause identity is required")
        if not self.old_field or not self.new_field:
            raise ValueError("approval pause field mapping is required")
        if not re.fullmatch(r"[0-9a-f]{64}", self.impact_fingerprint):
            raise ValueError("approval pause impact fingerprint must be SHA-256")
        if not self.repository_base_shas:
            raise ValueError("approval pause repository bases are required")
        if any(
            not name or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", sha)
            for name, sha in self.repository_base_shas.items()
        ):
            raise ValueError("approval pause repository base SHA is invalid")
        require_utc_timestamp(self.paused_at, "paused_at")

    def to_dict(self) -> dict[str, Any]:
        return {
            "impact_fingerprint": self.impact_fingerprint,
            "migration_id": self.migration_id,
            "new_field": self.new_field,
            "old_field": self.old_field,
            "owner_urn": self.owner_urn,
            "participant_id": self.participant_id,
            "paused_at": self.paused_at,
            "repository_base_shas": dict(sorted(self.repository_base_shas.items())),
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ApprovalPauseSnapshot:
        bases = value.get("repository_base_shas")
        if not isinstance(bases, Mapping):
            raise ResumeSnapshotError("approval pause repository bases are invalid")
        try:
            return cls(
                migration_id=str(value["migration_id"]),
                participant_id=str(value["participant_id"]),
                owner_urn=str(value["owner_urn"]),
                old_field=str(value["old_field"]),
                new_field=str(value["new_field"]),
                impact_fingerprint=str(value["impact_fingerprint"]),
                repository_base_shas={
                    str(name): str(sha) for name, sha in bases.items()
                },
                paused_at=str(value["paused_at"]),
                schema_version=int(value.get("schema_version", 0)),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ResumeSnapshotError(
                f"approval pause snapshot is invalid: {error}"
            ) from error


def resume_key_from_environment(environment_name: str) -> bytes:
    key = os.getenv(environment_name, "").encode("utf-8")
    if len(key) < 32:
        raise ResumeSnapshotError(
            f"{environment_name} must contain at least 32 bytes for signed resume state"
        )
    return key


def write_signed_pause_snapshot(
    path: Path,
    snapshot: ApprovalPauseSnapshot,
    *,
    key: bytes,
) -> None:
    if len(key) < 32:
        raise ValueError("resume snapshot HMAC key must contain at least 32 bytes")
    payload = snapshot.to_dict()
    payload_json = canonical_json(payload)
    signature = hmac.new(key, payload_json.encode("utf-8"), hashlib.sha256).hexdigest()
    envelope = {
        "algorithm": "HMAC-SHA256",
        "payload": payload,
        "signature": signature,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(envelope, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def read_signed_pause_snapshot(path: Path, *, key: bytes) -> ApprovalPauseSnapshot:
    if len(key) < 32:
        raise ValueError("resume snapshot HMAC key must contain at least 32 bytes")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as error:
        raise ResumeSnapshotError(f"cannot read approval pause snapshot: {error}") from error
    if not isinstance(raw, Mapping) or raw.get("algorithm") != "HMAC-SHA256":
        raise ResumeSnapshotError("approval pause snapshot envelope is invalid")
    payload = raw.get("payload")
    signature = raw.get("signature")
    if not isinstance(payload, Mapping) or not isinstance(signature, str):
        raise ResumeSnapshotError("approval pause snapshot signature is missing")
    expected = hmac.new(
        key,
        canonical_json(payload).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise ResumeSnapshotError("approval pause snapshot signature is invalid")
    return ApprovalPauseSnapshot.from_dict(payload)
