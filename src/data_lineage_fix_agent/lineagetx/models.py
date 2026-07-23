from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Mapping
from urllib.parse import urlsplit


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")
_COMMIT_SHA = re.compile(r"^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$")


def utc_now() -> str:
    """Return a stable, timezone-explicit timestamp suitable for persistence."""

    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def canonical_json(value: Mapping[str, Any]) -> str:
    """Serialize records deterministically for IDs, receipts, and SQLite storage."""

    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def require_utc_timestamp(value: str, field_name: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError(f"{field_name} must include the UTC timezone")


def require_https_url(value: str, field_name: str) -> None:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            f"{field_name} must be an absolute HTTPS URL without "
            "userinfo, query, or fragment"
        )


def is_commit_sha(value: str) -> bool:
    return _COMMIT_SHA.fullmatch(value) is not None


class MigrationStatus(str, Enum):
    DETECTED = "DETECTED"
    PREPARING = "PREPARING"
    NEEDS_APPROVAL = "NEEDS_APPROVAL"
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class ParticipantKind(str, Enum):
    DBT_SQL = "DBT_SQL"
    AIRFLOW_MAPPING = "AIRFLOW_MAPPING"
    SEMANTIC_APPROVAL = "SEMANTIC_APPROVAL"


class ParticipantStatus(str, Enum):
    DISCOVERED = "DISCOVERED"
    PREPARING = "PREPARING"
    NEEDS_APPROVAL = "NEEDS_APPROVAL"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


class RolloutPhase(str, Enum):
    CONTRACT = "CONTRACT"


class CoordinationReceiptKind(str, Enum):
    CANDIDATE_COMMIT = "CANDIDATE_COMMIT"
    COORDINATED_PR = "COORDINATED_PR"
    PRODUCER_GATE_RELEASED = "PRODUCER_GATE_RELEASED"


ALLOWED_MIGRATION_TRANSITIONS: dict[MigrationStatus, frozenset[MigrationStatus]] = {
    MigrationStatus.DETECTED: frozenset(
        {MigrationStatus.PREPARING, MigrationStatus.ABORTED}
    ),
    MigrationStatus.PREPARING: frozenset(
        {
            MigrationStatus.NEEDS_APPROVAL,
            MigrationStatus.PREPARED,
            MigrationStatus.ABORTED,
        }
    ),
    MigrationStatus.NEEDS_APPROVAL: frozenset(
        {MigrationStatus.PREPARED, MigrationStatus.ABORTED}
    ),
    MigrationStatus.PREPARED: frozenset(
        {MigrationStatus.COMMITTED, MigrationStatus.ABORTED}
    ),
    MigrationStatus.COMMITTED: frozenset(),
    MigrationStatus.ABORTED: frozenset(),
}


ALLOWED_PARTICIPANT_TRANSITIONS: dict[
    ParticipantStatus, frozenset[ParticipantStatus]
] = {
    ParticipantStatus.DISCOVERED: frozenset(
        {
            ParticipantStatus.PREPARING,
            ParticipantStatus.FAILED,
            ParticipantStatus.ABORTED,
        }
    ),
    ParticipantStatus.PREPARING: frozenset(
        {
            ParticipantStatus.NEEDS_APPROVAL,
            ParticipantStatus.VERIFIED,
            ParticipantStatus.FAILED,
            ParticipantStatus.ABORTED,
        }
    ),
    ParticipantStatus.NEEDS_APPROVAL: frozenset(
        {
            ParticipantStatus.PREPARING,
            ParticipantStatus.FAILED,
            ParticipantStatus.ABORTED,
        }
    ),
    ParticipantStatus.VERIFIED: frozenset(
        {ParticipantStatus.COMMITTED, ParticipantStatus.ABORTED}
    ),
    ParticipantStatus.FAILED: frozenset(
        {ParticipantStatus.PREPARING, ParticipantStatus.ABORTED}
    ),
    ParticipantStatus.COMMITTED: frozenset(),
    ParticipantStatus.ABORTED: frozenset(),
}


def can_transition(
    current: MigrationStatus,
    target: MigrationStatus,
) -> bool:
    return target in ALLOWED_MIGRATION_TRANSITIONS[current]


def can_transition_participant(
    current: ParticipantStatus,
    target: ParticipantStatus,
) -> bool:
    return target in ALLOWED_PARTICIPANT_TRANSITIONS[current]


@dataclass(frozen=True)
class ChangeIntent:
    """Immutable identity of one producer schema contract change."""

    migration_id: str
    producer_repository: str
    producer_pr_number: int
    producer_head_sha: str
    source_asset_urn: str
    old_field: str
    new_field: str
    created_at: str
    producer_base_sha: str
    contract_schema_fingerprint: str
    producer_pr_url: str = ""
    rollout_phase: RolloutPhase = RolloutPhase.CONTRACT
    intent_sha256: str = ""

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.migration_id):
            raise ValueError("migration_id must be a non-empty portable identifier")
        if not self.producer_repository.strip():
            raise ValueError("producer_repository is required")
        if self.producer_pr_number < 1:
            raise ValueError("producer_pr_number must be positive")
        if not self.producer_head_sha.strip():
            raise ValueError("producer_head_sha is required")
        if not self.producer_base_sha.strip():
            raise ValueError("producer_base_sha is required")
        if self.producer_base_sha == self.producer_head_sha:
            raise ValueError("producer_base_sha and producer_head_sha must differ")
        if not self.contract_schema_fingerprint.strip():
            raise ValueError("contract_schema_fingerprint is required")
        if not self.source_asset_urn.strip():
            raise ValueError("source_asset_urn is required")
        if not self.old_field.strip() or not self.new_field.strip():
            raise ValueError("old_field and new_field are required")
        if self.old_field == self.new_field:
            raise ValueError("old_field and new_field must differ")
        if not self.created_at.strip():
            raise ValueError("created_at is required")
        require_utc_timestamp(self.created_at, "created_at")
        if self.producer_pr_url:
            require_https_url(self.producer_pr_url, "producer_pr_url")
        identity_sha = self.derive_intent_sha256(
            producer_repository=self.producer_repository,
            producer_pr_number=self.producer_pr_number,
            producer_base_sha=self.producer_base_sha,
            producer_head_sha=self.producer_head_sha,
            source_asset_urn=self.source_asset_urn,
            old_field=self.old_field,
            new_field=self.new_field,
            rollout_phase=self.rollout_phase,
            contract_schema_fingerprint=self.contract_schema_fingerprint,
        )
        if self.intent_sha256 and self.intent_sha256 != identity_sha:
            raise ValueError("intent_sha256 does not match the change identity")
        object.__setattr__(self, "intent_sha256", identity_sha)
        expected_migration_id = f"ltx-{identity_sha[:24]}"
        if self.migration_id != expected_migration_id:
            raise ValueError("migration_id does not match the stable change identity")

    @staticmethod
    def derive_intent_sha256(
        *,
        producer_repository: str,
        producer_pr_number: int,
        producer_base_sha: str,
        producer_head_sha: str,
        source_asset_urn: str,
        old_field: str,
        new_field: str,
        contract_schema_fingerprint: str,
        rollout_phase: RolloutPhase = RolloutPhase.CONTRACT,
    ) -> str:
        identity = {
            "contract_schema_fingerprint": contract_schema_fingerprint,
            "new_field": new_field,
            "old_field": old_field,
            "producer_base_sha": producer_base_sha,
            "producer_head_sha": producer_head_sha,
            "producer_pr_number": producer_pr_number,
            "producer_repository": producer_repository,
            "rollout_phase": rollout_phase.value,
            "source_asset_urn": source_asset_urn,
            "version": 1,
        }
        return hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()

    @classmethod
    def create(
        cls,
        *,
        producer_repository: str,
        producer_pr_number: int,
        producer_base_sha: str,
        producer_head_sha: str,
        source_asset_urn: str,
        old_field: str,
        new_field: str,
        contract_schema_fingerprint: str,
        created_at: str | None = None,
        producer_pr_url: str = "",
        rollout_phase: RolloutPhase = RolloutPhase.CONTRACT,
    ) -> ChangeIntent:
        digest = cls.derive_intent_sha256(
            producer_repository=producer_repository,
            producer_pr_number=producer_pr_number,
            producer_base_sha=producer_base_sha,
            producer_head_sha=producer_head_sha,
            source_asset_urn=source_asset_urn,
            old_field=old_field,
            new_field=new_field,
            rollout_phase=rollout_phase,
            contract_schema_fingerprint=contract_schema_fingerprint,
        )
        return cls(
            migration_id=f"ltx-{digest[:24]}",
            producer_repository=producer_repository,
            producer_pr_number=producer_pr_number,
            producer_base_sha=producer_base_sha,
            producer_head_sha=producer_head_sha,
            source_asset_urn=source_asset_urn,
            old_field=old_field,
            new_field=new_field,
            contract_schema_fingerprint=contract_schema_fingerprint,
            created_at=created_at or utc_now(),
            producer_pr_url=producer_pr_url,
            rollout_phase=rollout_phase,
            intent_sha256=digest,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "created_at": self.created_at,
            "migration_id": self.migration_id,
            "intent_sha256": self.intent_sha256,
            "new_field": self.new_field,
            "old_field": self.old_field,
            "producer_base_sha": self.producer_base_sha,
            "producer_head_sha": self.producer_head_sha,
            "producer_pr_number": self.producer_pr_number,
            "producer_pr_url": self.producer_pr_url,
            "producer_repository": self.producer_repository,
            "rollout_phase": self.rollout_phase.value,
            "source_asset_urn": self.source_asset_urn,
            "contract_schema_fingerprint": self.contract_schema_fingerprint,
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ChangeIntent:
        return cls(
            migration_id=str(value["migration_id"]),
            producer_repository=str(value["producer_repository"]),
            producer_pr_number=int(value["producer_pr_number"]),
            producer_base_sha=str(value["producer_base_sha"]),
            producer_head_sha=str(value["producer_head_sha"]),
            source_asset_urn=str(value["source_asset_urn"]),
            old_field=str(value["old_field"]),
            new_field=str(value["new_field"]),
            contract_schema_fingerprint=str(value["contract_schema_fingerprint"]),
            created_at=str(value["created_at"]),
            producer_pr_url=str(value.get("producer_pr_url", "")),
            rollout_phase=RolloutPhase(
                str(value.get("rollout_phase", RolloutPhase.CONTRACT.value))
            ),
            intent_sha256=str(value.get("intent_sha256", "")),
        )

    @classmethod
    def from_json(cls, value: str) -> ChangeIntent:
        return cls.from_dict(json.loads(value))


@dataclass(frozen=True)
class Participant:
    """One downstream consumer taking part in a LineageTX migration."""

    participant_id: str
    migration_id: str
    kind: ParticipantKind
    asset_urn: str
    repository: str
    owner_urns: tuple[str, ...]
    files: tuple[str, ...]
    status: ParticipantStatus = ParticipantStatus.DISCOVERED
    version: int = 0
    base_sha: str = ""
    candidate_commit_sha: str = ""
    evidence_links: tuple[str, ...] = ()
    created_at: str = field(default_factory=utc_now)
    updated_at: str = ""

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.participant_id):
            raise ValueError("participant_id must be a non-empty portable identifier")
        if not _IDENTIFIER.fullmatch(self.migration_id):
            raise ValueError("migration_id must be a non-empty portable identifier")
        if not self.asset_urn.strip() or not self.repository.strip():
            raise ValueError("asset_urn and repository are required")
        if self.version < 0:
            raise ValueError("version cannot be negative")
        if self.candidate_commit_sha and not is_commit_sha(self.candidate_commit_sha):
            raise ValueError("candidate_commit_sha must be a 40- or 64-character hex SHA")
        if not self.updated_at:
            object.__setattr__(self, "updated_at", self.created_at)

    @classmethod
    def create(
        cls,
        *,
        migration_id: str,
        kind: ParticipantKind,
        asset_urn: str,
        repository: str,
        owner_urns: tuple[str, ...] = (),
        files: tuple[str, ...] = (),
        base_sha: str = "",
        created_at: str | None = None,
    ) -> Participant:
        identity = {
            "asset_urn": asset_urn,
            "kind": kind.value,
            "migration_id": migration_id,
            "repository": repository,
        }
        digest = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
        timestamp = created_at or utc_now()
        return cls(
            participant_id=f"consumer-{digest[:20]}",
            migration_id=migration_id,
            kind=kind,
            asset_urn=asset_urn,
            repository=repository,
            owner_urns=tuple(owner_urns),
            files=tuple(files),
            base_sha=base_sha,
            created_at=timestamp,
            updated_at=timestamp,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_urn": self.asset_urn,
            "base_sha": self.base_sha,
            "candidate_commit_sha": self.candidate_commit_sha,
            "created_at": self.created_at,
            "evidence_links": list(self.evidence_links),
            "files": list(self.files),
            "kind": self.kind.value,
            "migration_id": self.migration_id,
            "owner_urns": list(self.owner_urns),
            "participant_id": self.participant_id,
            "repository": self.repository,
            "status": self.status.value,
            "updated_at": self.updated_at,
            "version": self.version,
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Participant:
        return cls(
            participant_id=str(value["participant_id"]),
            migration_id=str(value["migration_id"]),
            kind=ParticipantKind(str(value["kind"])),
            asset_urn=str(value["asset_urn"]),
            repository=str(value["repository"]),
            owner_urns=tuple(str(item) for item in value.get("owner_urns", ())),
            files=tuple(str(item) for item in value.get("files", ())),
            status=ParticipantStatus(
                str(value.get("status", ParticipantStatus.DISCOVERED.value))
            ),
            version=int(value.get("version", 0)),
            base_sha=str(value.get("base_sha", "")),
            candidate_commit_sha=str(value.get("candidate_commit_sha", "")),
            evidence_links=tuple(
                str(item) for item in value.get("evidence_links", ())
            ),
            created_at=str(value["created_at"]),
            updated_at=str(value.get("updated_at", "")),
        )

    @classmethod
    def from_json(cls, value: str) -> Participant:
        return cls.from_dict(json.loads(value))


ConsumerParticipant = Participant


@dataclass(frozen=True)
class ApprovalReceipt:
    migration_id: str
    participant_id: str
    owner_urn: str
    approved_mapping: str
    approved_at: str
    evidence_url: str = ""
    verification: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.owner_urn.startswith("urn:li:"):
            raise ValueError("approval owner_urn must be a DataHub URN")
        if not self.approved_mapping.strip():
            raise ValueError("approved_mapping is required")
        if not self.approved_at.strip():
            raise ValueError("approved_at is required")
        require_utc_timestamp(self.approved_at, "approved_at")
        require_https_url(self.evidence_url, "approval evidence_url")
        if self.verification:
            required = {
                "actor_id",
                "actor_login",
                "author_association",
                "evidence_sha256",
                "provider",
                "resource_id",
                "resource_kind",
                "source_api_url",
            }
            if set(self.verification) != required:
                raise ValueError(
                    "authenticated approval verification has an invalid shape"
                )
            require_https_url(
                str(self.verification["source_api_url"]),
                "approval verification source_api_url",
            )
            if self.verification["provider"] != "github-rest-api-v3":
                raise ValueError("authenticated approval provider must be GitHub")
            if not re.fullmatch(
                r"[0-9a-f]{64}", str(self.verification["evidence_sha256"])
            ):
                raise ValueError("authenticated approval digest must be SHA-256")

    def to_dict(self) -> dict[str, Any]:
        return {
            "approved_at": self.approved_at,
            "approved_mapping": self.approved_mapping,
            "evidence_url": self.evidence_url,
            "migration_id": self.migration_id,
            "owner_urn": self.owner_urn,
            "participant_id": self.participant_id,
            "verification": dict(self.verification),
        }


@dataclass(frozen=True)
class CoordinationReceipt:
    """External proof required before the coordinator declares COMMITTED.

    A committed LineageTX transaction means coordinated candidate commits/PRs
    exist and the producer gate was released. It does not mean any PR was merged.
    """

    migration_id: str
    kind: CoordinationReceiptKind
    reference: str
    recorded_at: str
    commit_sha: str = ""
    evidence_url: str = ""
    merged: bool = False

    def __post_init__(self) -> None:
        if not self.reference.strip():
            raise ValueError("coordination receipt reference is required")
        require_utc_timestamp(self.recorded_at, "recorded_at")
        if self.kind is CoordinationReceiptKind.CANDIDATE_COMMIT and not is_commit_sha(
            self.commit_sha
        ):
            raise ValueError(
                "candidate commit receipt requires a 40- or 64-character hex SHA"
            )
        if self.kind is CoordinationReceiptKind.COORDINATED_PR:
            require_https_url(self.reference, "coordinated PR reference")
        if (
            self.kind is CoordinationReceiptKind.PRODUCER_GATE_RELEASED
            and not self.reference.startswith("check-run:")
        ):
            raise ValueError("producer gate receipt must reference a check-run")
        if self.evidence_url:
            require_https_url(self.evidence_url, "coordination evidence_url")
        if self.merged:
            raise ValueError("LineageTX does not auto-merge coordinated changes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "commit_sha": self.commit_sha,
            "evidence_url": self.evidence_url,
            "kind": self.kind.value,
            "merged": False,
            "migration_id": self.migration_id,
            "recorded_at": self.recorded_at,
            "reference": self.reference,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> CoordinationReceipt:
        return cls(
            migration_id=str(value["migration_id"]),
            kind=CoordinationReceiptKind(str(value["kind"])),
            reference=str(value["reference"]),
            recorded_at=str(value["recorded_at"]),
            commit_sha=str(value.get("commit_sha", "")),
            evidence_url=str(value.get("evidence_url", "")),
            merged=bool(value.get("merged", False)),
        )


@dataclass(frozen=True)
class AbortCleanupReceipt:
    """Proof that unmerged candidate material was cleaned up.

    LineageTX deliberately cannot use this receipt to claim that deployed systems
    were rolled back.
    """

    migration_id: str
    worktrees_removed: tuple[str, ...] = ()
    candidate_branches_deleted: tuple[str, ...] = ()
    cleanup_errors: tuple[str, ...] = ()
    recorded_at: str = field(default_factory=utc_now)
    deployed_systems_rolled_back: bool = False

    def __post_init__(self) -> None:
        require_utc_timestamp(self.recorded_at, "recorded_at")
        for value in self.worktrees_removed:
            path = PurePosixPath(value)
            if path.is_absolute() or ".." in path.parts or not path.parts:
                raise ValueError("worktree cleanup receipts must use portable relative scopes")
        if any(
            not branch.startswith("lineagetx/")
            for branch in self.candidate_branches_deleted
        ):
            raise ValueError("cleanup receipts may only name LineageTX candidate branches")
        if self.deployed_systems_rolled_back:
            raise ValueError(
                "ABORT only cleans unmerged candidate changes; deployed rollback "
                "cannot be claimed"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_branches_deleted": list(self.candidate_branches_deleted),
            "cleanup_errors": list(self.cleanup_errors),
            "deployed_systems_rolled_back": False,
            "migration_id": self.migration_id,
            "recorded_at": self.recorded_at,
            "scope": "unmerged_candidate_changes_only",
            "worktrees_removed": list(self.worktrees_removed),
        }


@dataclass(frozen=True)
class MigrationRecord:
    intent: ChangeIntent
    status: MigrationStatus
    version: int
    created_at: str
    updated_at: str

    @property
    def migration_id(self) -> str:
        return self.intent.migration_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "created_at": self.created_at,
            "intent": self.intent.to_dict(),
            "migration_id": self.migration_id,
            "status": self.status.value,
            "updated_at": self.updated_at,
            "version": self.version,
        }


@dataclass(frozen=True)
class StateEvent:
    migration_id: str
    sequence: int
    event_type: str
    from_status: MigrationStatus | ParticipantStatus | None
    to_status: MigrationStatus | ParticipantStatus
    version: int
    created_at: str
    participant_id: str = ""
    payload: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "created_at": self.created_at,
            "event_type": self.event_type,
            "from_status": self.from_status.value if self.from_status else None,
            "migration_id": self.migration_id,
            "participant_id": self.participant_id,
            "payload": dict(self.payload),
            "sequence": self.sequence,
            "to_status": self.to_status.value,
            "version": self.version,
        }
