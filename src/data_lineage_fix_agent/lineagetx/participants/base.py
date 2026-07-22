from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

from ..models import (
    ChangeIntent,
    Participant,
    ParticipantKind,
    ParticipantStatus,
)
from ..worktrees import RepositorySession, git_head_and_branch, git_status_paths


class CandidateRejected(RuntimeError):
    """A candidate proposal failed a deterministic safety check."""


@dataclass(frozen=True)
class PreparationResult:
    participant_id: str
    state: ParticipantStatus
    changed_files: tuple[str, ...] = ()
    checks: tuple[str, ...] = ()
    evidence: dict[str, Any] = field(default_factory=dict)
    required_owner_urn: str | None = None


@dataclass(frozen=True)
class TrustedParticipantPolicy:
    """Frozen adapter binding derived from ChangeIntent, Participant, and DataHub.

    Candidate proposals are untrusted. Adapters use this policy—not proposal
    fields—as the source for file allowlists, schemas, relation, and owners.
    The bounded implementation runs only against trusted fixture repositories
    and never executes repository-provided test commands.
    """

    migration_id: str
    participant_id: str
    kind: ParticipantKind
    repository: str
    allowed_paths: tuple[str, ...]
    old_field: str
    new_field: str
    expanded_columns: tuple[str, ...]
    contract_columns: tuple[str, ...]
    owner_urns: tuple[str, ...]
    asset_urn: str
    source_asset_urn: str
    intent_sha256: str
    relation: str | None = None
    dialect: str | None = None
    assignment_name: str | None = None
    config_key: str | None = None
    trusted_fixture_scope: bool = True

    def __post_init__(self) -> None:
        normalized = tuple(_safe_relative_path(path) for path in self.allowed_paths)
        if len(normalized) != len(set(normalized)) or not normalized:
            raise ValueError("trusted participant paths must be non-empty and unique")
        object.__setattr__(self, "allowed_paths", normalized)
        if not self.repository or not self.owner_urns:
            raise ValueError("trusted participant repository and owners are required")
        if not self.asset_urn or not self.source_asset_urn or not self.intent_sha256:
            raise ValueError("trusted policy must retain frozen DataHub and intent identity")
        if self.old_field == self.new_field:
            raise ValueError("trusted schema fields must differ")
        if len(self.expanded_columns) != len(set(self.expanded_columns)):
            raise ValueError("expanded schema contains duplicate columns")
        if len(self.contract_columns) != len(set(self.contract_columns)):
            raise ValueError("contract schema contains duplicate columns")
        expanded = set(self.expanded_columns)
        contract = set(self.contract_columns)
        if not {self.old_field, self.new_field}.issubset(expanded):
            raise ValueError("expanded schema must contain old and replacement fields")
        if self.old_field in contract or self.new_field not in contract:
            raise ValueError("contract schema must remove old and retain replacement")
        if self.kind is ParticipantKind.DBT_SQL:
            if not self.relation or self.dialect != "duckdb":
                raise ValueError(
                    "dbt trusted policy requires a fixed relation and duckdb dialect"
                )
            if self.assignment_name is not None or self.config_key is not None:
                raise ValueError("dbt trusted policy cannot declare Airflow keys")
        elif self.kind is ParticipantKind.AIRFLOW_MAPPING:
            if self.relation is not None or self.dialect is not None:
                raise ValueError("Airflow trusted policy cannot declare SQL settings")
            if (
                self.assignment_name != "FIELD_MAPPING"
                or self.config_key != "field_mapping"
            ):
                raise ValueError(
                    "Airflow trusted policy requires the fixed fixture mapping keys"
                )
        elif any(
            item is not None
            for item in (
                self.relation,
                self.dialect,
                self.assignment_name,
                self.config_key,
            )
        ):
            raise ValueError("semantic trusted policy cannot declare adapter settings")
        if not self.trusted_fixture_scope:
            raise ValueError(
                "this MVP adapter runner is restricted to trusted fixture repositories"
            )

    @classmethod
    def from_records(
        cls,
        intent: ChangeIntent,
        participant: Participant,
        *,
        expanded_columns: tuple[str, ...],
        contract_columns: tuple[str, ...],
        relation: str | None = None,
        dialect: str | None = None,
        assignment_name: str | None = None,
        config_key: str | None = None,
    ) -> TrustedParticipantPolicy:
        if participant.migration_id != intent.migration_id:
            raise ValueError("participant belongs to a different ChangeIntent")
        return cls(
            migration_id=intent.migration_id,
            participant_id=participant.participant_id,
            kind=participant.kind,
            repository=participant.repository,
            allowed_paths=participant.files,
            old_field=intent.old_field,
            new_field=intent.new_field,
            expanded_columns=tuple(expanded_columns),
            contract_columns=tuple(contract_columns),
            owner_urns=participant.owner_urns,
            asset_urn=participant.asset_urn,
            source_asset_urn=intent.source_asset_urn,
            intent_sha256=intent.intent_sha256,
            relation=relation,
            dialect=dialect,
            assignment_name=assignment_name,
            config_key=config_key,
        )


def bind_trusted_policy(
    session: RepositorySession,
    policy: TrustedParticipantPolicy,
    *,
    expected_kind: ParticipantKind,
    migration_id: str,
    participant_id: str,
    repository: str,
    paths: tuple[str, ...],
    old_field: str,
    new_field: str,
    expanded_columns: tuple[str, ...],
    contract_columns: tuple[str, ...],
    owner_urns: tuple[str, ...],
    relation: str | None = None,
    dialect: str | None = None,
    assignment_name: str | None = None,
    config_key: str | None = None,
) -> None:
    """Compare every untrusted binding field to a trusted frozen policy."""

    normalized_paths = tuple(_safe_relative_path(path) for path in paths)
    mismatches: list[str] = []
    comparisons = {
        "session migration_id": (session.migration_id, policy.migration_id),
        "session repository": (session.repo_id, policy.repository),
        "proposal migration_id": (migration_id, policy.migration_id),
        "proposal participant_id": (participant_id, policy.participant_id),
        "proposal repository": (repository, policy.repository),
        "proposal kind": (expected_kind, policy.kind),
        "proposal paths": (normalized_paths, policy.allowed_paths),
        "proposal old_field": (old_field, policy.old_field),
        "proposal new_field": (new_field, policy.new_field),
        "proposal expanded schema": (expanded_columns, policy.expanded_columns),
        "proposal contract schema": (contract_columns, policy.contract_columns),
        "proposal owners": (owner_urns, policy.owner_urns),
        "proposal relation": (relation, policy.relation),
        "proposal dialect": (dialect, policy.dialect),
        "proposal assignment_name": (assignment_name, policy.assignment_name),
        "proposal config_key": (config_key, policy.config_key),
    }
    for label, (actual, expected) in comparisons.items():
        if actual != expected:
            mismatches.append(label)
    if mismatches:
        raise CandidateRejected(
            "candidate does not match trusted participant policy: "
            + ", ".join(mismatches)
        )


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def safe_target(session: RepositorySession, relative_path: str) -> Path:
    posix = PurePosixPath(_safe_relative_path(relative_path))
    worktree = session.worktree.resolve()
    target = (worktree / Path(*posix.parts)).resolve()
    if not target.is_relative_to(worktree):
        raise CandidateRejected("candidate path escaped the isolated worktree")
    if not target.is_file():
        raise CandidateRejected(f"candidate file does not exist: {relative_path}")
    return target


def checked_text(
    session: RepositorySession,
    relative_path: str,
    expected_sha256: str,
) -> tuple[Path, str]:
    target = safe_target(session, relative_path)
    value = target.read_text(encoding="utf-8")
    actual = sha256_text(value)
    if actual != expected_sha256:
        raise CandidateRejected(
            f"stale candidate for {relative_path}: expected {expected_sha256}, got {actual}"
        )
    return target, value


def git_changed_files(session: RepositorySession) -> tuple[str, ...]:
    try:
        return git_status_paths(session.worktree)
    except Exception as exc:
        raise CandidateRejected(f"unable to inspect candidate worktree: {exc}") from exc


def require_clean_candidate(session: RepositorySession) -> None:
    head, branch = git_head_and_branch(session.worktree)
    if head != session.base_sha or branch != session.branch:
        raise CandidateRejected(
            "candidate worktree must remain on the pinned LineageTX base branch"
        )
    changed = git_changed_files(session)
    if changed:
        raise CandidateRejected(
            f"candidate worktree must be clean before PREPARING: {list(changed)}"
        )


def enforce_allowlist(
    session: RepositorySession,
    allowed_paths: Iterable[str],
    *,
    require_all: bool,
) -> tuple[str, ...]:
    allowed = {PurePosixPath(path).as_posix() for path in allowed_paths}
    changed = set(git_changed_files(session))
    if not changed.issubset(allowed):
        raise CandidateRejected(
            f"candidate changed files outside allowlist: {sorted(changed - allowed)}"
        )
    if require_all and changed != allowed:
        raise CandidateRejected(
            f"candidate must change exactly {sorted(allowed)}, got {sorted(changed)}"
        )
    return tuple(sorted(changed))


def _safe_relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise CandidateRejected(f"unsafe repository path: {value!r}")
    return path.as_posix()
