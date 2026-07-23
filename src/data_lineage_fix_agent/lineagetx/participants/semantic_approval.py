from __future__ import annotations

import copy
import json
from dataclasses import dataclass

from ..models import ParticipantKind
from ..worktrees import RepositorySession
from .base import (
    bind_trusted_policy,
    CandidateRejected,
    ParticipantStatus,
    PreparationResult,
    TrustedParticipantPolicy,
    checked_text,
    enforce_allowlist,
    require_clean_candidate,
    sha256_text,
)


@dataclass(frozen=True)
class SemanticMappingProposal:
    migration_id: str
    participant_id: str
    relative_path: str
    expected_sha256: str
    old_field: str
    new_field: str
    expected_occurrences: int
    required_owner_urn: str
    expanded_columns: tuple[str, ...]
    contract_columns: tuple[str, ...]
    repository: str
    owner_urns: tuple[str, ...]

    @property
    def allowed_paths(self) -> tuple[str, ...]:
        return (self.relative_path,)


@dataclass(frozen=True)
class OwnerApproval:
    migration_id: str
    participant_id: str
    owner_urn: str
    old_field: str
    new_field: str
    approved_at: str
    evidence_url: str
    decision: str = "APPROVED"


class SemanticApprovalParticipant:
    def prepare(
        self,
        session: RepositorySession,
        proposal: SemanticMappingProposal,
        policy: TrustedParticipantPolicy,
        approval: OwnerApproval | None = None,
    ) -> PreparationResult:
        bind_trusted_policy(
            session,
            policy,
            expected_kind=ParticipantKind.SEMANTIC_APPROVAL,
            migration_id=proposal.migration_id,
            participant_id=proposal.participant_id,
            repository=proposal.repository,
            paths=(proposal.relative_path,),
            old_field=proposal.old_field,
            new_field=proposal.new_field,
            expanded_columns=proposal.expanded_columns,
            contract_columns=proposal.contract_columns,
            owner_urns=proposal.owner_urns,
        )
        if (
            len(policy.owner_urns) != 1
            or proposal.required_owner_urn != policy.owner_urns[0]
        ):
            raise CandidateRejected(
                "semantic candidate owner does not match the trusted accountable owner"
            )
        require_clean_candidate(session)
        target, before = checked_text(
            session, policy.allowed_paths[0], proposal.expected_sha256
        )
        document = self._parse_document(before)
        self._validate_pending_document(document, proposal.expected_occurrences, policy)

        if approval is None:
            # The semantic meaning is intentionally not guessed. This code path
            # performs reads only and leaves both base checkout and worktree clean.
            return PreparationResult(
                participant_id=policy.participant_id,
                state=ParticipantStatus.NEEDS_APPROVAL,
                checks=(
                    "expected_sha256",
                    "ambiguous_semantic_mapping",
                    "owner_required",
                    "zero_write_before_approval",
                ),
                evidence={
                    "requested_mapping": {
                        "old_field": policy.old_field,
                        "new_field": policy.new_field,
                    },
                    "current_sha256": proposal.expected_sha256,
                },
                required_owner_urn=policy.owner_urns[0],
            )

        self._validate_approval(policy, approval)
        after_document = copy.deepcopy(document)
        dimensions = after_document["dimensions"]
        entry = dimensions.pop(policy.old_field)
        entry["source_field"] = policy.new_field
        dimensions[policy.new_field] = entry
        after = json.dumps(after_document, indent=2, sort_keys=True) + "\n"
        parsed_after = self._parse_document(after)
        self._validate_approved_document(parsed_after, policy)

        before_bytes = target.read_bytes()
        try:
            target.write_bytes(after.encode("utf-8"))
            changed = enforce_allowlist(
                session, policy.allowed_paths, require_all=True
            )
        except Exception:
            target.write_bytes(before_bytes)
            raise

        return PreparationResult(
            participant_id=policy.participant_id,
            state=ParticipantStatus.VERIFIED,
            changed_files=changed,
            checks=(
                "expected_sha256",
                "exact_owner_identity",
                "exact_mapping_approval",
                "semantic_config_schema",
                "path_allowlist",
                "expanded_schema_mapping",
                "contract_schema_mapping",
            ),
            evidence={
                "before_sha256": proposal.expected_sha256,
                "after_sha256": sha256_text(after),
                "approved_by": approval.owner_urn,
                "approved_at": approval.approved_at,
                "approval_evidence_url": approval.evidence_url,
                "approved_mapping": {
                    "old_field": approval.old_field,
                    "new_field": approval.new_field,
                },
                "execution_scope": "trusted_fixture_fixed_adapter_no_repo_commands",
            },
        )

    @staticmethod
    def _parse_document(source: str) -> dict[str, object]:
        try:
            document = json.loads(source)
        except json.JSONDecodeError as exc:
            raise CandidateRejected(f"semantic config is invalid JSON: {exc}") from exc
        if not isinstance(document, dict) or not isinstance(
            document.get("dimensions"), dict
        ):
            raise CandidateRejected("semantic config must contain a dimensions object")
        return document

    @staticmethod
    def _count_string(value: object, needle: str) -> int:
        if isinstance(value, dict):
            return sum(key == needle for key in value) + sum(
                SemanticApprovalParticipant._count_string(item, needle)
                for item in value.values()
            )
        if isinstance(value, list):
            return sum(
                SemanticApprovalParticipant._count_string(item, needle)
                for item in value
            )
        return int(value == needle)

    @classmethod
    def _validate_pending_document(
        cls,
        document: dict[str, object],
        expected_occurrences: int,
        policy: TrustedParticipantPolicy,
    ) -> None:
        dimensions = document["dimensions"]
        assert isinstance(dimensions, dict)
        if policy.old_field not in dimensions or policy.new_field in dimensions:
            raise CandidateRejected("semantic dimension changed after proposal generation")
        entry = dimensions[policy.old_field]
        if not isinstance(entry, dict) or entry.get("source_field") != policy.old_field:
            raise CandidateRejected("semantic source_field does not match the old dimension")
        occurrences = cls._count_string(document, policy.old_field)
        if occurrences != expected_occurrences:
            raise CandidateRejected(
                f"semantic occurrence count changed: expected "
                f"{expected_occurrences}, got {occurrences}"
            )

    @staticmethod
    def _validate_approval(
        policy: TrustedParticipantPolicy,
        approval: OwnerApproval,
    ) -> None:
        if approval.decision != "APPROVED":
            raise CandidateRejected("owner did not approve this mapping")
        expected = (
            policy.migration_id,
            policy.participant_id,
            policy.owner_urns[0],
            policy.old_field,
            policy.new_field,
        )
        actual = (
            approval.migration_id,
            approval.participant_id,
            approval.owner_urn,
            approval.old_field,
            approval.new_field,
        )
        if actual != expected:
            raise CandidateRejected(
                "approval must match migration, participant, owner, and exact mapping"
            )
        if not approval.approved_at or not approval.evidence_url:
            raise CandidateRejected("approval must include timestamp and evidence URL")

    @classmethod
    def _validate_approved_document(
        cls,
        document: dict[str, object],
        policy: TrustedParticipantPolicy,
    ) -> None:
        dimensions = document["dimensions"]
        assert isinstance(dimensions, dict)
        if policy.old_field in dimensions or policy.new_field not in dimensions:
            raise CandidateRejected("approved semantic dimension was not renamed exactly")
        entry = dimensions[policy.new_field]
        if not isinstance(entry, dict) or entry.get("source_field") != policy.new_field:
            raise CandidateRejected("approved source_field was not renamed exactly")
        if cls._count_string(document, policy.old_field):
            raise CandidateRejected("old field remains after approved semantic mapping")

        expanded = set(policy.expanded_columns)
        contract = set(policy.contract_columns)
        if policy.new_field not in expanded or policy.new_field not in contract:
            raise CandidateRejected("approved semantic mapping fails a schema variant")
