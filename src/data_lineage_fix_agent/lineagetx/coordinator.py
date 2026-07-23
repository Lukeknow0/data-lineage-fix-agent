from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol

from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

from .datahub_context import DataHubMigrationContext
from .evidence import EvidenceManifest, EvidenceRecorder
from .github_approval import (
    ApprovalVerificationError,
    GitHubApprovalExpectation,
    GitHubApprovalVerifier,
    VerifiedGitHubApproval,
)
from .models import (
    AbortCleanupReceipt,
    ApprovalReceipt,
    ChangeIntent,
    CoordinationReceipt,
    CoordinationReceiptKind,
    MigrationRecord,
    MigrationStatus,
    Participant,
    ParticipantKind,
    ParticipantStatus,
    require_https_url,
)
from .participants.airflow_mapping import (
    AirflowMappingParticipant,
    AirflowMappingProposal,
)
from .participants.base import PreparationResult, TrustedParticipantPolicy
from .participants.dbt_sql import DbtSqlParticipant, DbtSqlProposal
from .participants.semantic_approval import (
    OwnerApproval,
    SemanticApprovalParticipant,
    SemanticMappingProposal,
)
from .policy import (
    DiscoveryAttestation,
    DiscoveryDecision,
    LineageTXSafetyPolicy,
)
from .proposals import (
    CandidateEnvelope,
    CandidateProposal,
    CandidateProposalModel,
    ProposalError,
    ProposalRequest,
    proposal_fingerprint,
    proposal_request_from_session,
)
from .publisher import (
    CandidateCommit,
    CoordinationPublisher,
    PublicationRequest,
    PublicationResult,
)
from .state import PublicationReconciliationRequired, SQLiteStateStore
from .worktrees import RepositorySession, WorktreeManager
from .writeback import DataHubMigrationWriter, MigrationWriteback, WritebackReceipt


class CoordinationError(RuntimeError):
    """The transaction stopped because a safety precondition was not proved."""


class ImpactRevalidator(Protocol):
    async def assert_impact_unchanged(
        self, frozen: DataHubMigrationContext
    ) -> DataHubMigrationContext: ...


@dataclass(frozen=True)
class PrepareOutcome:
    migration: MigrationRecord
    participants: tuple[Participant, ...]
    results: Mapping[str, PreparationResult]
    pending_approval_participant_ids: tuple[str, ...]


@dataclass(frozen=True)
class CommitOutcome:
    migration: MigrationRecord
    participants: tuple[Participant, ...]
    publication: PublicationResult
    manifest: EvidenceManifest | None
    upstream_change_safe_to_merge: bool = True
    auto_merged: bool = False


@dataclass(frozen=True)
class AbortOutcome:
    migration: MigrationRecord
    cleanup: AbortCleanupReceipt
    manifest: EvidenceManifest | None
    deployed_systems_rolled_back: bool = False


class LineageTXCoordinator:
    """Coordinates one bounded schema migration through prepare/approve/commit.

    Database atomicity is not claimed. Candidate edits and commits live only on
    isolated LineageTX branches. COMMITTED means the unmerged candidate receipts,
    coordination PR receipt, and Producer gate receipt all exist.
    """

    def __init__(
        self,
        *,
        state: SQLiteStateStore,
        worktrees: WorktreeManager,
        proposal_model: CandidateProposalModel,
        publisher: CoordinationPublisher,
        impact_revalidator: ImpactRevalidator | None,
        migration_writer: DataHubMigrationWriter | None = None,
        safety_policy: LineageTXSafetyPolicy | None = None,
        evidence: EvidenceRecorder | None = None,
        evidence_base_url: str = "https://replay.lineagetx.invalid/evidence",
        dbt_adapter: DbtSqlParticipant | None = None,
        airflow_adapter: AirflowMappingParticipant | None = None,
        semantic_adapter: SemanticApprovalParticipant | None = None,
        approval_verifier: GitHubApprovalVerifier | None = None,
        allow_test_approvals: bool = False,
    ) -> None:
        require_https_url(evidence_base_url, "evidence_base_url")
        self.state = state
        self.worktrees = worktrees
        self.proposal_model = proposal_model
        self.publisher = publisher
        self.impact_revalidator = impact_revalidator
        self.migration_writer = migration_writer
        self.safety_policy = safety_policy or LineageTXSafetyPolicy()
        self.evidence = evidence
        self.evidence_base_url = evidence_base_url.rstrip("/")
        self.dbt_adapter = dbt_adapter or DbtSqlParticipant()
        self.airflow_adapter = airflow_adapter or AirflowMappingParticipant()
        self.semantic_adapter = semantic_adapter or SemanticApprovalParticipant()
        self.approval_verifier = approval_verifier
        self.allow_test_approvals = allow_test_approvals

        self._contexts: dict[str, DataHubMigrationContext] = {}
        self._discoveries: dict[str, DiscoveryDecision] = {}
        self._sessions: dict[tuple[str, str], RepositorySession] = {}
        self._requests: dict[tuple[str, str], ProposalRequest] = {}
        self._proposals: dict[tuple[str, str], CandidateEnvelope] = {}
        self._trusted_policies: dict[tuple[str, str], TrustedParticipantPolicy] = {}
        self._results: dict[tuple[str, str], PreparationResult] = {}
        self._verified_github_approvals: dict[str, VerifiedGitHubApproval] = {}
        self._publications: dict[str, PublicationResult] = {}

    async def detect(
        self,
        intent: ChangeIntent,
        attestation: DiscoveryAttestation,
        participants: tuple[Participant, ...],
    ) -> MigrationRecord:
        if self.evidence is not None and self.evidence.migration_id != intent.migration_id:
            raise ValueError("evidence recorder belongs to another migration")
        decision = self.safety_policy.validate_discovery(
            intent, attestation, participants
        )
        migration = self.state.create_migration(intent, participants)
        self._contexts[intent.migration_id] = attestation.context
        self._discoveries[intent.migration_id] = decision
        if self.evidence is not None:
            self.evidence.write_json("context/datahub.json", attestation.context)
            self.evidence.write_json("context/discovery-decision.json", decision)
            self.evidence.capture_state(self.state, "01-detected")
            self._seal_evidence()
        await self._writeback(migration, "detected")
        return migration

    async def prepare(
        self,
        migration_id: str,
        repository_roots: Mapping[str, Path],
    ) -> PrepareOutcome:
        migration = self.state.get_migration(migration_id)
        if migration.status is not MigrationStatus.DETECTED:
            raise CoordinationError(
                f"prepare requires DETECTED, got {migration.status.value}"
            )
        context = self._required_context(migration_id)
        decision = self._required_discovery(migration_id)
        migration = self.state.transition_migration(
            migration_id,
            MigrationStatus.PREPARING,
            expected_status=MigrationStatus.DETECTED,
            expected_version=migration.version,
            event_type="LINEAGETX_PREPARING",
            payload={"isolated_worktrees": True, "auto_merge": False},
        )
        active_participant_id = ""
        try:
            await self._writeback(migration, "preparing")
            for participant_id in decision.ordered_participant_ids:
                active_participant_id = participant_id
                participant = self.state.get_participant(participant_id)
                participant = self.state.transition_participant(
                    participant_id,
                    ParticipantStatus.PREPARING,
                    expected_status=ParticipantStatus.DISCOVERED,
                    expected_version=participant.version,
                    event_type="PARTICIPANT_PREPARING_IN_ISOLATED_WORKTREE",
                    payload={"repository": participant.repository},
                )
                try:
                    base_repo = Path(repository_roots[participant.repository])
                except KeyError as error:
                    raise CoordinationError(
                        f"repository root is missing for {participant.repository}"
                    ) from error
                session = self.worktrees.prepare(
                    repo_id=participant.repository,
                    candidate_id=participant.participant_id,
                    base_repo=base_repo,
                    migration_id=migration_id,
                    base_sha=participant.base_sha or None,
                )
                self._sessions[(migration_id, participant_id)] = session
                request = self._proposal_request(
                    migration.intent, participant, session, context
                )
                trusted = self._trusted_policy(request)
                envelope = self.proposal_model.propose(request)
                proposal = self.safety_policy.validate_candidate(request, envelope)
                result = self._run_adapter(session, proposal, trusted)
                self._requests[(migration_id, participant_id)] = request
                self._proposals[(migration_id, participant_id)] = envelope
                self._trusted_policies[(migration_id, participant_id)] = trusted
                self._results[(migration_id, participant_id)] = result
                self._record_preparation(
                    participant,
                    envelope,
                    trusted,
                    result,
                    session,
                )

                current = self.state.get_participant(participant_id)
                if result.state is ParticipantStatus.NEEDS_APPROVAL:
                    if participant.kind is not ParticipantKind.SEMANTIC_APPROVAL:
                        raise CoordinationError(
                            "only the semantic participant may require approval"
                        )
                    if result.changed_files or self.worktrees.changed_files(session):
                        raise CoordinationError(
                            "semantic NEEDS_APPROVAL must be a zero-write result"
                        )
                    self.state.transition_participant(
                        participant_id,
                        ParticipantStatus.NEEDS_APPROVAL,
                        expected_status=ParticipantStatus.PREPARING,
                        expected_version=current.version,
                        event_type="SEMANTIC_OWNER_APPROVAL_REQUIRED",
                        payload={
                            "required_owner_urn": result.required_owner_urn,
                            "zero_write": True,
                        },
                    )
                elif result.state is ParticipantStatus.VERIFIED:
                    self._commit_verified_candidate(
                        participant,
                        current,
                        result,
                        session,
                        envelope,
                    )
                else:
                    raise CoordinationError(
                        f"adapter returned non-preparable state {result.state.value}"
                    )

            participant_records = tuple(self.state.list_participants(migration_id))
            pending = tuple(
                item.participant_id
                for item in participant_records
                if item.status is ParticipantStatus.NEEDS_APPROVAL
            )
            if pending:
                if len(pending) != 1:
                    raise CoordinationError("bounded scenario requires one owner approval")
                migration = self.state.transition_migration(
                    migration_id,
                    MigrationStatus.NEEDS_APPROVAL,
                    expected_status=MigrationStatus.PREPARING,
                    expected_version=migration.version,
                    event_type="MIGRATION_WAITING_FOR_OWNER",
                    payload={"participant_ids": list(pending)},
                )
                stage = "03-needs-approval"
            else:
                migration = self.state.transition_migration(
                    migration_id,
                    MigrationStatus.PREPARED,
                    expected_status=MigrationStatus.PREPARING,
                    expected_version=migration.version,
                    event_type="ALL_CONSUMERS_PREPARED",
                )
                stage = "03-prepared"
            if self.evidence is not None:
                self.evidence.capture_state(self.state, stage)
                self._seal_evidence()
            await self._writeback(migration, migration.status.value.lower())
            return PrepareOutcome(
                migration=migration,
                participants=tuple(self.state.list_participants(migration_id)),
                results={
                    participant_id: self._results[(migration_id, participant_id)]
                    for participant_id in decision.ordered_participant_ids
                },
                pending_approval_participant_ids=pending,
            )
        except Exception as error:
            await self._abort_after_failure(
                migration_id, active_participant_id, error, "prepare"
            )
            raise AssertionError("unreachable")  # pragma: no cover

    async def approve(
        self,
        migration_id: str,
        approval: OwnerApproval,
    ) -> PrepareOutcome:
        """Apply a caller-created receipt only in an explicit fixture/test runtime.

        Production callers must use :meth:`approve_from_github`, which fetches
        actor identity and the exact approval body from GitHub's HTTPS API.
        """

        if not self.allow_test_approvals:
            raise CoordinationError(
                "caller-fabricated OwnerApproval is forbidden; use "
                "approve_from_github with authenticated external evidence"
            )
        return await self._apply_approval(migration_id, approval, verified=None)

    async def approve_from_github(
        self,
        migration_id: str,
        source_api_url: str,
    ) -> PrepareOutcome:
        """Fetch and authenticate the exact pending owner decision on GitHub."""

        if self.approval_verifier is None:
            raise CoordinationError("GitHub owner approval verifier is not configured")
        migration = self.state.get_migration(migration_id)
        if migration.status is not MigrationStatus.NEEDS_APPROVAL:
            raise CoordinationError(
                f"approval requires NEEDS_APPROVAL, got {migration.status.value}"
            )
        pending = tuple(
            participant
            for participant in self.state.list_participants(migration_id)
            if participant.status is ParticipantStatus.NEEDS_APPROVAL
        )
        if len(pending) != 1:
            raise CoordinationError("approval requires exactly one pending consumer")
        participant = pending[0]
        if (
            participant.kind is not ParticipantKind.SEMANTIC_APPROVAL
            or len(participant.owner_urns) != 1
        ):
            raise CoordinationError(
                "pending approval is not bound to one semantic DataHub owner"
            )
        expectation = GitHubApprovalExpectation(
            migration_id=migration_id,
            participant_id=participant.participant_id,
            owner_urn=participant.owner_urns[0],
            old_field=migration.intent.old_field,
            new_field=migration.intent.new_field,
        )
        try:
            verified = await asyncio.to_thread(
                self.approval_verifier.verify,
                expectation,
                source_api_url,
            )
        except ApprovalVerificationError as error:
            # Authentication failure must not mutate or abort a legitimate
            # waiting transaction. The real owner may retry with valid proof.
            raise CoordinationError(
                f"GitHub owner approval was not verified: {error}"
            ) from error
        outcome = await self._apply_approval(
            migration_id,
            verified.to_owner_approval(),
            verified=verified,
        )
        self._verified_github_approvals[migration_id] = verified
        return outcome

    async def _apply_approval(
        self,
        migration_id: str,
        approval: OwnerApproval,
        *,
        verified: VerifiedGitHubApproval | None,
    ) -> PrepareOutcome:
        migration = self.state.get_migration(migration_id)
        if migration.status is not MigrationStatus.NEEDS_APPROVAL:
            raise CoordinationError(
                f"approval requires NEEDS_APPROVAL, got {migration.status.value}"
            )
        participant = self.state.get_participant(approval.participant_id)
        if participant.status is not ParticipantStatus.NEEDS_APPROVAL:
            raise CoordinationError("approval participant is not waiting for approval")
        # Invalid approvals are rejected without aborting a legitimate pending
        # transaction, so the actual DataHub owner can still approve it later.
        self.safety_policy.validate_owner_approval(
            intent=migration.intent,
            participant=participant,
            approval=approval,
        )
        key = (migration_id, participant.participant_id)
        session = self._sessions.get(key)
        envelope = self._proposals.get(key)
        trusted = self._trusted_policies.get(key)
        if session is None or envelope is None or trusted is None:
            raise CoordinationError("pending isolated candidate session is unavailable")
        if not isinstance(envelope.proposal, SemanticMappingProposal):
            raise CoordinationError("pending approval is not a semantic proposal")

        try:
            receipt = ApprovalReceipt(
                migration_id=migration_id,
                participant_id=participant.participant_id,
                owner_urn=approval.owner_urn,
                approved_mapping=(
                    f"{migration.intent.old_field} -> {migration.intent.new_field}"
                ),
                approved_at=approval.approved_at,
                evidence_url=approval.evidence_url,
                verification=(
                    {
                        "actor_id": verified.actor_id,
                        "actor_login": verified.actor_login,
                        "author_association": verified.author_association,
                        "evidence_sha256": verified.evidence_sha256,
                        "provider": verified.verification_provider,
                        "resource_id": verified.resource_id,
                        "resource_kind": verified.resource_kind,
                        "source_api_url": verified.source_api_url,
                    }
                    if verified is not None
                    else {}
                ),
            )
            self.state.record_approval(receipt)
            participant = self.state.transition_participant(
                participant.participant_id,
                ParticipantStatus.PREPARING,
                expected_status=ParticipantStatus.NEEDS_APPROVAL,
                expected_version=participant.version,
                event_type="APPROVED_SEMANTIC_CANDIDATE_PREPARING",
                payload={"approval_evidence_url": approval.evidence_url},
            )
            result = self.semantic_adapter.prepare(
                session,
                envelope.proposal,
                trusted,
                approval,
            )
            if result.state is not ParticipantStatus.VERIFIED:
                raise CoordinationError("approved semantic adapter did not verify")
            self._results[key] = result
            self._record_preparation(
                participant,
                envelope,
                trusted,
                result,
                session,
                suffix="approved",
            )
            current = self.state.get_participant(participant.participant_id)
            self._commit_verified_candidate(
                participant,
                current,
                result,
                session,
                envelope,
            )
            participants = tuple(self.state.list_participants(migration_id))
            if any(item.status is not ParticipantStatus.VERIFIED for item in participants):
                raise CoordinationError("PREPARED requires all consumers VERIFIED")
            migration = self.state.transition_migration(
                migration_id,
                MigrationStatus.PREPARED,
                expected_status=MigrationStatus.NEEDS_APPROVAL,
                expected_version=migration.version,
                event_type="ALL_CONSUMERS_PREPARED_AFTER_OWNER_APPROVAL",
                payload={"approved_by": approval.owner_urn},
            )
            if self.evidence is not None:
                self.evidence.write_json("approval/owner-receipt.json", receipt)
                if verified is not None:
                    self.evidence.write_json(
                        "approval/github-verification.json", verified.to_dict()
                    )
                self.evidence.capture_state(self.state, "04-prepared")
                self._seal_evidence()
            await self._writeback(migration, "prepared")
            decision = self._required_discovery(migration_id)
            return PrepareOutcome(
                migration=migration,
                participants=participants,
                results={
                    participant_id: self._results[(migration_id, participant_id)]
                    for participant_id in decision.ordered_participant_ids
                },
                pending_approval_participant_ids=(),
            )
        except Exception as error:
            await self._abort_after_failure(
                migration_id, participant.participant_id, error, "approval"
            )
            raise AssertionError("unreachable")  # pragma: no cover

    async def commit(self, migration_id: str) -> CommitOutcome:
        migration = self.state.get_migration(migration_id)
        if migration.status is MigrationStatus.COMMITTED:
            return await self.reconcile_commit(migration_id)
        if migration.status is not MigrationStatus.PREPARED:
            raise CoordinationError(
                f"commit requires PREPARED, got {migration.status.value}"
            )
        context = self._required_context(migration_id)
        if self.impact_revalidator is None:
            raise CoordinationError(
                "official DataHub impact revalidation is required before gate release"
            )
        try:
            refreshed = await self.impact_revalidator.assert_impact_unchanged(context)
            if (
                refreshed is context
                or not refreshed.discovery_complete
                or refreshed.impact_fingerprint != context.impact_fingerprint
            ):
                raise CoordinationError(
                    "DataHub impact changed after PREPARING; refusing Producer gate release"
                )
            await self._writeback(
                migration,
                "precommit-refreshed",
                refreshed_context=refreshed,
            )
            if not self.allow_test_approvals:
                verified = self._verified_github_approvals.get(migration_id)
                if verified is None or self.approval_verifier is None:
                    raise CoordinationError(
                        "authenticated GitHub approval is required before gate release"
                    )
                expectation = GitHubApprovalExpectation(
                    migration_id=migration_id,
                    participant_id=verified.participant_id,
                    owner_urn=verified.owner_urn,
                    old_field=migration.intent.old_field,
                    new_field=migration.intent.new_field,
                )
                current_approval = await asyncio.to_thread(
                    self.approval_verifier.verify,
                    expectation,
                    verified.source_api_url,
                )
                if (
                    current_approval.evidence_sha256 != verified.evidence_sha256
                    or current_approval.resource_id != verified.resource_id
                    or current_approval.resource_node_id != verified.resource_node_id
                ):
                    raise CoordinationError(
                        "GitHub approval changed after preparation; refusing gate release"
                    )
                if self.evidence is not None:
                    self.evidence.write_json(
                        "approval/github-precommit-revalidation.json",
                        current_approval.to_dict(),
                    )
                    self._seal_evidence()
            participants = tuple(self.state.list_participants(migration_id))
            candidates: list[CandidateCommit] = []
            candidate_sessions: list[tuple[Participant, RepositorySession]] = []
            for participant in participants:
                if (
                    participant.status is not ParticipantStatus.VERIFIED
                    or not participant.candidate_commit_sha
                ):
                    raise CoordinationError(
                        f"participant {participant.participant_id} is not verified"
                    )
                session = self._sessions.get((migration_id, participant.participant_id))
                result = self._results.get((migration_id, participant.participant_id))
                if session is None or result is None:
                    raise CoordinationError("verified candidate session is unavailable")
                candidates.append(
                    CandidateCommit(
                        participant_id=participant.participant_id,
                        repository=participant.repository,
                        branch=session.branch,
                        commit_sha=participant.candidate_commit_sha,
                        changed_files=result.changed_files,
                        merged=False,
                    )
                )
                candidate_sessions.append((participant, session))
            if len(candidates) != 3:
                raise CoordinationError("commit requires exactly three candidate commits")
            body_lines = [
                "LineageTX verified all downstream consumers.",
                "",
                "Candidate changes remain unmerged; human merge control is preserved.",
                "",
                *(
                    f"- {item.participant_id}: `{item.commit_sha}` on `{item.branch}`"
                    for item in candidates
                ),
            ]
            request = PublicationRequest(
                intent=migration.intent,
                candidates=tuple(candidates),
                title=(
                    f"LineageTX {migration_id}: "
                    f"{migration.intent.old_field} to {migration.intent.new_field}"
                ),
                body="\n".join(body_lines),
                coordinated_head_branch=f"lineagetx/{migration_id}/coordination",
                auto_merge=False,
            )
            # This is deliberately the final local action before publication.
            # Earlier verification is not trusted as proof that refs remained
            # unchanged while the transaction waited for owner approval.
            for participant, session in candidate_sessions:
                self.worktrees.validate_committed_candidate(
                    session,
                    expected_commit_sha=participant.candidate_commit_sha,
                    allowed_paths=participant.files,
                )
            stage = self.publisher.stage(request)
            stage.validate(request)
        except Exception as error:
            if self.state.get_migration(migration_id).status is MigrationStatus.PREPARED:
                if self.state.publication_gate_armed(migration_id):
                    raise CoordinationError(
                        "publication reconciliation is already armed; pre-publication "
                        "retry failed but candidates were retained"
                    ) from error
                await self._abort_after_failure(migration_id, "", error, "commit")
            raise

        # From the first success-gate call onward, its outcome may be externally
        # visible even if the response is lost.  Never route any failure below
        # this line through ABORT: retain candidates and reconcile idempotently.
        try:
            self.state.arm_publication_gate(
                migration_id,
                expected_version=migration.version,
            )
        except PublicationReconciliationRequired as error:
            raise CoordinationError(
                "ABORT cleanup was already reserved; Producer gate release did not start"
            ) from error

        try:
            if self.evidence is not None:
                self.evidence.write_json(
                    "publication/staged.json",
                    {
                        "candidate_cleanup_permitted": False,
                        "candidate_receipts": [
                            item.to_dict() for item in stage.candidate_receipts
                        ],
                        "coordinated_pr": stage.coordinated_pr_receipt.to_dict(),
                        "recovery": "reconcile_publication",
                    },
                )
                self._seal_evidence()
            publication = self.publisher.reconcile(request, stage)
            if publication is None:
                publication = self.publisher.release_gate(request, stage)
            publication.validate(request)
        except Exception as error:
            if self.evidence is not None:
                self.evidence.write_json(
                    "publication/reconciliation-required.json",
                    {
                        "error_type": type(error).__name__,
                        "gate_outcome": "unknown_or_not_yet_confirmed",
                        "candidate_cleanup_permitted": False,
                    },
                )
                self._seal_evidence()
            raise CoordinationError(
                "Producer gate release could be externally visible; candidates were "
                "retained and commit must be retried to reconcile publication"
            ) from error

        self._publications[migration_id] = publication
        try:
            migration = self.state.transition_migration(
                migration_id,
                MigrationStatus.COMMITTED,
                expected_status=MigrationStatus.PREPARED,
                expected_version=migration.version,
                event_type="COORDINATED_PR_CREATED_AND_PRODUCER_GATE_RELEASED",
                payload={
                    "impact_fingerprint": refreshed.impact_fingerprint,
                    "unverified_consumers": 0,
                    "auto_merge": False,
                },
                coordination_receipts=publication.receipts,
            )
        except Exception as error:
            current = self.state.get_migration(migration_id)
            if current.status is MigrationStatus.COMMITTED:
                migration = current
            else:
                if self.evidence is not None:
                    self.evidence.write_json(
                        "publication/state-reconciliation-required.json",
                        {
                            "error_type": type(error).__name__,
                            "producer_gate_confirmed": True,
                            "candidate_cleanup_permitted": False,
                        },
                    )
                    self._seal_evidence()
                raise CoordinationError(
                    "Producer gate was released but COMMITTED state persistence "
                    "requires reconciliation; candidates were retained"
                ) from error

        try:
            await self._writeback(
                migration,
                "committed",
                refreshed_context=refreshed,
            )
        except Exception as error:
            if self.evidence is not None:
                self.evidence.write_json(
                    "writeback/committed-error.json",
                    {
                        "error_type": type(error).__name__,
                        "retry": "commit_or_reconcile_commit",
                    },
                )
                self._seal_evidence()
            raise CoordinationError(
                "coordination receipts were committed, but final DataHub write-back "
                f"failed and requires idempotent retry: {error}"
            ) from error

        manifest: EvidenceManifest | None = None
        if self.evidence is not None:
            self.evidence.write_json(
                "publication/receipts.json",
                {
                    "auto_merge": False,
                    "receipts": [item.to_dict() for item in publication.receipts],
                },
            )
            self.evidence.capture_state(self.state, "05-committed")
            manifest = self._seal_evidence()
        return CommitOutcome(
            migration=migration,
            participants=tuple(self.state.list_participants(migration_id)),
            publication=publication,
            manifest=manifest,
            upstream_change_safe_to_merge=True,
            auto_merged=False,
        )

    async def reconcile_commit(self, migration_id: str) -> CommitOutcome:
        """Idempotently finish final write-back after external gate release."""

        migration = self.state.get_migration(migration_id)
        if migration.status is not MigrationStatus.COMMITTED:
            raise CoordinationError(
                f"commit reconciliation requires COMMITTED, got {migration.status.value}"
            )
        context = self._required_context(migration_id)
        if self.impact_revalidator is None:
            raise CoordinationError(
                "official DataHub impact revalidation is required for reconciliation"
            )
        refreshed = await self.impact_revalidator.assert_impact_unchanged(context)
        if (
            refreshed is context
            or not refreshed.discovery_complete
            or refreshed.impact_fingerprint != context.impact_fingerprint
        ):
            raise CoordinationError(
                "DataHub impact changed after gate release; final write-back was not "
                "reconciled and candidates remain retained"
            )
        publication = self._publications.get(migration_id)
        if publication is None:
            publication = self._publication_from_state(migration_id)
            self._publications[migration_id] = publication
        try:
            await self._writeback(
                migration,
                "committed-reconciled",
                refreshed_context=refreshed,
            )
        except Exception as error:
            if self.evidence is not None:
                self.evidence.write_json(
                    "writeback/committed-reconciliation-error.json",
                    {
                        "error_type": type(error).__name__,
                        "retry": "reconcile_commit",
                    },
                )
                self._seal_evidence()
            raise CoordinationError(
                "final DataHub write-back reconciliation failed; COMMITTED state "
                "and unmerged candidates were retained for another retry"
            ) from error

        manifest: EvidenceManifest | None = None
        if self.evidence is not None:
            self.evidence.write_json(
                "publication/receipts.json",
                {
                    "auto_merge": False,
                    "receipts": [item.to_dict() for item in publication.receipts],
                },
            )
            self.evidence.capture_state(self.state, "05-committed-reconciled")
            manifest = self._seal_evidence()
        return CommitOutcome(
            migration=migration,
            participants=tuple(self.state.list_participants(migration_id)),
            publication=publication,
            manifest=manifest,
            upstream_change_safe_to_merge=True,
            auto_merged=False,
        )

    async def abort(self, migration_id: str, *, reason: str) -> AbortOutcome:
        migration = self.state.get_migration(migration_id)
        if migration.status in {MigrationStatus.COMMITTED, MigrationStatus.ABORTED}:
            raise CoordinationError(
                f"cannot abort terminal migration {migration.status.value}"
            )
        self.state.reserve_abort_cleanup(
            migration_id,
            expected_status=migration.status,
            expected_version=migration.version,
        )
        removed: list[str] = []
        branches: list[str] = []
        errors: list[str] = []
        sessions = [
            session
            for (known_migration, _), session in self._sessions.items()
            if known_migration == migration_id
        ]
        for session in sessions:
            try:
                self.worktrees.abort(session)
                removed.append(
                    session.worktree.relative_to(self.worktrees.root).as_posix()
                )
                branches.append(session.branch)
            except Exception as error:  # cleanup must audit every owned session.
                # Exception text can contain absolute checkout paths.  Abort
                # evidence records only the owned branch and exception class.
                errors.append(f"{session.branch}: {type(error).__name__}")
        cleanup = AbortCleanupReceipt(
            migration_id=migration_id,
            worktrees_removed=tuple(sorted(removed)),
            candidate_branches_deleted=tuple(sorted(branches)),
            cleanup_errors=tuple(sorted(errors)),
            deployed_systems_rolled_back=False,
        )
        if self.evidence is not None:
            self.evidence.write_json(
                "abort/cleanup.json",
                {
                    "reason_code": self._abort_reason_code(reason),
                    "receipt": cleanup.to_dict(),
                },
            )
        migration = self.state.abort_migration(
            migration_id,
            cleanup,
            expected_status=migration.status,
            expected_version=migration.version,
        )
        try:
            await self._writeback(migration, "aborted")
        except Exception as error:
            if self.evidence is not None:
                self.evidence.write_json(
                    "writeback/aborted-error.json",
                    {"error_type": type(error).__name__, "retry": "writeback-only"},
                )
                self._seal_evidence()
            raise CoordinationError(
                "candidate cleanup and ABORTED state succeeded, but DataHub write-back "
                f"requires reconcile_abort idempotent retry: {error}"
            ) from error
        manifest: EvidenceManifest | None = None
        if self.evidence is not None:
            self.evidence.capture_state(self.state, "99-aborted")
            manifest = self._seal_evidence()
        return AbortOutcome(migration, cleanup, manifest)

    async def reconcile_abort(self, migration_id: str) -> AbortOutcome:
        """Idempotently retry only the DataHub write-back for a durable ABORTED state."""

        migration = self.state.get_migration(migration_id)
        if migration.status is not MigrationStatus.ABORTED:
            raise CoordinationError(
                f"abort reconciliation requires ABORTED, got {migration.status.value}"
            )
        cleanup = self._abort_cleanup_from_state(migration_id)
        try:
            # Reuse the original stage name so a partial first attempt and its
            # retry converge on the same public evidence URL.
            await self._writeback(migration, "aborted")
        except Exception as error:
            if self.evidence is not None:
                self.evidence.write_json(
                    "writeback/aborted-reconciliation-error.json",
                    {
                        "error_type": type(error).__name__,
                        "retry": "reconcile_abort",
                    },
                )
                self._seal_evidence()
            raise CoordinationError(
                "ABORTED DataHub write-back reconciliation failed; local cleanup and "
                "terminal state were retained for another reconcile_abort retry"
            ) from error

        manifest: EvidenceManifest | None = None
        if self.evidence is not None:
            self.evidence.capture_state(self.state, "99-aborted-reconciled")
            manifest = self._seal_evidence()
        return AbortOutcome(migration, cleanup, manifest)

    def _proposal_request(
        self,
        intent: ChangeIntent,
        participant: Participant,
        session: RepositorySession,
        context: DataHubMigrationContext,
    ) -> ProposalRequest:
        expanded = tuple(field.field_path for field in context.source.schema)
        contract = tuple(item for item in expanded if item != intent.old_field)
        return proposal_request_from_session(
            session,
            intent=intent,
            participant=participant,
            expanded_columns=expanded,
            contract_columns=contract,
        )

    @staticmethod
    def _trusted_policy(request: ProposalRequest) -> TrustedParticipantPolicy:
        options: dict[str, str] = {}
        if request.participant.kind is ParticipantKind.DBT_SQL:
            snapshot = request.files[0]
            try:
                tree = parse_one(snapshot.content, read="duckdb")
            except ParseError as error:
                raise ProposalError(f"trusted dbt SQL is not parseable: {error}") from error
            relations = tuple(
                dict.fromkeys(item.name for item in tree.find_all(exp.Table) if item.name)
            )
            if len(relations) != 1:
                raise ProposalError("trusted dbt adapter requires exactly one relation")
            options = {"relation": relations[0], "dialect": "duckdb"}
        elif request.participant.kind is ParticipantKind.AIRFLOW_MAPPING:
            options = {
                "assignment_name": "FIELD_MAPPING",
                "config_key": "field_mapping",
            }
        return TrustedParticipantPolicy.from_records(
            request.intent,
            request.participant,
            expanded_columns=request.expanded_columns,
            contract_columns=request.contract_columns,
            **options,
        )

    def _run_adapter(
        self,
        session: RepositorySession,
        proposal: CandidateProposal,
        trusted: TrustedParticipantPolicy,
    ) -> PreparationResult:
        if isinstance(proposal, DbtSqlProposal):
            return self.dbt_adapter.prepare(session, proposal, trusted)
        if isinstance(proposal, AirflowMappingProposal):
            return self.airflow_adapter.prepare(session, proposal, trusted)
        if isinstance(proposal, SemanticMappingProposal):
            return self.semantic_adapter.prepare(session, proposal, trusted)
        raise CoordinationError(f"unsupported candidate type: {type(proposal).__name__}")

    def _commit_verified_candidate(
        self,
        participant: Participant,
        current: Participant,
        result: PreparationResult,
        session: RepositorySession,
        envelope: CandidateEnvelope,
    ) -> Participant:
        if result.participant_id != participant.participant_id:
            raise CoordinationError("adapter result belongs to another participant")
        if set(result.changed_files) != set(participant.files):
            raise CoordinationError("verified adapter result must change its exact allowlist")
        candidate_sha = self.worktrees.commit_candidate(
            session,
            allowed_paths=participant.files,
            message=(
                f"LineageTX {participant.kind.value}: "
                f"prepare {participant.participant_id}"
            ),
        )
        evidence_link = (
            f"{self.evidence_base_url}/{participant.migration_id}/"
            f"participants/{participant.participant_id}.json"
        )
        if self.evidence is not None:
            self.evidence.write_json(
                f"participants/{participant.participant_id}.json",
                {
                    "asset_urn": participant.asset_urn,
                    "candidate_branch": session.branch,
                    "candidate_commit_sha": candidate_sha,
                    "changed_files": list(result.changed_files),
                    "checks": list(result.checks),
                    "kind": participant.kind.value,
                    "migration_id": participant.migration_id,
                    "owner_urns": list(participant.owner_urns),
                    "participant_id": participant.participant_id,
                    "proposal_sha256": proposal_fingerprint(envelope),
                    "repository": participant.repository,
                    "unmerged": True,
                },
            )
        verified = self.state.transition_participant(
            participant.participant_id,
            ParticipantStatus.VERIFIED,
            expected_status=ParticipantStatus.PREPARING,
            expected_version=current.version,
            event_type="CANDIDATE_COMMITTED_AND_VERIFIED",
            payload={
                "candidate_commit_sha": candidate_sha,
                "checks": list(result.checks),
                "proposal_sha256": proposal_fingerprint(envelope),
                "unmerged": True,
            },
            candidate_commit_sha=candidate_sha,
            evidence_links=(evidence_link,),
        )
        if self.evidence is not None:
            self.evidence.write_text(
                f"diffs/{participant.participant_id}.patch",
                self.worktrees.diff(session),
            )
        return verified

    def _record_preparation(
        self,
        participant: Participant,
        envelope: CandidateEnvelope,
        trusted: TrustedParticipantPolicy,
        result: PreparationResult,
        session: RepositorySession,
        *,
        suffix: str = "initial",
    ) -> None:
        if self.evidence is None:
            return
        self.evidence.write_json(
            f"proposals/{participant.participant_id}.json",
            {
                "candidate": envelope.to_dict(),
                "candidate_sha256": proposal_fingerprint(envelope),
                "trusted_policy": asdict(trusted),
            },
        )
        self.evidence.write_json(
            f"verification/{participant.participant_id}-{suffix}.json",
            {
                "result": asdict(result),
                "base_checkout_unchanged": True,
                "candidate_branch": session.branch,
                "worktree_scope": "isolated-lineagetx-candidate",
            },
        )

    async def _abort_after_failure(
        self,
        migration_id: str,
        participant_id: str,
        error: Exception,
        phase: str,
    ) -> None:
        if participant_id:
            try:
                participant = self.state.get_participant(participant_id)
                if participant.status in {
                    ParticipantStatus.PREPARING,
                    ParticipantStatus.NEEDS_APPROVAL,
                }:
                    self.state.transition_participant(
                        participant_id,
                        ParticipantStatus.FAILED,
                        expected_status=participant.status,
                        expected_version=participant.version,
                        event_type="PARTICIPANT_SAFETY_CHECK_FAILED",
                        payload={"phase": phase, "error_type": type(error).__name__},
                    )
            except Exception:
                # abort_migration atomically moves every non-terminal participant
                # to ABORTED; a best-effort FAILED marker must not block cleanup.
                pass
        try:
            await self.abort(
                migration_id,
                reason=f"{phase}:{type(error).__name__}",
            )
        except Exception as cleanup_error:
            raise CoordinationError(
                f"{phase} failed ({error}); abort finalization also failed: "
                f"{cleanup_error}"
            ) from error
        raise CoordinationError(
            f"{phase} failed and unmerged candidates were aborted: {error}"
        ) from error

    @staticmethod
    def _abort_reason_code(reason: str) -> str:
        phase = reason.partition(":")[0].strip().lower()
        if phase in {"detect", "prepare", "approval", "commit"}:
            return f"{phase}_safety_failure"
        return "operator_requested"

    def _required_context(self, migration_id: str) -> DataHubMigrationContext:
        try:
            return self._contexts[migration_id]
        except KeyError as error:
            raise CoordinationError("frozen DataHub context is unavailable") from error

    def _required_discovery(self, migration_id: str) -> DiscoveryDecision:
        try:
            return self._discoveries[migration_id]
        except KeyError as error:
            raise CoordinationError("discovery decision is unavailable") from error

    def _publication_from_state(self, migration_id: str) -> PublicationResult:
        committed_event = next(
            (
                event
                for event in reversed(self.state.list_events(migration_id))
                if event.to_status is MigrationStatus.COMMITTED
            ),
            None,
        )
        if committed_event is None:
            raise CoordinationError("COMMITTED publication receipts are unavailable")
        raw_receipts = committed_event.payload.get("coordination_receipts")
        if not isinstance(raw_receipts, list):
            raise CoordinationError("COMMITTED event omitted publication receipts")
        try:
            receipts = tuple(
                CoordinationReceipt.from_dict(item)
                for item in raw_receipts
                if isinstance(item, Mapping)
            )
        except (KeyError, TypeError, ValueError) as error:
            raise CoordinationError("COMMITTED publication receipts are invalid") from error
        candidates = tuple(
            item
            for item in receipts
            if item.kind is CoordinationReceiptKind.CANDIDATE_COMMIT
        )
        coordinated = tuple(
            item
            for item in receipts
            if item.kind is CoordinationReceiptKind.COORDINATED_PR
        )
        gates = tuple(
            item
            for item in receipts
            if item.kind is CoordinationReceiptKind.PRODUCER_GATE_RELEASED
        )
        if len(candidates) != 3 or len(coordinated) != 1 or len(gates) != 1:
            raise CoordinationError("COMMITTED publication receipt set is incomplete")
        return PublicationResult(candidates, coordinated[0], gates[0])

    def _abort_cleanup_from_state(self, migration_id: str) -> AbortCleanupReceipt:
        aborted_event = next(
            (
                event
                for event in reversed(self.state.list_events(migration_id))
                if event.event_type == "MIGRATION_ABORTED"
                and event.to_status is MigrationStatus.ABORTED
            ),
            None,
        )
        if aborted_event is None:
            raise CoordinationError("ABORTED cleanup receipt is unavailable")
        raw = aborted_event.payload.get("cleanup")
        if not isinstance(raw, Mapping):
            raise CoordinationError("ABORTED event omitted its cleanup receipt")
        try:
            return AbortCleanupReceipt(
                migration_id=str(raw["migration_id"]),
                worktrees_removed=tuple(str(item) for item in raw["worktrees_removed"]),
                candidate_branches_deleted=tuple(
                    str(item) for item in raw["candidate_branches_deleted"]
                ),
                cleanup_errors=tuple(str(item) for item in raw["cleanup_errors"]),
                recorded_at=str(raw["recorded_at"]),
                deployed_systems_rolled_back=bool(
                    raw.get("deployed_systems_rolled_back", False)
                ),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise CoordinationError("ABORTED cleanup receipt is invalid") from error

    async def _writeback(
        self,
        migration: MigrationRecord,
        stage: str,
        *,
        refreshed_context: DataHubMigrationContext | None = None,
    ) -> WritebackReceipt | None:
        if self.migration_writer is None:
            return None
        context = self._required_context(migration.migration_id)
        participants = self.state.list_participants(migration.migration_id)
        if len(participants) != 3:
            raise CoordinationError("DataHub write-back requires exactly three consumers")
        # One stage receipt contains the exact source + three participant values
        # and the official MCP read-back proof. Every asset therefore points to
        # the same file that EvidenceRecorder actually emits and deploys.
        stage_evidence_url = (
            f"{self.evidence_base_url}/{migration.migration_id}/"
            f"writeback/{stage}.json"
        )
        writebacks: dict[str, MigrationWriteback] = {
            context.source_urn: MigrationWriteback(
                migration_id=migration.migration_id,
                status=migration.status.value,
                owner=context.source.governance.owner_urns[0],
                evidence_url=stage_evidence_url,
            )
        }
        for participant in participants:
            writebacks[participant.asset_urn] = MigrationWriteback(
                migration_id=migration.migration_id,
                status=participant.status.value,
                owner=participant.owner_urns[0],
                evidence_url=stage_evidence_url,
            )
        receipt = await self.migration_writer.write_assets(
            writebacks,
            refreshed_context=refreshed_context,
        )
        if self.evidence is not None:
            self.evidence.write_json(f"writeback/{stage}.json", receipt)
            self._seal_evidence()
        return receipt

    def _seal_evidence(self) -> EvidenceManifest | None:
        if self.evidence is None:
            return None
        manifest = self.evidence.build_manifest()
        self.evidence.verify_manifest()
        return manifest
