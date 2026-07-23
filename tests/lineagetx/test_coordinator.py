from __future__ import annotations

import asyncio
import json
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping

import pytest

from data_lineage_fix_agent.lineagetx.coordinator import CoordinationError
from data_lineage_fix_agent.lineagetx.github_approval import (
    GitHubApprovalExpectation,
    GitHubApprovalVerifier,
    VerifiedGitHubApproval,
)
from data_lineage_fix_agent.lineagetx.models import MigrationStatus, ParticipantStatus
from data_lineage_fix_agent.lineagetx.participants.semantic_approval import (
    OwnerApproval,
)
from data_lineage_fix_agent.lineagetx.policy import DiscoveryAttestation, PolicyViolation
from data_lineage_fix_agent.lineagetx.proposals import (
    CandidateEnvelope,
    DeterministicCandidateModel,
)
from data_lineage_fix_agent.lineagetx.publisher import (
    FixedGitHubPublisher,
    LocalReceiptPublisher,
    PublicationError,
)
from data_lineage_fix_agent.lineagetx.state import PublicationReconciliationRequired
from test_end_to_end import AIRFLOW, DBT, SEMANTIC, build_scenario


def _prepare_all_consumers(scenario: object) -> None:
    coordinator = getattr(scenario, "coordinator")
    intent = getattr(scenario, "intent")
    context = getattr(scenario, "context")
    participants = getattr(scenario, "participants")
    repos = getattr(scenario, "repos")
    asyncio.run(
        coordinator.detect(
            intent,
            DiscoveryAttestation(context, True),
            participants,
        )
    )
    first = asyncio.run(coordinator.prepare(intent.migration_id, repos))
    semantic_id = first.pending_approval_participant_ids[0]
    asyncio.run(
        coordinator.approve(
            intent.migration_id,
            OwnerApproval(
                migration_id=intent.migration_id,
                participant_id=semantic_id,
                owner_urn="urn:li:corpuser:identity-owner",
                old_field="customer_id",
                new_field="customer_key",
                approved_at="2026-07-17T01:03:00.000000Z",
                evidence_url="https://replay.lineagetx.invalid/approvals/owner.json",
            ),
        )
    )


class RepositoryTamperingProposalModel:
    """Returns typed data but tries to self-authorize a different repository."""

    def __init__(self) -> None:
        self.inner = DeterministicCandidateModel()

    def propose(self, request: object) -> CandidateEnvelope:
        envelope = self.inner.propose(request)  # type: ignore[arg-type]
        proposal = envelope.proposal
        if getattr(proposal, "repository", "") == "data-platform":
            proposal = replace(proposal, repository="attacker/untrusted-repository")
        return CandidateEnvelope("tampering-model", proposal)


def test_production_coordinator_rejects_caller_fabricated_approval_fields(
    tmp_path: Path,
) -> None:
    scenario = build_scenario(tmp_path)
    asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, True),
            scenario.participants,
        )
    )
    first_pass = asyncio.run(
        scenario.coordinator.prepare(scenario.intent.migration_id, scenario.repos)
    )
    semantic_id = first_pass.pending_approval_participant_ids[0]
    semantic = scenario.state.get_participant(semantic_id)
    forged = OwnerApproval(
        migration_id=scenario.intent.migration_id,
        participant_id=semantic_id,
        owner_urn=semantic.owner_urns[0],
        old_field=scenario.intent.old_field,
        new_field=scenario.intent.new_field,
        approved_at="2026-07-17T01:03:00.000000Z",
        evidence_url="https://github.com/example/looks-plausible",
    )
    scenario.coordinator.allow_test_approvals = False

    with pytest.raises(CoordinationError, match="caller-fabricated"):
        asyncio.run(
            scenario.coordinator.approve(scenario.intent.migration_id, forged)
        )

    assert (
        scenario.state.get_migration(scenario.intent.migration_id).status
        is MigrationStatus.NEEDS_APPROVAL
    )
    assert scenario.state.list_approvals(scenario.intent.migration_id) == []


def test_production_coordinator_applies_authenticated_github_owner_evidence(
    tmp_path: Path,
) -> None:
    scenario = build_scenario(tmp_path)
    asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, True),
            scenario.participants,
        )
    )
    first_pass = asyncio.run(
        scenario.coordinator.prepare(scenario.intent.migration_id, scenario.repos)
    )
    semantic_id = first_pass.pending_approval_participant_ids[0]
    semantic = scenario.state.get_participant(semantic_id)
    api_url = (
        "https://api.github.com/repos/Lukeknow0/data-lineage-fix-agent/"
        "issues/comments/42"
    )
    body = json.dumps(
        {
            "decision": "APPROVED",
            "migration_id": scenario.intent.migration_id,
            "participant_id": semantic_id,
            "owner_urn": semantic.owner_urns[0],
            "old_field": scenario.intent.old_field,
            "new_field": scenario.intent.new_field,
        }
    )

    class Transport:
        def get_json(self, url: str, *, token: str) -> dict[str, object]:
            assert url == api_url
            assert token == ""
            return {
                "id": 42,
                "node_id": "IC_lineagetx_42",
                "html_url": (
                    "https://github.com/Lukeknow0/data-lineage-fix-agent/"
                    "issues/7#issuecomment-42"
                ),
                "body": body,
                "created_at": "2026-07-17T02:00:00Z",
                "updated_at": "2026-07-17T02:00:00Z",
                "author_association": "MEMBER",
                "user": {"login": "identity-owner", "id": 314, "type": "User"},
            }

    scenario.coordinator.allow_test_approvals = False
    scenario.coordinator.approval_verifier = GitHubApprovalVerifier(
        owner_login_by_urn={semantic.owner_urns[0]: "identity-owner"},
        transport=Transport(),
        now=lambda: "2026-07-17T02:01:00Z",
    )

    approved = asyncio.run(
        scenario.coordinator.approve_from_github(
            scenario.intent.migration_id,
            api_url,
        )
    )

    assert approved.migration.status is MigrationStatus.PREPARED
    receipt = scenario.state.list_approvals(scenario.intent.migration_id)[0]
    assert receipt.verification["actor_login"] == "identity-owner"
    assert receipt.verification["source_api_url"] == api_url
    assert (
        scenario.evidence.root / "approval/github-verification.json"
    ).is_file()


def test_gate_release_rechecks_github_approval_and_aborts_if_evidence_changes(
    tmp_path: Path,
) -> None:
    scenario = build_scenario(tmp_path)
    asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, True),
            scenario.participants,
        )
    )
    first_pass = asyncio.run(
        scenario.coordinator.prepare(scenario.intent.migration_id, scenario.repos)
    )
    semantic = scenario.state.get_participant(
        first_pass.pending_approval_participant_ids[0]
    )
    api_url = (
        "https://api.github.com/repos/Lukeknow0/data-lineage-fix-agent/"
        "issues/comments/42"
    )

    class MutableEvidenceVerifier:
        calls = 0

        def verify(
            self,
            expected: GitHubApprovalExpectation,
            source_api_url: str,
        ) -> VerifiedGitHubApproval:
            self.calls += 1
            return VerifiedGitHubApproval(
                migration_id=expected.migration_id,
                participant_id=expected.participant_id,
                owner_urn=expected.owner_urn,
                old_field=expected.old_field,
                new_field=expected.new_field,
                approved_at="2026-07-17T02:00:00Z",
                evidence_url=source_api_url,
                browser_url=(
                    "https://github.com/Lukeknow0/data-lineage-fix-agent/"
                    "issues/7#issuecomment-42"
                ),
                source_api_url=source_api_url,
                resource_kind="issue_comment",
                resource_id=42,
                resource_node_id="IC_lineagetx_42",
                actor_login="identity-owner",
                actor_id=314,
                author_association="MEMBER",
                evidence_sha256=("a" if self.calls == 1 else "b") * 64,
                verified_at="2026-07-17T02:01:00Z",
            )

    verifier = MutableEvidenceVerifier()
    scenario.coordinator.allow_test_approvals = False
    scenario.coordinator.approval_verifier = verifier  # type: ignore[assignment]
    approved = asyncio.run(
        scenario.coordinator.approve_from_github(
            scenario.intent.migration_id,
            api_url,
        )
    )
    assert approved.migration.status is MigrationStatus.PREPARED

    with pytest.raises(CoordinationError, match="aborted"):
        asyncio.run(scenario.coordinator.commit(scenario.intent.migration_id))

    assert verifier.calls == 2
    assert (
        scenario.state.get_migration(scenario.intent.migration_id).status
        is MigrationStatus.ABORTED
    )


def test_typed_proposal_cannot_self_authorize_and_prepare_aborts_cleanly(
    tmp_path: Path,
) -> None:
    scenario = build_scenario(
        tmp_path,
        proposal_model=RepositoryTamperingProposalModel(),
    )
    asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, True),
            scenario.participants,
        )
    )

    with pytest.raises(CoordinationError, match="aborted"):
        asyncio.run(
            scenario.coordinator.prepare(scenario.intent.migration_id, scenario.repos)
        )

    migration = scenario.state.get_migration(scenario.intent.migration_id)
    participants = scenario.state.list_participants(scenario.intent.migration_id)
    assert migration.status is MigrationStatus.ABORTED
    assert all(item.status is ParticipantStatus.ABORTED for item in participants)
    for repository, repo in scenario.repos.items():
        assert repo.joinpath(".git").exists()
        assert scenario.base_shas[repository] == subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True
        ).strip()
        assert not subprocess.check_output(
            ["git", "-C", str(repo), "branch", "--list", "lineagetx/*"],
            text=True,
        ).strip()
    cleanup = scenario.evidence.root / "abort/cleanup.json"
    assert cleanup.is_file()
    assert scenario.evidence.verify_manifest().files


def test_invalid_owner_does_not_modify_semantic_candidate_and_manual_abort_is_scoped(
    tmp_path: Path,
) -> None:
    scenario = build_scenario(tmp_path)
    asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, True),
            scenario.participants,
        )
    )
    first_pass = asyncio.run(
        scenario.coordinator.prepare(scenario.intent.migration_id, scenario.repos)
    )
    semantic_id = first_pass.pending_approval_participant_ids[0]
    wrong_owner = OwnerApproval(
        migration_id=scenario.intent.migration_id,
        participant_id=semantic_id,
        owner_urn="urn:li:corpuser:not-the-owner",
        old_field="customer_id",
        new_field="customer_key",
        approved_at="2026-07-17T01:03:00.000000Z",
        evidence_url="https://replay.lineagetx.invalid/approvals/rejected.json",
    )

    with pytest.raises(PolicyViolation, match="not a discovered DataHub owner"):
        asyncio.run(
            scenario.coordinator.approve(scenario.intent.migration_id, wrong_owner)
        )

    assert scenario.state.get_migration(
        scenario.intent.migration_id
    ).status is MigrationStatus.NEEDS_APPROVAL
    semantic = scenario.state.get_participant(semantic_id)
    assert semantic.status is ParticipantStatus.NEEDS_APPROVAL
    assert semantic.candidate_commit_sha == ""
    abort = asyncio.run(
        scenario.coordinator.abort(
            scenario.intent.migration_id,
            reason="test operator cancelled pending semantic approval",
        )
    )
    assert abort.migration.status is MigrationStatus.ABORTED
    assert abort.deployed_systems_rolled_back is False
    assert abort.cleanup.deployed_systems_rolled_back is False
    assert abort.cleanup.cleanup_errors == ()
    assert len(abort.cleanup.candidate_branches_deleted) == 3


def test_impact_fingerprint_change_aborts_before_publisher_or_gate_release(
    tmp_path: Path,
) -> None:
    scenario = build_scenario(tmp_path)
    asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, True),
            scenario.participants,
        )
    )
    first_pass = asyncio.run(
        scenario.coordinator.prepare(scenario.intent.migration_id, scenario.repos)
    )
    semantic_id = first_pass.pending_approval_participant_ids[0]
    asyncio.run(
        scenario.coordinator.approve(
            scenario.intent.migration_id,
            OwnerApproval(
                migration_id=scenario.intent.migration_id,
                participant_id=semantic_id,
                owner_urn="urn:li:corpuser:identity-owner",
                old_field="customer_id",
                new_field="customer_key",
                approved_at="2026-07-17T01:03:00.000000Z",
                evidence_url="https://replay.lineagetx.invalid/approvals/owner.json",
            ),
        )
    )

    class ChangedImpact:
        async def assert_impact_unchanged(self, frozen: object) -> object:
            consumers = tuple(getattr(frozen, "consumers"))
            changed = (replace(consumers[0], degree="2"), *consumers[1:])
            return replace(frozen, consumers=changed)

    scenario.coordinator.impact_revalidator = ChangedImpact()  # type: ignore[assignment]
    with pytest.raises(CoordinationError, match="aborted"):
        asyncio.run(scenario.coordinator.commit(scenario.intent.migration_id))

    assert scenario.state.get_migration(
        scenario.intent.migration_id
    ).status is MigrationStatus.ABORTED
    assert not (scenario.evidence.root / "publication.json").exists()


def test_coordinator_writes_per_asset_states_and_final_verified_consumers(
    tmp_path: Path,
) -> None:
    class RecordingWriter:
        def __init__(self) -> None:
            self.calls: list[tuple[dict[str, object], object | None]] = []

        async def write_assets(
            self,
            writebacks: object,
            *,
            refreshed_context: object | None = None,
        ) -> dict[str, object]:
            assert isinstance(writebacks, dict)
            self.calls.append((dict(writebacks), refreshed_context))
            return {"verified": True, "assets": sorted(writebacks)}

    writer = RecordingWriter()
    scenario = build_scenario(tmp_path, migration_writer=writer)
    asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, True),
            scenario.participants,
        )
    )
    first = asyncio.run(
        scenario.coordinator.prepare(scenario.intent.migration_id, scenario.repos)
    )
    semantic_id = first.pending_approval_participant_ids[0]
    asyncio.run(
        scenario.coordinator.approve(
            scenario.intent.migration_id,
            OwnerApproval(
                migration_id=scenario.intent.migration_id,
                participant_id=semantic_id,
                owner_urn="urn:li:corpuser:identity-owner",
                old_field="customer_id",
                new_field="customer_key",
                approved_at="2026-07-17T01:03:00.000000Z",
                evidence_url="https://replay.lineagetx.invalid/approvals/owner.json",
            ),
        )
    )
    asyncio.run(scenario.coordinator.commit(scenario.intent.migration_id))

    assert len(writer.calls) == 6
    final_records, refreshed = writer.calls[-1]
    assert refreshed is not None
    statuses = {urn: getattr(record, "status") for urn, record in final_records.items()}
    assert statuses[scenario.context.source_urn] == "COMMITTED"
    assert {statuses[urn] for urn in (DBT, AIRFLOW, SEMANTIC)} == {"VERIFIED"}
    assert all(getattr(record, "owner").startswith("urn:li:") for record in final_records.values())
    evidence_urls = {
        getattr(record, "evidence_url") for record in final_records.values()
    }
    assert evidence_urls == {
        f"https://replay.lineagetx.invalid/evidence/{scenario.intent.migration_id}/"
        "writeback/committed.json"
    }
    assert (scenario.evidence.root / "writeback" / "committed.json").is_file()
    for participant in scenario.state.list_participants(scenario.intent.migration_id):
        assert participant.evidence_links == (
            f"https://replay.lineagetx.invalid/evidence/"
            f"{scenario.intent.migration_id}/participants/"
            f"{participant.participant_id}.json",
        )
        participant_file = (
            scenario.evidence.root
            / "participants"
            / f"{participant.participant_id}.json"
        )
        payload = json.loads(participant_file.read_text(encoding="utf-8"))
        assert payload["participant_id"] == participant.participant_id
        assert payload["candidate_commit_sha"] == participant.candidate_commit_sha
        assert payload["unmerged"] is True


def test_ref_rewrite_is_rejected_before_publication_and_aborted(tmp_path: Path) -> None:
    class CountingPublisher(LocalReceiptPublisher):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.stage_calls = 0

        def stage(self, request: object):
            self.stage_calls += 1
            return super().stage(request)  # type: ignore[arg-type]

    scenario = build_scenario(tmp_path)
    _prepare_all_consumers(scenario)
    publisher = CountingPublisher(scenario.evidence.root)
    scenario.coordinator.publisher = publisher
    session = next(iter(scenario.coordinator._sessions.values()))
    subprocess.run(
        ["git", "-C", str(session.worktree), "reset", "--hard", session.base_sha],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    with pytest.raises(CoordinationError, match="aborted"):
        asyncio.run(scenario.coordinator.commit(scenario.intent.migration_id))

    assert publisher.stage_calls == 0
    assert scenario.state.get_migration(
        scenario.intent.migration_id
    ).status is MigrationStatus.ABORTED


def test_success_gate_with_lost_response_retains_candidates_then_reconciles(
    tmp_path: Path,
) -> None:
    class LostResponseAPI:
        def __init__(self) -> None:
            self.persisted_status: Mapping[str, Any] | None = None
            self.status_creations = 0

        def create_pull_request(self, repository: str, **_: Any) -> Mapping[str, Any]:
            assert repository == "acme/coordination"
            return {
                "html_url": "https://github.com/acme/coordination/pull/9",
                "draft": True,
                "merged": False,
            }

        def create_commit_status(
            self,
            repository: str,
            sha: str,
            **kwargs: Any,
        ) -> Mapping[str, Any]:
            assert repository == "acme/commerce-producer"
            self.status_creations += 1
            self.persisted_status = {
                "context": kwargs["context"],
                "state": "success",
                "target_url": kwargs["target_url"],
            }
            raise PublicationError("response lost after success was persisted")

        def get_commit_status(
            self,
            repository: str,
            sha: str,
            *,
            context: str,
        ) -> Mapping[str, Any] | None:
            assert repository == "acme/commerce-producer"
            assert context == "lineagetx/safe-to-contract"
            return self.persisted_status

    scenario = build_scenario(tmp_path)
    _prepare_all_consumers(scenario)
    api = LostResponseAPI()
    scenario.coordinator.publisher = FixedGitHubPublisher(
        api,  # type: ignore[arg-type]
        coordination_repository="acme/coordination",
        producer_repository="acme/commerce-producer",
    )

    with pytest.raises(CoordinationError, match="retained"):
        asyncio.run(scenario.coordinator.commit(scenario.intent.migration_id))

    assert scenario.state.get_migration(
        scenario.intent.migration_id
    ).status is MigrationStatus.PREPARED
    assert not (scenario.evidence.root / "abort/cleanup.json").exists()
    assert all(session.worktree.exists() for session in scenario.coordinator._sessions.values())
    with pytest.raises(PublicationReconciliationRequired, match="cleanup is forbidden"):
        asyncio.run(
            scenario.coordinator.abort(
                scenario.intent.migration_id,
                reason="operator tried to abort after an uncertain gate response",
            )
        )
    assert all(session.worktree.exists() for session in scenario.coordinator._sessions.values())

    outcome = asyncio.run(scenario.coordinator.commit(scenario.intent.migration_id))
    assert outcome.migration.status is MigrationStatus.COMMITTED
    assert api.status_creations == 1


def test_final_writeback_failure_can_be_retried_without_republishing_or_abort(
    tmp_path: Path,
) -> None:
    class FailFinalOnceWriter:
        def __init__(self) -> None:
            self.failed = False
            self.committed_calls = 0

        async def write_assets(
            self,
            writebacks: object,
            *,
            refreshed_context: object | None = None,
        ) -> Mapping[str, Any]:
            assert isinstance(writebacks, dict)
            if any(getattr(item, "status") == "COMMITTED" for item in writebacks.values()):
                self.committed_calls += 1
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("transient final writeback failure")
            return {"verified": True, "assets": sorted(writebacks)}

    writer = FailFinalOnceWriter()
    scenario = build_scenario(tmp_path, migration_writer=writer)
    _prepare_all_consumers(scenario)

    with pytest.raises(CoordinationError, match="idempotent retry"):
        asyncio.run(scenario.coordinator.commit(scenario.intent.migration_id))
    assert scenario.state.get_migration(
        scenario.intent.migration_id
    ).status is MigrationStatus.COMMITTED
    assert not (scenario.evidence.root / "abort/cleanup.json").exists()

    recovered = asyncio.run(
        scenario.coordinator.reconcile_commit(scenario.intent.migration_id)
    )
    assert recovered.migration.status is MigrationStatus.COMMITTED
    assert writer.committed_calls == 2


def test_aborted_writeback_failure_has_explicit_idempotent_reconciliation(
    tmp_path: Path,
) -> None:
    class FailAbortedOnceWriter:
        def __init__(self) -> None:
            self.aborted_calls = 0

        async def write_assets(
            self,
            writebacks: object,
            *,
            refreshed_context: object | None = None,
        ) -> Mapping[str, Any]:
            assert isinstance(writebacks, dict)
            if any(getattr(item, "status") == "ABORTED" for item in writebacks.values()):
                self.aborted_calls += 1
                if self.aborted_calls == 1:
                    raise RuntimeError("transient aborted writeback failure")
            return {"verified": True, "assets": sorted(writebacks)}

    writer = FailAbortedOnceWriter()
    scenario = build_scenario(tmp_path, migration_writer=writer)
    asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, True),
            scenario.participants,
        )
    )

    with pytest.raises(CoordinationError, match="reconcile_abort"):
        asyncio.run(
            scenario.coordinator.abort(
                scenario.intent.migration_id,
                reason="operator cancelled before preparation",
            )
        )
    assert (
        scenario.state.get_migration(scenario.intent.migration_id).status
        is MigrationStatus.ABORTED
    )

    recovered = asyncio.run(
        scenario.coordinator.reconcile_abort(scenario.intent.migration_id)
    )
    assert recovered.migration.status is MigrationStatus.ABORTED
    assert recovered.cleanup.migration_id == scenario.intent.migration_id
    assert recovered.cleanup.cleanup_errors == ()
    assert writer.aborted_calls == 2
    assert (scenario.evidence.root / "writeback" / "aborted.json").is_file()
    assert (
        scenario.evidence.root / "state" / "99-aborted-reconciled.json"
    ).is_file()


def test_abort_evidence_never_records_operator_absolute_path(tmp_path: Path) -> None:
    scenario = build_scenario(tmp_path)
    asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, True),
            scenario.participants,
        )
    )
    private_path = str(tmp_path / "private" / "checkout")
    asyncio.run(
        scenario.coordinator.abort(
            scenario.intent.migration_id,
            reason=f"operator cancelled after error in {private_path}",
        )
    )
    payload = json.loads(
        (scenario.evidence.root / "abort/cleanup.json").read_text(encoding="utf-8")
    )
    assert payload["reason_code"] == "operator_requested"
    assert private_path not in json.dumps(payload)
