from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from data_lineage_fix_agent.lineagetx.models import (
    ChangeIntent,
    Participant,
    ParticipantKind,
    ParticipantStatus,
)
from data_lineage_fix_agent.lineagetx.participants import (
    CandidateRejected,
    OwnerApproval,
    SemanticApprovalParticipant,
    SemanticMappingProposal,
    TrustedParticipantPolicy,
)
from data_lineage_fix_agent.lineagetx.worktrees import WorktreeManager


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "fixtures/lineagetx/repos/analytics-governance"


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _setup(tmp_path: Path):
    repo = tmp_path / "analytics-governance"
    shutil.copytree(SOURCE, repo)
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "LineageTX Test")
    _git(repo, "config", "user.email", "lineagetx@example.invalid")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "fixture baseline")
    intent = ChangeIntent.create(
        producer_repository="commerce-producer",
        producer_pr_number=42,
        producer_base_sha="producer-base",
        producer_head_sha="producer-head",
        source_asset_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,commerce.orders,PROD)",
        old_field="customer_id",
        new_field="customer_key",
        contract_schema_fingerprint="sha256:contract",
    )
    participant = Participant.create(
        migration_id=intent.migration_id,
        kind=ParticipantKind.SEMANTIC_APPROVAL,
        asset_urn="urn:li:dataset:(urn:li:dataPlatform:semantic,customer_identity,PROD)",
        repository="analytics-governance",
        owner_urns=("urn:li:corpuser:identity-owner",),
        files=("semantic/customer_identity.json",),
    )
    manager = WorktreeManager(tmp_path / "worktrees")
    session = manager.prepare(
        repo_id="analytics-governance",
        base_repo=repo,
        migration_id=intent.migration_id,
    )
    relative = "semantic/customer_identity.json"
    before = (repo / relative).read_text(encoding="utf-8")
    proposal = SemanticMappingProposal(
        migration_id=session.migration_id,
        participant_id=participant.participant_id,
        relative_path=relative,
        expected_sha256=hashlib.sha256(before.encode()).hexdigest(),
        old_field="customer_id",
        new_field="customer_key",
        expected_occurrences=2,
        required_owner_urn="urn:li:corpuser:identity-owner",
        expanded_columns=("customer_id", "customer_key"),
        contract_columns=("customer_key",),
        repository=participant.repository,
        owner_urns=participant.owner_urns,
    )
    policy = TrustedParticipantPolicy.from_records(
        intent,
        participant,
        expanded_columns=proposal.expanded_columns,
        contract_columns=proposal.contract_columns,
    )
    approval = OwnerApproval(
        migration_id=session.migration_id,
        participant_id=proposal.participant_id,
        owner_urn=proposal.required_owner_urn,
        old_field=proposal.old_field,
        new_field=proposal.new_field,
        approved_at="2026-07-17T03:00:00Z",
        evidence_url="https://example.invalid/evidence/approval-001",
    )
    return repo, manager, session, proposal, policy, approval, before


def test_semantic_consumer_waits_for_owner_with_zero_writes(tmp_path: Path) -> None:
    repo, manager, session, proposal, policy, _, before = _setup(tmp_path)

    result = SemanticApprovalParticipant().prepare(session, proposal, policy)

    assert result.state is ParticipantStatus.NEEDS_APPROVAL
    assert result.changed_files == ()
    assert result.required_owner_urn == proposal.required_owner_urn
    assert manager.changed_files(session) == ()
    assert (repo / proposal.relative_path).read_text() == before
    assert (session.worktree / proposal.relative_path).read_text() == before


def test_semantic_rejects_wrong_owner_and_mapping_without_writing(
    tmp_path: Path,
) -> None:
    _, manager, session, proposal, policy, approval, _ = _setup(tmp_path)

    with pytest.raises(CandidateRejected, match="exact mapping"):
        SemanticApprovalParticipant().prepare(
            session,
            proposal,
            policy,
            replace(approval, owner_urn="urn:li:corpuser:not-the-owner"),
        )

    assert manager.changed_files(session) == ()


def test_semantic_applies_only_exact_owner_approved_mapping(tmp_path: Path) -> None:
    repo, manager, session, proposal, policy, approval, before = _setup(tmp_path)

    result = SemanticApprovalParticipant().prepare(
        session, proposal, policy, approval
    )

    assert result.state is ParticipantStatus.VERIFIED
    assert result.changed_files == (proposal.relative_path,)
    document = json.loads((session.worktree / proposal.relative_path).read_text())
    assert "customer_id" not in document["dimensions"]
    assert document["dimensions"]["customer_key"]["source_field"] == "customer_key"
    assert result.evidence["approved_by"] == proposal.required_owner_urn
    assert (repo / proposal.relative_path).read_text() == before
    assert manager.changed_files(session) == (proposal.relative_path,)


def test_semantic_rejects_model_selected_owner_and_path_before_writing(
    tmp_path: Path,
) -> None:
    _, manager, session, proposal, policy, _, _ = _setup(tmp_path)
    malicious = replace(
        proposal,
        relative_path="other/attacker.json",
        required_owner_urn="urn:li:corpuser:attacker",
        owner_urns=("urn:li:corpuser:attacker",),
    )

    with pytest.raises(CandidateRejected, match="trusted participant policy"):
        SemanticApprovalParticipant().prepare(session, malicious, policy)

    assert manager.changed_files(session) == ()
