from __future__ import annotations

import ast
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
    AirflowMappingParticipant,
    AirflowMappingProposal,
    CandidateRejected,
    TrustedParticipantPolicy,
)
from data_lineage_fix_agent.lineagetx.worktrees import WorktreeManager


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "fixtures/lineagetx/repos/data-platform"


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _setup(tmp_path: Path):
    repo = tmp_path / "data-platform"
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
        kind=ParticipantKind.AIRFLOW_MAPPING,
        asset_urn="urn:li:dataset:(urn:li:dataPlatform:airflow,customer_export,PROD)",
        repository="data-platform",
        owner_urns=("urn:li:corpuser:data-platform-owner",),
        files=(
            "airflow/dags/export_customers.py",
            "airflow/config/export_columns.json",
        ),
    )
    manager = WorktreeManager(tmp_path / "worktrees")
    session = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id=intent.migration_id,
    )
    python_path = "airflow/dags/export_customers.py"
    json_path = "airflow/config/export_columns.json"
    python_before = (repo / python_path).read_text(encoding="utf-8")
    json_before = (repo / json_path).read_text(encoding="utf-8")
    before_mapping = (
        ("customer_id", "customer_id"),
        ("order_id", "order_id"),
        ("total_amount", "total_amount"),
    )
    after_mapping = (
        ("customer_key", "customer_key"),
        ("order_id", "order_id"),
        ("total_amount", "total_amount"),
    )
    proposal = AirflowMappingProposal(
        migration_id=session.migration_id,
        participant_id=participant.participant_id,
        python_relative_path=python_path,
        json_relative_path=json_path,
        expected_python_sha256=hashlib.sha256(python_before.encode()).hexdigest(),
        expected_json_sha256=hashlib.sha256(json_before.encode()).hexdigest(),
        old_field="customer_id",
        new_field="customer_key",
        expected_mapping=before_mapping,
        proposed_mapping=after_mapping,
        expanded_columns=(
            "customer_id",
            "customer_key",
            "order_id",
            "total_amount",
        ),
        contract_columns=("customer_key", "order_id", "total_amount"),
        repository=participant.repository,
        owner_urns=participant.owner_urns,
    )
    policy = TrustedParticipantPolicy.from_records(
        intent,
        participant,
        expanded_columns=proposal.expanded_columns,
        contract_columns=proposal.contract_columns,
        assignment_name="FIELD_MAPPING",
        config_key="field_mapping",
    )
    return repo, manager, session, proposal, policy, python_before, json_before


def _python_mapping(source: str) -> dict[str, str]:
    tree = ast.parse(source)
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "FIELD_MAPPING"
    )
    return ast.literal_eval(assignment.value)


def test_airflow_updates_two_files_and_proves_consistency(tmp_path: Path) -> None:
    repo, manager, session, proposal, policy, python_before, json_before = _setup(
        tmp_path
    )

    result = AirflowMappingParticipant().prepare(session, proposal, policy)

    assert result.state is ParticipantStatus.VERIFIED
    assert result.changed_files == tuple(sorted(proposal.allowed_paths))
    python_after = (session.worktree / proposal.python_relative_path).read_text()
    json_after = json.loads(
        (session.worktree / proposal.json_relative_path).read_text()
    )["field_mapping"]
    assert _python_mapping(python_after) == json_after == dict(proposal.proposed_mapping)
    assert set(manager.changed_files(session)) == set(proposal.allowed_paths)
    assert (repo / proposal.python_relative_path).read_text() == python_before
    assert (repo / proposal.json_relative_path).read_text() == json_before
    assert "expanded_schema_mapping" in result.checks
    assert "contract_schema_mapping" in result.checks


def test_airflow_rejects_non_deterministic_model_mapping_with_zero_writes(
    tmp_path: Path,
) -> None:
    _, manager, session, proposal, policy, _, _ = _setup(tmp_path)
    unsafe = replace(
        proposal,
        proposed_mapping=(
            ("customer_key", "account_key"),
            ("order_id", "order_id"),
            ("total_amount", "total_amount"),
        ),
    )

    with pytest.raises(CandidateRejected, match="single deterministic"):
        AirflowMappingParticipant().prepare(session, unsafe, policy)

    assert manager.changed_files(session) == ()


def test_airflow_rejects_stale_second_file_hash_with_zero_writes(
    tmp_path: Path,
) -> None:
    _, manager, session, proposal, policy, _, _ = _setup(tmp_path)

    with pytest.raises(CandidateRejected, match="stale candidate"):
        AirflowMappingParticipant().prepare(
            session, replace(proposal, expected_json_sha256="0" * 64), policy
        )

    assert manager.changed_files(session) == ()


def test_airflow_rejects_model_selected_path_outside_trusted_registry(
    tmp_path: Path,
) -> None:
    _, manager, session, proposal, policy, _, _ = _setup(tmp_path)
    malicious = replace(
        proposal,
        python_relative_path="dbt/models/stg_orders.sql",
    )

    with pytest.raises(CandidateRejected, match="trusted participant policy"):
        AirflowMappingParticipant().prepare(session, malicious, policy)

    assert manager.changed_files(session) == ()


@pytest.mark.parametrize("failure_call", [1, 2])
def test_airflow_any_write_failure_restores_both_original_byte_sequences(
    tmp_path: Path,
    failure_call: int,
) -> None:
    _, manager, session, proposal, policy, python_before, json_before = _setup(
        tmp_path
    )
    calls = 0

    def injected_write_failure(path: Path, value: bytes) -> None:
        nonlocal calls
        calls += 1
        path.write_bytes(value)
        if calls == failure_call:
            raise OSError(f"injected write failure at call {failure_call}")

    adapter = AirflowMappingParticipant(file_writer=injected_write_failure)
    with pytest.raises(CandidateRejected, match="original byte sequences restored"):
        adapter.prepare(session, proposal, policy)

    assert (session.worktree / proposal.python_relative_path).read_bytes() == (
        python_before.encode("utf-8")
    )
    assert (session.worktree / proposal.json_relative_path).read_bytes() == (
        json_before.encode("utf-8")
    )
    assert manager.changed_files(session) == ()
