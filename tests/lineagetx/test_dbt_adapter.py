from __future__ import annotations

import hashlib
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
    DbtSqlParticipant,
    DbtSqlProposal,
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


def _setup(tmp_path: Path, *, sql_override: str | None = None):
    repo = tmp_path / "data-platform"
    shutil.copytree(SOURCE, repo)
    if sql_override is not None:
        (repo / "dbt/models/stg_orders.sql").write_text(
            sql_override, encoding="utf-8"
        )
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
        kind=ParticipantKind.DBT_SQL,
        asset_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.stg_orders,PROD)",
        repository="data-platform",
        owner_urns=("urn:li:corpuser:data-platform-owner",),
        files=("dbt/models/stg_orders.sql",),
    )
    manager = WorktreeManager(tmp_path / "worktrees")
    session = manager.prepare(
        repo_id="data-platform",
        base_repo=repo,
        migration_id=intent.migration_id,
    )
    relative = "dbt/models/stg_orders.sql"
    before = (repo / relative).read_text(encoding="utf-8")
    proposal = DbtSqlProposal(
        migration_id=session.migration_id,
        participant_id=participant.participant_id,
        relative_path=relative,
        expected_sha256=hashlib.sha256(before.encode()).hexdigest(),
        old_field="customer_id",
        new_field="customer_key",
        expected_occurrences=1,
        relation="raw_orders",
        expanded_columns=(
            "order_id",
            "customer_id",
            "customer_key",
            "total_amount",
        ),
        contract_columns=("order_id", "customer_key", "total_amount"),
        repository=participant.repository,
        owner_urns=participant.owner_urns,
    )
    policy = TrustedParticipantPolicy.from_records(
        intent,
        participant,
        expanded_columns=proposal.expanded_columns,
        contract_columns=proposal.contract_columns,
        relation="raw_orders",
        dialect="duckdb",
    )
    return repo, manager, session, proposal, policy, before


def test_dbt_candidate_passes_ast_hash_allowlist_and_both_schemas(
    tmp_path: Path,
) -> None:
    repo, manager, session, proposal, policy, before = _setup(tmp_path)

    result = DbtSqlParticipant().prepare(session, proposal, policy)

    assert result.state is ParticipantStatus.VERIFIED
    assert result.changed_files == (proposal.relative_path,)
    assert "expanded_schema_compile" in result.checks
    assert "contract_schema_compile" in result.checks
    assert "customer_key" in (
        session.worktree / proposal.relative_path
    ).read_text(encoding="utf-8")
    assert (repo / proposal.relative_path).read_text(encoding="utf-8") == before
    assert manager.changed_files(session) == (proposal.relative_path,)


def test_dbt_rejects_stale_hash_without_writing(tmp_path: Path) -> None:
    _, manager, session, proposal, policy, _ = _setup(tmp_path)

    with pytest.raises(CandidateRejected, match="stale candidate"):
        DbtSqlParticipant().prepare(
            session, replace(proposal, expected_sha256="0" * 64), policy
        )

    assert manager.changed_files(session) == ()


def test_dbt_rejects_ast_occurrence_drift_without_writing(tmp_path: Path) -> None:
    _, manager, session, proposal, policy, _ = _setup(tmp_path)

    with pytest.raises(CandidateRejected, match="occurrence count"):
        DbtSqlParticipant().prepare(
            session, replace(proposal, expected_occurrences=2), policy
        )

    assert manager.changed_files(session) == ()


def test_dbt_rejects_contract_that_still_contains_old_field(tmp_path: Path) -> None:
    _, manager, session, proposal, policy, _ = _setup(tmp_path)

    with pytest.raises(CandidateRejected, match="contract schema"):
        DbtSqlParticipant().prepare(
            session,
            replace(
                proposal,
                contract_columns=(
                    "order_id",
                    "customer_id",
                    "customer_key",
                    "total_amount",
                ),
            ),
            policy,
        )

    assert manager.changed_files(session) == ()


@pytest.mark.parametrize(
    "changes",
    [
        {"migration_id": "ltx-attacker"},
        {"participant_id": "consumer-attacker"},
        {"repository": "attacker/repository"},
        {"relative_path": "airflow/dags/export_customers.py"},
        {"old_field": "order_id"},
        {"new_field": "attacker_key"},
        {"expanded_columns": ("customer_id", "customer_key", "attacker")},
        {"contract_columns": ("customer_key", "attacker")},
        {"relation": "attacker_relation"},
        {"owner_urns": ("urn:li:corpuser:attacker",)},
        {"dialect": "sqlite"},
    ],
)
def test_dbt_rejects_every_model_controlled_registry_binding(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    _, manager, session, proposal, policy, _ = _setup(tmp_path)

    with pytest.raises(CandidateRejected, match="trusted participant policy"):
        DbtSqlParticipant().prepare(session, replace(proposal, **changes), policy)

    assert manager.changed_files(session) == ()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT customer_id FROM raw_orders; SELECT customer_id FROM raw_orders\n",
        "DELETE FROM raw_orders WHERE customer_id = 1\n",
        "SELECT customer_id FROM read_csv('https://attacker.invalid/data.csv')\n",
        "SELECT read_blob('/etc/passwd'), customer_id FROM raw_orders\n",
        "SELECT customer_id FROM other_relation\n",
        "SELECT customer_id FROM raw_orders JOIN other_relation USING (customer_id)\n",
    ],
)
def test_dbt_rejects_non_bounded_or_external_sql(
    tmp_path: Path,
    sql: str,
) -> None:
    _, manager, session, proposal, policy, _ = _setup(
        tmp_path, sql_override=sql
    )

    with pytest.raises(CandidateRejected):
        DbtSqlParticipant().prepare(session, proposal, policy)

    assert manager.changed_files(session) == ()
