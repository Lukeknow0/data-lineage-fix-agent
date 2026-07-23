from __future__ import annotations

import asyncio
import hashlib
import shutil
import subprocess
from dataclasses import dataclass, replace
from pathlib import Path

from data_lineage_fix_agent.lineagetx.coordinator import LineageTXCoordinator
from data_lineage_fix_agent.lineagetx.datahub_context import (
    AssetSnapshot,
    ConsumerLineage,
    DataHubMigrationContext,
    GovernanceSnapshot,
    SchemaFieldSnapshot,
)
from data_lineage_fix_agent.lineagetx.evidence import EvidenceRecorder
from data_lineage_fix_agent.lineagetx.models import (
    ChangeIntent,
    CoordinationReceiptKind,
    MigrationStatus,
    Participant,
    ParticipantKind,
    ParticipantStatus,
)
from data_lineage_fix_agent.lineagetx.participants.semantic_approval import (
    OwnerApproval,
)
from data_lineage_fix_agent.lineagetx.policy import DiscoveryAttestation
from data_lineage_fix_agent.lineagetx.proposals import DeterministicCandidateModel
from data_lineage_fix_agent.lineagetx.publisher import LocalReceiptPublisher
from data_lineage_fix_agent.lineagetx.state import SQLiteStateStore
from data_lineage_fix_agent.lineagetx.worktrees import WorktreeManager


ROOT = Path(__file__).resolve().parents[2]
FIXTURE_REPOS = ROOT / "fixtures" / "lineagetx" / "repos"
CREATED_AT = "2026-07-17T01:02:03.000000Z"
SOURCE = "urn:li:dataset:(urn:li:dataPlatform:postgres,commerce.orders,PROD)"
DBT = "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.stg_orders,PROD)"
AIRFLOW = "urn:li:dataset:(urn:li:dataPlatform:airflow,customer_export,PROD)"
SEMANTIC = "urn:li:dataset:(urn:li:dataPlatform:semantic,customer_identity,PROD)"


def _git(repo: Path, *args: str, check: bool = True) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if check:
        assert completed.returncode == 0, completed.stdout
    return completed.stdout.strip()


def _repository(tmp_path: Path, name: str) -> Path:
    repo = tmp_path / "repositories" / name
    shutil.copytree(FIXTURE_REPOS / name, repo)
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "LineageTX Test")
    _git(repo, "config", "user.email", "lineagetx@example.invalid")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "fixture baseline")
    return repo


def _field(name: str) -> SchemaFieldSnapshot:
    return SchemaFieldSnapshot(name, "BIGINT", False, f"Fixture {name}")


def _governance(owner: str, *, source: bool = False) -> GovernanceSnapshot:
    return GovernanceSnapshot(
        owner_urns=(owner,),
        tag_urns=("urn:li:tag:Tier1",) if source else (),
        structured_properties={},
    )


def _path(target: str) -> dict[str, object]:
    return {
        "pathCount": 1,
        "paths": [
            {
                "path": [
                    {"urn": f"{SOURCE}.customer_id", "type": "SCHEMA_FIELD"},
                    {"urn": f"{target}.customer_id", "type": "SCHEMA_FIELD"},
                ]
            }
        ],
    }


def _context() -> DataHubMigrationContext:
    source = AssetSnapshot(
        SOURCE,
        (
            _field("order_id"),
            _field("customer_id"),
            _field("customer_key"),
            _field("total_amount"),
        ),
        _governance("urn:li:corpuser:commerce-owner", source=True),
    )
    consumer_values = (
        ConsumerLineage(DBT, "1", ("customer_id",), (_path(DBT),)),
        ConsumerLineage(AIRFLOW, "2", ("customer_id",), (_path(AIRFLOW),)),
        ConsumerLineage(SEMANTIC, "3+", ("customer_id",), (_path(SEMANTIC),)),
    )
    assets = {
        SOURCE: source,
        DBT: AssetSnapshot(
            DBT,
            (_field("customer_id"),),
            _governance("urn:li:corpuser:data-platform-owner"),
        ),
        AIRFLOW: AssetSnapshot(
            AIRFLOW,
            (_field("customer_id"),),
            _governance("urn:li:corpuser:data-platform-owner"),
        ),
        SEMANTIC: AssetSnapshot(
            SEMANTIC,
            (_field("customer_id"),),
            _governance("urn:li:corpuser:identity-owner"),
        ),
    }
    traces = tuple(
        [{"tool": "list_schema_fields", "arguments": {"urn": urn}} for urn in assets]
        + [{"tool": "get_lineage", "arguments": {"max_hops": 3}}]
        + [{"tool": "get_entities", "arguments": {"urns": list(assets)}}]
        + [
            {"tool": "get_lineage_paths_between", "arguments": {"target_urn": urn}}
            for urn in (DBT, AIRFLOW, SEMANTIC)
        ]
    )
    return DataHubMigrationContext(
        source_urn=SOURCE,
        source_column="customer_id",
        replacement_column="customer_key",
        source=source,
        consumers=consumer_values,
        assets=assets,
        tool_traces=traces,
        transport="datahub-oss+official-mcp-server-datahub==0.6.0",
        discovery_complete=True,
    )


class FrozenImpactRevalidator:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def assert_impact_unchanged(
        self, frozen: DataHubMigrationContext
    ) -> DataHubMigrationContext:
        self.calls.append(frozen.impact_fingerprint)
        # A refresh must be a newly read immutable context, not the original
        # object handed back by a stub.
        return replace(frozen)


@dataclass
class Scenario:
    coordinator: LineageTXCoordinator
    state: SQLiteStateStore
    intent: ChangeIntent
    participants: tuple[Participant, ...]
    context: DataHubMigrationContext
    repos: dict[str, Path]
    base_shas: dict[str, str]
    base_contents: dict[str, bytes]
    evidence: EvidenceRecorder
    revalidator: FrozenImpactRevalidator
    worktrees: WorktreeManager


def build_scenario(
    tmp_path: Path,
    *,
    proposal_model: object | None = None,
    migration_writer: object | None = None,
) -> Scenario:
    repos = {
        "data-platform": _repository(tmp_path, "data-platform"),
        "analytics-governance": _repository(tmp_path, "analytics-governance"),
    }
    contract = (
        ROOT / "fixtures/lineagetx/producer/schema.contract.json"
    ).read_bytes()
    intent = ChangeIntent.create(
        producer_repository="acme/commerce-producer",
        producer_pr_number=42,
        producer_base_sha="1" * 40,
        producer_head_sha="2" * 40,
        source_asset_urn=SOURCE,
        old_field="customer_id",
        new_field="customer_key",
        contract_schema_fingerprint=hashlib.sha256(contract).hexdigest(),
        producer_pr_url="https://github.com/acme/commerce-producer/pull/42",
        created_at=CREATED_AT,
    )
    kinds = (
        ParticipantKind.DBT_SQL,
        ParticipantKind.AIRFLOW_MAPPING,
        ParticipantKind.SEMANTIC_APPROVAL,
    )
    assets = (DBT, AIRFLOW, SEMANTIC)
    repositories = ("data-platform", "data-platform", "analytics-governance")
    owners = (
        "urn:li:corpuser:data-platform-owner",
        "urn:li:corpuser:data-platform-owner",
        "urn:li:corpuser:identity-owner",
    )
    files = (
        ("dbt/models/stg_orders.sql",),
        (
            "airflow/dags/export_customers.py",
            "airflow/config/export_columns.json",
        ),
        ("semantic/customer_identity.json",),
    )
    participants = tuple(
        Participant.create(
            migration_id=intent.migration_id,
            kind=kind,
            asset_urn=asset,
            repository=repository,
            owner_urns=(owner,),
            files=paths,
            base_sha=_git(repos[repository], "rev-parse", "HEAD"),
            created_at=CREATED_AT,
        )
        for kind, asset, repository, owner, paths in zip(
            kinds, assets, repositories, owners, files, strict=True
        )
    )
    evidence = EvidenceRecorder(tmp_path / "evidence", intent.migration_id)
    state = SQLiteStateStore(tmp_path / "lineagetx.sqlite3")
    worktrees = WorktreeManager(tmp_path / "isolated-worktrees")
    revalidator = FrozenImpactRevalidator()
    coordinator = LineageTXCoordinator(
        state=state,
        worktrees=worktrees,
        proposal_model=proposal_model or DeterministicCandidateModel(),
        publisher=LocalReceiptPublisher(evidence.root),
        impact_revalidator=revalidator,
        migration_writer=migration_writer,  # type: ignore[arg-type]
        evidence=evidence,
        allow_test_approvals=True,
    )
    tracked_paths = (
        "dbt/models/stg_orders.sql",
        "airflow/dags/export_customers.py",
        "airflow/config/export_columns.json",
        "semantic/customer_identity.json",
    )
    base_contents: dict[str, bytes] = {}
    for repository, repo in repos.items():
        for relative in tracked_paths:
            target = repo / relative
            if target.is_file():
                base_contents[f"{repository}:{relative}"] = target.read_bytes()
    return Scenario(
        coordinator=coordinator,
        state=state,
        intent=intent,
        participants=participants,
        context=_context(),
        repos=repos,
        base_shas={name: _git(repo, "rev-parse", "HEAD") for name, repo in repos.items()},
        base_contents=base_contents,
        evidence=evidence,
        revalidator=revalidator,
        worktrees=worktrees,
    )


def test_fixture_transaction_forks_waits_for_owner_and_converges_without_merge(
    tmp_path: Path,
) -> None:
    scenario = build_scenario(tmp_path)

    detected = asyncio.run(
        scenario.coordinator.detect(
            scenario.intent,
            DiscoveryAttestation(scenario.context, discovery_complete=True),
            scenario.participants,
        )
    )
    prepared_first_pass = asyncio.run(
        scenario.coordinator.prepare(scenario.intent.migration_id, scenario.repos)
    )

    assert detected.status is MigrationStatus.DETECTED
    assert prepared_first_pass.migration.status is MigrationStatus.NEEDS_APPROVAL
    assert len(prepared_first_pass.pending_approval_participant_ids) == 1
    by_kind = {item.kind: item for item in prepared_first_pass.participants}
    assert by_kind[ParticipantKind.DBT_SQL].status is ParticipantStatus.VERIFIED
    assert by_kind[ParticipantKind.AIRFLOW_MAPPING].status is ParticipantStatus.VERIFIED
    semantic = by_kind[ParticipantKind.SEMANTIC_APPROVAL]
    assert semantic.status is ParticipantStatus.NEEDS_APPROVAL
    assert semantic.candidate_commit_sha == ""
    assert (
        prepared_first_pass.results[semantic.participant_id].changed_files == ()
    )

    approval = OwnerApproval(
        migration_id=scenario.intent.migration_id,
        participant_id=semantic.participant_id,
        owner_urn="urn:li:corpuser:identity-owner",
        old_field="customer_id",
        new_field="customer_key",
        approved_at="2026-07-17T01:03:00.000000Z",
        evidence_url="https://replay.lineagetx.invalid/approvals/identity-owner.json",
    )
    approved = asyncio.run(
        scenario.coordinator.approve(scenario.intent.migration_id, approval)
    )
    committed = asyncio.run(
        scenario.coordinator.commit(scenario.intent.migration_id)
    )

    assert approved.migration.status is MigrationStatus.PREPARED
    assert all(item.status is ParticipantStatus.VERIFIED for item in approved.participants)
    assert all(len(item.candidate_commit_sha) == 40 for item in approved.participants)
    assert committed.migration.status is MigrationStatus.COMMITTED
    assert committed.upstream_change_safe_to_merge is True
    assert committed.auto_merged is False
    assert len(committed.publication.candidate_receipts) == 3
    assert {
        item.commit_sha for item in committed.publication.candidate_receipts
    } == {item.candidate_commit_sha for item in approved.participants}
    assert committed.publication.coordinated_pr_receipt.kind is (
        CoordinationReceiptKind.COORDINATED_PR
    )
    assert committed.publication.producer_gate_receipt.kind is (
        CoordinationReceiptKind.PRODUCER_GATE_RELEASED
    )
    assert scenario.revalidator.calls == [scenario.context.impact_fingerprint]

    for repository, repo in scenario.repos.items():
        assert _git(repo, "rev-parse", "HEAD") == scenario.base_shas[repository]
        assert _git(repo, "status", "--porcelain=v1") == ""
    for key, expected in scenario.base_contents.items():
        repository, relative = key.split(":", 1)
        assert (scenario.repos[repository] / relative).read_bytes() == expected
    for participant in approved.participants:
        repo = scenario.repos[participant.repository]
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "merge-base",
                "--is-ancestor",
                participant.candidate_commit_sha,
                "main",
            ],
            check=False,
        )
        assert completed.returncode != 0

    manifest = scenario.evidence.verify_manifest()
    evidence_paths = {item.path for item in manifest.files}
    assert "context/datahub.json" in evidence_paths
    assert "approval/owner-receipt.json" in evidence_paths
    assert "publication.json" in evidence_paths
    assert "publication/receipts.json" in evidence_paths
    assert len([path for path in evidence_paths if path.startswith("diffs/")]) == 3
    final_events = scenario.state.list_events(scenario.intent.migration_id)
    assert final_events[-1].to_status is MigrationStatus.COMMITTED
    assert final_events[-1].payload["unverified_consumers"] == 0
