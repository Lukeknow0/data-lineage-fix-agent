from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping

from .agent import DataLineageFixAgent
from .context import FixtureContextProvider, OfflineReplayStatusWriter
from .evidence import EvidenceWriter


LINEAGETX_REPLAY_MARKER = ".lineagetx-local-replay.json"
LINEAGETX_REPLAY_KIND = "lineagetx-local-replay"


class LineageTXReplayError(RuntimeError):
    """The local replay fixture could not be materialized safely."""


@dataclass(frozen=True)
class LineageTXFixtureRuntime:
    root: Path
    intent: Any
    participants: tuple[Any, ...]
    context: Any
    repositories: Mapping[str, Path]
    base_shas: Mapping[str, str]
    coordinator: Any
    state: Any
    evidence: Any


class _FrozenFixtureImpactRevalidator:
    """Returns a fresh immutable view of the frozen fixture impact set."""

    async def assert_impact_unchanged(self, frozen: Any) -> Any:
        return replace(frozen)


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_lineagetx_work_root() -> Path:
    return project_root() / "artifacts" / "runs" / "lineagetx-local-replay"


def prepare_workspace(root: Path) -> tuple[Path, dict]:
    mapping = json.loads(
        (root / "fixture_pipeline" / "project_mapping.json").read_text(encoding="utf-8")
    )
    workspace = root / "fixture_pipeline" / "workspace"
    if workspace.exists():
        shutil.rmtree(workspace)
    shutil.copytree(root / "fixture_pipeline" / "broken", workspace)
    return workspace, mapping


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise LineageTXReplayError(f"required fixture is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise LineageTXReplayError(f"fixture is not valid JSON: {path}: {error}") from error
    if not isinstance(value, dict):
        raise LineageTXReplayError(f"fixture must contain a JSON object: {path}")
    return value


def _safe_fixture_path(root: Path, value: str) -> Path:
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise LineageTXReplayError(f"unsafe fixture path: {value!r}")
    candidate = root
    for part in relative.parts:
        candidate = candidate / part
        if candidate.is_symlink():
            raise LineageTXReplayError(f"fixture path may not use symlinks: {value!r}")
    target = candidate.resolve()
    if not target.is_relative_to(root.resolve()):
        raise LineageTXReplayError(f"fixture path escaped its root: {value!r}")
    return target


def _prepare_lineagetx_work_root(
    project: Path,
    work_root: Path,
    *,
    reset: bool,
) -> Path:
    unresolved = work_root.expanduser()
    if unresolved.is_symlink():
        raise LineageTXReplayError("replay work root may not be a symlink")
    root = unresolved.resolve()
    if root in {project.resolve(), (project / "fixtures" / "lineagetx").resolve()}:
        raise LineageTXReplayError("replay work root must not replace project fixtures")

    if root.exists() and any(root.iterdir()):
        if not reset:
            raise LineageTXReplayError(
                f"replay work root is not empty: {root}; pass --reset to replay"
            )
        marker = root / LINEAGETX_REPLAY_MARKER
        if not marker.is_file() or marker.is_symlink():
            raise LineageTXReplayError(
                f"refusing to reset a directory not owned by LineageTX: {root}"
            )
        marker_value = _read_json(marker)
        expected = {
            "kind": LINEAGETX_REPLAY_KIND,
            "project_root": str(project.resolve()),
            "schema_version": 1,
        }
        if marker_value != expected:
            raise LineageTXReplayError(
                f"refusing to reset a directory not owned by LineageTX: {root}"
            )
        shutil.rmtree(root)
    elif root.exists() and reset:
        root.rmdir()

    root.mkdir(parents=True, exist_ok=False)
    marker_value = {
        "kind": LINEAGETX_REPLAY_KIND,
        "project_root": str(project.resolve()),
        "schema_version": 1,
    }
    (root / LINEAGETX_REPLAY_MARKER).write_text(
        json.dumps(marker_value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return root


def _fixture_git(repo: Path, *args: str, created_at: str = "") -> str:
    git = shutil.which("git", path=os.defpath)
    if git is None:
        raise LineageTXReplayError("git is required for the isolated replay")
    environment = {
        "PATH": os.defpath,
        "HOME": "/nonexistent-lineagetx-home",
        "LANG": "C",
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_PAGER": "cat",
        "PAGER": "cat",
        "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
    }
    if created_at:
        environment["GIT_AUTHOR_DATE"] = created_at
        environment["GIT_COMMITTER_DATE"] = created_at
    try:
        completed = subprocess.run(
            [
                git,
                "--no-pager",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "commit.gpgSign=false",
                "-C",
                str(repo),
                *args,
            ],
            check=False,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=environment,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise LineageTXReplayError(f"git {' '.join(args)} failed: {error}") from error
    if completed.returncode != 0:
        raise LineageTXReplayError(
            f"git {' '.join(args)} failed in {repo}: {completed.stdout.strip()}"
        )
    return completed.stdout.strip()


def _initialize_fixture_repository(
    source: Path,
    destination: Path,
    *,
    created_at: str,
) -> str:
    if not source.is_dir() or source.is_symlink():
        raise LineageTXReplayError(f"fixture repository is missing or unsafe: {source}")
    for item in source.rglob("*"):
        if item.is_symlink():
            raise LineageTXReplayError(
                f"fixture repositories may not contain symlinks: {item}"
            )
    shutil.copytree(source, destination)
    _fixture_git(destination, "init", "-b", "main", created_at=created_at)
    _fixture_git(
        destination,
        "config",
        "user.name",
        "LineageTX Fixture",
        created_at=created_at,
    )
    _fixture_git(
        destination,
        "config",
        "user.email",
        "lineagetx@example.invalid",
        created_at=created_at,
    )
    _fixture_git(destination, "add", "--all", created_at=created_at)
    _fixture_git(
        destination,
        "commit",
        "-m",
        "LineageTX fixture baseline",
        created_at=created_at,
    )
    return _fixture_git(destination, "rev-parse", "HEAD")


def _schema_field_urn(dataset_urn: str, field: str) -> str:
    return f"urn:li:schemaField:({dataset_urn},{field})"


def _build_fixture_context(
    fixture_root: Path,
    intent: Any,
    manifest: Mapping[str, Any],
) -> Any:
    from .lineagetx.datahub_context import (
        AssetSnapshot,
        ConsumerLineage,
        DataHubMigrationContext,
        GovernanceSnapshot,
        SchemaFieldSnapshot,
    )

    scenario = _read_json(fixture_root / "datahub" / "scenario.json")
    if scenario.get("source_urn") != intent.source_asset_urn:
        raise LineageTXReplayError("DataHub scenario source does not match ChangeIntent")
    if scenario.get("source_column") != intent.old_field:
        raise LineageTXReplayError("DataHub scenario column does not match ChangeIntent")

    producer = manifest.get("producer")
    if not isinstance(producer, Mapping):
        raise LineageTXReplayError("migration fixture is missing producer schemas")
    expanded_path = _safe_fixture_path(fixture_root, str(producer["expanded_schema"]))
    contract_path = _safe_fixture_path(fixture_root, str(producer["contract_schema"]))
    expanded = _read_json(expanded_path)
    contract = _read_json(contract_path)
    contract_digest = hashlib.sha256(contract_path.read_bytes()).hexdigest()
    if contract_digest != intent.contract_schema_fingerprint:
        raise LineageTXReplayError("contract schema SHA-256 does not match ChangeIntent")
    if expanded.get("asset_urn") != intent.source_asset_urn:
        raise LineageTXReplayError("expanded schema asset does not match ChangeIntent")
    expanded_fields = expanded.get("fields")
    contract_fields = contract.get("fields")
    if not isinstance(expanded_fields, list) or not isinstance(contract_fields, list):
        raise LineageTXReplayError("producer schemas must contain field lists")
    expanded_names = {
        str(field.get("name")) for field in expanded_fields if isinstance(field, Mapping)
    }
    contract_names = {
        str(field.get("name")) for field in contract_fields if isinstance(field, Mapping)
    }
    if not {intent.old_field, intent.new_field}.issubset(expanded_names):
        raise LineageTXReplayError("expanded schema must contain old and replacement fields")
    if intent.old_field in contract_names or intent.new_field not in contract_names:
        raise LineageTXReplayError("contract schema must remove only the deprecated alias")

    source_schema = tuple(
        SchemaFieldSnapshot(
            field_path=str(field["name"]),
            native_type=str(field.get("type") or "UNKNOWN"),
            nullable=(
                field.get("nullable")
                if isinstance(field.get("nullable"), bool)
                else None
            ),
            description=(
                "Deprecated alias retained during EXPAND"
                if field.get("deprecated")
                else "Canonical LineageTX fixture field"
            ),
        )
        for field in expanded_fields
        if isinstance(field, Mapping) and isinstance(field.get("name"), str)
    )
    source_governance_tags = scenario.get("source_governance_tags", ())
    if not isinstance(source_governance_tags, list) or not all(
        isinstance(tag, str) and tag.startswith("urn:li:tag:")
        for tag in source_governance_tags
    ):
        raise LineageTXReplayError(
            "DataHub source_governance_tags must be tag URNs"
        )
    source = AssetSnapshot(
        urn=intent.source_asset_urn,
        schema=source_schema,
        governance=GovernanceSnapshot(
            owner_urns=(str(scenario["source_owner"]),),
            tag_urns=tuple(source_governance_tags),
            structured_properties={},
        ),
    )

    scenario_consumers = scenario.get("consumers")
    if not isinstance(scenario_consumers, list) or len(scenario_consumers) != 3:
        raise LineageTXReplayError("DataHub scenario must contain exactly three consumers")
    assets: dict[str, Any] = {intent.source_asset_urn: source}
    consumers: list[Any] = []
    lineage_chain = [
        intent.source_asset_urn,
        *(str(raw["urn"]) for raw in scenario_consumers if isinstance(raw, Mapping)),
    ]
    if len(lineage_chain) != 4:
        raise LineageTXReplayError("DataHub fixture did not define a complete chain")
    for index, raw in enumerate(scenario_consumers, start=1):
        if not isinstance(raw, Mapping):
            raise LineageTXReplayError("DataHub consumer fixture must be an object")
        urn = str(raw["urn"])
        owner = str(raw["owner"])
        degree = str(raw["degree"])
        governance_tags = raw.get("governance_tags", ())
        if not isinstance(governance_tags, list) or not all(
            isinstance(tag, str) and tag.startswith("urn:li:tag:")
            for tag in governance_tags
        ):
            raise LineageTXReplayError(
                "DataHub consumer governance_tags must be tag URNs"
            )
        assets[urn] = AssetSnapshot(
            urn=urn,
            schema=(
                SchemaFieldSnapshot(
                    intent.old_field,
                    "BIGINT",
                    False,
                    "Impacted downstream field from DataHub column lineage",
                ),
            ),
            governance=GovernanceSnapshot(
                owner_urns=(owner,),
                tag_urns=tuple(governance_tags),
                structured_properties={},
            ),
        )
        path = {
            "source": {"urn": intent.source_asset_urn, "column": intent.old_field},
            "target": {"urn": urn, "column": intent.old_field},
            "metadata": {"direction": "downstream"},
            "pathCount": 1,
            "paths": [
                {
                    "path": [
                        {
                            "urn": _schema_field_urn(asset_urn, intent.old_field),
                            "type": "SCHEMA_FIELD",
                        }
                        for asset_urn in lineage_chain[: index + 1]
                    ]
                }
            ],
        }
        consumers.append(
            ConsumerLineage(
                urn=urn,
                degree=degree,
                columns=(intent.old_field,),
                path_evidence=(path,),
            )
        )

    traces = tuple(
        [
            {"tool": "list_schema_fields", "arguments": {"urn": urn}}
            for urn in assets
        ]
        + [
            {
                "tool": "get_lineage",
                "arguments": {
                    "urn": intent.source_asset_urn,
                    "column": intent.old_field,
                    "max_hops": 3,
                },
            },
            {"tool": "get_entities", "arguments": {"urns": list(assets)}},
        ]
        + [
            {
                "tool": "get_lineage_paths_between",
                "arguments": {
                    "source_urn": intent.source_asset_urn,
                    "target_urn": consumer.urn,
                    "source_column": intent.old_field,
                    "target_column": intent.old_field,
                },
            }
            for consumer in consumers
        ]
    )
    return DataHubMigrationContext(
        source_urn=intent.source_asset_urn,
        source_column=intent.old_field,
        replacement_column=intent.new_field,
        source=source,
        consumers=tuple(consumers),
        assets=assets,
        tool_traces=traces,
        transport="test-double-for-official-datahub-mcp-local-replay",
        discovery_complete=True,
    )


def materialize_lineagetx_fixture(
    project: Path,
    work_root: Path,
    *,
    reset: bool = False,
    proposal_model: Any | None = None,
) -> LineageTXFixtureRuntime:
    """Create clean base repositories plus isolated LineageTX runtime state."""

    from .lineagetx.coordinator import LineageTXCoordinator
    from .lineagetx.evidence import EvidenceRecorder
    from .lineagetx.models import ChangeIntent, Participant
    from .lineagetx.policy import LineageTXSafetyPolicy
    from .lineagetx.proposals import DeterministicCandidateModel
    from .lineagetx.publisher import LocalReceiptPublisher
    from .lineagetx.state import SQLiteStateStore
    from .lineagetx.worktrees import WorktreeManager

    project = project.expanduser().resolve()
    fixture_root = project / "fixtures" / "lineagetx"
    manifest = _read_json(fixture_root / "migration.json")
    change_intent_path = _safe_fixture_path(
        fixture_root, str(manifest.get("change_intent", ""))
    )
    intent = ChangeIntent.from_dict(_read_json(change_intent_path))
    if manifest.get("migration_id") != intent.migration_id:
        raise LineageTXReplayError("migration fixture ID does not match ChangeIntent")

    raw_participants = manifest.get("participants")
    if not isinstance(raw_participants, list) or len(raw_participants) != 3:
        raise LineageTXReplayError("migration fixture must map exactly three participants")
    templates: list[Any] = []
    for raw in raw_participants:
        if not isinstance(raw, Mapping):
            raise LineageTXReplayError("participant fixture must be an object")
        template = Participant.from_dict(raw)
        expected = Participant.create(
            migration_id=intent.migration_id,
            kind=template.kind,
            asset_urn=template.asset_urn,
            repository=template.repository,
            owner_urns=template.owner_urns,
            files=template.files,
            created_at=template.created_at,
        )
        if expected.participant_id != template.participant_id:
            raise LineageTXReplayError(
                f"participant ID is not derived from its identity: {template.participant_id}"
            )
        templates.append(template)

    root = _prepare_lineagetx_work_root(project, work_root, reset=reset)
    repository_root = root / "repositories"
    repository_root.mkdir()
    repositories: dict[str, Path] = {}
    base_shas: dict[str, str] = {}
    for repository in sorted({item.repository for item in templates}):
        source = _safe_fixture_path(
            fixture_root / "repos", repository
        )
        destination = repository_root / repository
        base_sha = _initialize_fixture_repository(
            source,
            destination,
            created_at=intent.created_at,
        )
        repositories[repository] = destination
        base_shas[repository] = base_sha

    participants: list[Any] = []
    for template in templates:
        for relative in template.files:
            target = _safe_fixture_path(repositories[template.repository], relative)
            if not target.is_file():
                raise LineageTXReplayError(
                    f"participant file does not exist in fixture repository: {relative}"
                )
        participants.append(
            replace(
                template,
                base_sha=base_shas[template.repository],
            )
        )

    context = _build_fixture_context(fixture_root, intent, manifest)
    by_asset = {item.asset_urn: item for item in participants}
    if set(by_asset) != {item.urn for item in context.consumers}:
        raise LineageTXReplayError(
            "participant fixture is not exhaustive for the DataHub impact set"
        )
    for consumer in context.consumers:
        participant = by_asset[consumer.urn]
        owner = context.assets[consumer.urn].governance.owner_urns
        if participant.owner_urns != owner:
            raise LineageTXReplayError(
                f"participant owner does not match DataHub context: {consumer.urn}"
            )

    evidence = EvidenceRecorder(root / "evidence" / intent.migration_id, intent.migration_id)
    state = SQLiteStateStore(root / "lineagetx.sqlite3")
    coordinator = LineageTXCoordinator(
        state=state,
        worktrees=WorktreeManager(root / "isolated-worktrees"),
        proposal_model=(
            proposal_model
            if proposal_model is not None
            else DeterministicCandidateModel()
        ),
        publisher=LocalReceiptPublisher(evidence.root),
        impact_revalidator=_FrozenFixtureImpactRevalidator(),
        safety_policy=LineageTXSafetyPolicy(allow_test_transport=True),
        evidence=evidence,
        # This command is an offline deterministic fixture replay. Production
        # live runs require authenticated GitHub evidence instead.
        allow_test_approvals=True,
    )
    return LineageTXFixtureRuntime(
        root=root,
        intent=intent,
        participants=tuple(participants),
        context=context,
        repositories=repositories,
        base_shas=base_shas,
        coordinator=coordinator,
        state=state,
        evidence=evidence,
    )


def _base_repositories_untouched(runtime: LineageTXFixtureRuntime) -> bool:
    return all(
        _fixture_git(repo, "rev-parse", "HEAD") == runtime.base_shas[name]
        and _fixture_git(repo, "status", "--porcelain=v1") == ""
        for name, repo in runtime.repositories.items()
    )


def _candidate_branches(runtime: LineageTXFixtureRuntime) -> dict[str, list[str]]:
    branches: dict[str, list[str]] = {}
    for name, repo in runtime.repositories.items():
        output = _fixture_git(repo, "branch", "--list", "lineagetx/*")
        branches[name] = [line.lstrip("*+ ") for line in output.splitlines() if line]
    return branches


def _print_replay_summary(summary: Mapping[str, Any], output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(summary, indent=2, sort_keys=True))
        return
    for state in summary["transitions"]:
        print(f"state={state}")
    print(f"migration={summary['migration_id']}")
    print(f"fixture_transport={summary['context_transport']}")
    print(f"base_repositories_untouched={summary['base_repositories_untouched']}")
    print(f"auto_merged={summary['auto_merged']}")
    print(f"evidence={summary['evidence_path']}")
    if summary["status"] == "COMMITTED":
        print("0 unverified consumers — upstream change is safe to merge.")
    else:
        print("ABORT cleaned only unmerged candidate branches and worktrees.")


async def run_lineagetx_replay(args: argparse.Namespace) -> int:
    from .lineagetx.models import MigrationStatus, ParticipantKind, ParticipantStatus
    from .lineagetx.participants.semantic_approval import OwnerApproval
    from .lineagetx.policy import DiscoveryAttestation
    from .lineagetx.proposals import DeterministicCandidateModel

    runtime = materialize_lineagetx_fixture(
        Path(args.project_root),
        Path(args.work_root),
        reset=args.reset,
        proposal_model=DeterministicCandidateModel(),
    )
    transitions = [MigrationStatus.DETECTED.value]
    await runtime.coordinator.detect(
        runtime.intent,
        DiscoveryAttestation(runtime.context, discovery_complete=True),
        runtime.participants,
    )
    transitions.append(MigrationStatus.PREPARING.value)
    prepared = await runtime.coordinator.prepare(
        runtime.intent.migration_id,
        runtime.repositories,
    )
    transitions.append(prepared.migration.status.value)
    if prepared.migration.status is not MigrationStatus.NEEDS_APPROVAL:
        raise LineageTXReplayError("bounded replay must stop for one semantic approval")
    if len(prepared.pending_approval_participant_ids) != 1:
        raise LineageTXReplayError("bounded replay requires exactly one pending owner")
    semantic = next(
        item
        for item in prepared.participants
        if item.kind is ParticipantKind.SEMANTIC_APPROVAL
    )
    if semantic.status is not ParticipantStatus.NEEDS_APPROVAL:
        raise LineageTXReplayError("semantic consumer did not fail closed for approval")
    if prepared.results[semantic.participant_id].changed_files:
        raise LineageTXReplayError("semantic consumer changed before owner approval")

    publication: Any | None = None
    auto_merged = False
    if args.outcome == "abort":
        terminal = await runtime.coordinator.abort(
            runtime.intent.migration_id,
            reason="local replay operator selected ABORT before owner approval",
        )
        transitions.append(terminal.migration.status.value)
        final_migration = terminal.migration
        deployed_systems_rolled_back = terminal.deployed_systems_rolled_back
    else:
        owner = args.approval_owner or semantic.owner_urns[0]
        approved = await runtime.coordinator.approve(
            runtime.intent.migration_id,
            OwnerApproval(
                migration_id=runtime.intent.migration_id,
                participant_id=semantic.participant_id,
                owner_urn=owner,
                old_field=runtime.intent.old_field,
                new_field=runtime.intent.new_field,
                approved_at=args.approved_at,
                evidence_url=args.approval_evidence_url,
            ),
        )
        transitions.append(approved.migration.status.value)
        terminal = await runtime.coordinator.commit(runtime.intent.migration_id)
        transitions.append(terminal.migration.status.value)
        final_migration = terminal.migration
        publication = terminal.publication
        auto_merged = terminal.auto_merged
        deployed_systems_rolled_back = False

    participant_records = tuple(
        runtime.state.list_participants(runtime.intent.migration_id)
    )
    unverified = sum(
        item.status not in {ParticipantStatus.VERIFIED, ParticipantStatus.COMMITTED}
        for item in participant_records
    )
    if final_migration.status is MigrationStatus.COMMITTED and unverified != 0:
        raise LineageTXReplayError("COMMITTED replay still has unverified consumers")
    if not _base_repositories_untouched(runtime):
        raise LineageTXReplayError("an adapter modified a base repository checkout")

    manifest = runtime.evidence.verify_manifest()
    summary: dict[str, Any] = {
        "approval_mode": "scripted-test-only",
        "approval_performed": args.outcome == "commit",
        "auto_merged": auto_merged,
        "base_repositories_untouched": True,
        "candidate_branches": _candidate_branches(runtime),
        "context_transport": runtime.context.transport,
        "deployed_systems_rolled_back": deployed_systems_rolled_back,
        "evidence_aggregate_sha256": manifest.aggregate_sha256,
        "evidence_path": runtime.evidence.root.relative_to(runtime.root).as_posix(),
        "migration_id": runtime.intent.migration_id,
        "participants": [item.to_dict() for item in participant_records],
        "proposal_model": "deterministic",
        "status": final_migration.status.value,
        "transitions": transitions,
        "unverified_consumers": unverified,
    }
    if publication is not None:
        summary["coordinated_pr"] = publication.coordinated_pr_receipt.to_dict()
        summary["producer_gate"] = publication.producer_gate_receipt.to_dict()
    (runtime.root / "replay-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _print_replay_summary(summary, args.output_format)
    return 0


def show_lineagetx_replay(args: argparse.Namespace) -> int:
    from .lineagetx.state import SQLiteStateStore

    root = Path(args.work_root).expanduser().resolve()
    state_path = root / "lineagetx.sqlite3"
    if not state_path.is_file():
        raise LineageTXReplayError(f"LineageTX state database does not exist: {state_path}")
    state = SQLiteStateStore(state_path)
    migrations = state.list_migrations()
    if args.migration_id:
        migration = state.get_migration(args.migration_id)
    elif len(migrations) == 1:
        migration = migrations[0]
    else:
        raise LineageTXReplayError(
            "--migration-id is required when the state database is not unambiguous"
        )
    value = {
        "approvals": [
            item.to_dict() for item in state.list_approvals(migration.intent.migration_id)
        ],
        "events": [
            item.to_dict() for item in state.list_events(migration.intent.migration_id)
        ],
        "migration": migration.to_dict(),
        "participants": [
            item.to_dict()
            for item in state.list_participants(migration.intent.migration_id)
        ],
    }
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


async def run_agent(args: argparse.Namespace) -> int:
    root = Path(args.project_root).resolve()
    workspace, mapping = prepare_workspace(root)

    if args.mode == "fixture":
        context_provider = FixtureContextProvider(root / "fixtures" / "datahub_context.json")
        status_writer = OfflineReplayStatusWriter()
    else:
        from .live_datahub import LiveDataHubContextProvider, LiveDataHubStatusWriter

        context_provider = LiveDataHubContextProvider(args.datahub_url)
        status_writer = LiveDataHubStatusWriter(args.datahub_url, context_provider)

    agent = DataLineageFixAgent(
        context_provider=context_provider,
        status_writer=status_writer,
        evidence_writer=EvidenceWriter(root),
    )
    result = await agent.run(
        workspace=workspace,
        source_urn=mapping["source_urn"],
        target_urn=mapping["target_urn"],
        repository_relative_path=mapping["repository_file"],
        table_name=mapping["table_name"],
    )
    print(f"status={result.status}")
    print(f"context={result.context.transport}")
    print(f"finding={result.finding.finding_id}")
    print(f"red_exit={result.before.returncode}")
    print(f"green_exit={result.after.returncode}")
    print(f"writeback={result.writeback.get('status')}")
    print(f"evidence={result.evidence_dir}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="datalineage-fix",
        description="Find and repair a DataHub-grounded downstream schema drift.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run the evidence-producing repair loop")
    run.add_argument("--mode", choices=("fixture", "mcp"), default="fixture")
    run.add_argument("--project-root", default=str(project_root()))
    run.add_argument(
        "--datahub-url",
        default="http://localhost:8080",
        help="DataHub GMS URL; never a browser/frontend URL",
    )

    lineagetx = subparsers.add_parser(
        "lineagetx",
        aliases=("tx",),
        help="run or inspect the bounded three-consumer LineageTX scenario",
    )
    lineagetx_commands = lineagetx.add_subparsers(
        dest="lineagetx_command",
        required=True,
    )
    replay = lineagetx_commands.add_parser(
        "replay",
        help=(
            "run a scripted test-only fixture replay; live owner approval must "
            "come from authenticated GitHub evidence"
        ),
    )
    replay.add_argument("--project-root", default=str(project_root()))
    replay.add_argument("--work-root", default=str(default_lineagetx_work_root()))
    replay.add_argument(
        "--reset",
        action="store_true",
        help="replace only a work root carrying LineageTX's ownership marker",
    )
    replay.add_argument(
        "--outcome",
        choices=("commit", "abort"),
        default="commit",
        help="approve and release the gate, or abort before owner approval",
    )
    replay.add_argument(
        "--approval-owner",
        default="",
        help="defaults to the semantic consumer owner discovered in DataHub",
    )
    replay.add_argument(
        "--approved-at",
        default="2026-07-17T01:03:00.000000Z",
        help="UTC timestamp recorded in the deterministic approval receipt",
    )
    replay.add_argument(
        "--approval-evidence-url",
        default=(
            "https://replay.lineagetx.invalid/approvals/"
            "identity-data-owner.json"
        ),
        help="absolute HTTPS evidence URL bound to the owner approval",
    )
    replay.add_argument(
        "--output-format",
        choices=("text", "json"),
        default="text",
    )

    show = lineagetx_commands.add_parser(
        "show",
        help="print durable migration, participant, approval, and event state",
    )
    show.add_argument("--work-root", default=str(default_lineagetx_work_root()))
    show.add_argument("--migration-id", default="")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        if args.command == "run":
            raise SystemExit(asyncio.run(run_agent(args)))
        if args.command in {"lineagetx", "tx"}:
            if args.lineagetx_command == "replay":
                raise SystemExit(asyncio.run(run_lineagetx_replay(args)))
            if args.lineagetx_command == "show":
                raise SystemExit(show_lineagetx_replay(args))
    except LineageTXReplayError as error:
        print(f"lineagetx replay error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
