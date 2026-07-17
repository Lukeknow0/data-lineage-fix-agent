from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

import pytest

from data_lineage_fix_agent.agent import DataLineageFixAgent
from data_lineage_fix_agent.context import FixtureContextProvider, OfflineReplayStatusWriter
from data_lineage_fix_agent.evidence import EvidenceWriter
from data_lineage_fix_agent.models import DataHubContext
from data_lineage_fix_agent.planner import ContextRefusal, GroundedPatchPlanner


ROOT = Path(__file__).resolve().parents[1]
MAPPING = json.loads(
    (ROOT / "fixture_pipeline" / "project_mapping.json").read_text(encoding="utf-8")
)


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    shutil.copytree(ROOT / "fixture_pipeline" / "broken", workspace)
    return workspace


def _context() -> DataHubContext:
    provider = FixtureContextProvider(ROOT / "fixtures" / "datahub_context.json")
    return asyncio.run(provider.load(MAPPING["source_urn"], MAPPING["target_urn"]))


def test_correctly_identifies_affected_downstream(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    context = _context()
    finding = GroundedPatchPlanner().analyze(
        context,
        workspace / MAPPING["repository_file"],
        MAPPING["repository_file"],
    )

    assert finding.affected_entity_urn == MAPPING["target_urn"]
    assert finding.missing_field == "customer_id"
    assert finding.replacement_field == "customer_key"
    assert finding.reference_count == 3


def test_refuses_to_patch_without_datahub_context(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    sql_path = workspace / MAPPING["repository_file"]
    original = sql_path.read_text(encoding="utf-8")
    empty = DataHubContext(
        source_urn=MAPPING["source_urn"],
        target_urn=MAPPING["target_urn"],
        source_fields=[],
        downstream_urns=[],
        owner_urns=[],
        quality_signals=[],
        transport="",
        tool_traces=[],
    )

    with pytest.raises(ContextRefusal):
        GroundedPatchPlanner().analyze(empty, sql_path, MAPPING["repository_file"])

    assert sql_path.read_text(encoding="utf-8") == original


def test_patch_turns_generated_regression_from_red_to_green(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    agent = DataLineageFixAgent(
        FixtureContextProvider(ROOT / "fixtures" / "datahub_context.json"),
        OfflineReplayStatusWriter(),
        EvidenceWriter(tmp_path),
    )
    result = asyncio.run(
        agent.run(
            workspace,
            MAPPING["source_urn"],
            MAPPING["target_urn"],
            MAPPING["repository_file"],
            MAPPING["table_name"],
        )
    )

    assert result.before.returncode != 0
    assert "no such column: customer_id" in result.before.output
    assert result.after.returncode == 0
    assert "OK" in result.after.output
    assert "-  customer_id" in result.patch
    assert "+  customer_key" in result.patch


def test_evidence_is_traceable_to_datahub_entity(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    agent = DataLineageFixAgent(
        FixtureContextProvider(ROOT / "fixtures" / "datahub_context.json"),
        OfflineReplayStatusWriter(),
        EvidenceWriter(tmp_path),
    )
    result = asyncio.run(
        agent.run(
            workspace,
            MAPPING["source_urn"],
            MAPPING["target_urn"],
            MAPPING["repository_file"],
            MAPPING["table_name"],
        )
    )
    run_dir = Path(result.evidence_dir)
    finding = json.loads((run_dir / "finding.json").read_text(encoding="utf-8"))
    context = json.loads((run_dir / "context.json").read_text(encoding="utf-8"))
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))

    assert finding["source_urn"] == MAPPING["source_urn"]
    assert finding["affected_entity_urn"] == MAPPING["target_urn"]
    assert {trace["tool"] for trace in context["tool_traces"]} >= {
        "get_entities",
        "list_schema_fields",
        "get_lineage",
    }
    assert "patch.diff" in manifest
    assert len(manifest["patch.diff"]) == 64
