from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import pytest

from data_lineage_fix_agent.cli import (
    LineageTXReplayError,
    build_parser,
    materialize_lineagetx_fixture,
    run_lineagetx_replay,
    show_lineagetx_replay,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _arguments(
    work_root: Path,
    *,
    outcome: str = "commit",
    output_format: str = "json",
) -> argparse.Namespace:
    return argparse.Namespace(
        project_root=str(PROJECT_ROOT),
        work_root=str(work_root),
        reset=False,
        outcome=outcome,
        approval_owner="",
        approved_at="2026-07-17T01:03:00.000000Z",
        approval_evidence_url=(
            "https://replay.lineagetx.invalid/approvals/identity-data-owner.json"
        ),
        output_format=output_format,
    )


def test_parser_preserves_legacy_run_and_exposes_replay_alias() -> None:
    parser = build_parser()

    legacy = parser.parse_args(["run", "--mode", "fixture"])
    replay = parser.parse_args(["tx", "replay", "--outcome", "abort"])

    assert legacy.command == "run"
    assert legacy.mode == "fixture"
    assert replay.command == "tx"
    assert replay.lineagetx_command == "replay"
    assert replay.outcome == "abort"
    assert not hasattr(replay, "proposal_model")


def test_materializer_creates_clean_git_repositories_and_refuses_unsafe_reset(
    tmp_path: Path,
) -> None:
    runtime = materialize_lineagetx_fixture(
        PROJECT_ROOT,
        tmp_path / "owned-replay",
    )

    assert set(runtime.repositories) == {"data-platform", "analytics-governance"}
    assert len(runtime.participants) == 3
    assert [
        len(consumer.path_evidence[0]["paths"][0]["path"])
        for consumer in runtime.context.consumers
    ] == [2, 3, 4]
    assert {
        participant.base_sha for participant in runtime.participants
    } == set(runtime.base_shas.values())
    for participant in runtime.participants:
        for relative in participant.files:
            assert runtime.repositories[participant.repository].joinpath(relative).is_file()

    unowned = tmp_path / "unowned"
    unowned.mkdir()
    unowned.joinpath("keep.txt").write_text("belongs to the user\n", encoding="utf-8")
    with pytest.raises(LineageTXReplayError, match="not owned by LineageTX"):
        materialize_lineagetx_fixture(PROJECT_ROOT, unowned, reset=True)
    assert unowned.joinpath("keep.txt").read_text(encoding="utf-8") == (
        "belongs to the user\n"
    )


def test_replay_converges_after_owner_approval_without_touching_base_or_merging(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    work_root = tmp_path / "committed"

    assert asyncio.run(run_lineagetx_replay(_arguments(work_root))) == 0

    printed = json.loads(capsys.readouterr().out)
    persisted = json.loads(
        work_root.joinpath("replay-summary.json").read_text(encoding="utf-8")
    )
    assert printed == persisted
    assert persisted["transitions"] == [
        "DETECTED",
        "PREPARING",
        "NEEDS_APPROVAL",
        "PREPARED",
        "COMMITTED",
    ]
    assert persisted["status"] == "COMMITTED"
    assert persisted["unverified_consumers"] == 0
    assert persisted["base_repositories_untouched"] is True
    assert persisted["auto_merged"] is False
    assert persisted["approval_performed"] is True
    assert persisted["proposal_model"] == "deterministic"
    assert not Path(persisted["evidence_path"]).is_absolute()
    assert persisted["context_transport"].startswith(
        "test-double-for-official-datahub-mcp"
    )
    branches = [
        branch
        for repository_branches in persisted["candidate_branches"].values()
        for branch in repository_branches
    ]
    assert len(branches) == 3
    assert all(branch.startswith("lineagetx/") for branch in branches)
    assert all(
        participant["status"] == "VERIFIED"
        and len(participant["candidate_commit_sha"]) == 40
        for participant in persisted["participants"]
    )
    assert persisted["coordinated_pr"]["merged"] is False
    assert persisted["producer_gate"]["reference"].startswith("check-run:")
    assert len(persisted["evidence_aggregate_sha256"]) == 64
    private_root = str(work_root.resolve())
    evidence_root = work_root / persisted["evidence_path"]
    for artifact in evidence_root.rglob("*"):
        if artifact.is_file():
            assert private_root not in artifact.read_text(encoding="utf-8")

    assert (
        show_lineagetx_replay(
            argparse.Namespace(work_root=str(work_root), migration_id="")
        )
        == 0
    )
    shown = json.loads(capsys.readouterr().out)
    assert shown["migration"]["status"] == "COMMITTED"
    assert len(shown["approvals"]) == 1
    assert shown["approvals"][0]["owner_urn"].endswith("identity-data-owner")


def test_abort_removes_only_unmerged_candidates_and_never_claims_deploy_rollback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    work_root = tmp_path / "aborted"

    assert (
        asyncio.run(
            run_lineagetx_replay(_arguments(work_root, outcome="abort"))
        )
        == 0
    )

    summary = json.loads(capsys.readouterr().out)
    assert summary["transitions"] == [
        "DETECTED",
        "PREPARING",
        "NEEDS_APPROVAL",
        "ABORTED",
    ]
    assert summary["status"] == "ABORTED"
    assert summary["approval_performed"] is False
    assert summary["deployed_systems_rolled_back"] is False
    assert summary["base_repositories_untouched"] is True
    assert all(not branches for branches in summary["candidate_branches"].values())
    assert all(item["status"] == "ABORTED" for item in summary["participants"])
    assert "coordinated_pr" not in summary
    assert "producer_gate" not in summary
    private_root = str(work_root.resolve())
    evidence_root = work_root / summary["evidence_path"]
    for artifact in evidence_root.rglob("*"):
        if artifact.is_file():
            assert private_root not in artifact.read_text(encoding="utf-8")
