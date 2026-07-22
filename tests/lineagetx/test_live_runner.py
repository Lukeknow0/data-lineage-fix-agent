from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping

import pytest

from data_lineage_fix_agent.cli import materialize_lineagetx_fixture
from data_lineage_fix_agent.lineagetx.datahub_context import OfficialDataHubMCPClient
from data_lineage_fix_agent.lineagetx.github_approval import VerifiedGitHubApproval
from data_lineage_fix_agent.lineagetx.writeback import WritebackReceipt


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run_lineagetx_live.py"
GATE = ROOT / "scripts" / "gate_lineagetx_live.sh"
SPEC = importlib.util.spec_from_file_location("lineagetx_live_runner", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
LIVE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = LIVE
SPEC.loader.exec_module(LIVE)


class FakeOfficialClient:
    transport = OfficialDataHubMCPClient.transport

    def __init__(
        self,
        gms_url: str,
        *,
        operation_timeout_seconds: float,
    ) -> None:
        self.gms_url = gms_url
        self.operation_timeout_seconds = operation_timeout_seconds


class FixtureOfficialReader:
    def __init__(self, context: Any) -> None:
        self.context = context
        self.loads: list[tuple[str, str, str]] = []
        self.refreshes: list[str] = []

    async def load(
        self,
        source_urn: str,
        source_column: str,
        replacement_column: str,
    ) -> Any:
        self.loads.append((source_urn, source_column, replacement_column))
        return replace(self.context)

    async def assert_impact_unchanged(self, frozen: Any) -> Any:
        self.refreshes.append(frozen.impact_fingerprint)
        return replace(self.context)


class FakeLiveWriter:
    def __init__(self, client: Any, context: Any) -> None:
        assert client.transport == OfficialDataHubMCPClient.transport
        self.context = context
        self.calls: list[dict[str, Any]] = []

    async def write_assets(
        self,
        writebacks: Mapping[str, Any],
        *,
        supplied_urns: tuple[str, ...] | None = None,
        refreshed_context: Any | None = None,
    ) -> WritebackReceipt:
        assert supplied_urns is None
        assert tuple(writebacks) == self.context.asset_urns
        statuses = {urn: item.status for urn, item in writebacks.items()}
        owners = {urn: item.owner for urn, item in writebacks.items()}
        urls = {urn: item.evidence_url for urn, item in writebacks.items()}
        self.calls.append(
            {
                "statuses": statuses,
                "refreshed": refreshed_context is not None,
            }
        )
        digest = hashlib.sha256(
            json.dumps(statuses, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return WritebackReceipt(
            migration_id=next(iter(writebacks.values())).migration_id,
            status=(
                next(iter(set(statuses.values())))
                if len(set(statuses.values())) == 1
                else "MIXED"
            ),
            entity_urns=self.context.asset_urns,
            tag_urn="urn:li:tag:LineageTXMigration",
            evidence_url="multiple per-asset evidence URLs",
            readback_sha256=digest,
            tool_traces=(),
            asset_statuses=statuses,
            asset_owners=owners,
            asset_evidence_urls=urls,
            impact_fingerprint=self.context.impact_fingerprint,
            transport=OfficialDataHubMCPClient.transport,
            live_verified=True,
            journal=(),
            verification="live DataHub OSS write verified through official MCP read-back",
        )


def _args(tmp_path: Path) -> argparse.Namespace:
    return LIVE.parse_args(
        [
            "--project-root",
            str(ROOT),
            "--work-root",
            str(tmp_path / "live-run"),
            "--gms-url",
            "http://localhost:8080",
            "--phase",
            "scripted-test",
            "--evidence-base-url",
            "https://evidence.example.invalid/lineagetx",
            "--approval-evidence-url",
            "https://evidence.example.invalid/lineagetx/owner-approval",
        ]
    )


def test_live_runner_drives_real_coordinator_with_official_context_and_writebacks(
    tmp_path: Path,
) -> None:
    holder: dict[str, Any] = {}

    def materialize(*args: Any, **kwargs: Any) -> Any:
        runtime = materialize_lineagetx_fixture(*args, **kwargs)
        holder["runtime"] = runtime
        holder["context"] = replace(
            runtime.context,
            transport=OfficialDataHubMCPClient.transport,
        )
        return runtime

    def reader_factory(_: Any) -> FixtureOfficialReader:
        reader = FixtureOfficialReader(holder["context"])
        holder["reader"] = reader
        return reader

    def writer_factory(client: Any, context: Any) -> FakeLiveWriter:
        writer = FakeLiveWriter(client, context)
        holder["writer"] = writer
        return writer

    components = LIVE.RunnerComponents(
        materialize=materialize,
        client_factory=FakeOfficialClient,
        reader_factory=reader_factory,
        writer_factory=writer_factory,
    )
    receipt = asyncio.run(
        LIVE.run_live(_args(tmp_path), components=components)
    )

    assert receipt["migration_state"] == "COMMITTED"
    assert receipt["phase"] == "scripted-test"
    assert receipt["owner_approval"]["mode"] == "scripted-test-only"
    assert receipt["transitions"] == [
        "DETECTED",
        "PREPARING",
        "NEEDS_APPROVAL",
        "PREPARED",
        "COMMITTED",
    ]
    assert receipt["unverified_consumers"] == 0
    assert receipt["result"] == (
        "0 unverified consumers — upstream change is safe to merge."
    )
    assert receipt["base_checkouts_unchanged"] is True
    assert receipt["datahub"] == {
        "backend": "full-datahub-oss",
        "transport": OfficialDataHubMCPClient.transport,
        "live_verified": True,
        "discovery_complete": True,
        "impact_fingerprint": holder["context"].impact_fingerprint,
    }
    assert len(receipt["participants"]) == 3
    assert all(
        re.fullmatch(r"[0-9a-f]{40}", item["candidate_commit_sha"])
        for item in receipt["participants"]
    )
    assert all(item["merged"] is False for item in receipt["participants"])
    assert [item["stage"] for item in receipt["writeback_evidence"]] == list(
        LIVE.WRITEBACK_STAGES
    )
    assert len(holder["writer"].calls) == 6
    assert [item["refreshed"] for item in holder["writer"].calls] == [
        False,
        False,
        False,
        False,
        True,
        True,
    ]
    assert holder["reader"].refreshes == [holder["context"].impact_fingerprint]
    assert receipt["publication"] == {
        "publisher": "local-receipt-only",
        "scope": "local-oss-proof-only",
        "remote_pr_created": False,
        "coordinated_receipt": (
            f"https://replay.lineagetx.invalid/{receipt['migration_id']}/coordinated-pr"
        ),
        "producer_gate_receipt": "check-run:lineagetx/safe-to-contract:" + "2" * 40,
        "auto_merged": False,
        "merged": False,
    }
    serialized = json.dumps(receipt, sort_keys=True)
    assert str(tmp_path) not in serialized
    assert "DATAHUB_GMS_TOKEN" not in serialized
    assert receipt["contains_credentials"] is False
    holder["runtime"].evidence.verify_manifest()


def test_live_default_pauses_with_signed_durable_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "LINEAGETX_RESUME_HMAC_KEY",
        "test-only-resume-key-with-more-than-thirty-two-bytes",
    )
    holder: dict[str, Any] = {}

    def materialize(*args: Any, **kwargs: Any) -> Any:
        runtime = materialize_lineagetx_fixture(*args, **kwargs)
        holder["runtime"] = runtime
        holder["context"] = replace(
            runtime.context,
            transport=OfficialDataHubMCPClient.transport,
        )
        return runtime

    components = LIVE.RunnerComponents(
        materialize=materialize,
        client_factory=FakeOfficialClient,
        reader_factory=lambda _: FixtureOfficialReader(holder["context"]),
        writer_factory=FakeLiveWriter,
    )
    args = LIVE.parse_args(
        [
            "--project-root",
            str(ROOT),
            "--work-root",
            str(tmp_path / "paused-run"),
            "--gms-url",
            "http://localhost:8080",
            "--evidence-base-url",
            "https://evidence.example.invalid/lineagetx",
        ]
    )

    receipt = asyncio.run(LIVE.run_live(args, components=components))

    assert args.phase == "pause"
    assert receipt["migration_state"] == "NEEDS_APPROVAL"
    assert receipt["upstream_change_safe_to_merge"] is False
    assert receipt["publication"] is None
    assert receipt["resume"]["strategy"] == (
        "signed-context-deterministic-reprepare"
    )
    assert receipt["owner_approval_request"]["required_body"]["new_field"] == (
        "customer_key"
    )
    assert [item["stage"] for item in receipt["writeback_evidence"]] == list(
        LIVE.PAUSE_WRITEBACK_STAGES
    )
    assert (holder["runtime"].root / LIVE.PAUSE_SNAPSHOT_NAME).is_file()


class FakeGitHubApprovalVerifier:
    def __init__(self, **kwargs: Any) -> None:
        self.owner_login_by_urn = kwargs["owner_login_by_urn"]

    def verify(self, expectation: Any, source_api_url: str) -> VerifiedGitHubApproval:
        assert self.owner_login_by_urn == {expectation.owner_urn: "identity-owner"}
        return VerifiedGitHubApproval(
            migration_id=expectation.migration_id,
            participant_id=expectation.participant_id,
            owner_urn=expectation.owner_urn,
            old_field=expectation.old_field,
            new_field=expectation.new_field,
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
            evidence_sha256="a" * 64,
            verified_at="2026-07-17T02:01:00Z",
        )


def test_live_resume_reprepares_and_requires_authenticated_github_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "LINEAGETX_RESUME_HMAC_KEY",
        "test-only-resume-key-with-more-than-thirty-two-bytes",
    )
    work_root = tmp_path / "resume-run"
    holder: dict[str, Any] = {}

    def materialize(*args: Any, **kwargs: Any) -> Any:
        runtime = materialize_lineagetx_fixture(*args, **kwargs)
        holder["runtime"] = runtime
        holder["context"] = replace(
            runtime.context,
            transport=OfficialDataHubMCPClient.transport,
        )
        return runtime

    components = LIVE.RunnerComponents(
        materialize=materialize,
        client_factory=FakeOfficialClient,
        reader_factory=lambda _: FixtureOfficialReader(holder["context"]),
        writer_factory=FakeLiveWriter,
        approval_verifier_factory=FakeGitHubApprovalVerifier,
    )
    common = [
        "--project-root",
        str(ROOT),
        "--work-root",
        str(work_root),
        "--gms-url",
        "http://localhost:8080",
        "--evidence-base-url",
        "https://evidence.example.invalid/lineagetx",
    ]
    paused = asyncio.run(
        LIVE.run_live(LIVE.parse_args(common), components=components)
    )
    owner = paused["owner_approval_request"]["owner_urn"]
    api_url = (
        "https://api.github.com/repos/Lukeknow0/data-lineage-fix-agent/"
        "issues/comments/42"
    )

    resumed = asyncio.run(
        LIVE.run_live(
            LIVE.parse_args(
                [
                    *common,
                    "--phase",
                    "resume",
                    "--approval-api-url",
                    api_url,
                    "--owner-github-login",
                    f"{owner}=identity-owner",
                ]
            ),
            components=components,
        )
    )

    assert resumed["migration_state"] == "COMMITTED"
    assert resumed["owner_approval"]["mode"] == "authenticated-github-api"
    assert resumed["owner_approval"]["evidence_url"] == api_url
    assert resumed["owner_approval"]["verification"]["actor_login"] == (
        "identity-owner"
    )
    assert not (work_root / LIVE.PAUSE_SNAPSHOT_NAME).exists()


def test_live_resume_preserves_snapshot_when_destructive_reprepare_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    resume_key_text = "test-only-resume-key-with-more-than-thirty-two-bytes"
    monkeypatch.setenv("LINEAGETX_RESUME_HMAC_KEY", resume_key_text)
    work_root = tmp_path / "failed-resume-run"
    holder: dict[str, Any] = {}

    def materialize(*args: Any, **kwargs: Any) -> Any:
        runtime = materialize_lineagetx_fixture(*args, **kwargs)
        holder["context"] = replace(
            runtime.context,
            transport=OfficialDataHubMCPClient.transport,
        )
        return runtime

    components = LIVE.RunnerComponents(
        materialize=materialize,
        client_factory=FakeOfficialClient,
        reader_factory=lambda _: FixtureOfficialReader(holder["context"]),
        writer_factory=FakeLiveWriter,
        approval_verifier_factory=FakeGitHubApprovalVerifier,
    )
    common = [
        "--project-root",
        str(ROOT),
        "--work-root",
        str(work_root),
        "--gms-url",
        "http://localhost:8080",
        "--evidence-base-url",
        "https://evidence.example.invalid/lineagetx",
    ]
    paused = asyncio.run(
        LIVE.run_live(LIVE.parse_args(common), components=components)
    )
    owner = paused["owner_approval_request"]["owner_urn"]
    resume_args = LIVE.parse_args(
        [
            *common,
            "--phase",
            "resume",
            "--approval-api-url",
            "https://api.github.com/repos/Lukeknow0/data-lineage-fix-agent/issues/comments/42",
            "--owner-github-login",
            f"{owner}=identity-owner",
        ]
    )

    def fail_after_reset(*args: Any, **kwargs: Any) -> Any:
        materialize_lineagetx_fixture(*args, **kwargs)
        raise RuntimeError("injected failure after destructive reset")

    failing_components = replace(components, materialize=fail_after_reset)
    with pytest.raises(RuntimeError, match="injected failure"):
        asyncio.run(LIVE.run_live(resume_args, components=failing_components))

    staged = LIVE._resume_snapshot_staging_path(work_root)
    assert staged.is_file()
    LIVE.read_signed_pause_snapshot(staged, key=resume_key_text.encode("utf-8"))

    resumed = asyncio.run(LIVE.run_live(resume_args, components=components))
    assert resumed["migration_state"] == "COMMITTED"
    assert not staged.exists()
    assert not (work_root / LIVE.PAUSE_SNAPSHOT_NAME).exists()


def test_live_resume_rejects_drift_before_detect_or_candidate_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    resume_key_text = "test-only-resume-key-with-more-than-thirty-two-bytes"
    monkeypatch.setenv("LINEAGETX_RESUME_HMAC_KEY", resume_key_text)
    work_root = tmp_path / "drifted-resume-run"
    holder: dict[str, Any] = {"writers": []}

    def materialize(*args: Any, **kwargs: Any) -> Any:
        runtime = materialize_lineagetx_fixture(*args, **kwargs)
        holder["runtime"] = runtime
        holder["context"] = replace(
            runtime.context,
            transport=OfficialDataHubMCPClient.transport,
        )
        return runtime

    def writer_factory(client: Any, context: Any) -> FakeLiveWriter:
        writer = FakeLiveWriter(client, context)
        holder["writers"].append(writer)
        return writer

    components = LIVE.RunnerComponents(
        materialize=materialize,
        client_factory=FakeOfficialClient,
        reader_factory=lambda _: FixtureOfficialReader(holder["context"]),
        writer_factory=writer_factory,
    )
    common = [
        "--project-root",
        str(ROOT),
        "--work-root",
        str(work_root),
        "--gms-url",
        "http://localhost:8080",
        "--evidence-base-url",
        "https://evidence.example.invalid/lineagetx",
    ]
    paused = asyncio.run(
        LIVE.run_live(LIVE.parse_args(common), components=components)
    )
    owner = paused["owner_approval_request"]["owner_urn"]

    def materialize_with_drift(*args: Any, **kwargs: Any) -> Any:
        runtime = materialize(*args, **kwargs)
        drifted_bases = dict(runtime.base_shas)
        drifted_bases[next(iter(drifted_bases))] = "f" * 40
        drifted = replace(runtime, base_shas=drifted_bases)
        holder["runtime"] = drifted
        return drifted

    drifted_components = replace(components, materialize=materialize_with_drift)
    resume_args = LIVE.parse_args(
        [
            *common,
            "--phase",
            "resume",
            "--approval-api-url",
            "https://api.github.com/repos/Lukeknow0/data-lineage-fix-agent/issues/comments/42",
            "--owner-github-login",
            f"{owner}=identity-owner",
        ]
    )
    with pytest.raises(LIVE.LiveRunnerError, match="context changed"):
        asyncio.run(LIVE.run_live(resume_args, components=drifted_components))

    assert len(holder["writers"]) == 1
    assert holder["runtime"].state.list_migrations() == []
    for repository in holder["runtime"].repositories.values():
        branches = subprocess.run(
            ["git", "-C", str(repository), "branch", "--list", "lineagetx/*"],
            text=True,
            capture_output=True,
            check=True,
        )
        assert branches.stdout.strip() == ""
    snapshot = work_root / LIVE.PAUSE_SNAPSHOT_NAME
    assert snapshot.is_file()
    LIVE.read_signed_pause_snapshot(
        snapshot,
        key=resume_key_text.encode("utf-8"),
    )


def test_receipt_guard_rejects_secrets_and_absolute_paths() -> None:
    with pytest.raises(LIVE.LiveRunnerError, match="credential-shaped key"):
        LIVE._validate_receipt_is_credential_free({"access_token": "redacted"})
    with pytest.raises(LIVE.LiveRunnerError, match="absolute local path"):
        LIVE._validate_receipt_is_credential_free({"evidence": "/tmp/private.json"})


def test_live_runner_exposes_no_candidate_model_selector() -> None:
    args = LIVE.parse_args([])

    assert not hasattr(args, "proposal_model")


def test_live_gate_rejects_lite_before_health_or_seed() -> None:
    completed = subprocess.run(
        ["bash", str(GATE)],
        cwd=ROOT,
        env={
            **os.environ,
            "PYTHON": sys.executable,
            "DATAHUB_GMS_URL": "http://localhost:8979",
        },
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 2
    assert "8979" in completed.stderr
    assert "legacy compatibility bridge" in completed.stderr


def test_live_gate_order_and_shell_syntax() -> None:
    completed = subprocess.run(
        ["bash", "-n", str(GATE)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    source = GATE.read_text(encoding="utf-8")
    guard = 'if [[ "$PHASE" != "resume" ]]; then'
    guard_index = source.index(guard)
    seed_index = source.index("scripts/seed_lineagetx_datahub.py")
    guard_end_index = source.index("\nfi", seed_index)
    integration_index = source.index("pytest")
    runner_index = source.index("scripts/run_lineagetx_live.py")
    assert guard_index < seed_index < guard_end_index < integration_index < runner_index


def test_live_gate_resume_does_not_reseed_datahub(tmp_path: Path) -> None:
    fake_python = tmp_path / "fake-python"
    calls = tmp_path / "calls.log"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$FAKE_PYTHON_CALLS\"\n"
        "if [ \"$1\" = \"-\" ] && [ \"$#\" -ge 2 ]; then\n"
        "  printf '%s\\n' \"$2\"\n"
        "fi\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o700)

    completed = subprocess.run(
        ["bash", str(GATE)],
        cwd=ROOT,
        env={
            **os.environ,
            "PYTHON": str(fake_python),
            "FAKE_PYTHON_CALLS": str(calls),
            "LINEAGETX_PHASE": "resume",
            "DATAHUB_GMS_URL": "http://localhost:8080",
        },
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    recorded = calls.read_text(encoding="utf-8")
    assert "scripts/seed_lineagetx_datahub.py" not in recorded
    assert "-m pytest" in recorded
    assert "scripts/run_lineagetx_live.py" in recorded
