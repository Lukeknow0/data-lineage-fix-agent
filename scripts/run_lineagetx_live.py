#!/usr/bin/env python3
"""Run the bounded LineageTX transaction against a real DataHub OSS GMS.

The repositories are materialized as clean local fixtures, while discovery,
pre-commit impact revalidation, and every migration write-back go through the
official DataHub MCP server. Publication is deliberately a local, unmerged
receipt for this OSS proof; this runner never creates or merges a remote PR.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from data_lineage_fix_agent.cli import materialize_lineagetx_fixture
from data_lineage_fix_agent.lineagetx.coordinator import (
    CoordinationError,
    LineageTXCoordinator,
)
from data_lineage_fix_agent.lineagetx.datahub_context import (
    DataHubMCPContextReader,
    OfficialDataHubMCPClient,
    normalize_datahub_gms_url,
)
from data_lineage_fix_agent.lineagetx.github_approval import GitHubApprovalVerifier
from data_lineage_fix_agent.lineagetx.models import (
    MigrationStatus,
    ParticipantKind,
    ParticipantStatus,
)
from data_lineage_fix_agent.lineagetx.participants.semantic_approval import (
    OwnerApproval,
)
from data_lineage_fix_agent.lineagetx.policy import (
    DiscoveryAttestation,
    LineageTXSafetyPolicy,
)
from data_lineage_fix_agent.lineagetx.proposals import DeterministicCandidateModel
from data_lineage_fix_agent.lineagetx.publisher import LocalReceiptPublisher
from data_lineage_fix_agent.lineagetx.resume import (
    ApprovalPauseSnapshot,
    ResumeSnapshotError,
    read_signed_pause_snapshot,
    resume_key_from_environment,
    write_signed_pause_snapshot,
)
from data_lineage_fix_agent.lineagetx.worktrees import WorktreeManager
from data_lineage_fix_agent.lineagetx.writeback import (
    DataHubMigrationWriter,
    WritebackReceipt,
)


WRITEBACK_STAGES = (
    "detected",
    "preparing",
    "needs_approval",
    "prepared",
    "precommit-refreshed",
    "committed",
)
PAUSE_WRITEBACK_STAGES = WRITEBACK_STAGES[:3]
PAUSE_SNAPSHOT_NAME = "approval-pause.json"
DEFAULT_EVIDENCE_BASE_URL = (
    "https://datahub-agent-hackathon-2026.vercel.app/evidence"
)


class LiveRunnerError(RuntimeError):
    """The live proof failed one of its fail-closed completion checks."""


def _resume_snapshot_staging_path(work_root: Path) -> Path:
    """Return a crash-recoverable sidecar outside the reset-owned work root."""

    return work_root.parent / f".{work_root.name}.{PAUSE_SNAPSHOT_NAME}.pending-reset"


def _stage_resume_snapshot_for_reset(
    snapshot_path: Path,
    staging_path: Path,
    *,
    key: bytes,
) -> ApprovalPauseSnapshot:
    """Verify and atomically protect durable approval state before reset."""

    snapshot_exists = snapshot_path.is_file()
    staging_exists = staging_path.is_file()
    if snapshot_exists and staging_exists:
        raise ResumeSnapshotError(
            "both active and pending-reset approval snapshots exist"
        )
    source = snapshot_path if snapshot_exists else staging_path
    snapshot = read_signed_pause_snapshot(source, key=key)
    if snapshot_exists:
        snapshot_path.replace(staging_path)
    return snapshot


def _restore_staged_resume_snapshot(
    snapshot_path: Path,
    staging_path: Path,
) -> None:
    """Restore the canonical snapshot after a successful work-root reset."""

    if not staging_path.is_file():
        raise ResumeSnapshotError(
            "pending-reset approval snapshot disappeared during reprepare"
        )
    if snapshot_path.exists():
        raise ResumeSnapshotError(
            "approval snapshot path was unexpectedly recreated during reprepare"
        )
    staging_path.replace(snapshot_path)


@dataclass(frozen=True)
class RunnerComponents:
    """Small dependency seam used by focused tests; production uses real classes."""

    materialize: Callable[..., Any] = materialize_lineagetx_fixture
    client_factory: Callable[..., Any] = OfficialDataHubMCPClient
    reader_factory: Callable[..., Any] = DataHubMCPContextReader
    writer_factory: Callable[..., Any] = DataHubMigrationWriter
    coordinator_factory: Callable[..., Any] = LineageTXCoordinator
    publisher_factory: Callable[..., Any] = LocalReceiptPublisher
    worktree_factory: Callable[..., Any] = WorktreeManager
    approval_verifier_factory: Callable[..., Any] = GitHubApprovalVerifier


@dataclass
class RecordingMigrationWriter:
    """Record verified write-back receipts without changing writer semantics."""

    delegate: Any
    receipts: list[WritebackReceipt] = field(default_factory=list)

    async def write_assets(
        self,
        writebacks: Mapping[str, Any],
        *,
        supplied_urns: tuple[str, ...] | None = None,
        refreshed_context: Any | None = None,
    ) -> WritebackReceipt:
        receipt = await self.delegate.write_assets(
            writebacks,
            supplied_urns=supplied_urns,
            refreshed_context=refreshed_context,
        )
        self.receipts.append(receipt)
        return receipt


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the complete LineageTX coordinator against full DataHub OSS "
            "through the official MCP server."
        )
    )
    parser.add_argument(
        "--phase",
        choices=("pause", "resume", "scripted-test"),
        default="pause",
        help=(
            "pause durably at NEEDS_APPROVAL (default), resume using a GitHub "
            "approval, or use the explicitly test-only scripted path"
        ),
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root containing fixtures/lineagetx.",
    )
    parser.add_argument(
        "--work-root",
        type=Path,
        default=Path("artifacts/runs/lineagetx-live"),
        help="Owned runtime directory for isolated repositories and evidence.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Replace a previous LineageTX-owned work directory.",
    )
    parser.add_argument(
        "--gms-url",
        default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
        help="Credential-free DataHub GMS origin. Port 8979 is rejected.",
    )
    parser.add_argument(
        "--mcp-timeout-seconds",
        type=float,
        default=30.0,
        help="Per-operation official MCP timeout (default: 30 seconds).",
    )
    parser.add_argument(
        "--approval-api-url",
        default=os.getenv("LINEAGETX_APPROVAL_API_URL", ""),
        help=(
            "Canonical HTTPS api.github.com issue-comment or PR-review URL; "
            "required for --phase resume"
        ),
    )
    parser.add_argument(
        "--owner-github-login",
        action="append",
        default=[
            item
            for item in os.getenv("LINEAGETX_OWNER_GITHUB_LOGIN", "").split(",")
            if item
        ],
        metavar="OWNER_URN=LOGIN",
        help=(
            "Trusted DataHub-owner-to-GitHub-login mapping; repeatable and "
            "required for --phase resume"
        ),
    )
    parser.add_argument(
        "--approval-allow-login",
        action="append",
        default=[
            item
            for item in os.getenv("LINEAGETX_APPROVAL_ALLOW_LOGIN", "").split(",")
            if item
        ],
        help=(
            "Explicitly allow a mapped GitHub login without OWNER/MEMBER/"
            "COLLABORATOR author association"
        ),
    )
    parser.add_argument(
        "--github-token-env",
        default="GITHUB_TOKEN",
        help="Environment variable containing an optional GitHub API token.",
    )
    parser.add_argument(
        "--resume-hmac-key-env",
        default="LINEAGETX_RESUME_HMAC_KEY",
        help=(
            "Environment variable containing at least 32 bytes used to sign "
            "and verify durable NEEDS_APPROVAL state"
        ),
    )
    parser.add_argument(
        "--approved-at",
        default="2026-07-17T01:03:00.000000Z",
        help="Test-only timestamp used only by --phase scripted-test.",
    )
    parser.add_argument(
        "--approval-evidence-url",
        default=(
            "https://datahub-agent-hackathon-2026.vercel.app/"
            "evidence/semantic-owner-approval"
        ),
        help="Test-only evidence URL used only by --phase scripted-test.",
    )
    parser.add_argument(
        "--evidence-base-url",
        default=os.getenv("LINEAGETX_EVIDENCE_BASE_URL", DEFAULT_EVIDENCE_BASE_URL),
        help="Public HTTPS base used for per-stage DataHub evidence links.",
    )
    return parser.parse_args(argv)


def _owner_login_mapping(values: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for value in values:
        owner_urn, separator, login = value.partition("=")
        owner_urn = owner_urn.strip()
        login = login.strip()
        if (
            separator != "="
            or not owner_urn.startswith("urn:li:")
            or not login
            or owner_urn in mapping
        ):
            raise LiveRunnerError(
                "--owner-github-login must be a unique OWNER_URN=LOGIN mapping"
            )
        mapping[owner_urn] = login
    return mapping


def _assert_reprepared_snapshot(
    snapshot: ApprovalPauseSnapshot,
    *,
    runtime: Any,
    context: Any,
    semantic: Any,
) -> None:
    actual = ApprovalPauseSnapshot(
        migration_id=runtime.intent.migration_id,
        participant_id=semantic.participant_id,
        owner_urn=semantic.owner_urns[0],
        old_field=runtime.intent.old_field,
        new_field=runtime.intent.new_field,
        impact_fingerprint=context.impact_fingerprint,
        repository_base_shas=runtime.base_shas,
        paused_at=snapshot.paused_at,
    )
    if actual != snapshot:
        raise LiveRunnerError(
            "durable approval context changed; refusing to apply the owner decision"
        )


def _git(repo: Path, *arguments: str) -> str:
    git = shutil.which("git", path=os.defpath)
    if git is None:
        raise LiveRunnerError("git is unavailable for the base integrity check")
    completed = subprocess.run(
        [
            git,
            "--no-pager",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.pager=cat",
            *arguments,
        ],
        cwd=repo,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=15,
        env={
            "PATH": os.defpath,
            "HOME": "/nonexistent-lineagetx-home",
            "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
            "LANG": "C",
            "LC_ALL": "C",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_PAGER": "cat",
            "PAGER": "cat",
        },
    )
    if completed.returncode != 0:
        raise LiveRunnerError("a base repository integrity check failed")
    return completed.stdout.strip()


def _base_checkouts_unchanged(runtime: Any) -> bool:
    for repository, root in runtime.repositories.items():
        if _git(root, "rev-parse", "HEAD") != runtime.base_shas[repository]:
            return False
        if _git(root, "status", "--porcelain=v1"):
            return False
    return True


def _validate_receipt_is_credential_free(value: Any, *, path: str = "receipt") -> None:
    """Reject secret-shaped keys, local paths, and file URLs before stdout."""

    if isinstance(value, Mapping):
        for key, nested in value.items():
            key_text = str(key)
            normalized = key_text.lower().replace("-", "_")
            if any(
                marker in normalized
                for marker in (
                    "authorization",
                    "password",
                    "secret",
                    "api_key",
                    "access_token",
                    "gms_token",
                    "cookie",
                )
            ):
                raise LiveRunnerError(
                    f"credential-shaped key is forbidden in {path}: {key_text}"
                )
            _validate_receipt_is_credential_free(
                nested, path=f"{path}.{key_text}"
            )
        return
    if isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _validate_receipt_is_credential_free(
                nested, path=f"{path}[{index}]"
            )
        return
    if isinstance(value, str):
        if value.startswith(("/", "file://")) or re.match(r"^[A-Za-z]:[\\/]", value):
            raise LiveRunnerError(f"absolute local path is forbidden in {path}")


def _writeback_summary(
    migration_id: str,
    receipts: list[WritebackReceipt],
    evidence_root: Path,
    impact_fingerprint: str,
    *,
    expected_stages: tuple[str, ...] = WRITEBACK_STAGES,
) -> list[dict[str, Any]]:
    if len(receipts) != len(expected_stages):
        raise LiveRunnerError(
            "the live phase did not produce every expected per-stage write-back"
        )
    summaries: list[dict[str, Any]] = []
    for stage, receipt in zip(expected_stages, receipts, strict=True):
        if not (evidence_root / "writeback" / f"{stage}.json").is_file():
            raise LiveRunnerError(f"write-back evidence file is missing at {stage}")
        if not receipt.live_verified:
            raise LiveRunnerError(f"DataHub write-back was not live-verified at {stage}")
        if receipt.transport != OfficialDataHubMCPClient.transport:
            raise LiveRunnerError(f"write-back transport was not official MCP at {stage}")
        if receipt.migration_id != migration_id:
            raise LiveRunnerError("write-back receipt belongs to another migration")
        if receipt.impact_fingerprint != impact_fingerprint:
            raise LiveRunnerError(f"write-back fingerprint changed at {stage}")
        if not re.fullmatch(r"[0-9a-f]{64}", receipt.readback_sha256):
            raise LiveRunnerError(f"write-back read-back hash is invalid at {stage}")
        summaries.append(
            {
                "stage": stage,
                "live_verified": True,
                "readback_sha256": receipt.readback_sha256,
                "evidence_file": f"evidence/{migration_id}/writeback/{stage}.json",
            }
        )
    return summaries


async def run_live(
    args: argparse.Namespace,
    *,
    components: RunnerComponents | None = None,
) -> dict[str, Any]:
    components = components or RunnerComponents()
    if args.mcp_timeout_seconds <= 0:
        raise LiveRunnerError("--mcp-timeout-seconds must be positive")
    gms_origin = normalize_datahub_gms_url(args.gms_url)
    project_root = args.project_root.expanduser().resolve()
    work_root = args.work_root.expanduser()
    if not work_root.is_absolute():
        work_root = project_root / work_root

    pause_snapshot: ApprovalPauseSnapshot | None = None
    resume_key: bytes | None = None
    snapshot_path = work_root / PAUSE_SNAPSHOT_NAME
    staging_snapshot_path = _resume_snapshot_staging_path(work_root)
    if args.phase in {"pause", "resume"}:
        resume_key = resume_key_from_environment(args.resume_hmac_key_env)
    if args.phase == "resume":
        if args.reset:
            raise LiveRunnerError("--reset cannot be combined with --phase resume")
        if not args.approval_api_url:
            raise LiveRunnerError("--approval-api-url is required for --phase resume")
        assert resume_key is not None
        pause_snapshot = _stage_resume_snapshot_for_reset(
            snapshot_path,
            staging_snapshot_path,
            key=resume_key,
        )

    proposal_model = DeterministicCandidateModel()
    runtime = components.materialize(
        project_root,
        work_root,
        # Resume deliberately reconstructs fresh isolated candidates. The
        # signed request is checked against the newly discovered/reprepared
        # context before the external approval can be applied.
        reset=(args.reset or args.phase == "resume"),
        proposal_model=proposal_model,
    )
    if args.phase == "resume":
        _restore_staged_resume_snapshot(snapshot_path, staging_snapshot_path)
    client = components.client_factory(
        gms_origin,
        operation_timeout_seconds=args.mcp_timeout_seconds,
    )
    reader = components.reader_factory(client)
    context = await reader.load(
        runtime.intent.source_asset_urn,
        runtime.intent.old_field,
        runtime.intent.new_field,
    )
    if context.transport != OfficialDataHubMCPClient.transport:
        raise LiveRunnerError("discovery did not use the official DataHub MCP transport")
    if not context.discovery_complete:
        raise LiveRunnerError("official MCP discovery was not complete")

    if args.phase == "resume":
        assert pause_snapshot is not None
        semantic_templates = tuple(
            item
            for item in runtime.participants
            if item.kind is ParticipantKind.SEMANTIC_APPROVAL
        )
        if len(semantic_templates) != 1:
            raise LiveRunnerError(
                "bounded live scenario requires exactly one semantic participant"
            )
        # Bind the signed owner decision to the freshly read DataHub context and
        # repository bases before detect, prepare, write-back, or candidate work.
        _assert_reprepared_snapshot(
            pause_snapshot,
            runtime=runtime,
            context=context,
            semantic=semantic_templates[0],
        )

    recording_writer = RecordingMigrationWriter(
        components.writer_factory(client, context)
    )
    approval_verifier: Any | None = None
    if args.phase == "resume":
        owner_login_by_urn = _owner_login_mapping(args.owner_github_login)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.github_token_env):
            raise LiveRunnerError("--github-token-env is not a valid environment name")
        approval_verifier = components.approval_verifier_factory(
            owner_login_by_urn=owner_login_by_urn,
            explicit_login_allowlist=tuple(args.approval_allow_login),
            token=os.getenv(args.github_token_env, ""),
        )
    coordinator = components.coordinator_factory(
        state=runtime.state,
        worktrees=components.worktree_factory(runtime.root / "isolated-worktrees"),
        proposal_model=proposal_model,
        publisher=components.publisher_factory(runtime.evidence.root),
        impact_revalidator=reader,
        migration_writer=recording_writer,
        safety_policy=LineageTXSafetyPolicy(),
        evidence=runtime.evidence,
        evidence_base_url=args.evidence_base_url,
        approval_verifier=approval_verifier,
        allow_test_approvals=(args.phase == "scripted-test"),
    )

    transitions = [MigrationStatus.DETECTED.value]
    detected = await coordinator.detect(
        runtime.intent,
        DiscoveryAttestation(context, discovery_complete=True),
        runtime.participants,
    )
    if detected.status is not MigrationStatus.DETECTED:
        raise LiveRunnerError("detect did not produce DETECTED")

    transitions.append(MigrationStatus.PREPARING.value)
    prepared = await coordinator.prepare(
        runtime.intent.migration_id,
        runtime.repositories,
    )
    transitions.append(prepared.migration.status.value)
    if prepared.migration.status is not MigrationStatus.NEEDS_APPROVAL:
        raise LiveRunnerError("bounded live scenario did not stop for owner approval")
    if len(prepared.pending_approval_participant_ids) != 1:
        raise LiveRunnerError("bounded live scenario requires exactly one approval")
    semantic = next(
        item
        for item in prepared.participants
        if item.kind is ParticipantKind.SEMANTIC_APPROVAL
    )
    if (
        semantic.status is not ParticipantStatus.NEEDS_APPROVAL
        or semantic.participant_id != prepared.pending_approval_participant_ids[0]
        or prepared.results[semantic.participant_id].changed_files
    ):
        raise LiveRunnerError(
            "semantic consumer did not remain zero-write while awaiting its exact owner"
        )

    if args.phase == "pause":
        assert resume_key is not None
        pause_snapshot = ApprovalPauseSnapshot(
            migration_id=runtime.intent.migration_id,
            participant_id=semantic.participant_id,
            owner_urn=semantic.owner_urns[0],
            old_field=runtime.intent.old_field,
            new_field=runtime.intent.new_field,
            impact_fingerprint=context.impact_fingerprint,
            repository_base_shas=runtime.base_shas,
            paused_at=prepared.migration.updated_at,
        )
        write_signed_pause_snapshot(snapshot_path, pause_snapshot, key=resume_key)
        participants = tuple(
            runtime.state.list_participants(runtime.intent.migration_id)
        )
        if not _base_checkouts_unchanged(runtime):
            raise LiveRunnerError("an isolated candidate modified a base checkout")
        manifest = runtime.evidence.verify_manifest()
        writebacks = _writeback_summary(
            runtime.intent.migration_id,
            recording_writer.receipts,
            runtime.evidence.root,
            context.impact_fingerprint,
            expected_stages=PAUSE_WRITEBACK_STAGES,
        )
        final_writeback = recording_writer.receipts[-1]
        expected_statuses = {
            context.source_urn: MigrationStatus.NEEDS_APPROVAL.value,
            **{item.asset_urn: item.status.value for item in participants},
        }
        if dict(final_writeback.asset_statuses) != expected_statuses:
            raise LiveRunnerError(
                "NEEDS_APPROVAL DataHub read-back did not contain exact asset states"
            )
        receipt = {
            "schema_version": 1,
            "phase": "pause",
            "migration_id": runtime.intent.migration_id,
            "migration_state": MigrationStatus.NEEDS_APPROVAL.value,
            "transitions": transitions,
            "unverified_consumers": 1,
            "upstream_change_safe_to_merge": False,
            "result": "Owner approval required — upstream change remains blocked.",
            "base_checkouts_unchanged": True,
            "datahub": {
                "backend": "full-datahub-oss",
                "transport": context.transport,
                "live_verified": True,
                "discovery_complete": True,
                "impact_fingerprint": context.impact_fingerprint,
            },
            "participants": [
                {
                    "participant_id": item.participant_id,
                    "kind": item.kind.value,
                    "repository": item.repository,
                    "candidate_commit_sha": item.candidate_commit_sha,
                    "state": item.status.value,
                    "merged": False,
                }
                for item in participants
            ],
            "owner_approval_request": {
                "participant_id": semantic.participant_id,
                "owner_urn": semantic.owner_urns[0],
                "required_body": {
                    "decision": "APPROVED",
                    "migration_id": runtime.intent.migration_id,
                    "participant_id": semantic.participant_id,
                    "owner_urn": semantic.owner_urns[0],
                    "old_field": runtime.intent.old_field,
                    "new_field": runtime.intent.new_field,
                },
                "accepted_sources": [
                    "GitHub issue comment via HTTPS API",
                    "GitHub APPROVED PR review via HTTPS API",
                ],
            },
            "resume": {
                "strategy": "signed-context-deterministic-reprepare",
                "snapshot_file": PAUSE_SNAPSHOT_NAME,
                "in_memory_session_required": False,
            },
            "writeback_evidence": writebacks,
            "evidence": {
                "manifest_file": (
                    f"evidence/{runtime.intent.migration_id}/manifest.json"
                ),
                "aggregate_sha256": manifest.aggregate_sha256,
                "verified": True,
            },
            "publication": None,
            "contains_credentials": False,
        }
        _validate_receipt_is_credential_free(receipt)
        return receipt

    if args.phase == "resume":
        approved = await coordinator.approve_from_github(
            runtime.intent.migration_id,
            args.approval_api_url,
        )
    else:
        approved = await coordinator.approve(
            runtime.intent.migration_id,
            OwnerApproval(
                migration_id=runtime.intent.migration_id,
                participant_id=semantic.participant_id,
                owner_urn=semantic.owner_urns[0],
                old_field=runtime.intent.old_field,
                new_field=runtime.intent.new_field,
                approved_at=args.approved_at,
                evidence_url=args.approval_evidence_url,
            ),
        )
    transitions.append(approved.migration.status.value)
    if approved.migration.status is not MigrationStatus.PREPARED:
        raise LiveRunnerError("exact owner approval did not produce PREPARED")

    committed = await coordinator.commit(runtime.intent.migration_id)
    transitions.append(committed.migration.status.value)
    if committed.migration.status is not MigrationStatus.COMMITTED:
        raise LiveRunnerError("coordinator did not produce COMMITTED")
    if committed.auto_merged or not committed.upstream_change_safe_to_merge:
        raise LiveRunnerError("publication violated the unmerged gate-release contract")

    participants = tuple(runtime.state.list_participants(runtime.intent.migration_id))
    unverified = sum(
        item.status is not ParticipantStatus.VERIFIED for item in participants
    )
    if unverified:
        raise LiveRunnerError("COMMITTED still has unverified consumers")
    if any(not item.candidate_commit_sha for item in participants):
        raise LiveRunnerError("a verified participant is missing its candidate commit SHA")
    if not _base_checkouts_unchanged(runtime):
        raise LiveRunnerError("an isolated candidate modified a base checkout")

    manifest = runtime.evidence.verify_manifest()
    writebacks = _writeback_summary(
        runtime.intent.migration_id,
        recording_writer.receipts,
        runtime.evidence.root,
        context.impact_fingerprint,
    )
    final_writeback = recording_writer.receipts[-1]
    expected_final_statuses = {
        context.source_urn: MigrationStatus.COMMITTED.value,
        **{
            item.asset_urn: ParticipantStatus.VERIFIED.value
            for item in participants
        },
    }
    if dict(final_writeback.asset_statuses) != expected_final_statuses:
        raise LiveRunnerError("final DataHub read-back did not contain exact asset states")
    if final_writeback.impact_fingerprint != context.impact_fingerprint:
        raise LiveRunnerError("final write-back is not bound to the frozen impact fingerprint")

    publication = committed.publication
    approval_record = runtime.state.list_approvals(runtime.intent.migration_id)[0]
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "phase": args.phase,
        "migration_id": runtime.intent.migration_id,
        "migration_state": committed.migration.status.value,
        "transitions": transitions,
        "unverified_consumers": 0,
        "upstream_change_safe_to_merge": True,
        "result": "0 unverified consumers — upstream change is safe to merge.",
        "base_checkouts_unchanged": True,
        "datahub": {
            "backend": "full-datahub-oss",
            "transport": context.transport,
            "live_verified": True,
            "discovery_complete": True,
            "impact_fingerprint": context.impact_fingerprint,
        },
        "participants": [
            {
                "participant_id": item.participant_id,
                "kind": item.kind.value,
                "repository": item.repository,
                "candidate_commit_sha": item.candidate_commit_sha,
                "state": item.status.value,
                "merged": False,
            }
            for item in participants
        ],
        "owner_approval": {
            "mode": (
                "authenticated-github-api"
                if args.phase == "resume"
                else "scripted-test-only"
            ),
            "participant_id": semantic.participant_id,
            "owner_urn": semantic.owner_urns[0],
            "approved_mapping": (
                f"{runtime.intent.old_field} -> {runtime.intent.new_field}"
            ),
            "evidence_url": approval_record.evidence_url,
            "verification": dict(approval_record.verification),
        },
        "writeback_evidence": writebacks,
        "evidence": {
            "manifest_file": f"evidence/{runtime.intent.migration_id}/manifest.json",
            "aggregate_sha256": manifest.aggregate_sha256,
            "verified": True,
        },
        "publication": {
            "publisher": "local-receipt-only",
            "scope": "local-oss-proof-only",
            "remote_pr_created": False,
            "coordinated_receipt": (
                publication.coordinated_pr_receipt.reference
            ),
            "producer_gate_receipt": publication.producer_gate_receipt.reference,
            "auto_merged": False,
            "merged": False,
        },
        "contains_credentials": False,
    }
    _validate_receipt_is_credential_free(receipt)
    if args.phase == "resume":
        if snapshot_path.is_file():
            snapshot_path.unlink()
        if staging_snapshot_path.is_file():
            staging_snapshot_path.unlink()
    return receipt


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        receipt = asyncio.run(run_live(args))
    except (CoordinationError, LiveRunnerError, ResumeSnapshotError, ValueError) as error:
        raise SystemExit(f"LineageTX live coordinator failed: {error}") from error
    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
