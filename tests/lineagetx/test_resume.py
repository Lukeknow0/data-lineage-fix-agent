from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_lineage_fix_agent.lineagetx.resume import (
    ApprovalPauseSnapshot,
    ResumeSnapshotError,
    read_signed_pause_snapshot,
    write_signed_pause_snapshot,
)


KEY = b"a-dedicated-lineagetx-resume-key-32-bytes-minimum"


def snapshot() -> ApprovalPauseSnapshot:
    return ApprovalPauseSnapshot(
        migration_id="ltx-7ba06b0789512486f0f92f3c",
        participant_id="participant-semantic",
        owner_urn="urn:li:corpuser:identity-data-owner",
        old_field="customer_id",
        new_field="customer_key",
        impact_fingerprint="a" * 64,
        repository_base_shas={"repo-a": "b" * 40, "repo-b": "c" * 40},
        paused_at="2026-07-17T02:00:00Z",
    )


def test_signed_pause_snapshot_round_trips_without_persisting_key(tmp_path: Path) -> None:
    target = tmp_path / "approval-pause.json"

    write_signed_pause_snapshot(target, snapshot(), key=KEY)
    restored = read_signed_pause_snapshot(target, key=KEY)

    assert restored == snapshot()
    persisted = target.read_text(encoding="utf-8")
    assert KEY.decode("utf-8") not in persisted
    assert target.stat().st_mode & 0o777 == 0o600


def test_tampered_plain_resume_fields_are_rejected(tmp_path: Path) -> None:
    target = tmp_path / "approval-pause.json"
    write_signed_pause_snapshot(target, snapshot(), key=KEY)
    value = json.loads(target.read_text(encoding="utf-8"))
    value["payload"]["new_field"] = "attacker_field"
    target.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(ResumeSnapshotError, match="signature is invalid"):
        read_signed_pause_snapshot(target, key=KEY)


def test_wrong_resume_key_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "approval-pause.json"
    write_signed_pause_snapshot(target, snapshot(), key=KEY)

    with pytest.raises(ResumeSnapshotError, match="signature is invalid"):
        read_signed_pause_snapshot(target, key=b"x" * 40)
