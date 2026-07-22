from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import pytest

from data_lineage_fix_agent.lineagetx.evidence import EvidenceError, EvidenceRecorder


def test_serializes_frozen_dataclass_with_mappingproxy_context(tmp_path: Path) -> None:
    @dataclass(frozen=True)
    class FrozenContext:
        assets: Mapping[str, object]

    recorder = EvidenceRecorder(tmp_path / "evidence", "ltx-test-context")
    recorder.write_json(
        "context/datahub.json",
        FrozenContext(assets=MappingProxyType({"source": {"verified": True}})),
    )

    assert json.loads((recorder.root / "context/datahub.json").read_text()) == {
        "assets": {"source": {"verified": True}}
    }


def test_manifest_hashes_every_evidence_artifact_and_verifies_exact_file_set(
    tmp_path: Path,
) -> None:
    recorder = EvidenceRecorder(tmp_path / "evidence", "ltx-test-001")
    first = recorder.write_json("context/datahub.json", {"discovery_complete": True})
    second = recorder.write_text("diffs/dbt.patch", "- customer_id\n+ customer_key\n")

    manifest = recorder.build_manifest(generated_at="2026-07-17T01:02:03.000000Z")
    verified = recorder.verify_manifest()

    assert manifest == verified
    assert {item.path for item in manifest.files} == {
        "context/datahub.json",
        "diffs/dbt.patch",
    }
    assert {item.sha256 for item in manifest.files} == {first.sha256, second.sha256}
    serialized = json.loads((recorder.root / "manifest.json").read_text())
    assert serialized["manifest_self_hash_excluded"] is True
    assert all(len(item["sha256"]) == 64 for item in serialized["files"])


def test_manifest_detects_tampering_and_unmanifested_files(tmp_path: Path) -> None:
    recorder = EvidenceRecorder(tmp_path / "evidence", "ltx-test-002")
    recorder.write_text("receipt.txt", "verified\n")
    recorder.build_manifest()
    (recorder.root / "receipt.txt").write_text("tampered\n", encoding="utf-8")

    with pytest.raises(EvidenceError, match="SHA-256|size"):
        recorder.verify_manifest()


def test_manifest_paths_use_posix_string_order_for_browser_verifiers(
    tmp_path: Path,
) -> None:
    recorder = EvidenceRecorder(tmp_path / "evidence", "ltx-browser-order")
    recorder.write_json("publication/receipts.json", {"kind": "receipt"})
    recorder.write_json("publication.json", {"kind": "summary"})

    manifest = recorder.build_manifest()

    assert [item.path for item in manifest.files] == [
        "publication.json",
        "publication/receipts.json",
    ]

    recorder.build_manifest()
    (recorder.root / "unexpected.txt").write_text("not manifested\n", encoding="utf-8")
    with pytest.raises(EvidenceError, match="file set"):
        recorder.verify_manifest()


def test_recorder_rejects_paths_outside_bundle(tmp_path: Path) -> None:
    recorder = EvidenceRecorder(tmp_path / "evidence", "ltx-test-003")

    with pytest.raises(EvidenceError, match="unsafe"):
        recorder.write_text("../escape.txt", "no")


def test_recorder_and_manifest_reject_symbolic_links(tmp_path: Path) -> None:
    recorder = EvidenceRecorder(tmp_path / "evidence", "ltx-test-004")
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = recorder.root / "linked"
    linked.symlink_to(outside, target_is_directory=True)

    with pytest.raises(EvidenceError, match="symbolic links"):
        recorder.write_text("linked/escape.txt", "no")
    with pytest.raises(EvidenceError, match="symlink"):
        recorder.build_manifest()
