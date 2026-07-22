from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEMO = ROOT / "demo"
LEGACY_ROOT = DEMO / "evidence" / "20260716T091507467904Z"
LIVE_ROOT = DEMO / "evidence" / "ltx-7ba06b0789512486f0f92f3c"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
CANONICAL_LEGACY_FILES = {
    "EVIDENCE.md",
    "after.txt",
    "before.txt",
    "context.json",
    "finding.json",
    "patch.diff",
    "regression_test.py",
    "writeback.json",
}


def _strict_json(path: Path) -> Any:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            assert key not in result, f"duplicate JSON key in {path}: {key}"
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_keys)


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_legacy_manifest_has_exact_canonical_file_set_and_valid_hashes() -> None:
    manifest = _strict_json(LEGACY_ROOT / "manifest.json")

    assert isinstance(manifest, dict)
    assert set(manifest) == CANONICAL_LEGACY_FILES
    assert len(manifest) == 8
    for name, expected_hash in manifest.items():
        assert SHA256.fullmatch(expected_hash), name
        assert "/" not in name and "\\" not in name and name not in {".", ".."}
        assert _digest(LEGACY_ROOT / name) == expected_hash


def test_replay_manifest_covers_exactly_the_replay_fixture() -> None:
    manifest = _strict_json(DEMO / "lineagetx-replay.manifest.json")

    assert manifest.keys() == {"lineagetx-replay.json"}
    expected_hash = manifest["lineagetx-replay.json"]
    assert SHA256.fullmatch(expected_hash)
    assert _digest(DEMO / "lineagetx-replay.json") == expected_hash


def test_public_replay_discloses_verified_replay_and_published_live_proof() -> None:
    replay = _strict_json(DEMO / "lineagetx-replay.json")

    assert replay["mode"] == "interactive-synthetic-replay"
    assert replay["liveVerified"] is True
    assert "not connected" in replay["disclosure"].lower()
    assert "verified interactive replay" in replay["disclosure"].lower()
    assert set(replay["datahubContext"]["governanceSignals"]) == {
        "schema",
        "owner",
        "tag",
    }
    live = replay["liveEvidence"]
    manifest = _strict_json(LIVE_ROOT / "manifest.json")
    assert live["status"] == "published"
    assert live["manifestPath"] == (
        "evidence/ltx-7ba06b0789512486f0f92f3c/manifest.json"
    )
    assert live["expectedMigrationId"] == replay["migration"]["id"]
    assert live["expectedFiles"] == [item["path"] for item in manifest["files"]]
    assert manifest["manifest_self_hash_excluded"] is True
    assert manifest["migration_id"] == replay["migration"]["id"]

    descriptors = []
    for item in manifest["files"]:
        path = LIVE_ROOT / item["path"]
        assert path.is_file() and not path.is_symlink()
        assert path.stat().st_size == item["size_bytes"]
        assert _digest(path) == item["sha256"]
        descriptors.append(item)
    serialized = json.dumps(
        descriptors, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    assert hashlib.sha256(serialized).hexdigest() == manifest["aggregate_sha256"]


def test_demo_verifier_enforces_canonical_manifests_and_future_live_bundle() -> None:
    script = (DEMO / "app.js").read_text(encoding="utf-8")

    for name in sorted(CANONICAL_LEGACY_FILES | {"lineagetx-replay.json"}):
        assert f'"{name}"' in script
    assert "assertExactFileSet" in script
    assert "sha256Pattern = /^[0-9a-f]{64}$/" in script
    assert "verifyFlatManifest" in script
    assert "verifyPublishedLiveEvidence" in script
    assert "manifest_self_hash_excluded !== true" in script
    assert "Live evidence aggregate SHA-256 mismatch" in script


def test_public_copy_distinguishes_verified_replay_from_live_control_plane() -> None:
    paths = [
        ROOT / "README.md",
        ROOT / "docs" / "demo-script.md",
        ROOT / "docs" / "lineagetx-architecture.md",
        DEMO / "index.html",
        DEMO / "lineagetx-replay.json",
    ]
    combined = "\n".join(path.read_text(encoding="utf-8") for path in paths).lower()

    for unsupported_claim in (
        "previously verified run",
        "video shows live datahub",
        "video shows the real datahub",
    ):
        assert unsupported_claim not in combined

    current_demo_copy = (DEMO / "index.html").read_text(encoding="utf-8").lower()
    replay_copy = (DEMO / "lineagetx-replay.json").read_text(encoding="utf-8").lower()
    assert "quality" not in current_demo_copy
    assert "quality" not in replay_copy
    assert "verified interactive replay" in current_demo_copy
    assert re.search(r"not\s+connected to datahub or github", current_demo_copy)
    assert "live datahub oss evidence: published" in current_demo_copy
