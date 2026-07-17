from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from .models import RunResult


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class EvidenceWriter:
    def __init__(self, project_root: Path):
        self.project_root = project_root

    def write(self, result: RunResult) -> Path:
        run_dir = self.project_root / "artifacts" / "runs" / result.run_id
        run_dir.mkdir(parents=True, exist_ok=False)

        _write_json(run_dir / "context.json", result.context.to_dict())
        _write_json(run_dir / "finding.json", result.finding.to_dict())
        (run_dir / "patch.diff").write_text(result.patch, encoding="utf-8")
        (run_dir / "regression_test.py").write_text(result.regression_test, encoding="utf-8")
        (run_dir / "before.txt").write_text(
            self._sanitize(result.before.output), encoding="utf-8"
        )
        (run_dir / "after.txt").write_text(
            self._sanitize(result.after.output), encoding="utf-8"
        )
        _write_json(run_dir / "writeback.json", result.writeback)

        evidence = self._render_markdown(result)
        (run_dir / "EVIDENCE.md").write_text(evidence, encoding="utf-8")
        manifest = {
            path.name: _sha256(path)
            for path in sorted(run_dir.iterdir())
            if path.name != "manifest.json"
        }
        _write_json(run_dir / "manifest.json", manifest)

        root_evidence = self.project_root / "EVIDENCE.md"
        root_evidence.write_text(evidence, encoding="utf-8")
        result.evidence_dir = str(run_dir)
        return run_dir

    def _sanitize(self, value: str) -> str:
        return value.replace(str(self.project_root), "$PROJECT_ROOT")

    def _render_markdown(self, result: RunResult) -> str:
        before_tail = self._sanitize(result.before.output).strip()[-1600:]
        after_tail = self._sanitize(result.after.output).strip()[-1600:]
        tools = ", ".join(
            trace.get("tool", "unknown") for trace in result.context.tool_traces
        )
        return f"""# Evidence — {result.run_id}

Status: **{result.status}**

## DataHub grounding

- Context transport: `{result.context.transport}`
- Source entity: `{result.finding.source_urn}`
- Affected downstream entity: `{result.finding.affected_entity_urn}`
- Owner(s): {', '.join(f'`{owner}`' for owner in result.finding.owner_urns)}
- Quality/governance signals: {', '.join(f'`{signal}`' for signal in result.finding.quality_signals)}
- DataHub tool trace: `{tools}`

## Finding

`{result.finding.kind}` in `{result.finding.repository_file}`: the code referenced `{result.finding.missing_field}`, which is absent from the current DataHub schema. DataHub explicitly identifies `{result.finding.replacement_field}` as its replacement and lineage identifies the mapped asset as downstream.

## Minimal patch

```diff
{result.patch.rstrip()}
```

## Regression: red before patch

```text
{before_tail}
```

## Regression: green after patch

```text
{after_tail}
```

## DataHub write-back

- Status: `{result.writeback.get('status', 'unknown')}`
- Entity: `{result.writeback.get('entity_urn', result.finding.affected_entity_urn)}`
- Tag/status: `{result.writeback.get('tag_urn', 'offline-replay')}`
- Verification: `{result.writeback.get('verification', 'not available')}`

Each runtime run directory contains the sibling JSON/text/diff files and `manifest.json` that form the machine-verifiable evidence bundle.
"""
