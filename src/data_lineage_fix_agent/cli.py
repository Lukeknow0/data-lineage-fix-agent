from __future__ import annotations

import argparse
import asyncio
import json
import shutil
from pathlib import Path

from .agent import DataLineageFixAgent
from .context import FixtureContextProvider, OfflineReplayStatusWriter
from .evidence import EvidenceWriter


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def prepare_workspace(root: Path) -> tuple[Path, dict]:
    mapping = json.loads(
        (root / "fixture_pipeline" / "project_mapping.json").read_text(encoding="utf-8")
    )
    workspace = root / "fixture_pipeline" / "workspace"
    if workspace.exists():
        shutil.rmtree(workspace)
    shutil.copytree(root / "fixture_pipeline" / "broken", workspace)
    return workspace, mapping


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
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "run":
        raise SystemExit(asyncio.run(run_agent(args)))
