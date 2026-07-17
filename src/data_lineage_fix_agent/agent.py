from __future__ import annotations

from pathlib import Path
from typing import Protocol

from .evidence import EvidenceWriter, new_run_id
from .models import DataHubContext, RunResult
from .pipeline import apply_minimal_identifier_patch, build_regression_test, run_regression
from .planner import GroundedPatchPlanner


class ContextProvider(Protocol):
    async def load(self, source_urn: str, target_urn: str) -> DataHubContext: ...


class StatusWriter(Protocol):
    async def write_verified(self, entity_urn: str, run_id: str) -> dict: ...


class DataLineageFixAgent:
    def __init__(
        self,
        context_provider: ContextProvider,
        status_writer: StatusWriter,
        evidence_writer: EvidenceWriter,
    ):
        self.context_provider = context_provider
        self.status_writer = status_writer
        self.evidence_writer = evidence_writer
        self.planner = GroundedPatchPlanner()

    async def run(
        self,
        workspace: Path,
        source_urn: str,
        target_urn: str,
        repository_relative_path: str,
        table_name: str,
    ) -> RunResult:
        run_id = new_run_id()
        sql_path = workspace / repository_relative_path
        original_sql = sql_path.read_text(encoding="utf-8")

        context = await self.context_provider.load(source_urn, target_urn)
        finding = self.planner.analyze(context, sql_path, repository_relative_path)
        regression_path = build_regression_test(workspace, finding, table_name)
        regression_test = regression_path.read_text(encoding="utf-8")
        before = run_regression(workspace)
        if before.returncode == 0:
            raise RuntimeError("Regression test was not red before the patch")

        patch = apply_minimal_identifier_patch(sql_path, finding)
        after = run_regression(workspace)
        if after.returncode != 0:
            sql_path.write_text(original_sql, encoding="utf-8")
            raise RuntimeError("Patch failed the generated regression; source was restored")

        writeback = await self.status_writer.write_verified(target_urn, run_id)
        result = RunResult(
            run_id=run_id,
            status="verified-fixed",
            finding=finding,
            context=context,
            patch=patch,
            regression_test=regression_test,
            before=before,
            after=after,
            writeback=writeback,
        )
        self.evidence_writer.write(result)
        return result
