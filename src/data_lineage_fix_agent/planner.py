from __future__ import annotations

import re
from pathlib import Path

from sqlglot import exp, parse_one

from .models import DataHubContext, Finding


class ContextRefusal(RuntimeError):
    """Raised when a safe patch cannot be grounded in DataHub context."""


class NoGroundedFix(RuntimeError):
    """Raised when context exists but does not prove one unambiguous fix."""


def _explicit_rename(field_description: str, missing_field: str) -> bool:
    escaped = re.escape(missing_field)
    patterns = (
        rf"renamed\s+from\s+[`'\"]?{escaped}[`'\"]?",
        rf"replaces\s+[`'\"]?{escaped}[`'\"]?",
        rf"previous(?:ly)?\s+[`'\"]?{escaped}[`'\"]?",
    )
    return any(re.search(pattern, field_description, re.IGNORECASE) for pattern in patterns)


class GroundedPatchPlanner:
    """Plans only a narrow identifier replacement proven by DataHub metadata."""

    def analyze(
        self,
        context: DataHubContext,
        sql_path: Path,
        repository_relative_path: str,
    ) -> Finding:
        self._validate_context(context)
        sql = sql_path.read_text(encoding="utf-8")
        tree = parse_one(sql, read="sqlite")
        columns = [column.name for column in tree.find_all(exp.Column)]
        source_field_names = {field.name for field in context.source_fields}
        missing = sorted({name for name in columns if name not in source_field_names})

        if len(missing) != 1:
            raise NoGroundedFix(
                f"Expected exactly one schema-missing field, found {missing or 'none'}"
            )

        missing_field = missing[0]
        candidates = [
            field
            for field in context.source_fields
            if _explicit_rename(field.description, missing_field)
        ]
        rename_hint = context.source_properties.get(f"rename_hint.{missing_field}")
        if rename_hint:
            candidates.extend(
                field for field in context.source_fields if field.name == rename_hint
            )
        unique_candidates = {field.name: field for field in candidates}
        if len(unique_candidates) != 1:
            raise NoGroundedFix(
                "DataHub did not provide one explicit rename target; refusing to guess"
            )

        replacement = next(iter(unique_candidates.values()))
        return Finding(
            finding_id="DLFA-SCHEMA-001",
            kind="downstream-schema-drift",
            severity="high",
            source_urn=context.source_urn,
            affected_entity_urn=context.target_urn,
            repository_file=repository_relative_path,
            missing_field=missing_field,
            replacement_field=replacement.name,
            reference_count=columns.count(missing_field),
            owner_urns=context.owner_urns,
            quality_signals=context.quality_signals,
            rationale=(
                f"DataHub schema no longer contains '{missing_field}', field "
                f"'{replacement.name}' explicitly documents the rename, and DataHub "
                "lineage identifies the mapped repository asset as downstream."
            ),
        )

    @staticmethod
    def _validate_context(context: DataHubContext) -> None:
        failures: list[str] = []
        if not context.transport:
            failures.append("missing DataHub transport provenance")
        if not context.tool_traces:
            failures.append("missing DataHub tool traces")
        if not context.source_fields:
            failures.append("missing source schema")
        if context.target_urn not in context.downstream_urns:
            failures.append("target is not proven downstream by lineage")
        if not context.owner_urns:
            failures.append("missing ownership context")
        if failures:
            raise ContextRefusal("; ".join(failures))
