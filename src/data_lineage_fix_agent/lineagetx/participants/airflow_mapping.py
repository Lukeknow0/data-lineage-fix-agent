from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ..models import ParticipantKind
from ..worktrees import RepositorySession
from .base import (
    bind_trusted_policy,
    CandidateRejected,
    ParticipantStatus,
    PreparationResult,
    TrustedParticipantPolicy,
    checked_text,
    enforce_allowlist,
    require_clean_candidate,
    sha256_text,
)


@dataclass(frozen=True)
class AirflowMappingProposal:
    migration_id: str
    participant_id: str
    python_relative_path: str
    json_relative_path: str
    expected_python_sha256: str
    expected_json_sha256: str
    old_field: str
    new_field: str
    expected_mapping: tuple[tuple[str, str], ...]
    proposed_mapping: tuple[tuple[str, str], ...]
    expanded_columns: tuple[str, ...]
    contract_columns: tuple[str, ...]
    repository: str
    owner_urns: tuple[str, ...]
    assignment_name: str = "FIELD_MAPPING"
    config_key: str = "field_mapping"

    @property
    def allowed_paths(self) -> tuple[str, str]:
        return (self.python_relative_path, self.json_relative_path)


class AirflowMappingParticipant:
    def __init__(
        self,
        *,
        file_writer: Callable[[Path, bytes], None] | None = None,
    ) -> None:
        self._file_writer = file_writer or self._write_bytes

    def prepare(
        self,
        session: RepositorySession,
        proposal: AirflowMappingProposal,
        policy: TrustedParticipantPolicy,
    ) -> PreparationResult:
        bind_trusted_policy(
            session,
            policy,
            expected_kind=ParticipantKind.AIRFLOW_MAPPING,
            migration_id=proposal.migration_id,
            participant_id=proposal.participant_id,
            repository=proposal.repository,
            paths=(proposal.python_relative_path, proposal.json_relative_path),
            old_field=proposal.old_field,
            new_field=proposal.new_field,
            expanded_columns=proposal.expanded_columns,
            contract_columns=proposal.contract_columns,
            owner_urns=proposal.owner_urns,
            assignment_name=proposal.assignment_name,
            config_key=proposal.config_key,
        )
        self._validate_identifiers(policy)
        require_clean_candidate(session)
        python_path, python_before = checked_text(
            session,
            policy.allowed_paths[0],
            proposal.expected_python_sha256,
        )
        json_path, json_before = checked_text(
            session,
            policy.allowed_paths[1],
            proposal.expected_json_sha256,
        )
        python_before_bytes = python_path.read_bytes()
        json_before_bytes = json_path.read_bytes()

        expected = dict(proposal.expected_mapping)
        proposed = dict(proposal.proposed_mapping)
        if len(expected) != len(proposal.expected_mapping) or len(proposed) != len(
            proposal.proposed_mapping
        ):
            raise CandidateRejected("mapping proposals may not contain duplicate keys")
        derived = self._rename_mapping(expected, policy.old_field, policy.new_field)
        if proposed != derived:
            raise CandidateRejected(
                "candidate is not the single deterministic old-to-new mapping"
            )

        _, assignment = self._mapping_assignment(
            python_before, policy.assignment_name or ""
        )
        python_mapping = self._literal_mapping(assignment.value)
        if python_mapping != expected:
            raise CandidateRejected("Python mapping changed after proposal generation")

        try:
            json_document = json.loads(json_before)
        except json.JSONDecodeError as exc:
            raise CandidateRejected(f"mapping config is invalid JSON: {exc}") from exc
        if not isinstance(json_document, dict) or not isinstance(
            json_document.get(policy.config_key), dict
        ):
            raise CandidateRejected(
                f"JSON config must contain object key {policy.config_key!r}"
            )
        if json_document[policy.config_key] != expected:
            raise CandidateRejected("JSON mapping changed after proposal generation")
        if python_mapping != json_document[policy.config_key]:
            raise CandidateRejected("Python and JSON mappings disagree before PREPARING")

        python_after = self._replace_assignment(
            python_before,
            assignment,
            policy.assignment_name or "",
            proposed,
        )
        json_after_document: dict[str, Any] = dict(json_document)
        json_after_document[policy.config_key or ""] = proposed
        json_after = json.dumps(json_after_document, indent=2, sort_keys=True) + "\n"

        # Validate both candidates fully before either file is written.
        _, after_assignment = self._mapping_assignment(
            python_after, policy.assignment_name or ""
        )
        if self._literal_mapping(after_assignment.value) != proposed:
            raise CandidateRejected("patched Python mapping failed AST validation")
        parsed_json_after = json.loads(json_after)
        if parsed_json_after[policy.config_key or ""] != proposed:
            raise CandidateRejected("patched JSON mapping failed config validation")
        self._validate_schema_variants(policy, proposed)

        try:
            self._file_writer(python_path, python_after.encode("utf-8"))
            self._file_writer(json_path, json_after.encode("utf-8"))
            changed = enforce_allowlist(
                session, policy.allowed_paths, require_all=True
            )
        except Exception as write_error:
            restore_errors: list[str] = []
            for path, original in (
                (python_path, python_before_bytes),
                (json_path, json_before_bytes),
            ):
                try:
                    path.write_bytes(original)
                except OSError as restore_error:  # pragma: no cover - hard I/O failure.
                    restore_errors.append(f"{path}: {restore_error}")
            if restore_errors:
                raise CandidateRejected(
                    "two-file Airflow write failed and original bytes could not be "
                    "fully restored: " + "; ".join(restore_errors)
                ) from write_error
            raise CandidateRejected(
                "two-file Airflow write failed; both original byte sequences restored"
            ) from write_error

        return PreparationResult(
            participant_id=proposal.participant_id,
            state=ParticipantStatus.VERIFIED,
            changed_files=changed,
            checks=(
                "expected_sha256_both_files",
                "python_ast_literal_mapping",
                "json_config_schema",
                "cross_file_consistency",
                "deterministic_mapping_policy",
                "path_allowlist",
                "expanded_schema_mapping",
                "contract_schema_mapping",
            ),
            evidence={
                "python_before_sha256": proposal.expected_python_sha256,
                "python_after_sha256": sha256_text(python_after),
                "json_before_sha256": proposal.expected_json_sha256,
                "json_after_sha256": sha256_text(json_after),
                "approved_mapping": proposed,
                "expanded_columns": list(policy.expanded_columns),
                "contract_columns": list(policy.contract_columns),
                "owners": list(policy.owner_urns),
                "execution_scope": "trusted_fixture_fixed_adapter_no_repo_commands",
            },
        )

    @staticmethod
    def _validate_identifiers(policy: TrustedParticipantPolicy) -> None:
        if (
            len(policy.allowed_paths) != 2
            or policy.allowed_paths[0] == policy.allowed_paths[1]
        ):
            raise CandidateRejected("Airflow participant must update two distinct files")
        for identifier in (
            policy.old_field,
            policy.new_field,
            policy.assignment_name or "",
        ):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
                raise CandidateRejected(f"unsupported identifier: {identifier!r}")

    @staticmethod
    def _rename_mapping(
        before: dict[str, str], old_field: str, new_field: str
    ) -> dict[str, str]:
        if not any(old_field in pair for pair in before.items()):
            raise CandidateRejected("old field is absent from the expected mapping")
        after: dict[str, str] = {}
        for key, value in before.items():
            new_key = new_field if key == old_field else key
            new_value = new_field if value == old_field else value
            if new_key in after:
                raise CandidateRejected("rename would create a mapping-key collision")
            after[new_key] = new_value
        return after

    @staticmethod
    def _mapping_assignment(
        source: str, assignment_name: str
    ) -> tuple[ast.Module, ast.Assign]:
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            raise CandidateRejected(f"Airflow Python mapping is invalid: {exc}") from exc
        assignments = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == assignment_name
                for target in node.targets
            )
        ]
        if len(assignments) != 1:
            raise CandidateRejected(
                f"expected exactly one {assignment_name} assignment, got {len(assignments)}"
            )
        return tree, assignments[0]

    @staticmethod
    def _literal_mapping(node: ast.expr) -> dict[str, str]:
        try:
            value = ast.literal_eval(node)
        except (ValueError, SyntaxError) as exc:
            raise CandidateRejected("mapping assignment must be a literal dictionary") from exc
        if not isinstance(value, dict) or not all(
            isinstance(key, str) and isinstance(item, str)
            for key, item in value.items()
        ):
            raise CandidateRejected("mapping must be a string-to-string dictionary")
        return value

    @staticmethod
    def _replace_assignment(
        source: str,
        assignment: ast.Assign,
        assignment_name: str,
        mapping: dict[str, str],
    ) -> str:
        if assignment.end_lineno is None or assignment.end_col_offset is None:
            raise CandidateRejected("Python AST did not include assignment source offsets")
        lines = source.splitlines(keepends=True)
        start = sum(len(line) for line in lines[: assignment.lineno - 1]) + assignment.col_offset
        end = (
            sum(len(line) for line in lines[: assignment.end_lineno - 1])
            + assignment.end_col_offset
        )
        replacement = f"{assignment_name} = " + json.dumps(
            mapping, indent=4, sort_keys=True
        )
        return source[:start] + replacement + source[end:]

    @staticmethod
    def _validate_schema_variants(
        policy: TrustedParticipantPolicy, mapping: dict[str, str]
    ) -> None:
        expanded = set(policy.expanded_columns)
        contract = set(policy.contract_columns)
        referenced = set(mapping) | set(mapping.values())
        if not referenced.issubset(expanded):
            raise CandidateRejected("patched mapping fails the expanded schema")
        if not referenced.issubset(contract):
            raise CandidateRejected("patched mapping fails the contract schema")

    @staticmethod
    def _write_bytes(path: Path, value: bytes) -> None:
        path.write_bytes(value)
