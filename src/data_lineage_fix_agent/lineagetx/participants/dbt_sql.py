from __future__ import annotations

import re
from dataclasses import dataclass

import duckdb
from sqlglot import exp, parse
from sqlglot.errors import ParseError

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
class DbtSqlProposal:
    """A proposed rename; deterministic policy decides whether to apply it."""

    migration_id: str
    participant_id: str
    relative_path: str
    expected_sha256: str
    old_field: str
    new_field: str
    expected_occurrences: int
    relation: str
    expanded_columns: tuple[str, ...]
    contract_columns: tuple[str, ...]
    repository: str
    owner_urns: tuple[str, ...]
    dialect: str = "duckdb"

    @property
    def allowed_paths(self) -> tuple[str, ...]:
        return (self.relative_path,)


class DbtSqlParticipant:
    def prepare(
        self,
        session: RepositorySession,
        proposal: DbtSqlProposal,
        policy: TrustedParticipantPolicy,
    ) -> PreparationResult:
        bind_trusted_policy(
            session,
            policy,
            expected_kind=ParticipantKind.DBT_SQL,
            migration_id=proposal.migration_id,
            participant_id=proposal.participant_id,
            repository=proposal.repository,
            paths=(proposal.relative_path,),
            old_field=proposal.old_field,
            new_field=proposal.new_field,
            expanded_columns=proposal.expanded_columns,
            contract_columns=proposal.contract_columns,
            owner_urns=proposal.owner_urns,
            relation=proposal.relation,
            dialect=proposal.dialect,
        )
        self._validate_identifiers(policy)
        require_clean_candidate(session)
        target, before = checked_text(
            session, policy.allowed_paths[0], proposal.expected_sha256
        )
        before_tree = self._single_bounded_select(
            before, dialect=policy.dialect or "", relation=policy.relation or ""
        )

        references = [
            node
            for node in before_tree.find_all(exp.Column)
            if node.name == policy.old_field
        ]
        if proposal.expected_occurrences <= 0:
            raise CandidateRejected("expected_occurrences must be positive")
        if len(references) != proposal.expected_occurrences:
            raise CandidateRejected(
                "SQL AST occurrence count changed after proposal generation: "
                f"expected {proposal.expected_occurrences}, got {len(references)}"
            )

        identifier = re.compile(
            rf"(?<![A-Za-z0-9_]){re.escape(policy.old_field)}(?![A-Za-z0-9_])"
        )
        after, replacements = identifier.subn(policy.new_field, before)
        if replacements != proposal.expected_occurrences:
            raise CandidateRejected(
                "rename touched non-column text or missed an AST reference; refusing candidate"
            )

        after_tree = self._single_bounded_select(
            after, dialect=policy.dialect or "", relation=policy.relation or ""
        )
        if any(
            node.name == policy.old_field for node in after_tree.find_all(exp.Column)
        ):
            raise CandidateRejected("old field remains in the patched SQL AST")
        if sum(
            node.name == policy.new_field for node in after_tree.find_all(exp.Column)
        ) < proposal.expected_occurrences:
            raise CandidateRejected("replacement field is missing from the patched SQL AST")

        before_bytes = target.read_bytes()
        try:
            target.write_bytes(after.encode("utf-8"))
            changed = enforce_allowlist(
                session, policy.allowed_paths, require_all=True
            )
            self._compile_against_schema(
                after,
                policy.relation or "",
                policy.expanded_columns,
                "expanded",
            )
            self._compile_against_schema(
                after,
                policy.relation or "",
                policy.contract_columns,
                "contract",
            )
        except Exception:
            target.write_bytes(before_bytes)
            raise

        return PreparationResult(
            participant_id=proposal.participant_id,
            state=ParticipantStatus.VERIFIED,
            changed_files=changed,
            checks=(
                "expected_sha256",
                "sqlglot_ast",
                "column_occurrence_count",
                "path_allowlist",
                "expanded_schema_compile",
                "contract_schema_compile",
            ),
            evidence={
                "before_sha256": proposal.expected_sha256,
                "after_sha256": sha256_text(after),
                "old_field": policy.old_field,
                "new_field": policy.new_field,
                "occurrences": replacements,
                "expanded_columns": list(policy.expanded_columns),
                "contract_columns": list(policy.contract_columns),
                "owners": list(policy.owner_urns),
                "execution_scope": "trusted_fixture_fixed_adapter_no_repo_commands",
            },
        )

    @staticmethod
    def _validate_identifiers(policy: TrustedParticipantPolicy) -> None:
        for identifier in (policy.old_field, policy.new_field, policy.relation or ""):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
                raise CandidateRejected(f"unsupported SQL identifier: {identifier!r}")

    @staticmethod
    def _single_bounded_select(sql: str, *, dialect: str, relation: str) -> exp.Select:
        try:
            statements = [item for item in parse(sql, read=dialect) if item is not None]
        except ParseError as exc:
            raise CandidateRejected(f"SQLGlot rejected the proposed SQL: {exc}") from exc
        if len(statements) != 1 or not isinstance(statements[0], exp.Select):
            raise CandidateRejected("dbt adapter permits exactly one SELECT statement")
        tree = statements[0]
        if len(list(tree.find_all(exp.Select))) != 1:
            raise CandidateRejected("nested SELECTs and CTEs are outside the bounded adapter")
        tables = list(tree.find_all(exp.Table))
        if len(tables) != 1:
            raise CandidateRejected("dbt adapter permits exactly one fixed input relation")
        table = tables[0]
        if (
            not isinstance(table.this, exp.Identifier)
            or table.name != relation
            or bool(table.catalog)
            or bool(table.db)
        ):
            raise CandidateRejected(
                "table functions, qualified relations, and non-policy relations are forbidden"
            )
        if list(tree.find_all(exp.Join)):
            raise CandidateRejected("joins are outside the bounded trusted fixture adapter")
        external_prefixes = (
            "read_",
            "scan_",
            "http_",
            "glob",
            "parquet_",
            "csv_",
            "json_",
            "sqlite_",
            "postgres_",
            "mysql_",
        )
        external_function_names = {
            "getenv",
            "load_extension",
            "query",
            "query_table",
            "readfile",
            "shell",
            "writefile",
        }
        for function in tree.find_all(exp.Func):
            name = (function.name or function.sql_name()).lower()
            if (
                name in external_function_names
                or name.startswith(external_prefixes)
                or name.endswith(("_scan", "_read"))
            ):
                raise CandidateRejected(f"external-access SQL function is forbidden: {name}")
        return tree

    @staticmethod
    def _compile_against_schema(
        sql: str,
        relation: str,
        columns: tuple[str, ...],
        variant: str,
    ) -> None:
        if not columns:
            raise CandidateRejected(f"{variant} schema has no columns")
        quoted_columns = ", ".join(
            f'"{column.replace(chr(34), chr(34) * 2)}" BIGINT' for column in columns
        )
        quoted_relation = relation.replace('"', '""')
        connection = duckdb.connect(":memory:")
        try:
            connection.execute("SET enable_external_access = false")
            connection.execute(f'CREATE TABLE "{quoted_relation}" ({quoted_columns})')
            connection.execute(f"EXPLAIN {sql}")
        except Exception as exc:
            raise CandidateRejected(
                f"{variant} schema validation failed for {relation}: {exc}"
            ) from exc
        finally:
            connection.close()
