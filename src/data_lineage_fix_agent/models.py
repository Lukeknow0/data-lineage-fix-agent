from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class SchemaField:
    name: str
    native_type: str
    description: str = ""


@dataclass
class DataHubContext:
    source_urn: str
    target_urn: str
    source_fields: list[SchemaField]
    downstream_urns: list[str]
    owner_urns: list[str]
    quality_signals: list[str]
    transport: str
    tool_traces: list[dict[str, Any]] = field(default_factory=list)
    source_properties: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Finding:
    finding_id: str
    kind: str
    severity: str
    source_urn: str
    affected_entity_urn: str
    repository_file: str
    missing_field: str
    replacement_field: str
    reference_count: int
    owner_urns: list[str]
    quality_signals: list[str]
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CommandResult:
    command: list[str]
    returncode: int
    output: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RunResult:
    run_id: str
    status: str
    finding: Finding
    context: DataHubContext
    patch: str
    regression_test: str
    before: CommandResult
    after: CommandResult
    writeback: dict[str, Any]
    evidence_dir: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
