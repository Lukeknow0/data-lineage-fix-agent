from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import Any, Protocol, TypeAlias

from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

from .models import ChangeIntent, Participant, ParticipantKind
from .participants.airflow_mapping import AirflowMappingProposal
from .participants.dbt_sql import DbtSqlProposal
from .participants.semantic_approval import SemanticMappingProposal
from .worktrees import RepositorySession


class ProposalError(RuntimeError):
    """A candidate generator did not return an admissible structured candidate."""


CandidateProposal: TypeAlias = (
    DbtSqlProposal | AirflowMappingProposal | SemanticMappingProposal
)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _safe_relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ProposalError(f"unsafe candidate path: {value!r}")
    return path.as_posix()


@dataclass(frozen=True)
class FileSnapshot:
    """Read-only source material provided to a candidate generator."""

    relative_path: str
    sha256: str
    content: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "relative_path", _safe_relative_path(self.relative_path))
        if self.sha256 != _sha256(self.content):
            raise ValueError("file snapshot sha256 does not match its content")


@dataclass(frozen=True)
class ProposalRequest:
    """The complete and deliberately non-executable candidate input contract.

    The generator receives text plus immutable migration metadata and returns
    one of the three typed proposal dataclasses. There is no command, executable,
    or process field in either direction; deterministic adapters own all writes.
    """

    intent: ChangeIntent
    participant: Participant
    files: tuple[FileSnapshot, ...]
    expanded_columns: tuple[str, ...]
    contract_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.participant.migration_id != self.intent.migration_id:
            raise ValueError("proposal participant belongs to another migration")
        paths = tuple(item.relative_path for item in self.files)
        expected = tuple(_safe_relative_path(item) for item in self.participant.files)
        if len(paths) != len(set(paths)):
            raise ValueError("proposal request cannot contain duplicate files")
        if set(paths) != set(expected):
            raise ValueError("proposal request must snapshot every allow-listed file")
        expanded = set(self.expanded_columns)
        contract = set(self.contract_columns)
        if not {self.intent.old_field, self.intent.new_field}.issubset(expanded):
            raise ValueError("expanded schema must contain old and replacement fields")
        if self.intent.old_field in contract or self.intent.new_field not in contract:
            raise ValueError("contract schema must remove old and retain replacement field")


@dataclass(frozen=True)
class CandidateEnvelope:
    """Typed candidate output; rationale is evidence, never executable input."""

    model_id: str
    proposal: CandidateProposal
    rationale: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.model_id.strip():
            raise ValueError("model_id is required")
        if type(self.proposal) not in {
            DbtSqlProposal,
            AirflowMappingProposal,
            SemanticMappingProposal,
        }:
            raise ValueError("candidate must be one of the closed typed proposal records")

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "proposal_type": type(self.proposal).__name__,
            "proposal": asdict(self.proposal),
            "rationale": list(self.rationale),
        }


class CandidateProposalModel(Protocol):
    """Pure structured-candidate boundary used by trusted tests and local rules."""

    def propose(self, request: ProposalRequest) -> CandidateEnvelope: ...


class DeterministicCandidateModel:
    """Offline reference proposer used by the reproducible LineageTX scenario.

    Its output is treated as untrusted and must pass the policy and each
    participant adapter's SQLGlot/AST/config/schema checks before any commit.
    """

    model_id = "lineagetx-deterministic-reference-v1"

    def propose(self, request: ProposalRequest) -> CandidateEnvelope:
        if request.participant.kind is ParticipantKind.DBT_SQL:
            proposal = self._dbt(request)
        elif request.participant.kind is ParticipantKind.AIRFLOW_MAPPING:
            proposal = self._airflow(request)
        elif request.participant.kind is ParticipantKind.SEMANTIC_APPROVAL:
            proposal = self._semantic(request)
        else:  # pragma: no cover - ParticipantKind is a closed enum.
            raise ProposalError(f"unsupported participant kind: {request.participant.kind}")
        return CandidateEnvelope(
            model_id=self.model_id,
            proposal=proposal,
            rationale=(
                "candidate_only",
                "deterministic_policy_and_tests_must_authorize_application",
            ),
        )

    @staticmethod
    def _dbt(request: ProposalRequest) -> DbtSqlProposal:
        if len(request.files) != 1 or not request.files[0].relative_path.endswith(".sql"):
            raise ProposalError("dbt candidate requires exactly one SQL file")
        snapshot = request.files[0]
        try:
            tree = parse_one(snapshot.content, read="duckdb")
        except ParseError as error:
            raise ProposalError(f"cannot propose a patch for invalid SQL: {error}") from error
        occurrences = sum(
            node.name == request.intent.old_field for node in tree.find_all(exp.Column)
        )
        relations = tuple(
            dict.fromkeys(node.name for node in tree.find_all(exp.Table) if node.name)
        )
        if occurrences < 1 or len(relations) != 1:
            raise ProposalError(
                "dbt reference candidate requires the old column and one input relation"
            )
        return DbtSqlProposal(
            migration_id=request.intent.migration_id,
            participant_id=request.participant.participant_id,
            relative_path=snapshot.relative_path,
            expected_sha256=snapshot.sha256,
            old_field=request.intent.old_field,
            new_field=request.intent.new_field,
            expected_occurrences=occurrences,
            relation=relations[0],
            expanded_columns=request.expanded_columns,
            contract_columns=request.contract_columns,
            repository=request.participant.repository,
            owner_urns=request.participant.owner_urns,
        )

    @staticmethod
    def _airflow(request: ProposalRequest) -> AirflowMappingProposal:
        if len(request.files) != 2:
            raise ProposalError("Airflow candidate requires exactly two files")
        python_files = [item for item in request.files if item.relative_path.endswith(".py")]
        json_files = [item for item in request.files if item.relative_path.endswith(".json")]
        if len(python_files) != 1 or len(json_files) != 1:
            raise ProposalError("Airflow candidate requires one Python and one JSON file")
        python_snapshot = python_files[0]
        json_snapshot = json_files[0]
        try:
            document = json.loads(json_snapshot.content)
        except json.JSONDecodeError as error:
            raise ProposalError(f"cannot propose from invalid mapping JSON: {error}") from error
        mapping = document.get("field_mapping") if isinstance(document, dict) else None
        if not isinstance(mapping, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in mapping.items()
        ):
            raise ProposalError("field_mapping must be a string-to-string object")
        if not any(request.intent.old_field in item for item in mapping.items()):
            raise ProposalError("Airflow mapping does not reference the old field")
        proposed: dict[str, str] = {}
        for key, value in mapping.items():
            next_key = request.intent.new_field if key == request.intent.old_field else key
            next_value = (
                request.intent.new_field if value == request.intent.old_field else value
            )
            if next_key in proposed:
                raise ProposalError("field rename would collide with an existing mapping")
            proposed[next_key] = next_value
        return AirflowMappingProposal(
            migration_id=request.intent.migration_id,
            participant_id=request.participant.participant_id,
            python_relative_path=python_snapshot.relative_path,
            json_relative_path=json_snapshot.relative_path,
            expected_python_sha256=python_snapshot.sha256,
            expected_json_sha256=json_snapshot.sha256,
            old_field=request.intent.old_field,
            new_field=request.intent.new_field,
            expected_mapping=tuple(mapping.items()),
            proposed_mapping=tuple(proposed.items()),
            expanded_columns=request.expanded_columns,
            contract_columns=request.contract_columns,
            repository=request.participant.repository,
            owner_urns=request.participant.owner_urns,
        )

    @classmethod
    def _semantic(cls, request: ProposalRequest) -> SemanticMappingProposal:
        if len(request.files) != 1 or not request.files[0].relative_path.endswith(".json"):
            raise ProposalError("semantic candidate requires exactly one JSON file")
        if len(request.participant.owner_urns) != 1:
            raise ProposalError("semantic candidate requires exactly one accountable owner")
        snapshot = request.files[0]
        try:
            document = json.loads(snapshot.content)
        except json.JSONDecodeError as error:
            raise ProposalError(f"cannot propose from invalid semantic JSON: {error}") from error
        occurrences = cls._count_exact(document, request.intent.old_field)
        if occurrences < 1:
            raise ProposalError("semantic document does not reference the old field")
        return SemanticMappingProposal(
            migration_id=request.intent.migration_id,
            participant_id=request.participant.participant_id,
            relative_path=snapshot.relative_path,
            expected_sha256=snapshot.sha256,
            old_field=request.intent.old_field,
            new_field=request.intent.new_field,
            expected_occurrences=occurrences,
            required_owner_urn=request.participant.owner_urns[0],
            expanded_columns=request.expanded_columns,
            contract_columns=request.contract_columns,
            repository=request.participant.repository,
            owner_urns=request.participant.owner_urns,
        )

    @classmethod
    def _count_exact(cls, value: object, needle: str) -> int:
        if isinstance(value, dict):
            return sum(key == needle for key in value) + sum(
                cls._count_exact(item, needle) for item in value.values()
            )
        if isinstance(value, list):
            return sum(cls._count_exact(item, needle) for item in value)
        return int(value == needle)


def proposal_request_from_session(
    session: RepositorySession,
    *,
    intent: ChangeIntent,
    participant: Participant,
    expanded_columns: tuple[str, ...],
    contract_columns: tuple[str, ...],
) -> ProposalRequest:
    """Snapshot only allow-listed files from an isolated candidate worktree."""

    if session.migration_id != intent.migration_id:
        raise ProposalError("worktree belongs to another migration")
    root = session.worktree.resolve()
    snapshots: list[FileSnapshot] = []
    for raw_path in participant.files:
        relative_path = _safe_relative_path(raw_path)
        target = (root / relative_path).resolve()
        if not target.is_relative_to(root) or not target.is_file():
            raise ProposalError(f"candidate file is unavailable: {relative_path}")
        content = target.read_text(encoding="utf-8")
        snapshots.append(
            FileSnapshot(
                relative_path=relative_path,
                sha256=_sha256(content),
                content=content,
            )
        )
    return ProposalRequest(
        intent=intent,
        participant=participant,
        files=tuple(snapshots),
        expanded_columns=expanded_columns,
        contract_columns=contract_columns,
    )


def assert_structured_candidate(
    request: ProposalRequest,
    envelope: CandidateEnvelope,
) -> CandidateProposal:
    """Bind an untrusted typed proposal exactly to the discovered participant."""

    if type(envelope) is not CandidateEnvelope:
        raise ProposalError(
            "candidate generator must return a CandidateEnvelope, not free-form data"
        )
    proposal = envelope.proposal
    expected_type = {
        ParticipantKind.DBT_SQL: DbtSqlProposal,
        ParticipantKind.AIRFLOW_MAPPING: AirflowMappingProposal,
        ParticipantKind.SEMANTIC_APPROVAL: SemanticMappingProposal,
    }[request.participant.kind]
    if type(proposal) is not expected_type:
        raise ProposalError(
            f"participant {request.participant.participant_id} requires "
            f"{expected_type.__name__}"
        )
    if proposal.migration_id != request.intent.migration_id:
        raise ProposalError("candidate migration_id does not match the change intent")
    if proposal.participant_id != request.participant.participant_id:
        raise ProposalError("candidate participant_id does not match discovery")
    if proposal.old_field != request.intent.old_field:
        raise ProposalError("candidate old_field does not match the change intent")
    if proposal.new_field != request.intent.new_field:
        raise ProposalError("candidate new_field does not match the change intent")
    if proposal.repository != request.participant.repository:
        raise ProposalError("candidate repository does not match stored discovery")
    if tuple(proposal.owner_urns) != request.participant.owner_urns:
        raise ProposalError("candidate owners do not match stored DataHub governance")
    if tuple(proposal.expanded_columns) != request.expanded_columns:
        raise ProposalError("candidate changed the expanded schema contract")
    if tuple(proposal.contract_columns) != request.contract_columns:
        raise ProposalError("candidate changed the contract schema contract")
    snapshot_hashes = {item.relative_path: item.sha256 for item in request.files}
    allowed_paths = tuple(_safe_relative_path(path) for path in proposal.allowed_paths)
    if set(allowed_paths) != set(request.participant.files):
        raise ProposalError("candidate paths must equal the participant allowlist")
    if isinstance(proposal, DbtSqlProposal):
        hashes = {proposal.relative_path: proposal.expected_sha256}
        if proposal.dialect != "duckdb":
            raise ProposalError("dbt dialect is fixed by the trusted adapter registry")
        try:
            tree = parse_one(request.files[0].content, read="duckdb")
        except ParseError as error:  # pragma: no cover - generator catches this first.
            raise ProposalError(f"stored dbt source is not parseable: {error}") from error
        relations = {
            item.name for item in tree.find_all(exp.Table) if item.name
        }
        if relations != {proposal.relation}:
            raise ProposalError("dbt relation must be derived from the stored SQL source")
    elif isinstance(proposal, AirflowMappingProposal):
        hashes = {
            proposal.python_relative_path: proposal.expected_python_sha256,
            proposal.json_relative_path: proposal.expected_json_sha256,
        }
        if proposal.assignment_name != "FIELD_MAPPING":
            raise ProposalError("Airflow assignment is fixed by the trusted registry")
        if proposal.config_key != "field_mapping":
            raise ProposalError("Airflow config key is fixed by the trusted registry")
    else:
        hashes = {proposal.relative_path: proposal.expected_sha256}
        if len(request.participant.owner_urns) != 1:
            raise ProposalError("semantic participant must have exactly one trusted owner")
        if proposal.required_owner_urn != request.participant.owner_urns[0]:
            raise ProposalError("semantic required owner must come from stored governance")
    if hashes != snapshot_hashes:
        raise ProposalError("candidate hashes do not match the isolated file snapshots")
    return proposal


def proposal_fingerprint(envelope: CandidateEnvelope) -> str:
    serialized = json.dumps(
        envelope.to_dict(), sort_keys=True, separators=(",", ":"), default=str
    )
    return _sha256(serialized)
