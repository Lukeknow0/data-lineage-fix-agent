from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, Sequence

from .datahub_context import ConsumerLineage, DataHubMigrationContext
from .models import ChangeIntent, Participant, ParticipantKind, require_https_url
from .participants.semantic_approval import OwnerApproval
from .proposals import (
    CandidateEnvelope,
    CandidateProposal,
    ProposalError,
    ProposalRequest,
    assert_structured_candidate,
)


class PolicyViolation(RuntimeError):
    """LineageTX cannot prove a safety precondition and therefore fails closed."""


@dataclass(frozen=True)
class DiscoveryAttestation:
    """Explicit completion proof produced after exhausting official MCP pages."""

    context: DataHubMigrationContext
    discovery_complete: bool


@dataclass(frozen=True)
class DiscoveryDecision:
    source_urn: str
    consumer_by_participant: Mapping[str, ConsumerLineage]
    ordered_participant_ids: tuple[str, ...]
    checks: tuple[str, ...]


def _degree(value: str) -> int:
    matches = re.findall(r"\d+", value)
    if len(matches) != 1:
        raise PolicyViolation(f"unrecognized DataHub lineage degree: {value!r}")
    return int(matches[0])


class LineageTXSafetyPolicy:
    """Fail-closed discovery, proposal, and owner-approval policy."""

    OFFICIAL_TRANSPORT_PREFIX = "datahub-oss+official-"
    REQUIRED_TOOLS = frozenset(
        {
            "list_schema_fields",
            "get_lineage",
            "get_entities",
            "get_lineage_paths_between",
        }
    )

    def __init__(
        self,
        *,
        expected_consumer_count: int = 3,
        allow_test_transport: bool = False,
    ) -> None:
        if expected_consumer_count != 3:
            raise ValueError("the bounded LineageTX scenario has exactly three consumers")
        self.expected_consumer_count = expected_consumer_count
        self.allow_test_transport = allow_test_transport

    def validate_discovery(
        self,
        intent: ChangeIntent,
        attestation: DiscoveryAttestation,
        participants: Sequence[Participant],
    ) -> DiscoveryDecision:
        context = attestation.context
        if not attestation.discovery_complete:
            raise PolicyViolation("official MCP impact discovery is not complete")
        if not context.discovery_complete:
            raise PolicyViolation("frozen DataHub context is not marked discovery_complete")
        official = context.transport.startswith(self.OFFICIAL_TRANSPORT_PREFIX)
        test_double = context.transport.startswith("test-double-for-official-datahub-mcp")
        if not official and not (self.allow_test_transport and test_double):
            raise PolicyViolation(
                "context must come from full DataHub OSS through the official MCP server"
            )
        if context.source_urn != intent.source_asset_urn:
            raise PolicyViolation("DataHub source does not match the Producer ChangeIntent")
        if context.source_column != intent.old_field:
            raise PolicyViolation("DataHub lineage was not queried for the removed column")
        if context.replacement_column != intent.new_field:
            raise PolicyViolation("DataHub replacement column does not match the intent")

        source_fields = {field.field_path for field in context.source.schema}
        if not {intent.old_field, intent.new_field}.issubset(source_fields):
            raise PolicyViolation(
                "expanded source schema must contain old and replacement columns together"
            )
        source_governance = context.source.governance
        if not source_governance.owner_urns:
            raise PolicyViolation("source governance is missing an accountable owner")
        if not (
            source_governance.tag_urns or source_governance.structured_properties
        ):
            raise PolicyViolation("source governance signals are missing")

        if len(context.consumers) != self.expected_consumer_count:
            raise PolicyViolation(
                f"expected exactly three impacted consumers, got {len(context.consumers)}"
            )
        if len(participants) != self.expected_consumer_count:
            raise PolicyViolation(
                f"expected exactly three participant mappings, got {len(participants)}"
            )
        participant_ids = [item.participant_id for item in participants]
        participant_urns = [item.asset_urn for item in participants]
        if len(set(participant_ids)) != len(participant_ids):
            raise PolicyViolation("participant mapping contains duplicate IDs")
        if len(set(participant_urns)) != len(participant_urns):
            raise PolicyViolation("participant mapping contains duplicate assets")
        expected_kinds = {
            ParticipantKind.DBT_SQL,
            ParticipantKind.AIRFLOW_MAPPING,
            ParticipantKind.SEMANTIC_APPROVAL,
        }
        if {item.kind for item in participants} != expected_kinds:
            raise PolicyViolation("participants must map dbt, Airflow, and semantic consumers")

        consumers_by_urn = {item.urn: item for item in context.consumers}
        if len(consumers_by_urn) != len(context.consumers):
            raise PolicyViolation("DataHub returned duplicate impacted consumer assets")
        if set(participant_urns) != set(consumers_by_urn):
            missing = sorted(set(consumers_by_urn) - set(participant_urns))
            extra = sorted(set(participant_urns) - set(consumers_by_urn))
            raise PolicyViolation(
                f"participant mapping is not exhaustive (missing={missing}, extra={extra})"
            )
        degrees = sorted(_degree(item.degree) for item in context.consumers)
        if degrees != [1, 2, 3]:
            raise PolicyViolation(
                f"bounded scenario requires complete hops 1, 2, and 3; got {degrees}"
            )
        expected_assets = {context.source_urn, *consumers_by_urn}
        if set(context.assets) != expected_assets:
            raise PolicyViolation(
                "DataHub asset context must contain exactly source plus all consumers"
            )

        traces = context.tool_traces
        tools = {
            str(item.get("tool"))
            for item in traces
            if isinstance(item, Mapping) and item.get("tool")
        }
        missing_tools = sorted(self.REQUIRED_TOOLS - tools)
        if missing_tools:
            raise PolicyViolation(
                "official MCP evidence is missing required reads: " + ", ".join(missing_tools)
            )
        tool_counts = {
            tool: sum(
                isinstance(item, Mapping) and item.get("tool") == tool
                for item in traces
            )
            for tool in self.REQUIRED_TOOLS
        }
        if tool_counts["list_schema_fields"] < 4:
            raise PolicyViolation("official MCP schema evidence does not cover all assets")
        if tool_counts["get_lineage_paths_between"] < 3:
            raise PolicyViolation("official MCP path evidence does not cover all consumers")

        mapping: dict[str, ConsumerLineage] = {}
        ordered: list[tuple[int, str]] = []
        for participant in participants:
            if participant.migration_id != intent.migration_id:
                raise PolicyViolation("participant belongs to another migration")
            consumer = consumers_by_urn[participant.asset_urn]
            expected_degree = {
                ParticipantKind.DBT_SQL: 1,
                ParticipantKind.AIRFLOW_MAPPING: 2,
                ParticipantKind.SEMANTIC_APPROVAL: 3,
            }[participant.kind]
            if _degree(consumer.degree) != expected_degree:
                raise PolicyViolation(
                    f"{participant.kind.value} is not bound to hop {expected_degree}"
                )
            if intent.old_field not in consumer.columns:
                raise PolicyViolation(
                    f"column-level lineage for {consumer.urn} omitted {intent.old_field}"
                )
            if not consumer.path_evidence:
                raise PolicyViolation(f"lineage path evidence is missing for {consumer.urn}")
            asset = context.assets.get(consumer.urn)
            if asset is None:
                raise PolicyViolation(f"DataHub entity context is missing for {consumer.urn}")
            asset_fields = {field.field_path for field in asset.schema}
            if not set(consumer.columns).issubset(asset_fields):
                raise PolicyViolation(f"consumer schema is incomplete for {consumer.urn}")
            if not asset.governance.owner_urns:
                raise PolicyViolation(f"consumer owner is missing for {consumer.urn}")
            if not participant.owner_urns:
                raise PolicyViolation(f"participant owner mapping is missing for {consumer.urn}")
            if not set(participant.owner_urns).intersection(
                asset.governance.owner_urns
            ):
                raise PolicyViolation(
                    f"participant owner does not match DataHub governance for {consumer.urn}"
                )
            mapping[participant.participant_id] = consumer
            ordered.append((_degree(consumer.degree), participant.participant_id))

        return DiscoveryDecision(
            source_urn=context.source_urn,
            consumer_by_participant=mapping,
            ordered_participant_ids=tuple(item[1] for item in sorted(ordered)),
            checks=(
                "full_datahub_oss_official_mcp",
                "discovery_complete",
                "expanded_schema_old_and_new",
                "column_lineage_hops_1_2_3",
                "all_consumers_mapped_exactly_once",
                "schema_owner_and_governance_signals",
                "lineage_path_evidence",
            ),
        )

    @staticmethod
    def validate_candidate(
        request: ProposalRequest,
        envelope: CandidateEnvelope,
    ) -> CandidateProposal:
        try:
            return assert_structured_candidate(request, envelope)
        except (ProposalError, ValueError, TypeError) as error:
            raise PolicyViolation(str(error)) from error

    @staticmethod
    def validate_owner_approval(
        *,
        intent: ChangeIntent,
        participant: Participant,
        approval: OwnerApproval,
    ) -> None:
        if participant.kind is not ParticipantKind.SEMANTIC_APPROVAL:
            raise PolicyViolation("owner approval is only valid for semantic consumers")
        expected = (
            intent.migration_id,
            participant.participant_id,
            intent.old_field,
            intent.new_field,
        )
        actual = (
            approval.migration_id,
            approval.participant_id,
            approval.old_field,
            approval.new_field,
        )
        if actual != expected:
            raise PolicyViolation(
                "approval must bind the exact migration, consumer, and field mapping"
            )
        if approval.owner_urn not in participant.owner_urns:
            raise PolicyViolation("approval signer is not a discovered DataHub owner")
        if approval.decision != "APPROVED":
            raise PolicyViolation("semantic owner did not approve the mapping")
        try:
            require_https_url(approval.evidence_url, "approval evidence")
        except ValueError as error:
            raise PolicyViolation(str(error)) from error
        if not approval.approved_at:
            raise PolicyViolation("approval timestamp is required")
