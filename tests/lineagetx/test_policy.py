from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from data_lineage_fix_agent.lineagetx.datahub_context import (
    AssetSnapshot,
    ConsumerLineage,
    DataHubMigrationContext,
    GovernanceSnapshot,
    SchemaFieldSnapshot,
)
from data_lineage_fix_agent.lineagetx.models import (
    ChangeIntent,
    Participant,
    ParticipantKind,
)
from data_lineage_fix_agent.lineagetx.policy import (
    DiscoveryAttestation,
    LineageTXSafetyPolicy,
    PolicyViolation,
)
from data_lineage_fix_agent.lineagetx.proposals import (
    CandidateEnvelope,
    DeterministicCandidateModel,
    FileSnapshot,
    ProposalRequest,
)


CREATED_AT = "2026-07-17T01:02:03.000000Z"
SOURCE = "urn:li:dataset:(urn:li:dataPlatform:postgres,commerce.orders,PROD)"
CONSUMERS = (
    "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.stg_orders,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:airflow,customer_export,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:semantic,customer_identity,PROD)",
)
OWNERS = (
    "urn:li:corpuser:data-platform-owner",
    "urn:li:corpuser:data-platform-owner",
    "urn:li:corpuser:identity-owner",
)


def _field(name: str) -> SchemaFieldSnapshot:
    return SchemaFieldSnapshot(name, "BIGINT", False, f"fixture {name}")


def _governance(owner: str, *, tagged: bool = False) -> GovernanceSnapshot:
    return GovernanceSnapshot(
        owner_urns=(owner,),
        tag_urns=("urn:li:tag:Tier1",) if tagged else (),
        structured_properties={},
    )


def _intent() -> ChangeIntent:
    return ChangeIntent.create(
        producer_repository="acme/commerce-producer",
        producer_pr_number=42,
        producer_base_sha="producer-base",
        producer_head_sha="producer-head",
        source_asset_urn=SOURCE,
        old_field="customer_id",
        new_field="customer_key",
        contract_schema_fingerprint="sha256:contract-v2",
        created_at=CREATED_AT,
    )


def _participants(intent: ChangeIntent) -> tuple[Participant, ...]:
    kinds = (
        ParticipantKind.DBT_SQL,
        ParticipantKind.AIRFLOW_MAPPING,
        ParticipantKind.SEMANTIC_APPROVAL,
    )
    repositories = ("data-platform", "data-platform", "analytics-governance")
    files = (
        ("dbt/models/stg_orders.sql",),
        (
            "airflow/dags/export_customers.py",
            "airflow/config/export_columns.json",
        ),
        ("semantic/customer_identity.json",),
    )
    return tuple(
        Participant.create(
            migration_id=intent.migration_id,
            kind=kind,
            asset_urn=asset,
            repository=repository,
            owner_urns=(owner,),
            files=paths,
            base_sha="fixture-base",
            created_at=CREATED_AT,
        )
        for kind, asset, repository, owner, paths in zip(
            kinds, CONSUMERS, repositories, OWNERS, files, strict=True
        )
    )


def _context() -> DataHubMigrationContext:
    source = AssetSnapshot(
        SOURCE,
        (_field("order_id"), _field("customer_id"), _field("customer_key")),
        _governance("urn:li:corpuser:commerce-owner", tagged=True),
    )
    assets = {SOURCE: source}
    consumers: list[ConsumerLineage] = []
    for degree, urn, owner in zip((1, 2, 3), CONSUMERS, OWNERS, strict=True):
        assets[urn] = AssetSnapshot(
            urn,
            (_field("customer_id"),),
            _governance(owner),
        )
        consumers.append(
            ConsumerLineage(
                urn=urn,
                degree="3+" if degree == 3 else str(degree),
                columns=("customer_id",),
                path_evidence=({"pathCount": 1, "paths": [[SOURCE, urn]]},),
            )
        )
    # Keep the trace shape identical to DataHubMCPContextReader output while
    # making the minimum authoritative coverage explicit.
    tool_traces = tuple(
        [{"tool": "list_schema_fields", "arguments": {"urn": urn}} for urn in assets]
        + [{"tool": "get_lineage", "arguments": {"max_hops": 3}}]
        + [{"tool": "get_entities", "arguments": {"urns": list(assets)}}]
        + [
            {"tool": "get_lineage_paths_between", "arguments": {"target_urn": urn}}
            for urn in CONSUMERS
        ]
    )
    return DataHubMigrationContext(
        source_urn=SOURCE,
        source_column="customer_id",
        replacement_column="customer_key",
        source=source,
        consumers=tuple(consumers),
        assets=assets,
        tool_traces=tool_traces,
        transport="datahub-oss+official-mcp-server-datahub==0.6.0",
        discovery_complete=True,
    )


def test_discovery_requires_complete_exact_three_hop_official_mcp_mapping() -> None:
    intent = _intent()
    participants = _participants(intent)
    decision = LineageTXSafetyPolicy().validate_discovery(
        intent,
        DiscoveryAttestation(_context(), discovery_complete=True),
        participants,
    )

    assert decision.ordered_participant_ids == tuple(
        item.participant_id for item in participants
    )
    assert set(decision.consumer_by_participant) == {
        item.participant_id for item in participants
    }
    assert "discovery_complete" in decision.checks


@pytest.mark.parametrize(
    ("attestation", "participants_transform", "message"),
    [
        (DiscoveryAttestation(_context(), False), lambda value: value, "not complete"),
        (
            DiscoveryAttestation(
                replace(_context(), transport="legacy-compatibility-bridge"), True
            ),
            lambda value: value,
            "official MCP",
        ),
        (
            DiscoveryAttestation(
                replace(_context(), discovery_complete=False), True
            ),
            lambda value: value,
            "not marked discovery_complete",
        ),
    ],
)
def test_discovery_fails_closed_on_incomplete_or_untrusted_context(
    attestation: DiscoveryAttestation,
    participants_transform: object,
    message: str,
) -> None:
    intent = _intent()
    participants = _participants(intent)
    transform = participants_transform
    assert callable(transform)
    with pytest.raises(PolicyViolation, match=message):
        LineageTXSafetyPolicy().validate_discovery(
            intent, attestation, transform(participants)
        )


def test_discovery_rejects_unmapped_or_duplicate_consumers() -> None:
    intent = _intent()
    participants = list(_participants(intent))
    participants[2] = replace(participants[2], asset_urn=CONSUMERS[1])

    with pytest.raises(PolicyViolation, match="duplicate assets"):
        LineageTXSafetyPolicy().validate_discovery(
            intent,
            DiscoveryAttestation(_context(), True),
            participants,
        )


def test_model_boundary_returns_only_typed_candidate_bound_to_stored_policy() -> None:
    intent = _intent()
    participant = _participants(intent)[0]
    sql = "SELECT order_id, customer_id FROM raw_orders\n"
    request = ProposalRequest(
        intent=intent,
        participant=participant,
        files=(
            FileSnapshot(
                relative_path=participant.files[0],
                sha256=hashlib.sha256(sql.encode()).hexdigest(),
                content=sql,
            ),
        ),
        expanded_columns=("order_id", "customer_id", "customer_key"),
        contract_columns=("order_id", "customer_key"),
    )
    envelope = DeterministicCandidateModel().propose(request)

    proposal = LineageTXSafetyPolicy.validate_candidate(request, envelope)

    assert proposal.participant_id == participant.participant_id
    assert proposal.allowed_paths == participant.files
    assert not hasattr(envelope, "command")
    assert not hasattr(envelope, "executable")

    untrusted = CandidateEnvelope(
        model_id="untrusted",
        proposal=replace(proposal, relative_path="other.sql"),
    )
    with pytest.raises(PolicyViolation, match="paths"):
        LineageTXSafetyPolicy.validate_candidate(request, untrusted)
