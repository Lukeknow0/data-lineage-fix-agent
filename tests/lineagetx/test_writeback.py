from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest
from datahub.emitter.mce_builder import make_schema_field_urn

from data_lineage_fix_agent.lineagetx.datahub_context import (
    AssetSnapshot,
    ConsumerLineage,
    DataHubMigrationContext,
    GovernanceSnapshot,
    OfficialDataHubMCPClient,
    SchemaFieldSnapshot,
)
from data_lineage_fix_agent.lineagetx.writeback import (
    MIGRATION_EVIDENCE_PROPERTY,
    MIGRATION_ID_PROPERTY,
    MIGRATION_OWNER_PROPERTY,
    MIGRATION_STATUS_PROPERTY,
    MIGRATION_TAG,
    PROPERTY_QUALIFIED_NAMES,
    DataHubMigrationWriter,
    DataHubPartialWriteError,
    DataHubWritebackError,
    MigrationWriteback,
)


ROOT = Path(__file__).resolve().parents[2]
SCENARIO = json.loads(
    (ROOT / "fixtures" / "lineagetx" / "datahub" / "scenario.json").read_text(
        encoding="utf-8"
    )
)
SOURCE = SCENARIO["source_urn"]
DBT_CONSUMER, AIRFLOW_CONSUMER, SEMANTIC_CONSUMER = [
    item["urn"] for item in SCENARIO["consumers"]
]


ASSETS = (SOURCE, DBT_CONSUMER, AIRFLOW_CONSUMER, SEMANTIC_CONSUMER)


class StatefulWritebackMCP:
    transport = "test-double-for-official-datahub-mcp"

    def __init__(
        self,
        *,
        omit_tag_for: str | None = None,
        fail_structured_call: int | None = None,
        stale_readbacks: int = 0,
    ) -> None:
        self.omit_tag_for = omit_tag_for
        self.fail_structured_call = fail_structured_call
        self.stale_readbacks = stale_readbacks
        self.readback_count = 0
        self.structured_call_count = 0
        self.properties: dict[str, dict[str, list[str]]] = {urn: {} for urn in ASSETS}
        self.tags: dict[str, set[str]] = {urn: set() for urn in ASSETS}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _entity(self, urn: str) -> dict[str, Any]:
        tags = self.tags[urn]
        if urn == self.omit_tag_for:
            tags = tags - {MIGRATION_TAG}
        return {
            "urn": urn,
            "tags": {"tags": [{"tag": {"urn": tag}} for tag in sorted(tags)]},
            "structuredProperties": {
                "properties": [
                    {
                        "structuredProperty": {
                            "urn": property_urn,
                            "definition": {
                                "qualifiedName": PROPERTY_QUALIFIED_NAMES[property_urn]
                            },
                        },
                        "values": [
                            {"stringValue": value}
                            for value in self.properties[urn][property_urn]
                        ],
                    }
                    for property_urn in sorted(self.properties[urn])
                ]
            },
        }

    async def call_tools(
        self, calls: Sequence[tuple[str, Mapping[str, Any]]]
    ) -> list[tuple[Any, dict[str, Any]]]:
        responses: list[tuple[Any, dict[str, Any]]] = []
        for name, raw_args in calls:
            args = dict(raw_args)
            self.calls.append((name, args))
            if name == "add_structured_properties":
                self.structured_call_count += 1
                if self.structured_call_count == self.fail_structured_call:
                    raise RuntimeError("injected structured property failure")
                for urn in args["entity_urns"]:
                    self.properties[urn].update(args["property_values"])
                payload: Any = {"success": True, "message": "updated"}
            elif name == "add_tags":
                for urn in args["entity_urns"]:
                    self.tags[urn].update(args["tag_urns"])
                payload = {"success": True, "message": "tagged"}
            elif name == "get_entities":
                self.readback_count += 1
                if self.readback_count <= self.stale_readbacks:
                    payload = [
                        {
                            "urn": urn,
                            "tags": {"tags": []},
                            "structuredProperties": {"properties": []},
                        }
                        for urn in args["urns"]
                    ]
                else:
                    payload = [self._entity(urn) for urn in args["urns"]]
            else:
                raise AssertionError(f"unexpected tool {name}")
            responses.append((payload, {"tool": name, "arguments": args}))
        return responses


def _frozen_context(
    *,
    transport: str = StatefulWritebackMCP.transport,
) -> DataHubMigrationContext:
    owners = {
        SOURCE: SCENARIO["source_owner"],
        **{item["urn"]: item["owner"] for item in SCENARIO["consumers"]},
    }
    assets = {
        urn: AssetSnapshot(
            urn=urn,
            schema=(
                SchemaFieldSnapshot("customer_id", "BIGINT", False, "legacy key"),
                *(
                    (
                        SchemaFieldSnapshot(
                            "customer_key", "BIGINT", False, "replacement key"
                        ),
                    )
                    if urn == SOURCE
                    else ()
                ),
            ),
            governance=GovernanceSnapshot(
                owner_urns=(owners[urn],),
                tag_urns=(),
                structured_properties={},
            ),
        )
        for urn in ASSETS
    }
    consumers = tuple(
        ConsumerLineage(
            urn=item["urn"],
            degree=item["degree"],
            columns=("customer_id",),
            path_evidence=(
                {
                    "source": {"urn": SOURCE, "column": "customer_id"},
                    "target": {"urn": item["urn"], "column": "customer_id"},
                    "metadata": {"direction": "downstream"},
                    "pathCount": 1,
                    "paths": [
                        {
                            "path": [
                                {
                                    "type": "SCHEMA_FIELD",
                                    "urn": make_schema_field_urn(
                                        SOURCE, "customer_id"
                                    ),
                                },
                                {
                                    "type": "SCHEMA_FIELD",
                                    "urn": make_schema_field_urn(
                                        item["urn"], "customer_id"
                                    ),
                                },
                            ]
                        }
                    ],
                },
            ),
        )
        for item in SCENARIO["consumers"]
    )
    return DataHubMigrationContext(
        source_urn=SOURCE,
        source_column="customer_id",
        replacement_column="customer_key",
        source=assets[SOURCE],
        consumers=consumers,
        assets=assets,
        tool_traces=(),
        transport=transport,
        discovery_complete=True,
    )


def _writeback() -> MigrationWriteback:
    return MigrationWriteback(
        migration_id="ltx-customer-key-001",
        status="PREPARED",
        owner="urn:li:corpuser:data-platform-oncall",
        evidence_url="https://example.invalid/evidence/ltx-customer-key-001",
    )


def _owner_for(urn: str) -> str:
    if urn == SOURCE:
        return SCENARIO["source_owner"]
    return next(item["owner"] for item in SCENARIO["consumers"] if item["urn"] == urn)


def _records(status: str = "PREPARED") -> dict[str, MigrationWriteback]:
    return {
        urn: MigrationWriteback(
            migration_id="ltx-customer-key-001",
            status=status,
            owner=_owner_for(urn),
            evidence_url="https://example.invalid/evidence/ltx-customer-key-001",
        )
        for urn in ASSETS
    }


def test_writes_and_reads_back_all_properties_and_tag_on_four_assets() -> None:
    fake = StatefulWritebackMCP()
    context = _frozen_context()
    receipt = asyncio.run(
        DataHubMigrationWriter(fake, context).write_assets(_records())
    )

    assert receipt.entity_urns == ASSETS
    assert receipt.status == "PREPARED"
    assert receipt.tag_urn == MIGRATION_TAG
    assert len(receipt.readback_sha256) == 64
    assert receipt.impact_fingerprint == context.impact_fingerprint
    assert receipt.transport == fake.transport
    assert receipt.live_verified is False
    assert receipt.verification == "read-back verified by a non-live test transport"
    assert [trace["tool"] for trace in receipt.tool_traces] == [
        "add_structured_properties",
        "add_structured_properties",
        "add_structured_properties",
        "add_tags",
        "get_entities",
    ]
    for urn in ASSETS:
        assert fake.properties[urn] == {
            MIGRATION_ID_PROPERTY: ["ltx-customer-key-001"],
            MIGRATION_STATUS_PROPERTY: ["PREPARED"],
            MIGRATION_OWNER_PROPERTY: [_owner_for(urn)],
            MIGRATION_EVIDENCE_PROPERTY: [
                "https://example.invalid/evidence/ltx-customer-key-001"
            ],
        }
    assert all(MIGRATION_TAG in fake.tags[urn] for urn in ASSETS)


def test_fails_when_readback_omits_tag_from_one_consumer() -> None:
    fake = StatefulWritebackMCP(omit_tag_for=SEMANTIC_CONSUMER)

    with pytest.raises(DataHubPartialWriteError) as caught:
        asyncio.run(
            DataHubMigrationWriter(fake, _frozen_context()).write_assets(_records())
        )
    assert caught.value.successful_assets == ASSETS[:3]
    assert caught.value.failed_operation == "readback_mismatch"


def test_retries_readback_until_async_datahub_index_observes_the_write() -> None:
    fake = StatefulWritebackMCP(stale_readbacks=1)

    receipt = asyncio.run(
        DataHubMigrationWriter(fake, _frozen_context()).write_assets(_records())
    )

    assert receipt.readback_sha256
    assert sum(name == "get_entities" for name, _ in fake.calls) == 2


def test_matches_v160_urn_property_returned_in_values_and_value_entities() -> None:
    fake = StatefulWritebackMCP()
    fake.properties[SOURCE] = _records()[SOURCE].property_values
    fake.tags[SOURCE].add(MIGRATION_TAG)
    entity = fake._entity(SOURCE)
    owner_property = next(
        item
        for item in entity["structuredProperties"]["properties"]
        if item["structuredProperty"]["urn"] == MIGRATION_OWNER_PROPERTY
    )
    owner_property["valueEntities"] = [{"urn": _owner_for(SOURCE)}]

    assert DataHubMigrationWriter._full_match(entity, _records()[SOURCE]) is True


def test_supports_per_consumer_status_owner_and_evidence_values() -> None:
    fake = StatefulWritebackMCP()
    records = {
        SOURCE: MigrationWriteback(
            migration_id="ltx-customer-key-001",
            status="PREPARING",
            owner="urn:li:corpuser:data-platform-oncall",
            evidence_url="https://example.invalid/evidence/source",
        ),
        DBT_CONSUMER: MigrationWriteback(
            migration_id="ltx-customer-key-001",
            status="PREPARED",
            owner="urn:li:corpuser:analytics-engineering",
            evidence_url="https://example.invalid/evidence/dbt",
        ),
        AIRFLOW_CONSUMER: MigrationWriteback(
            migration_id="ltx-customer-key-001",
            status="PREPARED",
            owner="urn:li:corpuser:analytics-engineering",
            evidence_url="https://example.invalid/evidence/airflow",
        ),
        SEMANTIC_CONSUMER: MigrationWriteback(
            migration_id="ltx-customer-key-001",
            status="NEEDS_APPROVAL",
            owner="urn:li:corpuser:identity-data-owner",
            evidence_url="https://example.invalid/evidence/semantic",
        ),
    }

    receipt = asyncio.run(
        DataHubMigrationWriter(fake, _frozen_context()).write_assets(records)
    )

    assert receipt.status == "MIXED"
    assert receipt.asset_statuses[SEMANTIC_CONSUMER] == "NEEDS_APPROVAL"
    assert receipt.asset_owners[DBT_CONSUMER] == (
        "urn:li:corpuser:analytics-engineering"
    )
    assert fake.properties[SEMANTIC_CONSUMER][MIGRATION_STATUS_PROPERTY] == [
        "NEEDS_APPROVAL"
    ]
    assert fake.properties[DBT_CONSUMER][MIGRATION_EVIDENCE_PROPERTY] == [
        "https://example.invalid/evidence/dbt"
    ]


def test_refuses_partial_asset_set_before_mutating_datahub() -> None:
    fake = StatefulWritebackMCP()

    with pytest.raises(ValueError, match="exactly match the frozen impact set"):
        asyncio.run(
            DataHubMigrationWriter(fake, _frozen_context()).write(
                _writeback(), ASSETS[:3]
            )
        )
    assert fake.calls == []


def test_partial_group_failure_has_journal_and_idempotent_retry() -> None:
    fake = StatefulWritebackMCP(fail_structured_call=2)
    context = _frozen_context()
    records = {
        urn: MigrationWriteback(
            migration_id="ltx-customer-key-001",
            status="PREPARED" if urn != SEMANTIC_CONSUMER else "NEEDS_APPROVAL",
            owner=_owner_for(urn),
            evidence_url=f"https://example.invalid/evidence/{index}",
        )
        for index, urn in enumerate(ASSETS)
    }

    with pytest.raises(DataHubPartialWriteError) as caught:
        asyncio.run(DataHubMigrationWriter(fake, context).write_assets(records))
    assert caught.value.failed_operation == "upsert_structured_properties"
    assert caught.value.property_successful_assets
    assert caught.value.successful_assets == ()
    assert any(not item["success"] for item in caught.value.journal)

    fake.fail_structured_call = None
    receipt = asyncio.run(DataHubMigrationWriter(fake, context).write_assets(records))
    assert receipt.readback_sha256
    assert all(MIGRATION_TAG in fake.tags[urn] for urn in ASSETS)


def test_commit_write_requires_matching_refreshed_fingerprint() -> None:
    fake = StatefulWritebackMCP()
    context = _frozen_context()
    writer = DataHubMigrationWriter(fake, context)

    with pytest.raises(ValueError, match="pre-commit refreshed"):
        asyncio.run(writer.write_assets(_records("COMMITTED")))
    assert fake.calls == []

    refreshed = _frozen_context()
    receipt = asyncio.run(
        writer.write_assets(_records("COMMITTED"), refreshed_context=refreshed)
    )
    assert receipt.status == "COMMITTED"


def test_fake_cannot_claim_live_verification_even_if_transport_string_is_spoofed() -> None:
    class SpoofedFake(StatefulWritebackMCP):
        transport = OfficialDataHubMCPClient.transport

    fake = SpoofedFake()
    context = _frozen_context(transport=OfficialDataHubMCPClient.transport)
    receipt = asyncio.run(DataHubMigrationWriter(fake, context).write_assets(_records()))
    assert receipt.live_verified is False


def test_rejects_unknown_state_and_non_public_evidence_receipt() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        MigrationWriteback(
            migration_id="ltx-1",
            status="MERGED",
            owner="urn:li:corpuser:owner",
            evidence_url="https://example.invalid/evidence",
        )
    with pytest.raises(ValueError, match="HTTP"):
        MigrationWriteback(
            migration_id="ltx-1",
            status="ABORTED",
            owner="urn:li:corpuser:owner",
            evidence_url="artifacts/local-only.json",
        )


def test_accepts_participant_validation_states_without_expanding_coordinator_machine() -> None:
    for state in ("DISCOVERED", "VERIFIED", "FAILED"):
        record = MigrationWriteback(
            migration_id="ltx-1",
            status=state,
            owner="urn:li:corpuser:owner",
            evidence_url="https://example.invalid/evidence",
        )
        assert record.status == state
