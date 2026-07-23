from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.metadata.schema_classes import (
    AuditStampClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)

from data_lineage_fix_agent.lineagetx.datahub_context import (
    DataHubContextReadError,
    DataHubMCPContextReader,
    ImpactFingerprintChanged,
    OfficialDataHubMCPClient,
    normalize_datahub_gms_url,
)
import data_lineage_fix_agent.lineagetx.datahub_context as datahub_context_module
from data_lineage_fix_agent.lineagetx.writeback import (
    DataHubMigrationWriter,
    MigrationWriteback,
)


ROOT = Path(__file__).resolve().parents[2]
SEED_SPEC = importlib.util.spec_from_file_location(
    "lineagetx_seed_fixture", ROOT / "scripts" / "seed_lineagetx_datahub.py"
)
assert SEED_SPEC is not None and SEED_SPEC.loader is not None
SEED_MODULE = importlib.util.module_from_spec(SEED_SPEC)
SEED_SPEC.loader.exec_module(SEED_MODULE)

SOURCE = SEED_MODULE.SOURCE
DBT_CONSUMER = SEED_MODULE.DBT_CONSUMER
AIRFLOW_CONSUMER = SEED_MODULE.AIRFLOW_CONSUMER
SEMANTIC_CONSUMER = SEED_MODULE.SEMANTIC_CONSUMER
lineage_aspect = SEED_MODULE.lineage_aspect


CONSUMERS = (DBT_CONSUMER, AIRFLOW_CONSUMER, SEMANTIC_CONSUMER)


def _schema_field(name: str) -> dict[str, Any]:
    return {
        "fieldPath": name,
        "nativeDataType": "BIGINT",
        "nullable": False,
        "description": f"Fixture field {name}",
    }


def _entity(urn: str, owner: str, tags: list[str]) -> dict[str, Any]:
    return {
        "urn": urn,
        "ownership": {"owners": [{"owner": {"urn": owner}}]},
        "tags": {"tags": [{"tag": {"urn": tag}} for tag in tags]},
        "structuredProperties": {
            "properties": [
                {
                    "structuredProperty": {
                        "urn": "urn:li:structuredProperty:io.lineagetx.status",
                        "definition": {"qualifiedName": "io.lineagetx.status"},
                    },
                    "values": [{"stringValue": "DETECTED"}],
                }
            ]
        },
    }


class FakeDataHubMCP:
    transport = "test-double-for-official-datahub-mcp"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.schemas = {
            SOURCE: [
                _schema_field("order_id"),
                _schema_field("customer_id"),
                _schema_field("customer_key"),
            ],
            DBT_CONSUMER: [_schema_field("customer_id")],
            AIRFLOW_CONSUMER: [_schema_field("customer_id")],
            SEMANTIC_CONSUMER: [_schema_field("customer_id")],
        }
        owner = "urn:li:corpuser:data-platform-oncall"
        self.entities = {
            SOURCE: _entity(SOURCE, owner, ["urn:li:tag:CriticalData"]),
            DBT_CONSUMER: _entity(DBT_CONSUMER, owner, []),
            AIRFLOW_CONSUMER: _entity(AIRFLOW_CONSUMER, owner, []),
            SEMANTIC_CONSUMER: _entity(
                SEMANTIC_CONSUMER,
                "urn:li:corpuser:identity-data-owner",
                ["urn:li:tag:SemanticReviewRequired"],
            ),
        }
        self.lineage = [
            {
                "entity": {"urn": DBT_CONSUMER},
                "degree": 1,
                "lineageColumns": ["customer_id"],
            },
            {
                "entity": {"urn": AIRFLOW_CONSUMER},
                "degree": 2,
                "lineageColumns": ["customer_id"],
            },
            {
                "entity": {"urn": SEMANTIC_CONSUMER},
                "degree": "3+",
                "lineageColumns": ["customer_id"],
            },
        ]

    async def call_tools(
        self, calls: Sequence[tuple[str, Mapping[str, Any]]]
    ) -> list[tuple[Any, dict[str, Any]]]:
        responses: list[tuple[Any, dict[str, Any]]] = []
        for name, raw_args in calls:
            args = dict(raw_args)
            self.calls.append((name, args))
            if name == "list_schema_fields":
                fields = self.schemas[args["urn"]]
                offset = args["offset"]
                page = fields[offset : offset + args["limit"]]
                payload: Any = {
                    "urn": args["urn"],
                    "fields": page,
                    "totalFields": len(fields),
                    "returned": len(page),
                    "remainingCount": len(fields) - offset - len(page),
                    "matchingCount": None,
                    "offset": offset,
                }
            elif name == "get_lineage":
                offset = args["offset"]
                page = self.lineage[offset : offset + args["max_results"]]
                payload = {
                    "downstreams": {
                        "total": len(self.lineage),
                        "searchResults": page,
                        "offset": offset,
                        "returned": len(page),
                        "hasMore": offset + len(page) < len(self.lineage),
                    },
                    "metadata": {"queryType": "column-level-lineage"},
                }
            elif name == "get_entities":
                payload = [self.entities[urn] for urn in args["urns"]]
            elif name == "get_lineage_paths_between":
                payload = {
                    "source": {
                        "urn": args["source_urn"],
                        "column": args["source_column"],
                    },
                    "target": {
                        "urn": args["target_urn"],
                        "column": args["target_column"],
                    },
                    "paths": [
                        {
                            "path": [
                                {
                                    "urn": make_schema_field_urn(
                                        args["source_urn"], args["source_column"]
                                    ),
                                    "type": "SCHEMA_FIELD",
                                },
                                {
                                    "urn": make_schema_field_urn(
                                        args["target_urn"], args["target_column"]
                                    ),
                                    "type": "SCHEMA_FIELD",
                                },
                            ]
                        }
                    ],
                    "pathCount": 1,
                    "metadata": {"direction": "downstream"},
                }
            else:
                raise AssertionError(f"unexpected tool {name}")
            responses.append((payload, {"tool": name, "arguments": args}))
        return responses


def test_reads_complete_three_hop_column_context_through_official_tool_shapes() -> None:
    fake = FakeDataHubMCP()
    context = asyncio.run(
        DataHubMCPContextReader(fake, page_size=2).load(SOURCE, "customer_id")
    )

    assert len(context.consumers) == 3
    assert context.discovery_complete is True
    assert len(context.impact_fingerprint) == 64
    assert context.recompute_impact_fingerprint() == context.impact_fingerprint
    assert context.replacement_column == "customer_key"
    assert [consumer.urn for consumer in context.consumers] == list(CONSUMERS)
    assert {consumer.urn for consumer in context.consumers} == set(CONSUMERS)
    assert {consumer.degree for consumer in context.consumers} == {"1", "2", "3+"}
    assert all(consumer.columns == ("customer_id",) for consumer in context.consumers)
    assert all(len(consumer.path_evidence) == 1 for consumer in context.consumers)
    assert {field.field_path for field in context.source.schema} == {
        "order_id",
        "customer_id",
        "customer_key",
    }
    assert context.assets[SEMANTIC_CONSUMER].governance.owner_urns == (
        "urn:li:corpuser:identity-data-owner",
    )
    assert "urn:li:tag:SemanticReviewRequired" in (
        context.assets[SEMANTIC_CONSUMER].governance.tag_urns
    )
    assert (
        context.source.governance.structured_properties["io.lineagetx.status"]
        == ("DETECTED",)
    )

    lineage_calls = [args for name, args in fake.calls if name == "get_lineage"]
    assert [call["offset"] for call in lineage_calls] == [0, 2]
    assert all(call["column"] == "customer_id" for call in lineage_calls)
    assert all(call["upstream"] is False for call in lineage_calls)
    assert all(call["max_hops"] == 3 for call in lineage_calls)
    assert sum(name == "get_lineage_paths_between" for name, _ in fake.calls) == 3


def test_falls_back_to_dataset_discovery_when_v160_downstream_column_query_is_empty() -> None:
    class DataHubV160DirectionalMCP(FakeDataHubMCP):
        async def call_tools(
            self, calls: Sequence[tuple[str, Mapping[str, Any]]]
        ) -> list[tuple[Any, dict[str, Any]]]:
            name, raw_args = calls[0]
            args = dict(raw_args)
            if name != "get_lineage":
                return await super().call_tools(calls)

            self.calls.append((name, args))
            if args.get("column"):
                return [
                    (
                        {
                            "downstreams": {"facets": [], "total": 0},
                            "metadata": {"queryType": "column-level-lineage"},
                        },
                        {"tool": name, "arguments": args},
                    )
                ]

            offset = args["offset"]
            page = self.lineage[offset : offset + args["max_results"]]
            results = [
                {
                    "entity": item["entity"],
                    "degree": 3 if item["degree"] == "3+" else item["degree"],
                }
                for item in page
            ]
            return [
                (
                    {
                        "downstreams": {
                            "total": len(self.lineage),
                            "searchResults": results,
                            "offset": offset,
                            "returned": len(results),
                            "hasMore": offset + len(results) < len(self.lineage),
                        }
                    },
                    {"tool": name, "arguments": args},
                )
            ]

    fake = DataHubV160DirectionalMCP()
    context = asyncio.run(
        DataHubMCPContextReader(fake, page_size=2).load(SOURCE, "customer_id")
    )

    assert [consumer.urn for consumer in context.consumers] == list(CONSUMERS)
    assert {consumer.degree for consumer in context.consumers} == {"1", "2", "3+"}
    assert all(consumer.columns == ("customer_id",) for consumer in context.consumers)
    lineage_calls = [args for name, args in fake.calls if name == "get_lineage"]
    assert lineage_calls[0]["column"] == "customer_id"
    assert "column" not in lineage_calls[1]
    assert sum(name == "get_lineage_paths_between" for name, _ in fake.calls) == 3


def test_normalizes_v160_numeric_third_degree_in_column_lineage() -> None:
    fake = FakeDataHubMCP()
    fake.lineage[-1]["degree"] = 3

    context = asyncio.run(DataHubMCPContextReader(fake).load(SOURCE, "customer_id"))

    assert {consumer.degree for consumer in context.consumers} == {"1", "2", "3+"}


def test_fails_closed_when_lineage_pagination_stops_making_progress() -> None:
    class StalledLineageMCP(FakeDataHubMCP):
        async def call_tools(
            self, calls: Sequence[tuple[str, Mapping[str, Any]]]
        ) -> list[tuple[Any, dict[str, Any]]]:
            name, args = calls[0]
            if name == "get_lineage" and args["offset"] == 2:
                self.calls.append((name, dict(args)))
                return [
                    (
                        {
                            "downstreams": {
                                "total": 3,
                                "searchResults": [],
                                "offset": 2,
                                "returned": 0,
                                "hasMore": True,
                            }
                        },
                        {"tool": name, "arguments": dict(args)},
                    )
                ]
            return await super().call_tools(calls)

    with pytest.raises(DataHubContextReadError, match="no forward progress"):
        asyncio.run(
            DataHubMCPContextReader(StalledLineageMCP(), page_size=2).load(
                SOURCE, "customer_id"
            )
        )


def test_fails_closed_when_a_lineage_path_cannot_be_proved() -> None:
    class MissingPathMCP(FakeDataHubMCP):
        async def call_tools(
            self, calls: Sequence[tuple[str, Mapping[str, Any]]]
        ) -> list[tuple[Any, dict[str, Any]]]:
            name, args = calls[0]
            if name == "get_lineage_paths_between" and args["target_urn"] == AIRFLOW_CONSUMER:
                self.calls.append((name, dict(args)))
                return [
                    (
                        {"paths": [], "pathCount": 0},
                        {"tool": name, "arguments": dict(args)},
                    )
                ]
            return await super().call_tools(calls)

    with pytest.raises(DataHubContextReadError, match="lineage path metadata"):
        asyncio.run(
            DataHubMCPContextReader(MissingPathMCP(), page_size=2).load(
                SOURCE, "customer_id"
            )
        )


def test_refresh_detects_changed_impact_before_commit() -> None:
    fake = FakeDataHubMCP()
    reader = DataHubMCPContextReader(fake, page_size=2)
    frozen = asyncio.run(reader.load(SOURCE))
    refreshed = asyncio.run(reader.assert_impact_unchanged(frozen))
    assert refreshed.impact_fingerprint == frozen.impact_fingerprint

    fake.schemas[DBT_CONSUMER][0]["description"] = "changed after prepare"
    with pytest.raises(ImpactFingerprintChanged, match="commit must be aborted"):
        asyncio.run(reader.assert_impact_unchanged(frozen))


def test_requires_exact_unique_degrees_and_one_owner_per_consumer() -> None:
    duplicate_degree = FakeDataHubMCP()
    duplicate_degree.lineage[-1]["degree"] = 2
    with pytest.raises(DataHubContextReadError, match="degree 1, 2, and 3"):
        asyncio.run(DataHubMCPContextReader(duplicate_degree).load(SOURCE))

    ambiguous_owner = FakeDataHubMCP()
    ambiguous_owner.entities[SEMANTIC_CONSUMER]["ownership"]["owners"].append(
        {"owner": {"urn": "urn:li:corpuser:second-owner"}}
    )
    with pytest.raises(DataHubContextReadError, match="exactly one accountable"):
        asyncio.run(DataHubMCPContextReader(ambiguous_owner).load(SOURCE))


def test_rejects_unsafe_gms_urls_and_bounds_mcp_operations() -> None:
    assert normalize_datahub_gms_url("HTTP://LOCALHOST:8080/") == (
        "http://localhost:8080"
    )
    for unsafe in (
        "http://user:secret@localhost:8080",
        "http://localhost:8080?token=secret",
        "http://localhost:8080#fragment",
        "http://localhost:8080/api",
        "http://localhost:8979",
    ):
        with pytest.raises(ValueError):
            OfficialDataHubMCPClient(unsafe)

    client = OfficialDataHubMCPClient(
        "http://localhost:8080",
        operation_timeout_seconds=0.001,
    )
    with pytest.raises(DataHubContextReadError, match="initialize timed out"):
        asyncio.run(client._bounded("initialize", asyncio.sleep(0.05)))


def test_official_trace_has_transport_and_server_info_but_no_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def fake_stdio(_: Any):
        yield object(), object()

    class FakeSession:
        def __init__(self, *_: Any):
            pass

        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *_: Any) -> None:
            return None

        async def initialize(self) -> Any:
            return SimpleNamespace(
                serverInfo=SimpleNamespace(name="datahub-mcp", version="0.6.0")
            )

        async def list_tools(self) -> Any:
            return SimpleNamespace(tools=[SimpleNamespace(name="get_entities")])

        async def call_tool(self, *_: Any, **__: Any) -> Any:
            return SimpleNamespace(
                isError=False,
                structuredContent={"result": {"secret_result": "not-in-trace"}},
                content=[],
            )

    monkeypatch.setattr(datahub_context_module, "stdio_client", fake_stdio)
    monkeypatch.setattr(datahub_context_module, "ClientSession", FakeSession)
    client = OfficialDataHubMCPClient(
        "http://localhost:8080",
        executable=Path(sys.executable),
    )
    payload, trace = asyncio.run(
        client.call_tools([("get_entities", {"urns": [SOURCE]})])
    )[0]

    assert payload == {"secret_result": "not-in-trace"}
    assert trace["transport"] == OfficialDataHubMCPClient.transport
    assert trace["mcp_server"]["name"] == "datahub-mcp"
    assert "secret_result" not in json.dumps(trace)
    assert "result" not in trace


def test_seed_uses_real_sdk_fine_grained_lineage_for_customer_id() -> None:
    audit = AuditStampClass(
        time=1,
        actor="urn:li:corpuser:data-platform-oncall",
    )
    aspect = lineage_aspect(SOURCE, DBT_CONSUMER, audit)

    assert len(aspect.fineGrainedLineages) == 1
    fine = aspect.fineGrainedLineages[0]
    assert isinstance(fine, FineGrainedLineageClass)
    assert fine.upstreamType == FineGrainedLineageUpstreamTypeClass.FIELD_SET
    assert fine.downstreamType == FineGrainedLineageDownstreamTypeClass.FIELD
    assert fine.upstreams == [
        f"urn:li:schemaField:({SOURCE},customer_id)",
    ]
    assert fine.downstreams == [
        f"urn:li:schemaField:({DBT_CONSUMER},customer_id)",
    ]


def test_seed_constants_and_datahub_fixtures_are_canonical() -> None:
    scenario = json.loads(
        (ROOT / "fixtures" / "lineagetx" / "datahub" / "scenario.json").read_text(
            encoding="utf-8"
        )
    )
    replay = json.loads(
        (ROOT / "fixtures" / "lineagetx" / "datahub" / "mcp_context.json").read_text(
            encoding="utf-8"
        )
    )
    assert scenario["platform"] == replay["platform"] == "postgres"
    assert scenario["source_urn"] == replay["source"]["urn"] == SOURCE
    assert scenario["source_owner"] == SEED_MODULE.PLATFORM_OWNER
    assert [item["urn"] for item in scenario["consumers"]] == list(CONSUMERS)
    assert [item["owner"] for item in scenario["consumers"]] == [
        SEED_MODULE.ANALYTICS_OWNER,
        SEED_MODULE.ANALYTICS_OWNER,
        SEED_MODULE.IDENTITY_OWNER,
    ]
    assert SEED_MODULE.parse_args(["--no-verify"]).no_verify is True


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_LINEAGETX_DATAHUB_INTEGRATION") != "1",
    reason="set RUN_LINEAGETX_DATAHUB_INTEGRATION=1 with full DataHub OSS running",
)
def test_real_gms_official_mcp_seed_read_and_writeback() -> None:
    gms_origin = normalize_datahub_gms_url(
        os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "seed_lineagetx_datahub.py"),
            "--gms-url",
            gms_origin,
            "--verify-timeout-seconds",
            "180",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=240,
    )
    seed_receipt = json.loads(completed.stdout)
    assert seed_receipt["verification"]["live_verified"] is True

    async def exercise() -> None:
        client = OfficialDataHubMCPClient(
            gms_origin,
            operation_timeout_seconds=30,
        )
        reader = DataHubMCPContextReader(client)
        context = await reader.load(SOURCE)
        assert context.discovery_complete is True
        records = {
            urn: MigrationWriteback(
                migration_id="ltx-real-datahub-integration",
                status="PREPARED",
                owner=context.assets[urn].governance.owner_urns[0],
                evidence_url="https://example.invalid/lineagetx/integration-receipt",
            )
            for urn in context.asset_urns
        }
        receipt = await DataHubMigrationWriter(client, context).write_assets(records)
        assert receipt.live_verified is True
        assert receipt.impact_fingerprint == context.impact_fingerprint
        assert receipt.entity_urns == context.asset_urns

    asyncio.run(exercise())
