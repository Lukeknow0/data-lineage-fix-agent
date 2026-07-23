from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

from data_lineage_fix_agent.lineagetx.models import (
    ChangeIntent,
    Participant,
    ParticipantKind,
    ParticipantStatus,
)


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures" / "lineagetx"
SOURCE = "urn:li:dataset:(urn:li:dataPlatform:postgres,ecommerce.raw.orders,PROD)"
DBT = "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.stg_orders,PROD)"
AIRFLOW = "urn:li:dataset:(urn:li:dataPlatform:postgres,ops.customer_export,PROD)"
SEMANTIC = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,semantic.customer_identity,PROD)"
)
PLATFORM_OWNER = "urn:li:corpuser:data-platform-oncall"
ANALYTICS_OWNER = "urn:li:corpuser:analytics-engineering"
IDENTITY_OWNER = "urn:li:corpuser:identity-data-owner"


def _json(relative_path: str) -> dict[str, object]:
    return json.loads((FIXTURES / relative_path).read_text(encoding="utf-8"))


def _field_names(schema: dict[str, object]) -> tuple[str, ...]:
    fields = schema["fields"]
    assert isinstance(fields, list)
    return tuple(str(field["name"]) for field in fields)


def _python_mapping(source: str) -> dict[str, str]:
    tree = ast.parse(source)
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "FIELD_MAPPING"
    )
    value = ast.literal_eval(assignment.value)
    assert isinstance(value, dict)
    return {str(key): str(item) for key, item in value.items()}


def test_change_intent_is_a_stable_model_record_bound_to_contract_bytes() -> None:
    raw_intent = _json("producer/change_intent.json")
    intent = ChangeIntent.from_dict(raw_intent)
    contract_path = FIXTURES / "producer" / "schema.contract.json"

    assert intent.to_dict() == raw_intent
    assert intent.source_asset_urn == SOURCE
    assert (intent.old_field, intent.new_field) == ("customer_id", "customer_key")
    assert intent.producer_base_sha == "1" * 40
    assert intent.producer_head_sha == "2" * 40
    assert intent.producer_pr_url == "https://github.com/acme/commerce-producer/pull/42"
    assert intent.contract_schema_fingerprint == hashlib.sha256(
        contract_path.read_bytes()
    ).hexdigest()


def test_manifest_participants_are_model_compatible_and_match_datahub() -> None:
    migration = _json("migration.json")
    scenario = _json("datahub/scenario.json")
    replay = _json("datahub/mcp_context.json")
    intent = ChangeIntent.from_dict(_json("producer/change_intent.json"))
    raw_participants = migration["participants"]
    raw_consumers = scenario["consumers"]
    assert isinstance(raw_participants, list)
    assert isinstance(raw_consumers, list)
    participants = tuple(Participant.from_dict(item) for item in raw_participants)

    assert migration["migration_id"] == intent.migration_id
    assert migration["change_intent"] == "producer/change_intent.json"
    assert migration["lineage"] == {
        "context": "datahub/mcp_context.json",
        "source_asset_urn": SOURCE,
        "source_column_urn": f"urn:li:schemaField:({SOURCE},customer_id)",
        "max_hops": 3,
        "discovery_complete": True,
    }
    assert scenario["source_urn"] == replay["source"]["urn"] == SOURCE
    assert scenario["source_owner"] == replay["source"]["owners"][0] == PLATFORM_OWNER
    assert len(participants) == len(raw_consumers) == 3
    assert {item.kind for item in participants} == set(ParticipantKind)
    assert all(item.status is ParticipantStatus.DISCOVERED for item in participants)
    assert all(item.migration_id == intent.migration_id for item in participants)

    expected = (
        (
            ParticipantKind.DBT_SQL,
            DBT,
            "1",
            "data-platform",
            (ANALYTICS_OWNER,),
            ("dbt/models/stg_orders.sql",),
        ),
        (
            ParticipantKind.AIRFLOW_MAPPING,
            AIRFLOW,
            "2",
            "data-platform",
            (ANALYTICS_OWNER,),
            (
                "airflow/dags/export_customers.py",
                "airflow/config/export_columns.json",
            ),
        ),
        (
            ParticipantKind.SEMANTIC_APPROVAL,
            SEMANTIC,
            "3+",
            "analytics-governance",
            (IDENTITY_OWNER,),
            ("semantic/customer_identity.json",),
        ),
    )
    for raw, participant, consumer, values in zip(
        raw_participants, participants, raw_consumers, expected, strict=True
    ):
        kind, asset, degree, repository, owners, files = values
        derived = Participant.create(
            migration_id=intent.migration_id,
            kind=kind,
            asset_urn=asset,
            repository=repository,
            owner_urns=owners,
            files=files,
            created_at=str(raw["created_at"]),
        )
        assert participant.participant_id == derived.participant_id
        assert participant.kind is kind
        assert participant.asset_urn == consumer["urn"] == asset
        assert raw["lineage_degree"] == consumer["degree"] == degree
        assert participant.repository == consumer["repository"] == repository
        assert participant.owner_urns == (consumer["owner"],) == owners
        assert participant.files == tuple(consumer["files"]) == files
        assert all((FIXTURES / "repos" / repository / path).is_file() for path in files)


def test_column_lineage_is_one_exact_three_hop_chain() -> None:
    migration = _json("migration.json")
    replay = _json("datahub/mcp_context.json")
    participants = migration["participants"]
    lineage = replay["column_lineage"]
    assert isinstance(participants, list)
    assert isinstance(lineage, list)

    datasets = (SOURCE, DBT, AIRFLOW, SEMANTIC)
    assert [edge["hop"] for edge in lineage] == [1, 2, 3]
    assert [edge["participant_id"] for edge in lineage] == [
        participant["participant_id"] for participant in participants
    ]
    for hop, edge in enumerate(lineage, start=1):
        assert edge["upstream"] == (
            f"urn:li:schemaField:({datasets[hop - 1]},customer_id)"
        )
        assert edge["downstream"] == (
            f"urn:li:schemaField:({datasets[hop]},customer_id)"
        )
        assert edge["owners"] == participants[hop - 1]["owner_urns"]


def test_fixture_behaviors_cover_one_file_two_files_and_owner_approval() -> None:
    migration = _json("migration.json")
    expanded = _json("producer/schema.expanded.json")
    contract = _json("producer/schema.contract.json")
    participants = migration["participants"]
    assert isinstance(participants, list)

    assert expanded["asset_urn"] == contract["asset_urn"] == SOURCE
    assert expanded["dataset"] == contract["dataset"] == "ecommerce.raw.orders"
    assert expanded["platform"] == contract["platform"] == "postgres"
    assert {"customer_id", "customer_key"}.issubset(_field_names(expanded))
    assert "customer_id" not in _field_names(contract)
    assert "customer_key" in _field_names(contract)

    dbt = participants[0]
    sql = (FIXTURES / "repos" / dbt["repository"] / dbt["files"][0]).read_text(
        encoding="utf-8"
    )
    assert dbt["kind"] == "DBT_SQL"
    assert len(dbt["files"]) == 1
    assert sql.count("customer_id") == 1
    assert dbt["adapter_config"] == {
        "dialect": "duckdb",
        "relation": "raw_orders",
        "relation_asset_urn": SOURCE,
    }

    airflow = participants[1]
    assert airflow["kind"] == "AIRFLOW_MAPPING"
    assert len(airflow["files"]) == 2
    python_mapping = _python_mapping(
        (FIXTURES / "repos" / airflow["repository"] / airflow["files"][0]).read_text(
            encoding="utf-8"
        )
    )
    json_mapping = _json(
        f"repos/{airflow['repository']}/{airflow['files'][1]}"
    )["field_mapping"]
    assert python_mapping == json_mapping
    assert python_mapping["customer_id"] == "customer_id"

    semantic = participants[2]
    semantic_document = _json(
        f"repos/{semantic['repository']}/{semantic['files'][0]}"
    )
    assert semantic["kind"] == "SEMANTIC_APPROVAL"
    assert semantic["automatic_change"] is False
    assert semantic_document["owner"] == IDENTITY_OWNER
    assert semantic_document["dimensions"]["customer_id"]["source_field"] == (
        "customer_id"
    )
    assert "customer_key" not in semantic_document["dimensions"]


def test_no_retired_source_or_owner_identifiers_remain_in_lineagetx_fixtures() -> None:
    fixture_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(FIXTURES.rglob("*"))
        if path.is_file()
    )

    assert "urn:li:dataPlatform:dbt" not in fixture_text
    assert "urn:li:dataPlatform:airflow" not in fixture_text
    assert "urn:li:dataPlatform:semantic" not in fixture_text
    assert "urn:li:corpuser:data-platform-owner" not in fixture_text
    assert "urn:li:corpuser:identity-owner" not in fixture_text
    assert "urn:li:dataPlatform:postgres,commerce.orders,PROD" not in fixture_text
