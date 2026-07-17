#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn, make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.lite.duckdb_lite import DuckDBLite
from datahub.lite.duckdb_lite_config import DuckDBLiteConfig
from datahub.metadata.schema_classes import (
    AuditStampClass,
    CorpUserInfoClass,
    CorpUserKeyClass,
    DatasetKeyClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FabricTypeClass,
    GlobalTagsClass,
    NumberTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SystemMetadataClass,
    TagAssociationClass,
    TagKeyClass,
    TagPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)


ROOT = Path(__file__).resolve().parents[1]
ACTOR = "urn:li:corpuser:data-platform-oncall"
SOURCE = make_dataset_urn("postgres", "ecommerce.raw.orders", "PROD")
TARGET = make_dataset_urn("postgres", "analytics.mart_customer_revenue", "PROD")
DRIFT_TAG = make_tag_urn("SchemaDriftDetected")
VERIFIED_TAG = make_tag_urn("DataLineageFixVerified")


def _field(name: str, native_type: str, description: str, number: bool) -> SchemaFieldClass:
    value_type = NumberTypeClass() if number else StringTypeClass()
    return SchemaFieldClass(
        fieldPath=name,
        type=SchemaFieldDataTypeClass(type=value_type),
        nativeDataType=native_type,
        nullable=False,
        description=description,
    )


def _schema(name: str, fields: list[SchemaFieldClass]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName=name,
        platform=make_data_platform_urn("postgres"),
        version=0,
        hash="datalineage-fix-agent-fixture-v2",
        platformSchema=OtherSchemaClass(rawSchema="controlled hackathon fixture"),
        fields=fields,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file",
        default=str(ROOT / "artifacts" / "datahub-lite" / "datahub.duckdb"),
    )
    args = parser.parse_args()
    db_file = Path(args.file).resolve()
    db_file.parent.mkdir(parents=True, exist_ok=True)
    if db_file.exists():
        db_file.unlink()
    lite = DuckDBLite(DuckDBLiteConfig(file=str(db_file), read_only=False))
    now = int(time.time() * 1000)
    audit = AuditStampClass(time=now, actor=ACTOR)
    ownership = OwnershipClass(
        owners=[OwnerClass(owner=ACTOR, type=OwnershipTypeClass.TECHNICAL_OWNER)],
        lastModified=audit,
    )
    records = [
        (ACTOR, CorpUserKeyClass(username="data-platform-oncall")),
        (
            ACTOR,
            CorpUserInfoClass(
                active=True,
                displayName="Data Platform On-call",
                email="data-platform-oncall@example.invalid",
                title="Technical Owner",
            ),
        ),
        (DRIFT_TAG, TagKeyClass(name="SchemaDriftDetected")),
        (
            DRIFT_TAG,
            TagPropertiesClass(
                name="SchemaDriftDetected",
                description="A schema change has a proven downstream code impact.",
                colorHex="#D97706",
            ),
        ),
        (VERIFIED_TAG, TagKeyClass(name="DataLineageFixVerified")),
        (
            VERIFIED_TAG,
            TagPropertiesClass(
                name="DataLineageFixVerified",
                description="The downstream patch passed its generated regression test.",
                colorHex="#047857",
            ),
        ),
        (
            SOURCE,
            DatasetKeyClass(
                platform=make_data_platform_urn("postgres"),
                name="ecommerce.raw.orders",
                origin=FabricTypeClass.PROD,
            ),
        ),
        (
            SOURCE,
            DatasetPropertiesClass(
                name="orders",
                description="Controlled source dataset after the schema v2 customer-key migration.",
                customProperties={
                    "schema_contract": "v2",
                    "quality_signal": "breaking-schema-drift",
                    "rename_hint.customer_id": "customer_key",
                },
            ),
        ),
        (
            SOURCE,
            _schema(
                "ecommerce.raw.orders",
                [
                    _field("order_id", "INTEGER", "Stable order identifier.", True),
                    _field(
                        "customer_key",
                        "INTEGER",
                        "Canonical customer key; renamed from customer_id in schema v2.",
                        True,
                    ),
                    _field("total_amount", "DECIMAL", "Order total in USD.", True),
                    _field("order_ts", "TIMESTAMP", "Order creation time.", False),
                ],
            ),
        ),
        (SOURCE, ownership),
        (SOURCE, GlobalTagsClass(tags=[TagAssociationClass(tag=DRIFT_TAG)])),
        (
            TARGET,
            DatasetKeyClass(
                platform=make_data_platform_urn("postgres"),
                name="analytics.mart_customer_revenue",
                origin=FabricTypeClass.PROD,
            ),
        ),
        (
            TARGET,
            DatasetPropertiesClass(
                name="mart_customer_revenue",
                description="Customer lifetime revenue mart maintained by the controlled fixture pipeline.",
                customProperties={
                    "repository_file": "pipeline/customer_revenue.sql",
                    "fixture_state": "broken-before-agent",
                },
            ),
        ),
        (
            TARGET,
            _schema(
                "analytics.mart_customer_revenue",
                [
                    _field("customer_key", "INTEGER", "Canonical customer key.", True),
                    _field("lifetime_value", "DECIMAL", "Summed order value.", True),
                ],
            ),
        ),
        (TARGET, ownership),
        (
            TARGET,
            UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=SOURCE,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                        auditStamp=audit,
                    )
                ]
            ),
        ),
        (TARGET, GlobalTagsClass(tags=[])),
    ]
    for urn, aspect in records:
        lite.write(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=aspect,
                systemMetadata=SystemMetadataClass(lastObserved=now, properties={}),
            )
        )
    lite.close()
    receipt = {
        "backend": "DataHub Lite (DuckDB)",
        "file": str(db_file),
        "source_urn": SOURCE,
        "target_urn": TARGET,
        "owner_urn": ACTOR,
        "seeded_at_epoch_ms": now,
        "contains_credentials": False,
    }
    (ROOT / "artifacts" / "seed_receipt.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
