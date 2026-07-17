#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    CorpUserInfoClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
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
    TagAssociationClass,
    TagPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)


ROOT = Path(__file__).resolve().parents[1]
GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080").rstrip("/")
TOKEN = os.getenv("DATAHUB_GMS_TOKEN")
ACTOR = "urn:li:corpuser:data-platform-oncall"
SOURCE = make_dataset_urn("postgres", "ecommerce.raw.orders", "PROD")
TARGET = make_dataset_urn("postgres", "analytics.mart_customer_revenue", "PROD")
DRIFT_TAG = make_tag_urn("SchemaDriftDetected")
VERIFIED_TAG = make_tag_urn("DataLineageFixVerified")


def field(name: str, native_type: str, description: str, *, number: bool) -> SchemaFieldClass:
    data_type = NumberTypeClass() if number else StringTypeClass()
    return SchemaFieldClass(
        fieldPath=name,
        type=SchemaFieldDataTypeClass(type=data_type),
        nativeDataType=native_type,
        nullable=False,
        description=description,
    )


def schema(name: str, fields: list[SchemaFieldClass]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName=name,
        platform=make_data_platform_urn("postgres"),
        version=0,
        hash="datalineage-fix-agent-fixture-v2",
        platformSchema=OtherSchemaClass(rawSchema="controlled hackathon fixture"),
        fields=fields,
    )


def emit(emitter: DatahubRestEmitter, urn: str, aspect: object) -> None:
    emitter.emit_mcp(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))


def main() -> None:
    emitter = DatahubRestEmitter(
        gms_server=GMS_URL,
        token=TOKEN,
        datahub_component="datalineage-fix-agent-seeder/0.1.0",
    )
    emitter.test_connection()
    now = int(time.time() * 1000)
    audit = AuditStampClass(time=now, actor=ACTOR)

    emit(
        emitter,
        ACTOR,
        CorpUserInfoClass(
            active=True,
            displayName="Data Platform On-call",
            email="data-platform-oncall@example.invalid",
            title="Technical Owner",
        ),
    )
    emit(
        emitter,
        DRIFT_TAG,
        TagPropertiesClass(
            name="SchemaDriftDetected",
            description="A schema change has a proven downstream code impact.",
            colorHex="#D97706",
        ),
    )
    emit(
        emitter,
        VERIFIED_TAG,
        TagPropertiesClass(
            name="DataLineageFixVerified",
            description="The downstream patch passed its generated regression test.",
            colorHex="#047857",
        ),
    )

    common_ownership = OwnershipClass(
        owners=[OwnerClass(owner=ACTOR, type=OwnershipTypeClass.TECHNICAL_OWNER)],
        lastModified=audit,
    )
    emit(
        emitter,
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
    )
    emit(
        emitter,
        SOURCE,
        schema(
            "ecommerce.raw.orders",
            [
                field("order_id", "INTEGER", "Stable order identifier.", number=True),
                field(
                    "customer_key",
                    "INTEGER",
                    "Canonical customer key; renamed from customer_id in schema v2.",
                    number=True,
                ),
                field("total_amount", "DECIMAL", "Order total in USD.", number=True),
                field("order_ts", "TIMESTAMP", "Order creation time.", number=False),
            ],
        ),
    )
    emit(emitter, SOURCE, common_ownership)
    emit(emitter, SOURCE, GlobalTagsClass(tags=[TagAssociationClass(tag=DRIFT_TAG)]))

    emit(
        emitter,
        TARGET,
        DatasetPropertiesClass(
            name="mart_customer_revenue",
            description="Customer lifetime revenue mart maintained by the controlled fixture pipeline.",
            customProperties={
                "repository_file": "pipeline/customer_revenue.sql",
                "fixture_state": "broken-before-agent",
            },
        ),
    )
    emit(
        emitter,
        TARGET,
        schema(
            "analytics.mart_customer_revenue",
            [
                field("customer_key", "INTEGER", "Canonical customer key.", number=True),
                field("lifetime_value", "DECIMAL", "Summed order value.", number=True),
            ],
        ),
    )
    emit(emitter, TARGET, common_ownership)
    emit(
        emitter,
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
    )
    # Reset the previous gate's verified status before each live run.
    emit(emitter, TARGET, GlobalTagsClass(tags=[]))

    receipt = {
        "gms_url": GMS_URL,
        "seeded_at_epoch_ms": now,
        "source_urn": SOURCE,
        "target_urn": TARGET,
        "owner_urn": ACTOR,
        "drift_tag_urn": DRIFT_TAG,
        "verified_tag_urn": VERIFIED_TAG,
        "contains_credentials": False,
    }
    (ROOT / "artifacts" / "seed_receipt.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
