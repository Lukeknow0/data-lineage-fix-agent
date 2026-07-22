#!/usr/bin/env python3
"""Seed the complete three-hop LineageTX scenario into real DataHub OSS.

This script talks directly to GMS through the DataHub SDK. It does not use the
legacy Lite compatibility bridge. The official DataHub MCP server is used by the
runtime reader after these entities and lineage aspects have been emitted.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from typing import Iterable

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    CorpUserInfoClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
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
    StructuredPropertyDefinitionClass,
    TagAssociationClass,
    TagPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)

from data_lineage_fix_agent.lineagetx.datahub_context import (
    DataHubContextReadError,
    DataHubMCPContextReader,
    DataHubMigrationContext,
    OfficialDataHubMCPClient,
    normalize_datahub_gms_url,
)


SOURCE = make_dataset_urn("postgres", "ecommerce.raw.orders", "PROD")
DBT_CONSUMER = make_dataset_urn("postgres", "analytics.stg_orders", "PROD")
AIRFLOW_CONSUMER = make_dataset_urn("postgres", "ops.customer_export", "PROD")
SEMANTIC_CONSUMER = make_dataset_urn(
    "postgres", "semantic.customer_identity", "PROD"
)
ASSETS = (SOURCE, DBT_CONSUMER, AIRFLOW_CONSUMER, SEMANTIC_CONSUMER)

PLATFORM_OWNER = "urn:li:corpuser:data-platform-oncall"
ANALYTICS_OWNER = "urn:li:corpuser:analytics-engineering"
IDENTITY_OWNER = "urn:li:corpuser:identity-data-owner"

MIGRATION_TAG = make_tag_urn("LineageTXMigration")
CRITICAL_TAG = make_tag_urn("CriticalData")
APPROVAL_TAG = make_tag_urn("SemanticReviewRequired")

PROPERTY_DEFINITIONS = {
    "urn:li:structuredProperty:io.lineagetx.migrationId": (
        "io.lineagetx.migrationId",
        "LineageTX Migration ID",
        "Stable identifier for the coordinated schema migration.",
    ),
    "urn:li:structuredProperty:io.lineagetx.status": (
        "io.lineagetx.status",
        "LineageTX Status",
        "Current state in the LineageTX migration state machine.",
    ),
    "urn:li:structuredProperty:io.lineagetx.owner": (
        "io.lineagetx.owner",
        "LineageTX Owner",
        "DataHub owner URN responsible for the migration decision.",
    ),
    "urn:li:structuredProperty:io.lineagetx.evidenceUrl": (
        "io.lineagetx.evidenceUrl",
        "LineageTX Evidence URL",
        "Public immutable evidence receipt for this migration.",
    ),
}


def field(
    name: str,
    native_type: str,
    description: str,
    *,
    number: bool = False,
) -> SchemaFieldClass:
    return SchemaFieldClass(
        fieldPath=name,
        type=SchemaFieldDataTypeClass(
            type=NumberTypeClass() if number else StringTypeClass()
        ),
        nativeDataType=native_type,
        nullable=False,
        description=description,
    )


def schema(name: str, fields: list[SchemaFieldClass]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName=name,
        platform=make_data_platform_urn("postgres"),
        version=0,
        hash="lineagetx-customer-key-contract-v1",
        platformSchema=OtherSchemaClass(rawSchema="LineageTX controlled fixture"),
        fields=fields,
    )


def lineage_aspect(
    upstream_urn: str,
    downstream_urn: str,
    audit: AuditStampClass,
) -> UpstreamLineageClass:
    """Build one real SDK dataset edge plus customer_id field-level edge."""

    return UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=upstream_urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
                auditStamp=audit,
            )
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[make_schema_field_urn(upstream_urn, "customer_id")],
                downstreams=[make_schema_field_urn(downstream_urn, "customer_id")],
                transformOperation="IDENTITY",
                confidenceScore=1.0,
            )
        ],
    )


def emit(emitter: DatahubRestEmitter, urn: str, aspect: object) -> None:
    emitter.emit_mcp(MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect))


def ownership(owner_urn: str, audit: AuditStampClass) -> OwnershipClass:
    return OwnershipClass(
        owners=[
            OwnerClass(owner=owner_urn, type=OwnershipTypeClass.TECHNICAL_OWNER)
        ],
        lastModified=audit,
    )


def emit_tags(
    emitter: DatahubRestEmitter,
    urn: str,
    tag_urns: Iterable[str],
) -> None:
    emit(
        emitter,
        urn,
        GlobalTagsClass(tags=[TagAssociationClass(tag=tag) for tag in tag_urns]),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed and verify the canonical LineageTX DataHub OSS fixture."
    )
    parser.add_argument(
        "--gms-url",
        default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
        help="Credential-free DataHub GMS origin (default: DATAHUB_GMS_URL or localhost:8080).",
    )
    parser.add_argument(
        "--verify-timeout-seconds",
        type=float,
        default=120.0,
        help="Bounded time for official MCP read-back after seeding (default: 120).",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Development only: skip official MCP read-back; receipt is explicitly unverified.",
    )
    return parser.parse_args(argv)


async def wait_for_official_readback(
    gms_origin: str,
    timeout_seconds: float,
) -> DataHubMigrationContext:
    if timeout_seconds <= 0:
        raise ValueError("verify timeout must be positive")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    last_error: Exception | None = None
    while loop.time() < deadline:
        remaining = deadline - loop.time()
        client = OfficialDataHubMCPClient(
            gms_origin,
            operation_timeout_seconds=max(0.1, min(15.0, remaining)),
        )
        try:
            return await asyncio.wait_for(
                DataHubMCPContextReader(client).load(
                    SOURCE,
                    "customer_id",
                    "customer_key",
                ),
                timeout=remaining,
            )
        except (DataHubContextReadError, TimeoutError, OSError) as error:
            last_error = error
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(2.0, remaining))
    raise DataHubContextReadError(
        f"official MCP read-back did not become complete within {timeout_seconds:g}s"
    ) from last_error


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        gms_origin = normalize_datahub_gms_url(args.gms_url)
    except ValueError as error:
        raise SystemExit(f"invalid --gms-url: {error}") from error
    if args.verify_timeout_seconds <= 0:
        raise SystemExit("--verify-timeout-seconds must be positive")

    emitter = DatahubRestEmitter(
        gms_server=gms_origin,
        token=os.getenv("DATAHUB_GMS_TOKEN"),
        timeout_sec=min(30.0, args.verify_timeout_seconds),
        connect_timeout_sec=min(10.0, args.verify_timeout_seconds),
        read_timeout_sec=min(30.0, args.verify_timeout_seconds),
        retry_max_times=2,
        datahub_component="lineagetx-seeder/0.1.0",
    )
    emitter.test_connection()
    now = int(time.time() * 1000)
    audit = AuditStampClass(time=now, actor=PLATFORM_OWNER)

    people = {
        PLATFORM_OWNER: ("Data Platform On-call", "data-platform@example.invalid"),
        ANALYTICS_OWNER: (
            "Analytics Engineering",
            "analytics-engineering@example.invalid",
        ),
        IDENTITY_OWNER: ("Identity Data Owner", "identity-owner@example.invalid"),
    }
    for urn, (display_name, email) in people.items():
        emit(
            emitter,
            urn,
            CorpUserInfoClass(
                active=True,
                displayName=display_name,
                email=email,
                title="Technical Owner",
            ),
        )

    tags = {
        MIGRATION_TAG: (
            "LineageTXMigration",
            "Asset participates in a coordinated LineageTX migration.",
            "#38C6D9",
        ),
        CRITICAL_TAG: (
            "CriticalData",
            "Production-critical data asset requiring verified change control.",
            "#E56A61",
        ),
        APPROVAL_TAG: (
            "SemanticReviewRequired",
            "Automated change is blocked until the semantic owner approves it.",
            "#F0B44D",
        ),
    }
    for urn, (name, description, color) in tags.items():
        emit(
            emitter,
            urn,
            TagPropertiesClass(name=name, description=description, colorHex=color),
        )

    for urn, (qualified_name, display_name, description) in PROPERTY_DEFINITIONS.items():
        emit(
            emitter,
            urn,
            StructuredPropertyDefinitionClass(
                qualifiedName=qualified_name,
                displayName=display_name,
                description=description,
                valueType="urn:li:dataType:datahub.string",
                entityTypes=["urn:li:entityType:datahub.dataset"],
                cardinality="SINGLE",
                immutable=False,
            ),
        )

    dataset_specs = {
        SOURCE: (
            "orders",
            "Producer expand-contract schema: customer_id is deprecated; customer_key is canonical.",
            PLATFORM_OWNER,
            [CRITICAL_TAG],
            schema(
                "ecommerce.raw.orders",
                [
                    field("order_id", "BIGINT", "Stable order identifier.", number=True),
                    field(
                        "customer_id",
                        "BIGINT",
                        "Deprecated compatibility alias scheduled for removal.",
                        number=True,
                    ),
                    field(
                        "customer_key",
                        "BIGINT",
                        "Canonical customer key replacing customer_id.",
                        number=True,
                    ),
                    field("total_amount", "DECIMAL", "Order total.", number=True),
                ],
            ),
            "producer",
        ),
        DBT_CONSUMER: (
            "stg_orders",
            "dbt SQL consumer eligible for deterministic automatic repair.",
            ANALYTICS_OWNER,
            [CRITICAL_TAG],
            schema(
                "analytics.stg_orders",
                [
                    field("order_id", "BIGINT", "Order identifier.", number=True),
                    field("customer_id", "BIGINT", "Legacy customer key.", number=True),
                    field("total_amount", "DECIMAL", "Order total.", number=True),
                ],
            ),
            "dbt_sql",
        ),
        AIRFLOW_CONSUMER: (
            "customer_export",
            "Airflow field mapping represented by a two-file coordinated repair.",
            ANALYTICS_OWNER,
            [CRITICAL_TAG],
            schema(
                "ops.customer_export",
                [
                    field("customer_id", "BIGINT", "Exported customer key.", number=True),
                    field("exported_at", "TIMESTAMP", "Export timestamp."),
                ],
            ),
            "airflow_mapping",
        ),
        SEMANTIC_CONSUMER: (
            "customer_identity",
            "Semantic consumer whose customer identifier meaning requires owner approval.",
            IDENTITY_OWNER,
            [CRITICAL_TAG, APPROVAL_TAG],
            schema(
                "semantic.customer_identity",
                [
                    field(
                        "customer_id",
                        "BIGINT",
                        "Business identity key; mapping cannot be inferred safely.",
                        number=True,
                    ),
                    field("identity_tier", "VARCHAR", "Identity confidence tier."),
                ],
            ),
            "semantic_approval",
        ),
    }

    for urn, (name, description, owner, tag_urns, schema_aspect, consumer_type) in dataset_specs.items():
        emit(
            emitter,
            urn,
            DatasetPropertiesClass(
                name=name,
                description=description,
                customProperties={
                    "lineagetx.consumer_type": consumer_type,
                    "lineagetx.change_intent": "customer_id -> customer_key",
                    "lineagetx.fixture": "three-hop-v1",
                },
            ),
        )
        emit(emitter, urn, schema_aspect)
        emit(emitter, urn, ownership(owner, audit))
        emit_tags(emitter, urn, tag_urns)

    # Three real DataHub SDK column-lineage edges: source -> dbt -> airflow -> semantic.
    emit(emitter, DBT_CONSUMER, lineage_aspect(SOURCE, DBT_CONSUMER, audit))
    emit(
        emitter,
        AIRFLOW_CONSUMER,
        lineage_aspect(DBT_CONSUMER, AIRFLOW_CONSUMER, audit),
    )
    emit(
        emitter,
        SEMANTIC_CONSUMER,
        lineage_aspect(AIRFLOW_CONSUMER, SEMANTIC_CONSUMER, audit),
    )

    verification: dict[str, object]
    if args.no_verify:
        verification = {
            "live_verified": False,
            "mode": "skipped-development-only",
            "discovery_complete": False,
            "impact_fingerprint": None,
        }
    else:
        context = asyncio.run(
            wait_for_official_readback(gms_origin, args.verify_timeout_seconds)
        )
        verification = {
            "live_verified": True,
            "mode": "official-mcp-readback",
            "transport": context.transport,
            "discovery_complete": context.discovery_complete,
            "impact_fingerprint": context.impact_fingerprint,
        }

    print(
        json.dumps(
            {
                "backend": "full-datahub-oss",
                "gms_origin": gms_origin,
                "seeded_at_epoch_ms": now,
                "source_urn": SOURCE,
                "consumer_urns": [DBT_CONSUMER, AIRFLOW_CONSUMER, SEMANTIC_CONSUMER],
                "column": "customer_id",
                "column_lineage_hops": 3,
                "structured_property_urns": sorted(PROPERTY_DEFINITIONS),
                "migration_tag_urn": MIGRATION_TAG,
                "verification": verification,
                "contains_credentials": False,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
