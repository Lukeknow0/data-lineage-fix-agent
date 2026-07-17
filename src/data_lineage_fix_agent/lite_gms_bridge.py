from __future__ import annotations

import argparse
import os
import time
from functools import lru_cache
from typing import Any

import uvicorn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.lite.duckdb_lite import DuckDBLite
from datahub.lite.duckdb_lite_config import DuckDBLiteConfig
from datahub.metadata.schema_classes import (
    CorpUserInfoClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    OwnershipClass,
    SchemaMetadataClass,
    TagAssociationClass,
    TagPropertiesClass,
    SystemMetadataClass,
    UpstreamLineageClass,
)
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route


@lru_cache(maxsize=1)
def catalog() -> DuckDBLite:
    db_file = os.environ["DATAHUB_LITE_FILE"]
    return DuckDBLite(DuckDBLiteConfig(file=db_file, read_only=False))


def _typed(urn: str) -> dict[str, Any]:
    result = catalog().get(urn, aspects=None, typed=True)
    if not result:
        raise KeyError(urn)
    return result  # type: ignore[return-value]


def _tag(urn: str) -> dict[str, Any]:
    aspects = _typed(urn)
    properties = aspects.get("tagProperties")
    name = properties.name if isinstance(properties, TagPropertiesClass) else urn.rsplit(":", 1)[-1]
    description = properties.description if isinstance(properties, TagPropertiesClass) else None
    return {"urn": urn, "type": "TAG", "properties": {"name": name, "description": description}}


def _owner(urn: str) -> dict[str, Any]:
    try:
        info = _typed(urn).get("corpUserInfo")
    except KeyError:
        info = None
    properties: dict[str, Any] = {}
    if isinstance(info, CorpUserInfoClass):
        properties = {
            "active": info.active,
            "displayName": info.displayName,
            "title": info.title,
            "email": info.email,
        }
    return {"urn": urn, "properties": properties}


def _entity(urn: str) -> dict[str, Any]:
    aspects = _typed(urn)
    if urn.startswith("urn:li:tag:"):
        return _tag(urn)

    properties = aspects.get("datasetProperties")
    schema = aspects.get("schemaMetadata")
    ownership = aspects.get("ownership")
    global_tags = aspects.get("globalTags")
    name = urn
    property_payload: dict[str, Any] = {}
    if isinstance(properties, DatasetPropertiesClass):
        name = properties.name or urn
        property_payload = {
            "name": properties.name,
            "description": properties.description,
            "customProperties": [
                {"key": key, "value": value}
                for key, value in (properties.customProperties or {}).items()
            ],
        }

    entity: dict[str, Any] = {
        "urn": urn,
        "type": "DATASET",
        "name": name,
        "properties": property_payload,
    }
    if isinstance(schema, SchemaMetadataClass):
        platform_name = schema.platform.rsplit(":", 1)[-1]
        entity["platform"] = {"urn": schema.platform, "name": platform_name}
        entity["schemaMetadata"] = {
            "datasetUrn": urn,
            "name": schema.schemaName,
            "platformUrn": schema.platform,
            "fields": [pre_json_transform(field.to_obj()) for field in schema.fields],
            "primaryKeys": schema.primaryKeys or [],
        }
    if isinstance(ownership, OwnershipClass):
        entity["ownership"] = {
            "owners": [
                {"owner": _owner(owner.owner), "type": str(owner.type)}
                for owner in ownership.owners
            ]
        }
    if isinstance(global_tags, GlobalTagsClass):
        entity["tags"] = {
            "tags": [{"tag": _tag(association.tag)} for association in global_tags.tags]
        }
    return entity


def _downstream(source_urn: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for urn in catalog().list_ids():
        try:
            lineage = _typed(urn).get("upstreamLineage")
        except KeyError:
            continue
        if isinstance(lineage, UpstreamLineageClass) and any(
            upstream.dataset == source_urn for upstream in lineage.upstreams
        ):
            results.append({"entity": _entity(urn), "degree": 1})
    return results


def config() -> dict[str, Any]:
    return {
        "noCode": "true",
        "managedIngestion": {"enabled": False},
        "versions": {
            "acryldata/datahub": {
                "version": "v1.6.0",
                "commit": "datahub-lite-bridge-0.1.0",
            }
        },
        "datahub": {"serverEnv": "core", "serverType": "datahub-lite"},
    }


def get_aspect(
    urn: str,
    aspect: str,
    version: int = 0,
) -> dict[str, Any]:
    del version
    try:
        aspects = _typed(urn)
    except KeyError as error:
        raise LookupError("Entity not found") from error
    value = aspects.get(aspect)
    if value is None:
        raise LookupError("Aspect not found")
    full_name = value.RECORD_SCHEMA.fullname.replace(".pegasus2avro", "")
    return {"aspect": {full_name: pre_json_transform(value.to_obj())}}


async def graphql_payload(request: Request) -> dict[str, Any]:
    body = await request.json()
    operation = body.get("operationName")
    variables = body.get("variables") or {}

    if operation == "GetEntity":
        try:
            value = _entity(variables["urn"])
        except KeyError:
            value = None
        return {"data": {"entity": value}}

    if operation == "getRelatedDocuments":
        return {"data": {"entity": {"relatedDocuments": None}}}

    if operation == "GetEntityLineage":
        inputs = variables.get("input") or {}
        source = inputs.get("urn")
        direction = inputs.get("direction")
        results = _downstream(source) if direction == "DOWNSTREAM" else []
        return {
            "data": {
                "searchAcrossLineage": {
                    "searchResults": results,
                    "total": len(results),
                }
            }
        }

    if operation == "getTags":
        entities = []
        for urn in variables.get("urns", []):
            try:
                entities.append(_tag(urn))
            except KeyError:
                entities.append(None)
        return {"data": {"entities": entities}}

    if operation == "batchAddTags":
        inputs = variables.get("input") or {}
        tag_urns = inputs.get("tagUrns") or []
        for resource in inputs.get("resources") or []:
            entity_urn = resource["resourceUrn"]
            aspects = _typed(entity_urn)
            existing = aspects.get("globalTags")
            current = list(existing.tags) if isinstance(existing, GlobalTagsClass) else []
            current_urns = {association.tag for association in current}
            current.extend(
                TagAssociationClass(tag=tag_urn)
                for tag_urn in tag_urns
                if tag_urn not in current_urns
            )
            catalog().write(
                MetadataChangeProposalWrapper(
                    entityUrn=entity_urn,
                    aspect=GlobalTagsClass(tags=current),
                    systemMetadata=SystemMetadataClass(
                        lastObserved=time.time_ns() // 1_000_000,
                        properties={},
                    ),
                )
            )
        return {"data": {"batchAddTags": True}}

    return {
        "errors": [
            {"message": f"Lite bridge does not implement GraphQL operation {operation}"}
        ]
    }


async def config_route(request: Request) -> JSONResponse:
    del request
    return JSONResponse(config())


async def aspect_route(request: Request) -> JSONResponse:
    try:
        payload = get_aspect(
            request.path_params["urn"],
            request.query_params["aspect"],
            int(request.query_params.get("version", "0")),
        )
    except (KeyError, LookupError) as error:
        return JSONResponse({"detail": str(error)}, status_code=404)
    return JSONResponse(payload)


async def graphql_route(request: Request) -> JSONResponse:
    return JSONResponse(await graphql_payload(request))


app = Starlette(
    routes=[
        Route("/config", config_route, methods=["GET"]),
        Route("/aspects/{urn:path}", aspect_route, methods=["GET"]),
        Route("/api/graphql", graphql_route, methods=["POST"]),
    ]
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8979)
    args = parser.parse_args()
    os.environ["DATAHUB_LITE_FILE"] = os.path.abspath(args.file)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
