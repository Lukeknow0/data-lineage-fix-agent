from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from .models import DataHubContext, SchemaField


VERIFIED_TAG_URN = "urn:li:tag:DataLineageFixVerified"


def _json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _decode_tool_result(result: Any) -> Any:
    if getattr(result, "isError", False):
        message = "\n".join(
            getattr(content, "text", str(content)) for content in result.content
        )
        raise RuntimeError(f"DataHub MCP tool failed: {message}")

    structured = getattr(result, "structuredContent", None)
    if structured is not None:
        if isinstance(structured, dict) and set(structured) == {"result"}:
            return structured["result"]
        return structured

    texts = [
        getattr(content, "text", "")
        for content in getattr(result, "content", [])
        if getattr(content, "text", "")
    ]
    text = "\n".join(texts)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"text": text}


def _custom_properties(entity: dict[str, Any]) -> dict[str, str]:
    raw = (entity.get("properties") or {}).get("customProperties") or []
    if isinstance(raw, dict):
        return {str(key): str(value) for key, value in raw.items()}
    return {
        str(item["key"]): str(item["value"])
        for item in raw
        if isinstance(item, dict) and "key" in item and "value" in item
    }


def _owner_urns(entity: dict[str, Any]) -> list[str]:
    owners = (entity.get("ownership") or {}).get("owners") or []
    values = []
    for association in owners:
        owner = association.get("owner") or {}
        if owner.get("urn"):
            values.append(owner["urn"])
    return sorted(set(values))


def _tag_urns(entity: dict[str, Any]) -> list[str]:
    tags = (entity.get("tags") or {}).get("tags") or []
    values = []
    for association in tags:
        tag = association.get("tag") or {}
        if tag.get("urn"):
            values.append(tag["urn"])
    return sorted(set(values))


class LiveDataHubContextProvider:
    """Reads a local/remote DataHub instance through the official MCP server."""

    def __init__(self, gms_url: str):
        self.gms_url = gms_url.rstrip("/")

    def _server_parameters(self) -> StdioServerParameters:
        executable = Path(sys.executable).parent / "mcp-server-datahub"
        if not executable.exists():
            raise RuntimeError("mcp-server-datahub executable is not installed in this environment")
        forwarded_keys = (
            "PATH",
            "LANG",
            "LC_ALL",
            "TMPDIR",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "DATAHUB_GMS_TOKEN",
        )
        env = {
            key: os.environ[key]
            for key in forwarded_keys
            if os.environ.get(key)
        }
        env.update(
            {
                "DATAHUB_GMS_URL": self.gms_url,
                "DATAHUB_TELEMETRY_ENABLED": "false",
                "LOGURU_LEVEL": "WARNING",
                "TOOLS_IS_MUTATION_ENABLED": "true",
                "DATAHUB_MCP_DOCUMENT_TOOLS_DISABLED": "true",
            }
        )
        return StdioServerParameters(command=str(executable), args=[], env=env)

    async def call_tools(
        self, calls: list[tuple[str, dict[str, Any]]]
    ) -> list[tuple[Any, dict[str, Any]]]:
        traces: list[tuple[Any, dict[str, Any]]] = []
        async with stdio_client(self._server_parameters()) as (read, write):
            async with ClientSession(read, write) as session:
                initialized = await session.initialize()
                available = {tool.name for tool in (await session.list_tools()).tools}
                missing = {name for name, _ in calls} - available
                if missing:
                    raise RuntimeError(f"DataHub MCP server did not expose tools: {sorted(missing)}")
                server_info = initialized.serverInfo
                for name, arguments in calls:
                    result = await session.call_tool(name, arguments=arguments)
                    payload = _decode_tool_result(result)
                    serialized = _json_text(payload)
                    trace = {
                        "tool": name,
                        "arguments": arguments,
                        "mcp_server": {
                            "name": server_info.name,
                            "version": server_info.version,
                            "package": "mcp-server-datahub==0.6.0",
                        },
                        "result_sha256": hashlib.sha256(serialized.encode()).hexdigest(),
                        "result": payload,
                    }
                    traces.append((payload, trace))
        return traces

    async def load(self, source_urn: str, target_urn: str) -> DataHubContext:
        responses = await self.call_tools(
            [
                ("get_entities", {"urns": [source_urn, target_urn]}),
                ("list_schema_fields", {"urn": source_urn, "limit": 100, "offset": 0}),
                (
                    "get_lineage",
                    {
                        "urn": source_urn,
                        "upstream": False,
                        "max_hops": 1,
                        "max_results": 30,
                        "offset": 0,
                    },
                ),
            ]
        )
        entities_payload, schema_payload, lineage_payload = [item[0] for item in responses]
        traces = [item[1] for item in responses]

        entities = entities_payload if isinstance(entities_payload, list) else [entities_payload]
        by_urn = {
            entity.get("urn"): entity
            for entity in entities
            if isinstance(entity, dict) and entity.get("urn")
        }
        if source_urn not in by_urn or target_urn not in by_urn:
            raise RuntimeError("MCP get_entities did not return both requested DataHub entities")

        fields = [
            SchemaField(
                name=field["fieldPath"],
                native_type=field.get("nativeDataType", "UNKNOWN"),
                description=field.get("description") or field.get("editedDescription") or "",
            )
            for field in schema_payload.get("fields", [])
            if field.get("fieldPath")
        ]
        downstream_results = (
            (lineage_payload.get("downstreams") or {}).get("searchResults") or []
        )
        downstream_urns = sorted(
            {
                result["entity"]["urn"]
                for result in downstream_results
                if (result.get("entity") or {}).get("urn")
            }
        )
        source = by_urn[source_urn]
        target = by_urn[target_urn]
        properties = _custom_properties(source)
        signals = _tag_urns(source)
        signals.extend(
            f"{key}={value}"
            for key, value in properties.items()
            if key in {"schema_contract", "quality_signal"}
        )

        return DataHubContext(
            source_urn=source_urn,
            target_urn=target_urn,
            source_fields=fields,
            downstream_urns=downstream_urns,
            owner_urns=_owner_urns(target) or _owner_urns(source),
            quality_signals=sorted(set(signals)),
            transport=(
                os.getenv("DATALINEAGE_DATAHUB_BACKEND", "datahub-oss-v1.6.0")
                + "+official-mcp-server-v0.6.0"
            ),
            tool_traces=traces,
            source_properties=properties,
        )


class LiveDataHubStatusWriter:
    def __init__(self, gms_url: str, provider: LiveDataHubContextProvider):
        self.gms_url = gms_url
        self.provider = provider

    async def write_verified(self, entity_urn: str, run_id: str) -> dict:
        responses = await self.provider.call_tools(
            [
                (
                    "add_tags",
                    {"tag_urns": [VERIFIED_TAG_URN], "entity_urns": [entity_urn]},
                ),
                ("get_entities", {"urns": entity_urn}),
            ]
        )
        mutation, verification = responses[0][0], responses[1][0]
        mutation_ok = bool(mutation.get("success")) if isinstance(mutation, dict) else False
        readback = _json_text(verification)
        if not mutation_ok or VERIFIED_TAG_URN not in readback:
            raise RuntimeError("DataHub write-back could not be verified by MCP read-back")
        return {
            "status": "written-and-read-back-via-datahub-mcp",
            "entity_urn": entity_urn,
            "tag_urn": VERIFIED_TAG_URN,
            "run_id": run_id,
            "verification": "get_entities returned the verified tag after add_tags",
            "mutation": mutation,
            "readback_sha256": hashlib.sha256(readback.encode()).hexdigest(),
            "tool_traces": [responses[0][1], responses[1][1]],
        }
