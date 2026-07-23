from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Protocol, Sequence
from urllib.parse import urlsplit

from datahub.emitter.mce_builder import make_schema_field_urn
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


OFFICIAL_MCP_PACKAGE = "mcp-server-datahub==0.6.0"
LINEAGETX_TAG_URN = "urn:li:tag:LineageTXMigration"


class DataHubContextReadError(RuntimeError):
    """Raised when DataHub cannot prove that the impact set is complete."""


class ImpactFingerprintChanged(DataHubContextReadError):
    """Raised when a pre-commit refresh no longer matches frozen discovery."""


class MCPToolCaller(Protocol):
    """Small seam used by production MCP transport and deterministic unit fakes."""

    transport: str

    async def call_tools(
        self, calls: Sequence[tuple[str, Mapping[str, Any]]]
    ) -> list[tuple[Any, dict[str, Any]]]: ...


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def normalize_datahub_gms_url(gms_url: str) -> str:
    """Validate a credential-free GMS origin and return its canonical origin."""

    try:
        parsed = urlsplit(gms_url)
        port = parsed.port
    except ValueError as error:
        raise ValueError("gms_url contains an invalid host or port") from error
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.netloc or not parsed.hostname:
        raise ValueError("gms_url must be an absolute HTTP(S) origin")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("gms_url must not contain userinfo or credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("gms_url must not contain a query or fragment")
    if parsed.path not in {"", "/"}:
        raise ValueError("gms_url must be an origin without a path")
    if port == 8979:
        raise ValueError(
            "LineageTX requires full DataHub OSS; port 8979 is reserved for "
            "the legacy compatibility bridge"
        )
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    return f"{scheme}://{host}" + (f":{port}" if port is not None else "")


def _degree_sort_key(item: tuple[str, tuple[str, set[str]]]) -> tuple[int, str]:
    urn, (degree, _) = item
    numeric = "".join(character for character in degree if character.isdigit())
    return (int(numeric) if numeric else 1_000_000, urn)


def _decode_tool_result(result: Any) -> Any:
    if getattr(result, "isError", False):
        detail = "\n".join(
            getattr(item, "text", str(item))
            for item in getattr(result, "content", [])
        )
        raise DataHubContextReadError(f"DataHub MCP tool failed: {detail}")

    structured = getattr(result, "structuredContent", None)
    if structured is not None:
        if isinstance(structured, dict) and set(structured) == {"result"}:
            return structured["result"]
        return structured

    text = "\n".join(
        getattr(item, "text", "")
        for item in getattr(result, "content", [])
        if getattr(item, "text", "")
    )
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"text": text}


class OfficialDataHubMCPClient:
    """Official MCP stdio client configured for a full DataHub OSS GMS.

    LineageTX deliberately rejects the legacy local compatibility bridge port. The
    caller must point this client at a real DataHub OSS GMS (normally port 8080).
    """

    transport = f"datahub-oss+official-{OFFICIAL_MCP_PACKAGE}"
    is_live_official_transport = True

    def __init__(
        self,
        gms_url: str,
        executable: Path | None = None,
        *,
        operation_timeout_seconds: float = 30.0,
    ):
        if operation_timeout_seconds <= 0:
            raise ValueError("operation_timeout_seconds must be positive")
        self.gms_url = normalize_datahub_gms_url(gms_url)
        self.executable = executable or Path(sys.executable).parent / "mcp-server-datahub"
        self.operation_timeout_seconds = operation_timeout_seconds

    async def _bounded(self, operation: str, awaitable: Any) -> Any:
        try:
            return await asyncio.wait_for(
                awaitable,
                timeout=self.operation_timeout_seconds,
            )
        except TimeoutError as error:
            raise DataHubContextReadError(
                f"official DataHub MCP {operation} timed out after "
                f"{self.operation_timeout_seconds:g}s"
            ) from error

    def _server_parameters(self) -> StdioServerParameters:
        if not self.executable.exists():
            raise RuntimeError(
                "mcp-server-datahub is not installed in the active Python environment"
            )
        forwarded = (
            "PATH",
            "LANG",
            "LC_ALL",
            "TMPDIR",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "DATAHUB_GMS_TOKEN",
        )
        env = {key: os.environ[key] for key in forwarded if os.environ.get(key)}
        env.update(
            {
                "DATAHUB_GMS_URL": self.gms_url,
                "DATAHUB_TELEMETRY_ENABLED": "false",
                "LOGURU_LEVEL": "WARNING",
                "TOOLS_IS_MUTATION_ENABLED": "true",
            }
        )
        return StdioServerParameters(
            command=str(self.executable),
            args=[],
            env=env,
        )

    async def call_tools(
        self, calls: Sequence[tuple[str, Mapping[str, Any]]]
    ) -> list[tuple[Any, dict[str, Any]]]:
        traces: list[tuple[Any, dict[str, Any]]] = []
        async with stdio_client(self._server_parameters()) as (read, write):
            async with ClientSession(read, write) as session:
                initialized = await self._bounded("initialize", session.initialize())
                listed = await self._bounded("list_tools", session.list_tools())
                available = {tool.name for tool in listed.tools}
                missing = {name for name, _ in calls} - available
                if missing:
                    raise DataHubContextReadError(
                        f"official DataHub MCP server did not expose {sorted(missing)}"
                    )
                for name, raw_arguments in calls:
                    arguments = dict(raw_arguments)
                    result = await self._bounded(
                        f"call_tool:{name}",
                        session.call_tool(name, arguments=arguments),
                    )
                    payload = _decode_tool_result(result)
                    traces.append(
                        (
                            payload,
                            {
                                "tool": name,
                                "arguments": arguments,
                                "transport": self.transport,
                                "mcp_server": {
                                    "name": initialized.serverInfo.name,
                                    "version": initialized.serverInfo.version,
                                    "package": OFFICIAL_MCP_PACKAGE,
                                },
                            },
                        )
                    )
        return traces


@dataclass(frozen=True)
class SchemaFieldSnapshot:
    field_path: str
    native_type: str
    nullable: bool | None
    description: str


@dataclass(frozen=True)
class GovernanceSnapshot:
    owner_urns: tuple[str, ...]
    tag_urns: tuple[str, ...]
    structured_properties: Mapping[str, tuple[str | float, ...]]


@dataclass(frozen=True)
class AssetSnapshot:
    urn: str
    schema: tuple[SchemaFieldSnapshot, ...]
    governance: GovernanceSnapshot


@dataclass(frozen=True)
class ConsumerLineage:
    urn: str
    degree: str
    columns: tuple[str, ...]
    path_evidence: tuple[Mapping[str, Any], ...]


def _path_signatures(
    evidence: Sequence[Mapping[str, Any]],
) -> list[list[dict[str, str]]]:
    signatures: list[list[dict[str, str]]] = []
    for result in evidence:
        for path_object in result.get("paths") or []:
            path = path_object.get("path") if isinstance(path_object, dict) else None
            if not isinstance(path, list):
                continue
            signatures.append(
                [
                    {
                        "type": str(item.get("type") or ""),
                        "urn": str(item.get("urn") or ""),
                    }
                    for item in path
                    if isinstance(item, dict)
                ]
            )
    return sorted(signatures, key=_stable_json)


def compute_impact_fingerprint(
    *,
    source_urn: str,
    source_column: str,
    replacement_column: str,
    consumers: Sequence[ConsumerLineage],
    assets: Mapping[str, AssetSnapshot],
) -> str:
    """Hash the validated impact set while excluding LineageTX's own write-back."""

    asset_payload: list[dict[str, Any]] = []
    for urn in sorted(assets):
        asset = assets[urn]
        governance = asset.governance
        properties = {
            key: list(values)
            for key, values in sorted(governance.structured_properties.items())
            if "io.lineagetx." not in key
        }
        asset_payload.append(
            {
                "urn": urn,
                "schema": [
                    {
                        "description": field.description,
                        "field_path": field.field_path,
                        "native_type": field.native_type,
                        "nullable": field.nullable,
                    }
                    for field in sorted(asset.schema, key=lambda item: item.field_path)
                ],
                "owners": list(governance.owner_urns),
                "tags": [
                    tag for tag in governance.tag_urns if tag != LINEAGETX_TAG_URN
                ],
                "structured_properties": properties,
            }
        )
    payload = {
        "version": 1,
        "source_urn": source_urn,
        "source_column": source_column,
        "replacement_column": replacement_column,
        "assets": asset_payload,
        "consumers": [
            {
                "urn": consumer.urn,
                "degree": consumer.degree,
                "columns": list(consumer.columns),
                "paths": _path_signatures(consumer.path_evidence),
            }
            for consumer in sorted(
                consumers,
                key=lambda item: _degree_sort_key(
                    (item.urn, (item.degree, set(item.columns)))
                ),
            )
        ],
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DataHubMigrationContext:
    source_urn: str
    source_column: str
    replacement_column: str
    source: AssetSnapshot
    consumers: tuple[ConsumerLineage, ...]
    assets: Mapping[str, AssetSnapshot]
    tool_traces: tuple[Mapping[str, Any], ...]
    transport: str
    discovery_complete: bool = False
    impact_fingerprint: str = ""

    def __post_init__(self) -> None:
        expected_assets = (self.source_urn, *(item.urn for item in self.consumers))
        if len(expected_assets) != 4 or set(expected_assets) != set(self.assets):
            raise ValueError("frozen context must contain one source and exactly three consumers")
        if self.source != self.assets[self.source_urn]:
            raise ValueError("source snapshot must match the frozen assets mapping")
        if self.discovery_complete:
            degrees = [item.degree for item in self.consumers]
            if len(set(degrees)) != 3 or set(degrees) != {"1", "2", "3+"}:
                raise ValueError("complete discovery requires unique degrees 1, 2, and 3+")
            for consumer in self.consumers:
                if len(self.assets[consumer.urn].governance.owner_urns) != 1:
                    raise ValueError(
                        "complete discovery requires exactly one owner per consumer"
                    )
        computed = self.recompute_impact_fingerprint()
        if self.impact_fingerprint and computed != self.impact_fingerprint:
            raise ValueError("impact_fingerprint does not match the frozen DataHub context")
        object.__setattr__(self, "impact_fingerprint", computed)

    @property
    def asset_urns(self) -> tuple[str, ...]:
        return (self.source_urn, *(item.urn for item in self.consumers))

    def recompute_impact_fingerprint(self) -> str:
        return compute_impact_fingerprint(
            source_urn=self.source_urn,
            source_column=self.source_column,
            replacement_column=self.replacement_column,
            consumers=self.consumers,
            assets=self.assets,
        )


def _association_urn(value: Any, key: str) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        nested = value.get(key)
        if isinstance(nested, str):
            return nested
        if isinstance(nested, dict) and isinstance(nested.get("urn"), str):
            return nested["urn"]
        if isinstance(value.get("urn"), str):
            return value["urn"]
    return None


def extract_governance(entity: Mapping[str, Any]) -> GovernanceSnapshot:
    owners_raw = ((entity.get("ownership") or {}).get("owners") or [])
    tags_raw = ((entity.get("tags") or {}).get("tags") or [])
    owners = sorted(
        {
            urn
            for item in owners_raw
            if (urn := _association_urn(item, "owner")) is not None
        }
    )
    tags = sorted(
        {
            urn
            for item in tags_raw
            if (urn := _association_urn(item, "tag")) is not None
        }
    )

    properties: dict[str, tuple[str | float, ...]] = {}
    entries = ((entity.get("structuredProperties") or {}).get("properties") or [])
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        definition = entry.get("structuredProperty") or {}
        property_urn = definition.get("urn") if isinstance(definition, dict) else None
        property_definition = (
            definition.get("definition") if isinstance(definition, dict) else None
        ) or {}
        qualified_name = property_definition.get("qualifiedName")
        key = qualified_name or property_urn
        if not isinstance(key, str):
            continue
        values: list[str | float] = []
        for value in entry.get("values") or []:
            if not isinstance(value, dict):
                continue
            if "stringValue" in value:
                values.append(str(value["stringValue"]))
            elif "numberValue" in value:
                values.append(float(value["numberValue"]))
        for value_entity in entry.get("valueEntities") or []:
            if isinstance(value_entity, dict) and isinstance(value_entity.get("urn"), str):
                values.append(value_entity["urn"])
        canonical_values = tuple(dict.fromkeys(values))
        properties[key] = canonical_values
        if isinstance(property_urn, str):
            properties[property_urn] = canonical_values

    return GovernanceSnapshot(
        owner_urns=tuple(owners),
        tag_urns=tuple(tags),
        structured_properties=MappingProxyType(properties),
    )


class DataHubMCPContextReader:
    """Reads a complete, column-level migration impact set and fails closed."""

    def __init__(
        self,
        tool_client: MCPToolCaller,
        *,
        page_size: int = 100,
        max_pages: int = 100,
    ):
        if page_size < 1:
            raise ValueError("page_size must be positive")
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        self.tool_client = tool_client
        self.page_size = page_size
        self.max_pages = max_pages

    async def _call_one(
        self,
        name: str,
        arguments: Mapping[str, Any],
        traces: list[Mapping[str, Any]],
    ) -> Any:
        response = await self.tool_client.call_tools([(name, arguments)])
        if len(response) != 1:
            raise DataHubContextReadError(
                f"MCP {name} returned {len(response)} envelopes; expected one"
            )
        payload, trace = response[0]
        traces.append(trace)
        return payload

    async def _schema(
        self,
        urn: str,
        traces: list[Mapping[str, Any]],
    ) -> tuple[SchemaFieldSnapshot, ...]:
        offset = 0
        expected_total: int | None = None
        fields: list[SchemaFieldSnapshot] = []
        for _ in range(self.max_pages):
            payload = await self._call_one(
                "list_schema_fields",
                {"urn": urn, "limit": self.page_size, "offset": offset},
                traces,
            )
            if not isinstance(payload, dict):
                raise DataHubContextReadError("list_schema_fields returned a non-object")
            raw_fields = payload.get("fields")
            total = payload.get("totalFields")
            returned = payload.get("returned")
            remaining = payload.get("remainingCount")
            returned_offset = payload.get("offset")
            if (
                not isinstance(raw_fields, list)
                or not isinstance(total, int)
                or not isinstance(returned, int)
                or not isinstance(remaining, int)
                or returned_offset != offset
                or returned != len(raw_fields)
                or total < 0
                or remaining < 0
            ):
                raise DataHubContextReadError(
                    "list_schema_fields pagination metadata was incomplete or inconsistent"
                )
            if expected_total is None:
                expected_total = total
            elif total != expected_total:
                raise DataHubContextReadError("schema changed while it was being paged")

            for raw in raw_fields:
                if not isinstance(raw, dict) or not isinstance(raw.get("fieldPath"), str):
                    raise DataHubContextReadError("schema page contained an invalid field")
                fields.append(
                    SchemaFieldSnapshot(
                        field_path=raw["fieldPath"],
                        native_type=str(raw.get("nativeDataType") or "UNKNOWN"),
                        nullable=(
                            raw.get("nullable")
                            if isinstance(raw.get("nullable"), bool)
                            else None
                        ),
                        description=str(
                            raw.get("description") or raw.get("editedDescription") or ""
                        ),
                    )
                )

            offset += returned
            if remaining == 0:
                if expected_total != len(fields):
                    raise DataHubContextReadError(
                        "list_schema_fields claimed completion before all fields arrived"
                    )
                names = [item.field_path for item in fields]
                if len(names) != len(set(names)):
                    raise DataHubContextReadError("schema pagination returned duplicate fields")
                return tuple(fields)
            if returned == 0 or offset >= total:
                raise DataHubContextReadError("schema pagination made no forward progress")

        raise DataHubContextReadError("schema pagination exceeded the safety page limit")

    async def _column_lineage(
        self,
        source_urn: str,
        source_column: str,
        traces: list[Mapping[str, Any]],
    ) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
        offset = 0
        expected_total: int | None = None
        merged: dict[str, tuple[str, set[str]]] = {}
        for _ in range(self.max_pages):
            # The official MCP implementation applies offset after fetching
            # max_results from GraphQL. Fetch cumulatively so page N can reach
            # results beyond the previous offset.
            cumulative_limit = offset + self.page_size
            payload = await self._call_one(
                "get_lineage",
                {
                    "urn": source_urn,
                    "column": source_column,
                    "upstream": False,
                    "max_hops": 3,
                    "max_results": cumulative_limit,
                    "offset": offset,
                },
                traces,
            )
            if not isinstance(payload, dict):
                raise DataHubContextReadError("get_lineage returned a non-object")
            direction = payload.get("downstreams")
            if not isinstance(direction, dict):
                raise DataHubContextReadError(
                    "column-level get_lineage omitted downstream results"
                )
            results = direction.get("searchResults")
            total = direction.get("total")
            returned = direction.get("returned")
            has_more = direction.get("hasMore")
            returned_offset = direction.get("offset")
            if (
                offset == 0
                and total == 0
                and results in (None, [])
                and returned in (None, 0)
                and has_more in (None, False)
                and returned_offset in (None, 0)
            ):
                return await self._dataset_lineage(
                    source_urn,
                    source_column,
                    traces,
                )
            if (
                not isinstance(results, list)
                or not isinstance(total, int)
                or total < 0
                or not isinstance(returned, int)
                or not isinstance(has_more, bool)
                or returned_offset != offset
                or returned != len(results)
            ):
                raise DataHubContextReadError(
                    "get_lineage pagination metadata was incomplete or inconsistent"
                )
            if expected_total is None:
                expected_total = total
            elif total != expected_total:
                raise DataHubContextReadError("lineage changed while it was being paged")

            for item in results:
                if not isinstance(item, dict):
                    raise DataHubContextReadError("lineage page contained an invalid result")
                entity = item.get("entity") or {}
                urn = entity.get("urn") if isinstance(entity, dict) else None
                columns = item.get("lineageColumns")
                if not isinstance(urn, str) or not isinstance(columns, list) or not columns:
                    raise DataHubContextReadError(
                        "column-level lineage result omitted its entity or lineageColumns"
                    )
                if not all(isinstance(column, str) and column for column in columns):
                    raise DataHubContextReadError("lineageColumns contained an invalid value")
                degree = str(item.get("degree") or "unknown")
                if degree == "3":
                    degree = "3+"
                if urn in merged:
                    previous_degree, known_columns = merged[urn]
                    if previous_degree != degree:
                        raise DataHubContextReadError(
                            f"lineage degree changed between pages for {urn}"
                        )
                    known_columns.update(columns)
                else:
                    merged[urn] = (degree, set(columns))

            offset += returned
            authoritative_has_more = offset < total
            if not authoritative_has_more:
                if offset != total:
                    raise DataHubContextReadError(
                        "get_lineage returned more results than its total"
                    )
                if not merged:
                    raise DataHubContextReadError(
                        f"DataHub found no downstream lineage for {source_column}"
                    )
                return tuple(
                    (urn, degree, tuple(sorted(columns)))
                    for urn, (degree, columns) in sorted(
                        merged.items(), key=_degree_sort_key
                    )
                )
            if returned == 0:
                raise DataHubContextReadError("lineage pagination made no forward progress")
            if not has_more and authoritative_has_more:
                # mcp-server-datahub 0.6.0 derives hasMore from the fetched
                # window. Its authoritative GraphQL total still proves more
                # results exist, so continue with a cumulative fetch window.
                continue

        raise DataHubContextReadError("lineage pagination exceeded the safety page limit")

    async def _dataset_lineage(
        self,
        source_urn: str,
        source_column: str,
        traces: list[Mapping[str, Any]],
    ) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
        """Discover v1.6 downstream datasets before proving each column path."""

        offset = 0
        expected_total: int | None = None
        discovered: list[tuple[str, str, tuple[str, ...]]] = []
        for _ in range(self.max_pages):
            cumulative_limit = offset + self.page_size
            payload = await self._call_one(
                "get_lineage",
                {
                    "urn": source_urn,
                    "upstream": False,
                    "max_hops": 3,
                    "max_results": cumulative_limit,
                    "offset": offset,
                },
                traces,
            )
            direction = payload.get("downstreams") if isinstance(payload, dict) else None
            if not isinstance(direction, dict):
                raise DataHubContextReadError(
                    "dataset-level get_lineage omitted downstream results"
                )
            results = direction.get("searchResults")
            total = direction.get("total")
            returned = direction.get("returned")
            has_more = direction.get("hasMore")
            returned_offset = direction.get("offset")
            if (
                not isinstance(results, list)
                or not isinstance(total, int)
                or total < 0
                or not isinstance(returned, int)
                or not isinstance(has_more, bool)
                or returned_offset != offset
                or returned != len(results)
            ):
                raise DataHubContextReadError(
                    "dataset-level lineage pagination metadata was incomplete or inconsistent"
                )
            if expected_total is None:
                expected_total = total
            elif total != expected_total:
                raise DataHubContextReadError(
                    "dataset lineage changed while it was being paged"
                )

            for item in results:
                entity = item.get("entity") if isinstance(item, dict) else None
                urn = entity.get("urn") if isinstance(entity, dict) else None
                raw_degree = item.get("degree") if isinstance(item, dict) else None
                if not isinstance(urn, str) or not urn or raw_degree is None:
                    raise DataHubContextReadError(
                        "dataset lineage page contained an invalid result"
                    )
                degree = str(raw_degree)
                if degree == "3":
                    degree = "3+"
                discovered.append((urn, degree, (source_column,)))

            offset += returned
            if offset >= total:
                if offset != total or not discovered:
                    raise DataHubContextReadError(
                        f"DataHub found no downstream lineage for {source_column}"
                    )
                urns = [urn for urn, _, _ in discovered]
                if len(urns) != len(set(urns)):
                    raise DataHubContextReadError(
                        "dataset lineage pagination returned duplicate assets"
                    )
                return tuple(discovered)
            if returned == 0:
                raise DataHubContextReadError(
                    "dataset lineage pagination made no forward progress"
                )
            if not has_more and offset < total:
                continue

        raise DataHubContextReadError(
            "dataset lineage pagination exceeded the safety page limit"
        )

    @staticmethod
    def _validate_path_evidence(
        path: Any,
        *,
        source_urn: str,
        source_column: str,
        target_urn: str,
        target_column: str,
    ) -> Mapping[str, Any]:
        if not isinstance(path, dict):
            raise DataHubContextReadError("lineage path response was not an object")
        source = path.get("source")
        target = path.get("target")
        metadata = path.get("metadata")
        paths = path.get("paths")
        path_count = path.get("pathCount")
        if (
            not isinstance(source, dict)
            or source.get("urn") != source_urn
            or source.get("column") != source_column
            or not isinstance(target, dict)
            or target.get("urn") != target_urn
            or target.get("column") != target_column
            or not isinstance(metadata, dict)
            or metadata.get("direction") != "downstream"
            or not isinstance(paths, list)
            or not isinstance(path_count, int)
            or path_count != len(paths)
            or path_count < 1
        ):
            raise DataHubContextReadError(
                f"lineage path metadata did not match {source_urn}.{source_column} "
                f"-> {target_urn}.{target_column}"
            )

        expected_start = make_schema_field_urn(source_urn, source_column)
        expected_end = make_schema_field_urn(target_urn, target_column)
        for path_object in paths:
            entities = (
                path_object.get("path")
                if isinstance(path_object, dict)
                else None
            )
            if not isinstance(entities, list) or len(entities) < 2:
                raise DataHubContextReadError("lineage path was empty or malformed")
            first = entities[0]
            last = entities[-1]
            if (
                not isinstance(first, dict)
                or first.get("type") != "SCHEMA_FIELD"
                or first.get("urn") != expected_start
                or not isinstance(last, dict)
                or last.get("type") != "SCHEMA_FIELD"
                or last.get("urn") != expected_end
            ):
                raise DataHubContextReadError(
                    "lineage path endpoints were not the requested schemaField URNs"
                )
        return path

    async def load(
        self,
        source_urn: str,
        source_column: str = "customer_id",
        replacement_column: str = "customer_key",
    ) -> DataHubMigrationContext:
        traces: list[Mapping[str, Any]] = []
        source_schema = await self._schema(source_urn, traces)
        if source_column not in {field.field_path for field in source_schema}:
            raise DataHubContextReadError(
                f"source schema does not contain migration column {source_column}"
            )
        if replacement_column not in {field.field_path for field in source_schema}:
            raise DataHubContextReadError(
                f"source schema does not contain replacement column {replacement_column}"
            )

        lineage = await self._column_lineage(source_urn, source_column, traces)
        degrees = [degree for _, degree, _ in lineage]
        if (
            len(lineage) != 3
            or len(set(degrees)) != 3
            or set(degrees) != {"1", "2", "3+"}
        ):
            raise DataHubContextReadError(
                "LineageTX requires exactly one consumer at each degree 1, 2, and 3+"
            )
        consumer_urns = [urn for urn, _, _ in lineage]
        if len(set(consumer_urns)) != 3:
            raise DataHubContextReadError("consumer URNs must be unique")
        all_urns = [source_urn, *consumer_urns]
        entities_payload = await self._call_one(
            "get_entities", {"urns": all_urns}, traces
        )
        entities = (
            entities_payload
            if isinstance(entities_payload, list)
            else [entities_payload]
        )
        by_urn: dict[str, Mapping[str, Any]] = {}
        for entity in entities:
            if not isinstance(entity, dict) or entity.get("error"):
                raise DataHubContextReadError(
                    "get_entities failed for at least one impacted asset"
                )
            urn = entity.get("urn")
            if isinstance(urn, str):
                by_urn[urn] = entity
        if set(all_urns) != set(by_urn):
            missing = sorted(set(all_urns) - set(by_urn))
            raise DataHubContextReadError(f"get_entities omitted impacted assets: {missing}")

        schemas: dict[str, tuple[SchemaFieldSnapshot, ...]] = {source_urn: source_schema}
        for urn in consumer_urns:
            schemas[urn] = await self._schema(urn, traces)

        consumers: list[ConsumerLineage] = []
        for urn, degree, columns in lineage:
            schema_columns = {field.field_path for field in schemas[urn]}
            missing_columns = sorted(set(columns) - schema_columns)
            if missing_columns:
                raise DataHubContextReadError(
                    f"lineage referenced columns absent from {urn}: {missing_columns}"
                )
            path_evidence: list[Mapping[str, Any]] = []
            for target_column in columns:
                path = await self._call_one(
                    "get_lineage_paths_between",
                    {
                        "source_urn": source_urn,
                        "target_urn": urn,
                        "source_column": source_column,
                        "target_column": target_column,
                        "direction": "downstream",
                    },
                    traces,
                )
                path_evidence.append(
                    self._validate_path_evidence(
                        path,
                        source_urn=source_urn,
                        source_column=source_column,
                        target_urn=urn,
                        target_column=target_column,
                    )
                )
            consumers.append(
                ConsumerLineage(
                    urn=urn,
                    degree=degree,
                    columns=columns,
                    path_evidence=tuple(path_evidence),
                )
            )

        assets_dict = {
            urn: AssetSnapshot(
                urn=urn,
                schema=schemas[urn],
                governance=extract_governance(by_urn[urn]),
            )
            for urn in all_urns
        }
        for urn in consumer_urns:
            owners = assets_dict[urn].governance.owner_urns
            if len(owners) != 1:
                raise DataHubContextReadError(
                    f"consumer {urn} must have exactly one accountable DataHub owner"
                )
        assets = MappingProxyType(assets_dict)
        fingerprint = compute_impact_fingerprint(
            source_urn=source_urn,
            source_column=source_column,
            replacement_column=replacement_column,
            consumers=consumers,
            assets=assets,
        )
        return DataHubMigrationContext(
            source_urn=source_urn,
            source_column=source_column,
            replacement_column=replacement_column,
            source=assets[source_urn],
            consumers=tuple(consumers),
            assets=assets,
            tool_traces=tuple(traces),
            transport=getattr(self.tool_client, "transport", "test-double"),
            discovery_complete=True,
            impact_fingerprint=fingerprint,
        )

    async def refresh(
        self,
        frozen: DataHubMigrationContext,
    ) -> DataHubMigrationContext:
        """Re-read DataHub immediately before commit and return a new frozen view."""

        return await self.load(
            frozen.source_urn,
            frozen.source_column,
            frozen.replacement_column,
        )

    async def refresh_fingerprint(self, frozen: DataHubMigrationContext) -> str:
        return (await self.refresh(frozen)).impact_fingerprint

    async def assert_impact_unchanged(
        self,
        frozen: DataHubMigrationContext,
    ) -> DataHubMigrationContext:
        refreshed = await self.refresh(frozen)
        if refreshed.impact_fingerprint != frozen.impact_fingerprint:
            raise ImpactFingerprintChanged(
                "DataHub impact changed after prepare; commit must be aborted and rediscovered"
            )
        return refreshed
