from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from .datahub_context import (
    DataHubMigrationContext,
    MCPToolCaller,
    OfficialDataHubMCPClient,
    extract_governance,
)
from .models import require_https_url


MIGRATION_ID_PROPERTY = "urn:li:structuredProperty:io.lineagetx.migrationId"
MIGRATION_STATUS_PROPERTY = "urn:li:structuredProperty:io.lineagetx.status"
MIGRATION_OWNER_PROPERTY = "urn:li:structuredProperty:io.lineagetx.owner"
MIGRATION_EVIDENCE_PROPERTY = "urn:li:structuredProperty:io.lineagetx.evidenceUrl"
MIGRATION_TAG = "urn:li:tag:LineageTXMigration"

PROPERTY_QUALIFIED_NAMES = {
    MIGRATION_ID_PROPERTY: "io.lineagetx.migrationId",
    MIGRATION_STATUS_PROPERTY: "io.lineagetx.status",
    MIGRATION_OWNER_PROPERTY: "io.lineagetx.owner",
    MIGRATION_EVIDENCE_PROPERTY: "io.lineagetx.evidenceUrl",
}
READBACK_ATTEMPTS = 10
LIVE_READBACK_RETRY_SECONDS = 0.5

COORDINATOR_STATES = {
    "DETECTED",
    "PREPARING",
    "NEEDS_APPROVAL",
    "PREPARED",
    "COMMITTED",
    "ABORTED",
}
PARTICIPANT_STATES = {
    "DISCOVERED",
    "VERIFIED",
    "FAILED",
}
# DataHub stores both the coordinator state and each participant's validation
# state in the same structured property. Keep the union as the backwards-
# compatible public validation set without expanding the coordinator machine.
MIGRATION_STATES = COORDINATOR_STATES | PARTICIPANT_STATES


class DataHubWritebackError(RuntimeError):
    """Raised when mutation or read-back cannot prove the DataHub write-back."""


class DataHubPartialWriteError(DataHubWritebackError):
    """A write failed after at least one idempotent operation was attempted."""

    def __init__(
        self,
        *,
        failed_operation: str,
        attempted_assets: Sequence[str],
        successful_assets: Sequence[str],
        property_successful_assets: Sequence[str],
        journal: Sequence[Mapping[str, Any]],
        transport: str,
        readback_sha256: str | None,
    ) -> None:
        self.failed_operation = failed_operation
        self.attempted_assets = tuple(attempted_assets)
        self.successful_assets = tuple(successful_assets)
        self.property_successful_assets = tuple(property_successful_assets)
        self.journal = tuple(journal)
        self.transport = transport
        self.readback_sha256 = readback_sha256
        super().__init__(
            f"DataHub partial write during {failed_operation}; fully successful assets: "
            f"{list(self.successful_assets)}. Operations are idempotent and may be retried."
        )


@dataclass(frozen=True)
class MigrationWriteback:
    migration_id: str
    status: str
    owner: str
    evidence_url: str

    def __post_init__(self) -> None:
        status = getattr(self.status, "value", self.status)
        if not isinstance(status, str):
            raise ValueError("status must be a LineageTX state string")
        object.__setattr__(self, "status", status)
        if not self.migration_id.strip():
            raise ValueError("migration_id cannot be empty")
        if self.status not in MIGRATION_STATES:
            raise ValueError(f"unsupported LineageTX status: {self.status}")
        if not self.owner.startswith("urn:li:"):
            raise ValueError("owner must be a DataHub URN")
        require_https_url(self.evidence_url, "evidence_url")

    @property
    def property_values(self) -> dict[str, list[str]]:
        return {
            MIGRATION_ID_PROPERTY: [self.migration_id],
            MIGRATION_STATUS_PROPERTY: [self.status],
            MIGRATION_OWNER_PROPERTY: [self.owner],
            MIGRATION_EVIDENCE_PROPERTY: [self.evidence_url],
        }


@dataclass(frozen=True)
class WritebackReceipt:
    migration_id: str
    status: str
    entity_urns: tuple[str, ...]
    tag_urn: str
    evidence_url: str
    readback_sha256: str
    tool_traces: tuple[Mapping[str, Any], ...]
    asset_statuses: Mapping[str, str]
    asset_owners: Mapping[str, str]
    asset_evidence_urls: Mapping[str, str]
    impact_fingerprint: str
    transport: str
    live_verified: bool
    journal: tuple[Mapping[str, Any], ...]
    verification: str


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


class DataHubMigrationWriter:
    """Idempotently writes state bound to one frozen four-asset impact set."""

    def __init__(
        self,
        tool_client: MCPToolCaller,
        context: DataHubMigrationContext,
    ) -> None:
        if context.discovery_complete is not True:
            raise ValueError("writer requires a discovery_complete DataHub context")
        if context.recompute_impact_fingerprint() != context.impact_fingerprint:
            raise ValueError("frozen DataHub impact fingerprint is no longer valid")
        if len(context.asset_urns) != 4 or len(set(context.asset_urns)) != 4:
            raise ValueError("writer requires the exact source plus three consumers")
        if getattr(tool_client, "transport", None) != context.transport:
            raise ValueError("writer transport must match the frozen discovery transport")
        self.tool_client = tool_client
        self.context = context
        self.entity_urns = context.asset_urns

    @staticmethod
    def _successful(payload: Any) -> bool:
        return isinstance(payload, dict) and payload.get("success") is True

    @staticmethod
    def _property_match(
        entity: Mapping[str, Any],
        writeback: MigrationWriteback,
    ) -> bool:
        governance = extract_governance(entity)
        for property_urn, expected_values in writeback.property_values.items():
            values = governance.structured_properties.get(property_urn)
            if values is None:
                values = governance.structured_properties.get(
                    PROPERTY_QUALIFIED_NAMES[property_urn]
                )
            if values != tuple(expected_values):
                return False
        return True

    @classmethod
    def _full_match(
        cls,
        entity: Mapping[str, Any],
        writeback: MigrationWriteback,
    ) -> bool:
        return (
            cls._property_match(entity, writeback)
            and MIGRATION_TAG in extract_governance(entity).tag_urns
        )

    @staticmethod
    def _journal_entry(
        sequence: int,
        operation: str,
        assets: Sequence[str],
        *,
        success: bool,
        error: BaseException | str | None = None,
    ) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "sequence": sequence,
            "operation": operation,
            "assets": list(assets),
            "success": success,
            "idempotent_retry_safe": True,
        }
        if error is not None:
            entry["error"] = type(error).__name__ if isinstance(error, BaseException) else error
        return entry

    async def _one_call(
        self,
        name: str,
        arguments: Mapping[str, Any],
    ) -> tuple[Any, Mapping[str, Any]]:
        responses = await self.tool_client.call_tools([(name, arguments)])
        if len(responses) != 1:
            raise DataHubWritebackError(
                f"DataHub MCP {name} returned {len(responses)} envelopes"
            )
        return responses[0]

    @staticmethod
    def _readback_entities(
        payload: Any,
        expected_urns: Sequence[str],
    ) -> dict[str, Mapping[str, Any]]:
        entities = payload if isinstance(payload, list) else [payload]
        by_urn = {
            entity.get("urn"): entity
            for entity in entities
            if isinstance(entity, dict) and isinstance(entity.get("urn"), str)
        }
        if set(by_urn) != set(expected_urns):
            missing = sorted(set(expected_urns) - set(by_urn))
            raise DataHubWritebackError(
                f"read-back omitted migration assets: {missing}"
            )
        return by_urn

    def _validate_refreshed_context(
        self,
        refreshed: DataHubMigrationContext | None,
    ) -> None:
        if refreshed is None:
            raise ValueError("COMMITTED write-back requires a pre-commit refreshed context")
        if refreshed is self.context:
            raise ValueError("pre-commit refresh must be a newly read DataHub context")
        if refreshed.discovery_complete is not True:
            raise ValueError("refreshed context discovery is incomplete")
        if refreshed.asset_urns != self.entity_urns:
            raise ValueError("refreshed context changed the exact impact assets")
        if refreshed.impact_fingerprint != self.context.impact_fingerprint:
            raise ValueError("refreshed context impact fingerprint changed")
        if refreshed.transport != self.context.transport:
            raise ValueError("refreshed context transport changed")

    async def write(
        self,
        writeback: MigrationWriteback,
        entity_urns: Sequence[str] | None = None,
        *,
        refreshed_context: DataHubMigrationContext | None = None,
    ) -> WritebackReceipt:
        supplied = tuple(entity_urns) if entity_urns is not None else self.entity_urns
        return await self.write_assets(
            {urn: writeback for urn in supplied},
            supplied_urns=supplied,
            refreshed_context=refreshed_context,
        )

    async def write_assets(
        self,
        writebacks: Mapping[str, MigrationWriteback],
        *,
        supplied_urns: Sequence[str] | None = None,
        refreshed_context: DataHubMigrationContext | None = None,
    ) -> WritebackReceipt:
        """Write per-asset values; a retry safely replays every upsert and tag."""

        if self.context.recompute_impact_fingerprint() != self.context.impact_fingerprint:
            raise ValueError("frozen DataHub context was mutated after discovery")
        input_urns = (
            tuple(supplied_urns)
            if supplied_urns is not None
            else tuple(writebacks)
        )
        if input_urns != self.entity_urns or set(writebacks) != set(self.entity_urns):
            raise ValueError("write-back assets must exactly match the frozen impact set")
        migration_ids = {record.migration_id for record in writebacks.values()}
        if len(migration_ids) != 1:
            raise ValueError("all asset write-backs must use the same migration_id")
        for urn in self.entity_urns:
            discovered_owners = self.context.assets[urn].governance.owner_urns
            if len(discovered_owners) != 1 or writebacks[urn].owner != discovered_owners[0]:
                raise ValueError(
                    f"write-back owner must match the frozen DataHub owner for {urn}"
                )
        if any(record.status == "COMMITTED" for record in writebacks.values()):
            self._validate_refreshed_context(refreshed_context)

        grouped: dict[str, tuple[dict[str, list[str]], list[str]]] = {}
        for urn in self.entity_urns:
            values = writebacks[urn].property_values
            key = _stable_json(values)
            if key not in grouped:
                grouped[key] = (values, [])
            grouped[key][1].append(urn)

        journal: list[Mapping[str, Any]] = []
        traces: list[Mapping[str, Any]] = []
        property_mutation_assets: set[str] = set()
        failed_operation = ""
        failure: Exception | None = None

        for values, urns in grouped.values():
            operation = "upsert_structured_properties"
            try:
                payload, trace = await self._one_call(
                    "add_structured_properties",
                    {"property_values": values, "entity_urns": urns},
                )
                traces.append(trace)
                if not self._successful(payload):
                    raise DataHubWritebackError(
                        "structured property mutation was not successful"
                    )
                property_mutation_assets.update(urns)
                journal.append(
                    self._journal_entry(len(journal) + 1, operation, urns, success=True)
                )
            except Exception as error:
                failed_operation = operation
                failure = error
                journal.append(
                    self._journal_entry(
                        len(journal) + 1,
                        operation,
                        urns,
                        success=False,
                        error=error,
                    )
                )
                break

        tag_mutation_succeeded = False
        if failure is None:
            try:
                payload, trace = await self._one_call(
                    "add_tags",
                    {
                        "tag_urns": [MIGRATION_TAG],
                        "entity_urns": list(self.entity_urns),
                    },
                )
                traces.append(trace)
                if not self._successful(payload):
                    raise DataHubWritebackError("tag mutation was not successful")
                tag_mutation_succeeded = True
                journal.append(
                    self._journal_entry(
                        len(journal) + 1,
                        "add_migration_tag",
                        self.entity_urns,
                        success=True,
                    )
                )
            except Exception as error:
                failed_operation = "add_migration_tag"
                failure = error
                journal.append(
                    self._journal_entry(
                        len(journal) + 1,
                        failed_operation,
                        self.entity_urns,
                        success=False,
                        error=error,
                    )
                )

        readback_payload: Any = None
        by_urn: dict[str, Mapping[str, Any]] = {}
        readback_error: Exception | None = None
        property_successful_assets: tuple[str, ...] = ()
        fully_successful_assets: tuple[str, ...] = ()
        readback_sha256: str | None = None
        terminal_success = False
        attempts = READBACK_ATTEMPTS if failure is None else 1
        retry_seconds = (
            LIVE_READBACK_RETRY_SECONDS
            if isinstance(self.tool_client, OfficialDataHubMCPClient)
            else 0
        )
        for attempt in range(1, attempts + 1):
            by_urn = {}
            readback_error = None
            try:
                readback_payload, trace = await self._one_call(
                    "get_entities", {"urns": list(self.entity_urns)}
                )
                traces.append(trace)
                by_urn = self._readback_entities(readback_payload, self.entity_urns)
                journal.append(
                    self._journal_entry(
                        len(journal) + 1,
                        "readback_fetch",
                        self.entity_urns,
                        success=True,
                    )
                )
            except Exception as error:
                readback_error = error
                journal.append(
                    self._journal_entry(
                        len(journal) + 1,
                        "readback_fetch",
                        self.entity_urns,
                        success=False,
                        error=error,
                    )
                )

            property_successful_assets = tuple(
                urn
                for urn in self.entity_urns
                if (
                    self._property_match(by_urn[urn], writebacks[urn])
                    if urn in by_urn
                    else urn in property_mutation_assets
                )
            )
            fully_successful_assets = tuple(
                urn
                for urn in self.entity_urns
                if (
                    self._full_match(by_urn[urn], writebacks[urn])
                    if urn in by_urn
                    else tag_mutation_succeeded and urn in property_mutation_assets
                )
            )
            readback_sha256 = (
                hashlib.sha256(
                    _stable_json(readback_payload).encode("utf-8")
                ).hexdigest()
                if readback_payload is not None
                else None
            )
            terminal_success = (
                readback_error is None and len(fully_successful_assets) == 4
            )
            journal.append(
                self._journal_entry(
                    len(journal) + 1,
                    "readback_match",
                    fully_successful_assets,
                    success=terminal_success,
                    error=(
                        None
                        if terminal_success
                        else "desired state was not fully observed"
                    ),
                )
            )
            if terminal_success:
                break
            if attempt < attempts:
                await asyncio.sleep(retry_seconds)

        if not terminal_success:
            raise DataHubPartialWriteError(
                failed_operation=(
                    failed_operation
                    or ("readback_fetch" if readback_error is not None else "readback_mismatch")
                ),
                attempted_assets=self.entity_urns,
                successful_assets=fully_successful_assets,
                property_successful_assets=property_successful_assets,
                journal=journal,
                transport=getattr(self.tool_client, "transport", "unknown"),
                readback_sha256=readback_sha256,
            ) from (failure or readback_error)

        statuses = {urn: writebacks[urn].status for urn in self.entity_urns}
        owners = {urn: writebacks[urn].owner for urn in self.entity_urns}
        evidence_urls = {urn: writebacks[urn].evidence_url for urn in self.entity_urns}
        unique_statuses = set(statuses.values())
        unique_evidence_urls = set(evidence_urls.values())
        transport = getattr(self.tool_client, "transport", "unknown")
        live_verified = (
            isinstance(self.tool_client, OfficialDataHubMCPClient)
            and transport == OfficialDataHubMCPClient.transport
            and self.context.transport == OfficialDataHubMCPClient.transport
        )
        return WritebackReceipt(
            migration_id=next(iter(migration_ids)),
            status=next(iter(unique_statuses)) if len(unique_statuses) == 1 else "MIXED",
            entity_urns=self.entity_urns,
            tag_urn=MIGRATION_TAG,
            evidence_url=(
                next(iter(unique_evidence_urls))
                if len(unique_evidence_urls) == 1
                else "multiple per-asset evidence URLs"
            ),
            readback_sha256=readback_sha256 or "",
            tool_traces=tuple(traces),
            asset_statuses=statuses,
            asset_owners=owners,
            asset_evidence_urls=evidence_urls,
            impact_fingerprint=self.context.impact_fingerprint,
            transport=transport,
            live_verified=live_verified,
            journal=tuple(journal),
            verification=(
                "live DataHub OSS write verified through official MCP read-back"
                if live_verified
                else "read-back verified by a non-live test transport"
            ),
        )
