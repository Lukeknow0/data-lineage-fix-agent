from __future__ import annotations

import json
from pathlib import Path

from .models import DataHubContext, SchemaField


class FixtureContextProvider:
    """Offline replay only; never represented as a live DataHub run."""

    def __init__(self, fixture_path: Path):
        self.fixture_path = fixture_path

    async def load(self, source_urn: str, target_urn: str) -> DataHubContext:
        payload = json.loads(self.fixture_path.read_text(encoding="utf-8"))
        if payload["source_urn"] != source_urn or payload["target_urn"] != target_urn:
            raise ValueError("Fixture URNs do not match the requested entities")
        return DataHubContext(
            source_urn=payload["source_urn"],
            target_urn=payload["target_urn"],
            source_fields=[SchemaField(**field) for field in payload["source_fields"]],
            downstream_urns=payload["downstream_urns"],
            owner_urns=payload["owner_urns"],
            quality_signals=payload["quality_signals"],
            transport=payload["transport"],
            tool_traces=payload["tool_traces"],
            source_properties=payload.get("source_properties", {}),
        )


class OfflineReplayStatusWriter:
    async def write_verified(self, entity_urn: str, run_id: str) -> dict:
        return {
            "status": "offline-replay-only",
            "entity_urn": entity_urn,
            "tag_urn": "urn:li:tag:DataLineageFixVerified",
            "run_id": run_id,
            "verification": "fixture mode does not claim a live DataHub mutation",
        }
