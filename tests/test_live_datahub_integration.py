from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from data_lineage_fix_agent.live_datahub import (
    VERIFIED_TAG_URN,
    LiveDataHubContextProvider,
)


ROOT = Path(__file__).resolve().parents[1]
MAPPING = json.loads(
    (ROOT / "fixture_pipeline" / "project_mapping.json").read_text(encoding="utf-8")
)


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_DATAHUB_INTEGRATION") != "1",
    reason="set RUN_DATAHUB_INTEGRATION=1 with local DataHub running",
)
def test_live_mcp_context_and_writeback_are_readable() -> None:
    provider = LiveDataHubContextProvider(
        os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
    )
    context = asyncio.run(provider.load(MAPPING["source_urn"], MAPPING["target_urn"]))
    assert {field.name for field in context.source_fields} >= {
        "order_id",
        "customer_key",
        "total_amount",
    }
    assert MAPPING["target_urn"] in context.downstream_urns
    assert "urn:li:corpuser:data-platform-oncall" in context.owner_urns
    assert context.transport.startswith("datahub-oss")

    readback = asyncio.run(
        provider.call_tools([("get_entities", {"urns": MAPPING["target_urn"]})])
    )[0][0]
    assert VERIFIED_TAG_URN in json.dumps(readback, sort_keys=True)
