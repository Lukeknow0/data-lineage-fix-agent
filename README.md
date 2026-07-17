# DataLineage Fix Agent

DataLineage Fix Agent turns DataHub context into an executable repair. It reads schema, lineage, ownership, and quality/governance signals; proves that a mapped downstream SQL asset is broken; generates a narrow patch and a regression test; runs the test red-to-green; writes verified status back to DataHub; and packages every claim as evidence.

This is not a chat interface. The product boundary is a guarded action loop:

```text
DataHub OSS + official MCP tools
        ↓
schema + lineage + ownership + signal
        ↓
context refusal or one grounded finding
        ↓
generated regression → red
        ↓
minimal SQL patch → green
        ↓
DataHub status write-back + evidence bundle
```

## Quick start

Prerequisites: Python 3.11, Bash, Make, and curl. Run these commands from a
source checkout; the controlled fixtures are repository assets rather than
wheel-packaged resources.

```bash
./scripts/bootstrap.sh
```

One command to start the deterministic offline replay:

```bash
make start
```

One command to verify the product contract:

```bash
make verify
```

The replay is intentionally labeled `offline-replay-of-datahub-mcp`; it is useful to judges and CI but never presented as a live catalog call. The live feasibility evidence uses DataHub OSS plus the official DataHub MCP Server.

## Live DataHub gate

One command runs the full gate against DataHub Lite (DataHub's official embedded DuckDB catalog) through the official `mcp-server-datahub` process:

```bash
make gate-live
```

The small compatibility bridge in `lite_gms_bridge.py` only exposes the GMS operations exercised by the official MCP tools. It keeps judge setup light; it is not presented as a production GMS replacement.

To validate against a full DataHub GMS instead, start the official Docker quickstart and point the same gate at port 8080:

```bash
.venv/bin/datahub docker quickstart --version v1.6.0
DATAHUB_GMS_URL=http://localhost:8080 make gate-live
```

The gate fails unless all of these are true:

- official MCP `get_entities`, `list_schema_fields`, and `get_lineage` calls return the seeded DataHub entities;
- the target URN is proven downstream and ownership is present;
- the current DataHub schema explicitly proves the rename;
- the generated regression is red before the patch and green after it;
- verified status is written to the downstream DataHub entity and read back;
- the evidence bundle contains the exact URNs, diff, test outputs, write-back result, and SHA-256 manifest.

The default lightweight gate starts the local bridge at
`http://127.0.0.1:8979`. A full DataHub OSS Quickstart normally exposes GMS at
`http://127.0.0.1:8080` and requires a running Docker daemon with sufficient
memory. For authenticated DataHub, provide `DATAHUB_GMS_TOKEN` only as a
server-side environment variable. Tokens are excluded from traces, artifacts,
frontend code, and Git.

## Scenario

The controlled pipeline models a real schema migration:

- `ecommerce.raw.orders.customer_id` was renamed to `customer_key` in schema contract v2;
- `analytics.mart_customer_revenue` is downstream according to DataHub lineage;
- its SQL still references `customer_id` in `SELECT`, `GROUP BY`, and `ORDER BY`;
- the agent refuses a fuzzy guess and patches only because DataHub documents the rename;
- an in-memory SQLite regression makes the failure and repair independently executable.

## Evidence

Each run creates `artifacts/runs/<run-id>/` with:

- `context.json` — normalized context plus MCP tool trace;
- `finding.json` — the exact source/downstream URNs and grounded diagnosis;
- `patch.diff` — minimal repository patch;
- `regression_test.py` — generated test;
- `before.txt` / `after.txt` — red and green execution output;
- `writeback.json` — DataHub status and verification;
- `EVIDENCE.md` — human-readable summary;
- `manifest.json` — SHA-256 digest for every evidence file.

The latest run is also summarized in [`EVIDENCE.md`](EVIDENCE.md). A compact committed sample lives in [`examples/verified-run`](examples/verified-run).

## Tests

The automated suite covers the required safety contract:

1. correct affected-downstream identification;
2. refusal to patch without DataHub context;
3. generated patch turns the regression from red to green;
4. evidence traces back to DataHub URNs and MCP tool calls.

Integration tests are marked `integration` and require a seeded local DataHub
plus the preceding repair/write-back. The supported end-to-end entry point is:

```bash
make gate-live
```

Running `pytest -m integration` by itself intentionally skips unless
`RUN_DATAHUB_INTEGRATION=1` is set and the required DataHub state already
exists.

## Design constraints

- No API key is accepted by browser code or committed configuration.
- No arbitrary shell from model output is executed.
- A patch is allowed only for one explicitly documented schema rename.
- Source is restored if the generated patch fails its regression.
- Write-back happens only after green verification.
- Fixture mode never claims a live write-back.

## Hackathon track

Primary challenge: **Agents That Do Real Work**. The agent reads DataHub, acts in a repository, verifies the result, and writes status back so the graph carries the outcome. It does not claim simultaneous optimization for all four challenges.

See [`RULES_SNAPSHOT.md`](RULES_SNAPSHOT.md), [`GO_NO_GO.md`](GO_NO_GO.md), [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md), and [`SUBMISSION_DRAFT.md`](SUBMISSION_DRAFT.md) for the frozen rules, gate result, provenance, and submission material.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
