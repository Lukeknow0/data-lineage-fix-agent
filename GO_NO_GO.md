# Four-Hour Feasibility Gate

> Historical feasibility gate for the original bounded repair loop. LineageTX
> retains these safety proofs while adding a multi-consumer migration coordinator.

Decision: **GO**

Gate started at 2026-07-16 16:28 CST. The first complete live path passed at 17:10 CST, well inside the four-hour limit. A clean repeat passed at 17:11 CST.

## Why this is a GO

The gate produced an executable path, not a prose answer or static interface:

| Required proof | Observed result |
|---|---|
| Real DataHub context | Metadata was stored in DataHub Lite, DataHub's embedded OSS catalog, then read by the official `mcp-server-datahub==0.6.0` process. |
| Schema | MCP `list_schema_fields` returned `customer_key` and no `customer_id`, including the explicit rename description. |
| Lineage | MCP `get_lineage` returned `analytics.mart_customer_revenue` one hop downstream from `ecommerce.raw.orders`. |
| Ownership and signal | MCP `get_entities` returned `data-platform-oncall`, `SchemaDriftDetected`, `schema_contract=v2`, and `quality_signal=breaking-schema-drift`. |
| Real failure | The generated SQLite regression failed with `no such column: customer_id`. |
| Minimal repair | A three-reference identifier patch changed only `customer_id` to `customer_key`. |
| Green verification | The same generated regression passed after the patch. |
| DataHub write-back | Official MCP `add_tags` added `DataLineageFixVerified` to the downstream URN. A subsequent MCP `get_entities` read the tag back. |
| Traceability | Context, finding, diff, generated test, red/green output, write-back, and SHA-256 manifest were bundled under one run ID. |
| Safety refusal | Automated tests prove that missing schema, lineage, ownership, transport provenance, or MCP traces block any patch. |

Repeatable command:

```bash
make gate-live
```

Last clean result:

```text
status=verified-fixed
context=datahub-oss-lite-v1.6.0+official-mcp-server-v0.6.0
finding=DLFA-SCHEMA-001
red_exit=1
green_exit=0
writeback=written-and-read-back-via-datahub-mcp
LIVE GATE: PASS
```

## Primary challenge

**Agents That Do Real Work** is the single primary challenge. The product reads the graph, acts on a repository, verifies the action, and writes the verified outcome back to the graph. Code generation is an implementation detail, not a second claimed category.

## Honest limitations

- The lightweight gate uses official DataHub Lite storage plus a project-owned compatibility bridge so the official MCP server can exercise its normal tools without a multi-container stack. The bridge implements only the GMS calls used by the gate and is not a production GMS replacement.
- A full DataHub v1.6.0 Docker quickstart was attempted, but image pulls hit repeated external Docker Registry EOF failures. This did not block the product proof because the embedded DataHub catalog and official MCP process completed the same context/read/write contract. A full-GMS rerun remains desirable before recording the final video.
- The repair planner is intentionally deterministic and bounded. It behaves as an autonomous agent but does not require a model API to make the demo work. If a model-assisted planner is added later, its proposal must pass the same context and regression guards.
- Only one controlled schema-drift repair is in scope today. More bug classes should be added only after preserving the same evidence quality.
- The public repository and hosted evidence replay are available. Video upload and
  Devpost submission remain intentionally deferred pending the LineageTX upgrade.

## Stop conditions retained

The project returns to NO-GO if a later refactor allows a patch without DataHub lineage/schema evidence, removes the red-to-green test, cannot verify write-back, leaks a credential, or makes the judge depend on private data.
