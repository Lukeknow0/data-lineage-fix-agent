# Devpost Submission Draft

## Project name

DataLineage Fix Agent

## Tagline

Turn DataHub lineage and schema context into a tested patch, then write the proof back.

## Primary challenge

Agents That Do Real Work

## Short description

DataLineage Fix Agent is an autonomous repair loop for schema drift. It reads schema, lineage, ownership, and quality/governance signals from DataHub through the official MCP Server; maps an affected downstream entity to repository code; refuses to guess unless DataHub explicitly proves the change; generates a regression test and minimal SQL patch; executes the test red-to-green; writes a verified status tag back to DataHub; and emits a traceable evidence bundle.

## Inspiration

Data incidents are easy to describe after they happen: a producer renamed a column, a downstream model still referenced the old name, and ownership/lineage existed somewhere else. The hard part is closing the loop safely. We wanted an agent that does not merely recommend a fix. It must prove which downstream asset is affected, make the smallest justified change, execute the repair, and leave durable evidence in DataHub for the next human or agent.

## What it does

The controlled demo starts with `ecommerce.raw.orders` on schema contract v2, where `customer_id` became `customer_key`. DataHub marks `analytics.mart_customer_revenue` as downstream and records its technical owner and drift signal. The repository SQL still uses `customer_id` three times.

The agent invokes official DataHub MCP tools (`get_entities`, `list_schema_fields`, and `get_lineage`), checks all safety preconditions, and generates a SQLite regression. The test fails on the missing column. It then applies a three-reference identifier-only patch, reruns the exact test successfully, invokes MCP `add_tags`, and confirms through MCP `get_entities` that `DataLineageFixVerified` is present on the downstream entity.

Every run produces context/finding JSON, a unified diff, generated test, red and green logs, write-back proof, a Markdown explanation, and SHA-256 manifest.

## How we built it

- DataHub OSS metadata model and DataHub Lite for a small, reproducible catalog;
- official `mcp-server-datahub` over stdio for context reads and write-back;
- Python 3.11 orchestration;
- SQLGlot to validate the exact missing identifier and prevent stale/broad patches;
- SQLite as the portable execution target for the generated regression;
- pytest for product safety contracts.

The project-owned Lite bridge implements the narrow GMS compatibility surface used by the official MCP tools, keeping the public fixture runnable without Docker. The same agent can point at a full DataHub GMS by changing `DATAHUB_GMS_URL`.

## What is original

DataHub already provides metadata and lineage. This project composes them into a guarded repository action loop with a red-to-green proof and verified graph write-back. The key behavior is context refusal: no schema, lineage, owner, or MCP provenance means no patch.

## Challenges we ran into

The main design challenge was avoiding a demo that looked correct while bypassing DataHub. We separated the offline replay from the live gate, recorded MCP result hashes, and made live write-back require MCP read-back. We also constrained patching to one explicit rename instead of allowing free-form model output to modify arbitrary files.

## Accomplishments

- Complete DataHub context → finding → regression → patch → green test → DataHub write-back path.
- One-command live gate with synthetic, publishable data.
- Four required safety/evidence tests plus a live MCP integration test.
- No model or DataHub credential required for the judge fixture.

## What we learned

Lineage alone is not enough. A safe repair needs a conjunction of lineage, current schema, an explicit rename signal, ownership, executable behavior, and a durable write-back. Treating the evidence bundle as a first-class artifact made every product claim testable.

## What's next

1. Validate the same gate against a full DataHub GMS in the final recording environment.
2. Add dbt and Airflow repository adapters while preserving identifier-only guards.
3. Add a bounded model-assisted proposal layer whose output cannot bypass the deterministic context/test policy.
4. Turn verified outcomes into DataHub incidents or structured properties when those workflows are available.

## Try it

```bash
./scripts/bootstrap.sh
make gate-live
```

Fast replay and tests:

```bash
make start
make verify
```

## Technologies used

DataHub OSS / Core Platform; DataHub MCP Server; DataHub Lite; Python; MCP; SQLGlot; DuckDB; SQLite; pytest.

## AI and pre-existing work disclosure

An AI coding assistant (OpenAI Codex) was used during implementation, as permitted by the rules. The product's default repair policy is deterministic and independently testable; it does not send catalog data to a model provider. Architectural ideas—not source code—from pre-period local bounty monitoring/solver scripts informed context packaging and the bounded test loop. Full details are in `THIRD_PARTY_NOTICES.md`.

## URLs — confirmation required before publication

- Project/demo URL: `[NOT PUBLISHED — USER CONFIRMATION REQUIRED]`
- Public Apache-2.0 repository: `[NOT CREATED — USER CONFIRMATION REQUIRED]`
- Public video under three minutes: `[NOT UPLOADED — USER CONFIRMATION REQUIRED]`

## Testing instructions for judges

1. Use Python 3.11+.
2. Run `./scripts/bootstrap.sh` once.
3. Run `make gate-live` and confirm `LIVE GATE: PASS`.
4. Open `EVIDENCE.md` and `examples/verified-run/`.
5. Run `make verify` to exercise the refusal, lineage, red-to-green, and traceability contracts.
> Release note (2026-07-16): public repository and Demo deployment are authorized. Devpost is not being submitted yet.
