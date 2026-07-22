# LineageTX

> **LineageTX — a schema-change safety gate across the data graph, inspired by two-phase commit.**

LineageTX turns one Producer schema change into a coordinated, evidence-backed
decision across every known downstream consumer. Real DataHub OSS discovers the
column-level impact set through the official DataHub MCP server. An agent
proposes candidate repairs on isolated Git branches; deterministic validators,
tests, lineage, and owner policy decide whether those candidates are allowed.
Only after all consumers verify does LineageTX release the Producer PR gate.

This is not a database transaction. LineageTX does **not** claim atomicity across
DataHub, GitHub, repositories, or deployed systems. It does not automatically
merge a PR. ABORT cleans only LineageTX-owned, unmerged candidate changes; it
does not roll back code or data that has already been deployed.

Primary hackathon track: **Agents That Do Real Work**.

## The bounded demonstration

The canonical ChangeIntent renames:

```text
ecommerce.raw.orders.customer_id -> customer_key
```

DataHub supplies one source and a three-hop column-lineage path:

| Hop | Consumer | Agent action | Acceptance rule |
| --- | --- | --- | --- |
| 1 | dbt SQL, `analytics.stg_orders` | Propose one narrow SQL repair. | SQLGlot AST, exact relation/path policy, and expanded/contract-schema checks pass. |
| 2 | Airflow mapping, `ops.customer_export` | Update one Python mapping and one JSON configuration together. | Both files pass AST/JSON, cross-file, schema, and exact allow-list checks. |
| 3 | Semantic mapping, `semantic.customer_identity` | Refuse the ambiguous edit and wait. | Zero writes occur until the exact DataHub owner approves the exact mapping. |

The result is a visible fork-and-converge story: two automatic repairs become
verified, one semantic repair pauses at `NEEDS_APPROVAL`, and the gate remains
closed until all three converge.

## Safety-gate workflow

```text
Producer PR / ChangeIntent
          |
          v
DataHub OSS + official MCP
schema + 3-hop column lineage + owners + governance
          |
          v
DETECTED -> PREPARING -> NEEDS_APPROVAL -> PREPARED
              |                 |              |
              |                 |              +-- fresh DataHub impact check
              |                 +-- exact owner approval
              +-- isolated candidate branches + deterministic validation
          |
          +------ any failure ------> ABORTED
          |
          v
COMMITTED = candidate receipts + coordinated PR + Producer gate released
            candidates remain unmerged; human merge control remains
```

LineageTX writes the migration ID, per-asset state, accountable owner, and
evidence URL back to the exact source-plus-three-consumer set with DataHub
Structured Properties and a `LineageTXMigration` Tag. Write-back is read back
and hashed; partial mutations produce an explicit idempotent-retry journal.

## Quick start

Requirements:

- Python 3.11 (the package currently declares `>=3.11,<3.12`);
- Git, Bash, and Make; and
- Docker with enough memory for the full DataHub OSS Quickstart when running the
  live path.

Install the project and pinned development dependencies from a source checkout:

```bash
./scripts/bootstrap.sh
```

Run the default automated suite. The real-OSS integration test is skipped here
unless its explicit environment gate is enabled:

```bash
make verify
```

Run only the LineageTX safety and coordination suite:

```bash
.venv/bin/python -m pytest -q tests/lineagetx
```

Run the deterministic LineageTX transaction. This executes the real coordinator,
worktrees, adapters, owner approval, local publication receipts, and evidence
writer against controlled fixture repositories; it is not a live DataHub claim.

```bash
make demo-lineagetx
```

Equivalent direct CLI invocation:

```bash
.venv/bin/datalineage-fix lineagetx replay \
  --project-root "$PWD" \
  --work-root "$PWD/artifacts/runs/lineagetx-local-replay" \
  --reset
```

The replay prints every coordinator transition and ends with:

```text
0 unverified consumers — upstream change is safe to merge.
```

It writes durable replay state and SHA-256-sealed evidence below
`artifacts/runs/lineagetx-local-replay/`. Inspect the migration, participant,
approval, and event records with:

```bash
.venv/bin/datalineage-fix lineagetx show \
  --work-root artifacts/runs/lineagetx-local-replay
```

Exercise the scoped cleanup path separately:

```bash
make demo-lineagetx-abort
```

The replay always uses deterministic structured candidates. Trusted-policy
binding and deterministic validators remain authoritative for every change.

## Real DataHub OSS path

Start a full DataHub OSS Quickstart. The `--arch m1` option may be added on
Apple Silicon when required by the DataHub CLI.

```bash
.venv/bin/datahub docker quickstart --version v1.6.0
```

Seed the canonical source, three consumers, owners, governance signals,
Structured Property definitions, Tags, and three field-lineage edges. By
default the seeder finishes with a bounded read-back through the official
`mcp-server-datahub`; the command fails if that proof does not become complete.

```bash
DATAHUB_GMS_URL=http://127.0.0.1:8080 make seed-lineagetx
```

The seeder output must report:

- `backend: full-datahub-oss`;
- `column_lineage_hops: 3`;
- `verification.mode: official-mcp-readback`;
- `verification.live_verified: true`; and
- `verification.discovery_complete: true`.

`--no-verify` exists only for development. Its receipt explicitly says
`live_verified: false` and must not be used as live evidence. The legacy Lite
bridge on port 8979 is also not accepted as live LineageTX proof.

For authenticated DataHub, set `DATAHUB_GMS_TOKEN` only in the server-side
environment. Do not place it in a command-line URL, fixture, evidence file,
browser bundle, or Git history.

Run the live DataHub OSS gate. The safe default stops durably at
`NEEDS_APPROVAL`; it does not invent an owner decision:

```bash
export LINEAGETX_RESUME_HMAC_KEY="$(openssl rand -hex 32)"
DATAHUB_GMS_URL=http://127.0.0.1:8080 make gate-lineagetx-live
```

That target rejects the Lite bridge, health-checks full DataHub OSS, seeds the
canonical graph for a new pause or scripted test, runs the official-MCP
integration boundary test, then drives the real LineageTX coordinator over clean
isolated Git repositories. Resume deliberately does not reseed DataHub, so drift
since the signed pause remains observable and fails closed. The pause receipt
contains the exact JSON body the owner must post in a GitHub issue comment or PR
review, plus a signed resume-state filename. The semantic participant remains
zero-write and the Producer gate remains closed.

After the owner posts that body, resume with the stable GitHub API resource and
an explicit DataHub-owner-to-GitHub-login mapping:

```bash
export LINEAGETX_PHASE=resume
export LINEAGETX_APPROVAL_API_URL="https://api.github.com/repos/OWNER/REPO/issues/comments/COMMENT_ID"
export LINEAGETX_OWNER_GITHUB_LOGIN="urn:li:corpuser:identity-data-owner=GITHUB_LOGIN"
DATAHUB_GMS_URL=http://127.0.0.1:8080 make gate-lineagetx-live
```

For a public repository `GITHUB_TOKEN` is optional; setting it only in the
server-side environment improves API rate limits. It is never persisted.

Resume verifies the signed pause state, protects it outside the reset-owned work
root while repositories are recreated, and re-reads the DataHub impact
fingerprint and repository base SHAs before detect, write-back, or candidate
work. It then deterministically creates fresh isolated candidates, authenticates
the GitHub actor and repository association, and performs
`NEEDS_APPROVAL -> PREPARED -> COMMITTED`. The final gate fails unless all three
candidate commits exist, six per-stage write-back/read-back receipts verify, the
base checkouts remain unchanged, and zero consumers remain unverified. The
reference runner re-prepares candidates instead of serializing live Python
sessions; any context drift fails closed and requires a new pause.

`LINEAGETX_PHASE=scripted-test` retains a deterministic caller-created receipt
for automated fixtures only. Its output says `scripted-test-only`; it is not
valid owner-approval evidence for a live run.

Publication in this reproducible OSS gate uses an explicitly labeled local,
unmerged coordination receipt. It does not create a remote GitHub PR or post a
real Producer status check. The separate fixed-scope GitHub publisher supports
those two operations and deliberately exposes no merge method; remote
publication requires configured repositories, a pre-pushed coordination branch,
and a server-side token.

## Demo modes and disclosure

- **Local deterministic replay:** executes the LineageTX coordinator and Git
  safety machinery on controlled repositories with disclosed replay context.
- **Public interactive synthetic replay:** a viewer can start, approve, abort,
  reset, and inspect the deterministic transaction story. Browser code has no
  DataHub or GitHub credentials and performs no external mutation. Its replay
  fixture and preserved legacy baseline have separate canonical SHA-256
  manifests.
- **Live evidence run:** real DataHub OSS v1.6.0 plus official MCP discovery,
  pre-commit refresh, write-back, and read-back. The captured evidence bundle
  is published; the video is still pending.

Public deployment:
[datahub-agent-hackathon-2026.vercel.app](https://datahub-agent-hackathon-2026.vercel.app/).
The page is a deterministic replay, not a browser-to-DataHub control plane.

The public page currently discloses:

> Verified interactive replay. The browser runs deterministic playback, is not
> connected to DataHub or GitHub, and performs no external mutation. The linked
> bundle records the DataHub OSS live run; the video is pending.

The live path is evidenced separately from the browser replay. Its manifest is
[`demo/evidence/ltx-7ba06b0789512486f0f92f3c/manifest.json`](demo/evidence/ltx-7ba06b0789512486f0f92f3c/manifest.json).

## State and evidence

Coordinator states are:

```text
DETECTED -> PREPARING -> NEEDS_APPROVAL -> PREPARED -> COMMITTED
    |           |               |              |
    +-----------+---------------+--------------+-> ABORTED
```

Evidence is captured throughout the transaction rather than reconstructed at
the end. A complete run includes:

- immutable ChangeIntent and DataHub discovery context;
- official MCP tool traces and the impact fingerprint;
- typed proposals plus their trusted-policy bindings;
- one diff and deterministic verification record per participant;
- proof of zero semantic writes before approval;
- an owner approval receipt;
- candidate branch names and commit SHAs;
- pre-commit DataHub impact revalidation;
- coordinated-PR and Producer-gate receipts with `auto_merge: false`;
- per-asset DataHub write-back and official-MCP read-back;
- append-only state events; and
- a SHA-256 manifest covering the evidence files.

The SHA-256 manifest is an integrity comparison, not a digital signature or a
trusted timestamp.

See:

- [`docs/lineagetx-architecture.md`](docs/lineagetx-architecture.md) for the
  runtime architecture and exact commit/abort semantics;
- [`docs/security-model.md`](docs/security-model.md) for trust boundaries and
  fail-closed controls; and
- [`docs/demo-script.md`](docs/demo-script.md) for the 2–3 minute video script
  and evidence checklist.

## Preserved legacy single-SQL baseline

The original **DataLineage Fix Agent** remains in Git history and in the
repository as a clearly labeled baseline. It demonstrates one documented schema
rename, one downstream SQL repair, a red-to-green SQLite regression, a DataHub
status write-back, and an eight-file evidence bundle. LineageTX extends that
work; it does not rewrite or relabel the original run.

Legacy offline replay:

```bash
make start
```

Legacy DataHub Lite compatibility gate:

```bash
make gate-live
```

Those commands exercise the single-SQL product, not the three-consumer
LineageTX transaction. `make gate-live` defaults to the Lite compatibility
bridge unless `DATAHUB_GMS_URL` is explicitly set; it is retained for
reproducibility and is not the winning-path live proof.

Preserved artifacts:

- [`EVIDENCE.md`](EVIDENCE.md) — original run summary;
- [`examples/verified-run`](examples/verified-run) — compact committed sample;
- [`demo/evidence/20260716T091507467904Z`](demo/evidence/20260716T091507467904Z)
  — original browser evidence bundle; and
- [`demo/evidence/ltx-7ba06b0789512486f0f92f3c`](demo/evidence/ltx-7ba06b0789512486f0f92f3c)
  — verified DataHub OSS v1.6.0 and official-MCP transaction evidence; and
- [`RULES_SNAPSHOT.md`](RULES_SNAPSHOT.md), [`GO_NO_GO.md`](GO_NO_GO.md),
  [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md), and
  [`SUBMISSION_DRAFT.md`](SUBMISSION_DRAFT.md) — frozen supporting material.

## Repository map

```text
src/data_lineage_fix_agent/lineagetx/
  coordinator.py         detect / prepare / approve / commit / abort
  github_approval.py     authenticated GitHub owner-evidence verifier
  resume.py              signed pause snapshot and fail-closed reprepare binding
  datahub_context.py     official-MCP discovery and impact fingerprint
  models.py + state.py   immutable records and SQLite state machine
  participants/          dbt, Airflow, and semantic validators
  proposals.py           typed deterministic candidate proposer
  worktrees.py           pinned isolated Git candidates
  publisher.py           local/GitHub receipts, deliberately no merge API
  writeback.py           Structured Properties, Tag, and MCP read-back
  evidence.py            SHA-256-sealed transaction evidence

fixtures/lineagetx/      controlled Producer, DataHub, and repository inputs
tests/lineagetx/         state, policy, adapter, security, and E2E coverage
scripts/seed_lineagetx_datahub.py
                         real DataHub OSS fixture plus official-MCP read-back
scripts/run_lineagetx_live.py
                         full live coordinator over OSS plus isolated Git
scripts/gate_lineagetx_live.sh
                         seed, integration proof, and final live transaction
demo/                    public deterministic interactive replay
```

## Project constraints

- No arbitrary candidate-generated shell or repository command is executed.
- No candidate can authorize its own repository, owner, paths, or schema.
- No write occurs outside an isolated, pinned LineageTX worktree.
- No semantic change occurs before an exact owner-bound approval.
- No Producer gate release occurs without a fresh unchanged DataHub impact
  fingerprint and receipts covering every consumer.
- No browser secret or automatic merge path exists.
- No ABORT path claims to reverse an already merged or deployed system.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
