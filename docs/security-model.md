# LineageTX security model

LineageTX is a fail-closed change coordinator for one bounded schema migration.
Its primary security objective is to prevent untrusted candidate data, incomplete
lineage, stale repository state, or an invalid approval from releasing the
Producer gate.

This document describes the implemented boundary. It is not a claim of database
atomicity, automatic merging, or rollback of deployed systems.

## Protected outcomes

LineageTX protects these invariants:

1. The Producer gate stays closed until the complete three-consumer impact set
   is verified.
2. Candidate edits occur only in LineageTX-owned worktrees and branches pinned
   to recorded base SHAs.
3. A candidate may change only the participant's exact allow-listed files.
4. A candidate cannot grant itself repository, path,
   owner, schema, or execution authority.
5. The semantic consumer receives zero writes before the discovered owner
   approves the exact mapping.
6. `COMMITTED` requires receipts for every candidate commit, a coordinated PR,
   and the Producer gate release; it never implies merge or deployment.
7. ABORT removes only unmerged LineageTX-owned candidates and records cleanup
   errors; it never claims to reverse a deployed change.

## Trust boundaries

| Input or component | Treatment |
| --- | --- |
| Producer ChangeIntent | Immutable identity derived from PR/repository, SHAs, source asset, mapping, rollout phase, and contract fingerprint. |
| DataHub OSS metadata | Required context, but still validated for exact asset count, complete hops, schemas, owners, governance signals, tool traces, and path evidence. |
| Official MCP responses | Normalized and bounded by timeouts; discovery must be complete and produces a frozen impact fingerprint. |
| Candidate data | Untrusted proposal-only input constrained to closed records and rebound to trusted policy. Never executed as shell or code. |
| Repository contents and Git configuration | Treated as untrusted until the base SHA, clean status, file digests, paths, and executable Git configuration are checked. |
| Owner approval | Production accepts only a stable HTTPS GitHub issue-comment or PR-review resource fetched from `api.github.com`. GitHub supplies the actor, timestamp, association, stable IDs, and evidence URL; the body must bind the exact migration, participant, discovered owner URN, and old/new mapping. |
| GitHub and DataHub mutations | External side effects represented by explicit receipts and read-back; partial failure is surfaced rather than hidden. |
| Public browser replay | Presentation-only. It receives no secrets and performs no catalog, GitHub, or repository mutation. |

## Discovery controls

- Live LineageTX policy accepts full DataHub OSS through the official MCP
  transport. The legacy Lite bridge is excluded from live proof.
- Discovery must contain exactly the source plus three unique consumers at hops
  1, 2, and 3+.
- Every participant must map one-to-one to a discovered consumer, and the set
  must contain exactly dbt, Airflow, and semantic participant kinds.
- The source expanded schema must contain both `customer_id` and `customer_key`;
  the contract schema removes the old field and retains the replacement.
- Owners and governance signals must be present. The semantic participant must
  have exactly one accountable owner.
- Required MCP traces include schema, lineage, entity/governance, and path
  queries. Pagination and path endpoints are validated.
- A SHA-256 impact fingerprint binds the accepted schemas, lineage paths,
  owners, and governance context. Commit performs a fresh read and fails closed
  if the fingerprint changes.

## Candidate boundary

The success path uses deterministic structured candidates and receives no
command tool. A proposal can express only the minimal candidate shape for its
participant. LineageTX independently binds and checks:

- migration and participant IDs;
- repository and exact relative paths;
- old and replacement fields;
- source asset and ChangeIntent digest;
- expanded and contract schemas;
- discovered owner URNs; and
- adapter-specific relation, dialect, assignment, and configuration keys.

The candidate rationale is evidence only. It is never an authorization input.

## Repository isolation

Before preparing a candidate, LineageTX requires the supplied path to be the Git
repository root, the checkout to be clean, and `HEAD` to equal the pinned base
SHA. Worktrees live outside the base repository and branches use the
`lineagetx/` prefix.

Git runs with a minimal environment, no terminal prompt, no pager, disabled
hooks, disabled fsmonitor, disabled signing, bounded timeouts, and a fixed Git
binary path. Repository-local settings capable of launching filters, helpers,
hooks, pagers, external diff or merge drivers, editors, signing programs, or
similar processes are rejected.

Paths must be relative, cannot contain `..`, and must resolve inside the owned
worktree. File SHA-256 values guard against stale proposals. Git porcelain is
parsed in NUL-delimited form, including both sides of rename/copy records, so a
rename cannot hide a path outside the allow-list.

The base checkout's SHA and status are rechecked throughout preparation and
publication. Every accepted participant changes exactly its allow-list; no
generic repository command supplied by a candidate is available.

## Deterministic validation

### dbt SQL

- exactly one allow-listed SQL file;
- one bounded `SELECT` against one expected relation;
- no joins, table functions, external access, or additional statements;
- SQLGlot AST validation and exact deterministic field substitution;
- compilation against expanded and contract schema variants; and
- DuckDB external access disabled during validation.

### Airflow mapping

- exact Python and JSON file hashes;
- one literal Python mapping assignment parsed with the Python AST;
- valid JSON configuration and pre-change cross-file equality;
- a deterministic old-to-new mapping with collision checks;
- expanded- and contract-schema validation; and
- both candidate byte sequences validated before writing, with both originals
  restored if the two-file write fails.

### Semantic mapping

- expected source digest and document shape;
- ambiguous mapping returns `NEEDS_APPROVAL` without changing a file;
- exact discovered-owner identity and mapping-bound approval;
- approval timestamp and evidence URL; and
- post-approval document and schema-variant validation.

### Authenticated owner approval and durable resume

The production coordinator rejects a caller-created `OwnerApproval`, even when
all of its strings look correct. `approve_from_github` performs a read-only GET
of one canonical `https://api.github.com/repos/.../issues/comments/{id}` or
`.../pulls/{number}/reviews/{id}` resource. Redirects, query strings, non-GitHub
hosts, edited issue comments, non-approved reviews, bots, mismatched stable IDs,
and browser URLs for another repository fail closed.

The comment or review body must be one JSON object containing exactly:

```json
{
  "decision": "APPROVED",
  "migration_id": "ltx-...",
  "participant_id": "participant-...",
  "owner_urn": "urn:li:corpuser:identity-data-owner",
  "old_field": "customer_id",
  "new_field": "customer_key"
}
```

The GitHub login is mapped to the owner URN by operator-supplied trusted
configuration. The actor must also have `OWNER`, `MEMBER`, or `COLLABORATOR`
author association, unless that same mapped login is explicitly allow-listed.
The verified receipt records the GitHub numeric and node IDs, actor, author
association, API and browser URLs, and a SHA-256 digest of the authenticated
evidence snapshot. The coordinator fetches the same stable resource again just
before gate release and requires the resource IDs and digest to remain equal.
API tokens never enter the receipt.

The live runner defaults to `pause`. At `NEEDS_APPROVAL` it writes a
HMAC-SHA256-signed context snapshot using a key supplied only through
`LINEAGETX_RESUME_HMAC_KEY`. A later `resume` verifies the signature, recreates
fresh isolated fixture repositories and candidates, re-reads DataHub, and
requires the migration, participant, owner, mapping, impact fingerprint, and
repository base SHAs to match the signed request before it reads GitHub.
The gate does not reseed DataHub during resume. The runner stages the signed
snapshot outside the reset-owned directory while recreating repositories, then
checks the refreshed fingerprint and base SHAs before detect, write-back, or
candidate creation.

This reference build deliberately uses deterministic re-prepare/revalidation;
it does **not** deserialize live coordinator sessions or preserve an untrusted
Python object graph across processes. Therefore a resume replaces the prior
unmerged candidate worktrees with equivalent freshly verified candidates. A
repository or DataHub context change makes resume fail closed and requires a
new migration. The `scripted-test` phase is the only caller-created approval
path and is labeled test-only in CLI output and evidence.

## State and receipt controls

The SQLite store uses transactions, expected state/version checks, and
append-only state events. Terminal migrations cannot transition again.
`PREPARED` requires every participant to be `VERIFIED` with a candidate commit
SHA. `COMMITTED` requires candidate receipts covering all three SHAs plus a
coordinated-PR receipt and a Producer-gate receipt.

Evidence files are sealed in a SHA-256 manifest. The manifest detects accidental
or deliberate file changes after capture; it is an integrity check, not a
digital signature or trusted timestamp.

DataHub write-back is restricted to the exact frozen source-plus-three-consumer
set. Per-asset owner values must equal the frozen DataHub owners. Structured
Properties and the migration Tag are read back through MCP; partial writes
produce an explicit operation journal and are safe to retry idempotently.

## Credentials and browser safety

- `DATAHUB_GMS_TOKEN` and GitHub tokens must remain
  server-side environment variables and must never enter evidence, fixtures,
  browser JavaScript, query strings, or Git history.
- The DataHub GMS URL is normalized as an origin and rejects embedded user info,
  query strings, fragments, and the legacy Lite port for live LineageTX reads.
- GitHub endpoints require HTTPS and fixed repository identifiers. The
  publisher has no merge method.
- The public demo loads only static replay and evidence assets. It does not ask
  for credentials or send mutation requests.

## Failure semantics

Preparation, authenticated approval application, and pre-publication failures drive the
migration to ABORT when candidate cleanup succeeds. Invalid approval attempts
do not mutate the candidate and leave the legitimate approval path open.

If a candidate is already reachable from another branch, ABORT refuses to
delete it. If local cleanup or DataHub write-back is incomplete, the error and
journal are retained for operator action. No failure path is described as a
rollback of an already merged or deployed system.

## Explicit non-goals

- distributed database atomicity or consensus;
- automatically merging Producer or consumer PRs;
- undoing deployed code, data backfills, or database DDL;
- executing arbitrary candidate-generated scripts;
- discovering an unbounded number of repositories in this reference build;
- accepting a public replay as proof of a live DataHub run.
