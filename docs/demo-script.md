# LineageTX demo script

Target length: **2 minutes 30 seconds**, with a hard stop before 3 minutes.

The planned recording should show the same real DataHub OSS run represented by
the published evidence bundle. The public browser experience is labeled
**verified interactive replay**. It is a narrative control surface, not a live
DataHub or GitHub connection.

## Before recording

- Start a full DataHub OSS Quickstart and seed the canonical four-asset scenario.
- Complete an official-MCP read-back and save its transport/version trace.
- Prepare clean fixture Git repositories at known base SHAs.
- Open the Producer ChangeIntent for `customer_id -> customer_key`.
- Open DataHub's lineage view at `ecommerce.raw.orders.customer_id`.
- Open the LineageTX interactive replay at `DETECTED`.
- Keep the final evidence directory, test output, candidate SHAs, and DataHub
  write-back read-back ready in separate terminal tabs.
- Do not display tokens, local secrets, private repository URLs, or browser
  developer tools containing credentials.

If any live prerequisite fails, do not splice in a successful state and call it
live. Record the deterministic replay separately and label the missing live
evidence.

## Shot-by-shot script

### 0:00–0:15 — Hook: one PR, a whole graph

**Screen:** Producer PR or ChangeIntent showing `customer_id -> customer_key` and
the LineageTX check blocked.

**Narration:**

> This pull request changes one column. But three downstream consumers, across
> multiple files and owners, depend on it. LineageTX is a schema-change safety
> gate across the data graph, inspired by two-phase commit.

On screen, emphasize that the Producer change is not yet safe to merge.

### 0:15–0:38 — DataHub discovers the complete impact

**Screen:** Real DataHub OSS lineage, then a compact terminal view of official
MCP traces/read-back.

**Narration:**

> LineageTX asks real DataHub OSS, through the official MCP server, for the
> source schema, column lineage paths, owners, and governance signals. It finds
> one source and exactly three consumers at hops one, two, and three. That
> frozen impact set gets a fingerprint, so it cannot silently change before
> the gate opens.

Show the four canonical asset URNs or names and an explicit
`discovery_complete: true`. Do not use the Lite bridge in this shot.

### 0:38–1:05 — Prepare two automatic candidates

**Screen:** Start the interactive transaction. Show `DETECTED -> PREPARING`,
then the dbt and Airflow branches becoming verified. Briefly cut to the actual
candidate diffs and tests in the isolated worktrees.

**Narration:**

> The deterministic generator emits typed candidates. It gets no shell and no authority
> to choose repositories or files. The dbt change is accepted only after
> SQLGlot and expanded-versus-contract schema checks pass. The Airflow repair
> must update its Python mapping and JSON configuration together, and both
> files must agree.

Show one dbt file and two Airflow files. Keep the base checkout visibly clean.

### 1:05–1:30 — Refuse the ambiguous semantic change

**Screen:** The third consumer becomes `NEEDS_APPROVAL`; highlight `0 writes`
and the blocked Producer gate.

**Narration:**

> The third `customer_id` has ambiguous business meaning. LineageTX refuses to
> guess. It changes zero files and asks the exact owner discovered in DataHub
> to approve the exact mapping. Two green consumers are not enough; the
> upstream PR stays blocked.

Pause long enough for the viewer to read `NEEDS_APPROVAL` and `0 writes`.

### 1:30–1:52 — Owner approval and convergence

**Screen:** Show the approval receipt with owner URN, mapping, timestamp, and
evidence URL. Trigger the replay approval, then show the semantic candidate
verification and `PREPARED`.

**Narration:**

> The accountable owner approves only `customer_id` to `customer_key` for this
> migration and participant. The deterministic adapter applies that mapping,
> verifies the result, and commits it on an unmerged candidate branch. Now all
> three consumers have converged at `PREPARED`.

Do not imply that a button click alone is authorization in the real workflow;
the public control is a simulated replay of the captured approval receipt.

### 1:52–2:15 — Revalidate, publish receipts, release the gate

**Screen:** Show a fresh official-MCP impact fingerprint matching the frozen
one, three candidate commit SHAs, the coordinated PR receipt, and the Producer
check result. End on the transaction graph at `COMMITTED`.

**Narration:**

> Before release, LineageTX reads DataHub again. The impact fingerprint is
> unchanged, every candidate commit is present, and a coordinated PR is ready.
> It releases the Producer check but does not merge anything automatically.

The final UI line must be:

> **0 unverified consumers — upstream change is safe to merge.**

### 2:15–2:32 — Write the result back to DataHub

**Screen:** DataHub asset properties and the LineageTX migration Tag, followed
by the MCP read-back receipt.

**Narration:**

> The graph keeps the result. Structured Properties and a Tag record the
> migration ID, per-asset status, accountable owner, and evidence link, so the
> next person or agent inherits verified context instead of a chat transcript.

Show the source as `COMMITTED`, all three consumers as `VERIFIED`, and the same
migration ID on all four assets.

### 2:32–2:45 — Close on the safety boundary

**Screen:** Evidence manifest verification, then the final graph.

**Narration:**

> `COMMITTED` means candidate commits, a coordinated PR, and a released safety
> check exist. It does not mean database atomicity, automatic merge, or deployed
> rollback. GitHub PRs change code. LineageTX changes the whole data graph
> safely.

If more time is needed elsewhere, cut this section after the first two
sentences rather than exceeding 3 minutes.

## Optional 10-second ABORT insert

Use this only if the main cut is under time. From `PREPARING` or
`NEEDS_APPROVAL`, trigger ABORT and show the cleanup receipt.

**Narration:**

> ABORT deletes only LineageTX-owned, unmerged candidate worktrees and branches.
> It does not claim to roll back a deployed system.

Return to a fresh successful run for the final shot; never edit an ABORT into a
successful transaction without disclosing the reset.

## Evidence checklist

Every claim used in the video or README should have a captured artifact:

- [ ] DataHub OSS Quickstart version and healthy service evidence.
- [ ] Canonical seed receipt: source, three consumer URNs, three column-lineage
      hops, owner URNs, governance signals, and `contains_credentials: false`.
- [ ] Official MCP server package/version and tool traces for schema, lineage,
      entity/governance, and path reads.
- [ ] `discovery_complete: true` and the frozen impact fingerprint.
- [ ] Producer ChangeIntent, base/head SHAs, contract fingerprint, and stable
      migration ID.
- [ ] Clean base repository status and pinned base SHA for both repositories.
- [ ] Typed proposal and trusted-policy binding for each participant.
- [ ] dbt diff plus SQLGlot and expanded/contract-schema verification results.
- [ ] Airflow Python and JSON diffs plus AST, JSON, and cross-file results.
- [ ] Semantic `NEEDS_APPROVAL` result proving zero changed files.
- [ ] Owner approval receipt bound to migration, participant, owner, exact
      mapping, timestamp, and evidence URL.
- [ ] Three unmerged candidate branch names and commit SHAs.
- [ ] Fresh pre-commit DataHub fingerprint equal to the frozen fingerprint.
- [ ] Coordinated PR receipt and Producer gate-release receipt with
      `auto_merge: false` visible.
- [ ] DataHub write-back journal and official-MCP read-back for all four assets.
- [ ] Final state with source `COMMITTED`, consumers `VERIFIED`, and zero
      unverified consumers.
- [ ] SHA-256 manifest generated after every evidence file is present, followed
      by a successful manifest verification.
- [ ] Full automated test output from the same revision used for the recording.
- [ ] Git commit SHA of the application revision and the immutable evidence URL
      used by the public replay.
- [ ] Secret scan output covering tracked files, generated evidence, and the
      static demo bundle.

## Public replay disclosure

Place this language above the fold and in the video description:

> Verified interactive replay. The browser runs deterministic playback, is not
> connected to DataHub or GitHub, and performs no external mutation. The linked
> bundles and video record verified DataHub OSS live runs.

The public page links the live evidence manifest and the watched, published
2:56 demonstration video.
