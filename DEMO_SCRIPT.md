# Demo Script, Recording Order, and Screenshot Checklist

> Legacy feasibility script. Retained to preserve the verified single-consumer
> baseline; it is superseded for final judging by the LineageTX transaction demo.

Target length: **2:35**. Hard stop before 3:00.

## Recording preparation

- Use a clean terminal at 1440×900 or 1920×1080 with font size 18–20.
- Hide notifications, shell history, environment variables, bookmarks, and unrelated windows.
- Use the synthetic fixture only. Do not show `.env`, tokens, Docker settings, or private repositories.
- Run `./scripts/bootstrap.sh` before recording so installation time is not in the video.
- Keep these files pre-opened: broken SQL, `context.json` from the latest live run, `patch.diff`, `before.txt`, `after.txt`, and `writeback.json`.
- No music or third-party logos beyond descriptive DataHub usage.

## Spoken script and screen actions

### 0:00–0:18 — Problem

Screen: title and one-line architecture in `README.md`.

Voice: “A schema change can break dozens of downstream assets. Most agents only suggest what to do. DataLineage Fix Agent reads DataHub, repairs the mapped repository code, proves the repair, and writes verified status back.”

### 0:18–0:35 — Planted but realistic break

Screen: `fixture_pipeline/broken/pipeline/customer_revenue.sql` with the three `customer_id` references highlighted.

Voice: “The source moved to schema contract v2 and renamed `customer_id` to `customer_key`. This downstream revenue mart still uses the old field in SELECT, GROUP BY, and ORDER BY.”

### 0:35–1:02 — Run the real gate

Screen: terminal.

```bash
make gate-live
```

Voice: “This command seeds synthetic metadata into DataHub Lite, launches the official DataHub MCP Server, and runs the complete action loop. The replay is not used here.”

Pause on:

```text
context=datahub-oss-lite-v1.6.0+official-mcp-server-v0.6.0
red_exit=1
green_exit=0
writeback=written-and-read-back-via-datahub-mcp
LIVE GATE: PASS
```

### 1:02–1:28 — Prove DataHub grounding

Screen: compact `context.json`/`examples/verified-run/context_summary.json`.

Voice: “The MCP trace contains `get_entities`, `list_schema_fields`, and `get_lineage`. DataHub returns the current fields, the explicit rename description, the affected downstream URN, the technical owner, and the drift signals. If any required context is absent, the agent refuses to patch.”

Highlight the exact source and downstream URNs and MCP result hashes.

### 1:28–1:56 — Red to green

Screen sequence: `before.txt` → `patch.diff` → `after.txt`.

Voice: “The generated regression first fails with `no such column: customer_id`. SQLGlot verifies the planned references, the agent applies only the proven identifier replacement, and the exact same test turns green.”

### 1:56–2:20 — DataHub write-back

Screen: `writeback.json`.

Voice: “Only after green verification does the agent call MCP `add_tags`. It immediately reads the downstream entity again and proves that `DataLineageFixVerified` is present. This result and its hashes are included in the evidence bundle.”

### 2:20–2:35 — Close

Screen: `EVIDENCE.md` and the evidence-file list.

Voice: “In one reproducible run, the judge can see real DataHub context, a real failure, the minimal patch, a passing regression, and verified graph state—not a chat recommendation.”

## Recording order

1. Record the entire terminal run once without cuts.
2. Record short file close-ups in the order: SQL, context, red output, diff, green output, write-back, evidence manifest.
3. Assemble with only straight cuts; keep the command/result chronology intact.
4. Add concise English captions for URNs and red/green exits.
5. Export at 1080p, watch the final file end-to-end, and confirm duration under 3:00.
6. Upload only after user confirmation; make the final video public on an allowed host.

## Screenshot checklist

- [ ] Hero: architecture + “DataHub context → verified fix”.
- [ ] Broken SQL with all three stale references visible.
- [ ] MCP context showing schema, lineage, owner, and signals.
- [ ] Context refusal test name in `make verify` output.
- [ ] Red regression with `no such column: customer_id`.
- [ ] Minimal unified diff.
- [ ] Green regression with `OK`.
- [ ] MCP `add_tags` success and `get_entities` read-back hash.
- [ ] Evidence bundle file list and manifest.
- [ ] Final `LIVE GATE: PASS` terminal frame.

Avoid screenshots of registration counts, prize totals, private tokens, or only the static Markdown summary.
