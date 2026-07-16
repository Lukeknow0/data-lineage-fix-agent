# Project Status

Last updated: 2026-07-16 17:11 CST

Overall: **GO — executable gate passed**

Primary challenge: **Agents That Do Real Work**

## Completed

- [x] Official rules, eligibility, AI-use, new-project, Apache-2.0, judging, submission, and payout snapshot.
- [x] New Apache-2.0 project isolated to this directory.
- [x] Synthetic DataHub entities for schema, lineage, ownership, and drift signals.
- [x] Official DataHub MCP Server integration over stdio.
- [x] Safe context refusal and explicit-rename planner.
- [x] Generated executable regression test.
- [x] Minimal SQL identifier patch.
- [x] Red-to-green execution proof.
- [x] DataHub verified-status write-back and MCP read-back.
- [x] Per-run evidence bundle and SHA-256 manifest.
- [x] Offline replay fixture captured from a live local run.
- [x] Required automated tests and live MCP integration test.
- [x] One-command start (`make start`), verify (`make verify`), and live gate (`make gate-live`).
- [x] README, rules snapshot, gate decision, evidence, third-party disclosure, submission draft, and demo plan.

## Verification record

```text
make verify
4 passed, 1 live integration test skipped by default

make gate-live
status=verified-fixed
red_exit=1
green_exit=0
writeback=written-and-read-back-via-datahub-mcp
1 live integration test passed
LIVE GATE: PASS
```

## Not performed — explicit approval gates

- [ ] Create or publish a public Git repository.
- [ ] Deploy a public demo.
- [ ] Upload a public video.
- [ ] Register or submit on Devpost.
- [ ] Share any entrant identity, KYC, tax, or banking information.

## Remaining pre-submission work

1. Recheck official rules and get written payout/KYC clarification for mainland-China residence plus Hong Kong ZA Bank.
2. Preferably repeat the gate against a full DataHub v1.6.0 GMS; the first Docker attempt was blocked by external image-registry EOF errors.
3. Decide whether to add a model-assisted proposal layer. It must remain optional and may not bypass the deterministic context/test guard.
4. After approval, create the public Apache-2.0 repository and set the license in the repository About section.
5. After approval, record/upload the under-three-minute video and fill the URL placeholders in `SUBMISSION_DRAFT.md`.
6. Run a clean-machine verification and scan the repository for secrets before publication.

## Current risk assessment

- Product-proof risk: low; the core loop is executable and repeatable.
- DataHub-integration risk: low for the gate; medium until full-GMS video validation.
- Judging-position risk: medium; deterministic safety is strong, but optional model-assisted planning may improve the “AI agent” story if added without weakening evidence.
- Payout risk: medium; official bank transfer language is favorable, but processor/ZA Bank/mainland-China support is not guaranteed.
- Publication risk: controlled; all consequential external actions remain blocked on confirmation.
## Public release authorization (2026-07-16)

The owner authorized creating a public repository and deploying the static Demo. Devpost submission remains intentionally deferred. The `demo/` directory is a zero-secret evidence replay; the real live gate remains local (`make gate-live`).
