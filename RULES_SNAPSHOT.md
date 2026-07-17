# Build with DataHub: The Agent Hackathon — Rules Snapshot

> Frozen at 2026-07-16 16:28 CST (UTC+8). This is an operational snapshot, not legal advice. Recheck the official rules immediately before registration, publishing the repository/demo/video, and final submission.

## Change check against the 2026-07-16 bounty screening note

**No critical rule change found.** The current official rules still support the
planned build: AI coding assistants are allowed, a new project may disclose
pre-existing inputs, the repository must use Apache-2.0, and prize delivery may
be electronic after winner verification. Volatile participant counts are not a
product or eligibility signal and are intentionally omitted.

One source-detail difference is worth preserving: the overview summarizes video hosts as YouTube or Vimeo, while the full Official Rules also allow Youku. The full rules control; the safer submission choice remains a public YouTube or Vimeo link if accessible to the judges.

## Frozen terms

### Dates and deadline

- Registration/submission: July 6, 2026 at 09:00 ET through **August 10, 2026 at 17:00 ET**.
- At the event date this is EDT (UTC-4), so the submission deadline is **August 11, 2026 at 05:00 China Standard Time (UTC+8)**.
- Judging: August 17, 2026 at 10:00 ET through August 31, 2026 at 17:00 ET.
- Winners: on or around September 8, 2026 at 14:00 ET.
- Do not plan to the converted minute; use an internal submission cutoff at least 24 hours earlier.

### Eligibility

- Individuals must be at least 18 or the age of majority where they reside. Eligible teams and legally formed organizations may also enter.
- Exclusions include places where US or local law prohibits participation or prize receipt; the rules name Brazil, Quebec, Russia, Crimea, Cuba, Iran, North Korea, and other OFAC-designated locations.
- Entrants must independently confirm that neither US nor local law prohibits
  their participation or receipt of a prize.
- Sponsor/administrator personnel, judges, specified relatives/households/affiliates, and conflicts of interest are excluded.
- Award is conditional on identity, qualification, and authorship-role verification. A prospective winner is not final until required affidavits/forms are completed and verified.

Operational conclusion: prospective entrants must independently revalidate their
location eligibility and the sponsor's payout/KYC process before submission. The
published rules do not guarantee support for any specific bank, country, currency,
or transfer rail.

### Prizes

Total advertised cash: **USD 20,500**.

| Award | Quantity | Cash per award | Other benefit |
|---|---:|---:|---|
| Grand Prize | 1 | $6,000 | DataHub Town Hall presentation, community promotion, LinkedIn badge |
| Challenge Winner | 4 | $3,000 | One per challenge; community promotion, LinkedIn badge |
| Honourable Mention | 2 | $1,000 | LinkedIn badge |
| Most Valuable Feedback Survey | 10 | $50 | Individual feedback award |

- Each eligible project submission may win one project prize.
- Prizes are non-transferable; the sponsor may substitute equal-or-greater value and may decline to award if no eligible submission exists.
- Registration/participant counts are not a judging criterion and are intentionally excluded from the GO/NO-GO decision.

### Payment and verification path

1. Potential winner completes the winner affidavit and other required forms within **10 business days** after they are sent.
2. Non-US winners may be required to provide tax/compliance information such as **W-8BEN**.
3. After the completed forms are received, a monetary prize may be mailed or **sent electronically to the entrant/representative/organization bank account**.
4. Delivery is due within **60 days after Sponsor or Devpost receives the completed required forms**.
5. Winner bears wire, FX, tax, reporting, and local foreign-exchange/banking compliance obligations; withholding may apply.

The rules do not identify the payment processor, supported bank countries,
transfer currency/rail, or intermediary-bank requirements. Reconfirm these
details in writing before final submission if possible.

### New-project and AI-use boundary

- The project and submitted work must be newly created during the July 6–August 10 submission period.
- Standard tools, frameworks, libraries, starter templates, and **AI coding assistants are explicitly allowed**.
- Any other pre-existing code or work incorporated into the project must be disclosed.
- The submission must be original, solely owned by the entrant/team/organization, authorized for all third-party integrations/data, and compliant with open-source licenses.

This repository uses the architectural ideas—not copied source—from the pre-period local `bounty_monitor.py` (context packaging and evidence capture) and `bounty_solver.py` (bounded test-fix-test loop). The implementation in this repository is new. See `THIRD_PARTY_NOTICES.md` for the final disclosure.

### Mandatory DataHub integration

The project must be a working application that uses **DataHub open source** plus at least one of:

- DataHub MCP Server;
- Agent Context Kit;
- DataHub Skills; or
- Analytics Agent.

The planned gate uses the official **DataHub MCP Server** against a local DataHub OSS instance. It must read real catalog context (schema, lineage, ownership and/or quality/governance signals), take a real repository action, and write evidence/status back to DataHub where appropriate. A JSON fixture alone is an offline replay aid, not the claimed live integration.

### Challenge categories

1. Agents That Do Real Work
2. Metadata-Aware Code Generation & Development
3. Production ML Agents
4. Open / Wildcard

The final primary challenge is selected only after the executable gate. The current product shape best matches **Agents That Do Real Work** because it reads the graph, diagnoses downstream breakage, patches code, verifies the repair, and writes the outcome back. It will not claim simultaneous optimization for every category.

### Judging

Stage One is pass/fail for baseline viability, theme fit, and reasonable use of the required DataHub APIs/SDKs. Stage Two uses equally weighted criteria:

1. meaningful DataHub use, with write-back favored where appropriate;
2. technical execution and end-to-end correctness;
3. originality beyond rebuilding an existing DataHub feature;
4. real-world usefulness;
5. submission/demo/README quality.

Meaningful open-source contributions to DataHub (connectors, skills, fixes, RFCs, docs) are an optional bonus.

### Repository, demo, video and submission requirements

- A URL giving judges easy access to a working project (website, functioning demo, test build, or repository with clear runnable instructions).
- A **public** source repository with all source/assets/instructions and an **Apache License 2.0** file visible/detectable at the repository top level/About area.
- A text description of features, behavior, technologies, and data.
- A public demonstration video **under three minutes**. Judges need not watch beyond three minutes. It must show the project actually functioning.
- The full rules permit public YouTube, Vimeo, or Youku hosting; the overview mentions YouTube/Vimeo.
- No unlicensed third-party marks, copyrighted music, or other unlicensed material in the video.
- English submission materials, or English translations for all non-English materials.
- Free, unrestricted judge access through the end of judging. Private demos must include credentials.
- Sample generated outputs in an `examples/` folder are recommended because judges may decide solely from description, images, and video and are not required to run the project.
- No material submission edits after the deadline except changes expressly permitted by Sponsor/Devpost.

### Publication gates

Do **not** create a public repository, deploy a public demo, upload a video, register/submit, or expose any private data without the user's explicit confirmation. Local implementation and local verification are authorized.

## Four-hour feasibility gate

GO requires one reproducible path with machine evidence:

`DataHub OSS + official MCP context → planted real pipeline bug → grounded diagnosis → minimal patch + regression test → red-to-green test → evidence/status traceable to a DataHub URN`

Immediate NO-GO if the result is only LLM prose/static UI, if DataHub context is mocked in the claimed live run, if the repair cannot execute, or if the evidence cannot tie the changed code and test result to a DataHub entity.

## Official sources

- Official Rules: https://datahub.devpost.com/rules
- Official overview/submission page: https://datahub.devpost.com/
- Official DataHub announcement: https://datahub.com/blog/build-with-datahub-agent-hackathon/
- Official resources: https://datahub.devpost.com/resources
- Official DataHub MCP Server: https://github.com/acryldata/mcp-server-datahub

## Pre-registration / pre-submission recheck checklist

- [ ] Deadline and timezone unchanged.
- [ ] Entrant location remains eligible under the current official rules.
- [ ] AI assistant and pre-existing-code disclosure language unchanged.
- [ ] Apache-2.0/public-repository requirements unchanged.
- [ ] Primary challenge and required DataHub technology still listed.
- [ ] Project/demo/video fields and public-host requirements unchanged.
- [ ] Electronic bank payout and 60-day clock unchanged.
- [ ] Confirm location eligibility, payment processor/rail, and required KYC/tax
      documents directly with Devpost or the sponsor.
