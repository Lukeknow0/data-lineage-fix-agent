"use strict";

const runPath = "evidence/20260716T091507467904Z";
const replayManifestPath = "lineagetx-replay.manifest.json";
const canonicalLegacyEvidenceFiles = Object.freeze([
  "EVIDENCE.md",
  "after.txt",
  "before.txt",
  "context.json",
  "finding.json",
  "patch.diff",
  "regression_test.py",
  "writeback.json",
]);
const canonicalReplayFiles = Object.freeze(["lineagetx-replay.json"]);
const sha256Pattern = /^[0-9a-f]{64}$/;
const statePath = ["DETECTED", "PREPARING", "NEEDS_APPROVAL", "PREPARED", "COMMITTED"];
const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const fallbackReplay = {
  migration: {
    id: "ltx-7ba06b0789512486f0f92f3c",
    changeIntent: "customer_id → customer_key",
  },
  timing: {
    detectedToPreparing: 380,
    dbtVerified: 930,
    airflowFirstFile: 1420,
    airflowVerified: 1910,
    approvalRequired: 2380,
    ownerApprovedToPrepared: 520,
    preparedToCommitted: 1080,
  },
};

let replayFixture = fallbackReplay;
let runToken = 0;
let currentState = "DETECTED";
let eventTotal = 0;
const timers = new Set();

const elements = {
  migrationState: document.querySelector("#migration-state"),
  start: document.querySelector("#start-replay"),
  reset: document.querySelector("#reset-replay"),
  abort: document.querySelector("#abort-replay"),
  approve: document.querySelector("#approve-replay"),
  producerGate: document.querySelector("#producer-gate-status"),
  verifiedCount: document.querySelector("#verified-count"),
  gate: document.querySelector("#gate-node"),
  gateMessage: document.querySelector("#gate-message"),
  gateReceipt: document.querySelector("#gate-receipt"),
  semanticWrites: document.querySelector("#semantic-writes"),
  eventLog: document.querySelector("#event-log"),
  eventCount: document.querySelector("#event-count"),
  writebackState: document.querySelector("#writeback-state"),
  writebackConsumers: document.querySelector("#writeback-consumers"),
  verifyEvidence: document.querySelector("#verify-evidence"),
  evidenceStatus: document.querySelector("#evidence-status"),
  liveProofStatus: document.querySelector("#live-proof-status"),
  legacyDiff: document.querySelector("#legacy-diff"),
};

const consumers = {
  dbt: {
    row: document.querySelector("#consumer-dbt"),
    state: document.querySelector("#dbt-state"),
    verification: document.querySelector("#dbt-verification"),
  },
  airflow: {
    row: document.querySelector("#consumer-airflow"),
    state: document.querySelector("#airflow-state"),
    verification: document.querySelector("#airflow-verification"),
  },
  semantic: {
    row: document.querySelector("#consumer-semantic"),
    state: document.querySelector("#semantic-state"),
    verification: document.querySelector("#semantic-verification"),
  },
};

function cancelTimers() {
  timers.forEach((timer) => window.clearTimeout(timer));
  timers.clear();
  runToken += 1;
}

function schedule(token, milliseconds, callback) {
  const delay = prefersReducedMotion ? Math.min(milliseconds, 40) : milliseconds;
  const timer = window.setTimeout(() => {
    timers.delete(timer);
    if (token === runToken) {
      callback();
    }
  }, delay);
  timers.add(timer);
}

function stateClassName(state) {
  return `state-${state.toLowerCase().replaceAll("_", "-")}`;
}

function setMigrationState(state) {
  currentState = state;
  elements.migrationState.textContent = state;
  elements.migrationState.className = `state-badge ${stateClassName(state)}`;
  elements.writebackState.textContent = state;

  document.querySelectorAll("[data-state-step]").forEach((step) => {
    const stepState = step.dataset.stateStep;
    step.classList.remove("is-current", "is-complete", "is-aborted");

    if (state === "ABORTED") {
      if (stepState === "ABORTED") {
        step.classList.add("is-current", "is-aborted");
      }
      return;
    }

    if (stepState === "ABORTED") {
      return;
    }

    const stepIndex = statePath.indexOf(stepState);
    const stateIndex = statePath.indexOf(state);
    if (stepIndex < stateIndex) {
      step.classList.add("is-complete");
    } else if (stepIndex === stateIndex) {
      step.classList.add("is-current");
    }
  });
}

function setConsumer(consumerName, state, label, verification) {
  const consumer = consumers[consumerName];
  consumer.row.dataset.consumerState = state;
  consumer.state.textContent = label || state;
  if (verification !== undefined) {
    consumer.verification.textContent = verification;
  }
  updateVerifiedCount();
}

function updateVerifiedCount() {
  const count = Object.values(consumers).filter(
    (consumer) => consumer.row.dataset.consumerState === "VERIFIED",
  ).length;
  elements.verifiedCount.textContent = String(count);
  elements.writebackConsumers.textContent = `${count} / 3`;
}

function setGate(state, message, receipt) {
  elements.gate.dataset.gateState = state;
  elements.gateMessage.textContent = message;
  elements.gateReceipt.textContent = receipt;
}

function appendEvent(time, state, message) {
  const item = document.createElement("li");
  const timeNode = document.createElement("time");
  const stateNode = document.createElement("strong");
  const messageNode = document.createElement("span");

  timeNode.textContent = time;
  stateNode.textContent = state;
  messageNode.textContent = message;
  item.append(timeNode, stateNode, messageNode);
  elements.eventLog.append(item);

  eventTotal += 1;
  elements.eventCount.textContent = `${eventTotal} ${eventTotal === 1 ? "event" : "events"}`;
  elements.eventLog.scrollTop = elements.eventLog.scrollHeight;
}

function resetEventLog() {
  elements.eventLog.replaceChildren();
  eventTotal = 0;
  appendEvent("T+00.0s", "DETECTED", "ChangeIntent registered; Producer PR gate closed.");
}

function resetReplay() {
  cancelTimers();
  setMigrationState("DETECTED");
  setConsumer("dbt", "PENDING", "PENDING", "No candidate written");
  setConsumer("airflow", "PENDING", "PENDING", "0 / 2 files verified");
  setConsumer("semantic", "PENDING", "PENDING", "Owner: Identity Data Owner");
  elements.semanticWrites.textContent = "0 writes";
  elements.approve.disabled = true;
  elements.approve.textContent = "Simulate owner approval";
  elements.abort.disabled = true;
  elements.start.disabled = false;
  elements.start.textContent = "Start replay";
  elements.producerGate.textContent = "BLOCKED";
  setGate(
    "BLOCKED",
    "3 consumers unverified — upstream change remains blocked.",
    "No coordination PR · no merge",
  );
  resetEventLog();
}

function startReplay() {
  resetReplay();
  const token = runToken;
  const timing = replayFixture.timing || fallbackReplay.timing;

  elements.start.disabled = true;
  elements.abort.disabled = false;

  schedule(token, timing.detectedToPreparing, () => {
    setMigrationState("PREPARING");
    setConsumer("dbt", "PREPARING", "PROPOSING", "Deterministic candidate constrained to one SQL path");
    setConsumer("airflow", "PREPARING", "PROPOSING", "Two-file allowlist loaded in isolated worktree");
    appendEvent("T+00.4s", "PREPARING", "Isolated worktrees opened at pinned base SHAs.");
  });

  schedule(token, timing.dbtVerified, () => {
    setConsumer("dbt", "VERIFIED", "VERIFIED", "SQLGlot AST + expanded/contract tests passed");
    appendEvent("T+00.9s", "VERIFIED", "dbt SQL candidate accepted by deterministic validators.");
  });

  schedule(token, timing.airflowFirstFile, () => {
    setConsumer("airflow", "VERIFYING", "VERIFYING 1/2", "1 / 2 files verified · cross-file consistency pending");
    appendEvent("T+01.4s", "VERIFYING", "Airflow DAG syntax passed; mapping parity still required.");
  });

  schedule(token, timing.airflowVerified, () => {
    setConsumer("airflow", "VERIFIED", "VERIFIED 2/2", "2 / 2 files verified · DAG/config mapping consistent");
    appendEvent("T+01.9s", "VERIFIED", "Airflow Python and JSON changes passed together.");
  });

  schedule(token, timing.approvalRequired, () => {
    setConsumer(
      "semantic",
      "NEEDS_APPROVAL",
      "NEEDS_APPROVAL",
      "Owner: Identity Data Owner · explicit mapping required",
    );
    elements.semanticWrites.textContent = "0 writes · policy hold";
    setMigrationState("NEEDS_APPROVAL");
    elements.approve.disabled = false;
    elements.start.disabled = false;
    elements.start.textContent = "Restart replay";
    setGate(
      "BLOCKED",
      "1 consumer unverified — upstream change remains blocked.",
      "2 candidate checks green · semantic write count: 0",
    );
    appendEvent("T+02.4s", "NEEDS_APPROVAL", "Ambiguous customer identity meaning refused; 0 semantic writes.");
  });
}

function approveReplay() {
  if (currentState !== "NEEDS_APPROVAL") {
    return;
  }

  const token = runToken;
  const timing = replayFixture.timing || fallbackReplay.timing;
  elements.approve.disabled = true;
  elements.start.disabled = true;
  elements.approve.textContent = "Simulated approval recorded";
  setConsumer(
    "semantic",
    "PREPARING",
    "OWNER APPROVED",
    "Simulated replay approval · deterministic mapping candidate staged",
  );
  elements.semanticWrites.textContent = "1 approved candidate write";
  appendEvent("T+02.6s", "APPROVED", "Simulated owner approval recorded for this replay.");

  schedule(token, timing.ownerApprovedToPrepared, () => {
    setConsumer(
      "semantic",
      "VERIFIED",
      "VERIFIED",
      "Approved mapping + contract-schema test passed",
    );
    setMigrationState("PREPARED");
    elements.producerGate.textContent = "READY";
    setGate(
      "BLOCKED",
      "0 consumers unverified — coordination receipt is being prepared.",
      "3 candidate commits ready · producer gate not yet released",
    );
    appendEvent("T+03.1s", "PREPARED", "All consumers verified; candidate commits sealed.");
    appendEvent(
      "T+03.2s",
      "WRITEBACK",
      "Replay shows the migration properties, Tag, owners, and evidence receipt that a live run writes to DataHub.",
    );
  });

  schedule(token, timing.preparedToCommitted, () => {
    setMigrationState("COMMITTED");
    elements.producerGate.textContent = "SAFE TO MERGE";
    setGate(
      "OPEN",
      "0 unverified consumers — upstream change is safe to merge.",
      "Candidate commits + coordination PR ready · not merged",
    );
    elements.abort.disabled = true;
    elements.start.disabled = false;
    elements.start.textContent = "Replay again";
    appendEvent("T+03.7s", "COMMITTED", "Producer gate released; coordination PR remains unmerged.");
  });
}

function abortReplay() {
  if (elements.abort.disabled) {
    return;
  }

  cancelTimers();
  setMigrationState("ABORTED");
  setConsumer("dbt", "ABORTED", "CLEANED", "Unmerged dbt candidate removed");
  setConsumer("airflow", "ABORTED", "CLEANED", "Unmerged two-file candidate removed");
  setConsumer("semantic", "ABORTED", "NO WRITE", "No deployed system rollback claimed");
  elements.semanticWrites.textContent = "0 retained candidate writes";
  elements.approve.disabled = true;
  elements.abort.disabled = true;
  elements.start.disabled = false;
  elements.start.textContent = "Start replay";
  elements.producerGate.textContent = "BLOCKED";
  setGate(
    "ABORTED",
    "Migration aborted — unmerged candidate changes were cleaned.",
    "No deployed rollback claimed · no coordination PR · no merge",
  );
  appendEvent("T+ABORT", "ABORTED", "LineageTX-owned worktrees and unmerged candidate branches cleaned.");
}

async function fetchChecked(path, responseType) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response[responseType]();
}

function hexDigest(buffer) {
  return Array.from(new Uint8Array(buffer), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

async function sha256(buffer) {
  return hexDigest(await crypto.subtle.digest("SHA-256", buffer));
}

function assertSafeRelativePath(path, label) {
  if (typeof path !== "string" || path.length === 0 || path.includes("\\")) {
    throw new Error(`${label} contains an invalid path`);
  }
  const segments = path.split("/");
  if (
    path.startsWith("/") ||
    segments.some(
      (segment) =>
        segment === "" ||
        segment === "." ||
        segment === ".." ||
        !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(segment),
    )
  ) {
    throw new Error(`${label} contains an unsafe path: ${path}`);
  }
  return path;
}

function staticUrl(path) {
  return assertSafeRelativePath(path, "Static URL")
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function assertSha256(value, label) {
  if (typeof value !== "string" || !sha256Pattern.test(value)) {
    throw new Error(`${label} must be a lowercase 64-character SHA-256`);
  }
  return value;
}

function assertExactFileSet(actualNames, expectedNames, label) {
  if (new Set(actualNames).size !== actualNames.length) {
    throw new Error(`${label} contains duplicate file paths`);
  }
  const actual = [...actualNames].sort();
  const expected = [...expectedNames].sort();
  if (
    actual.length !== expected.length ||
    actual.some((name, index) => name !== expected[index])
  ) {
    throw new Error(
      `${label} file set must be exactly: ${expectedNames.join(", ")}`,
    );
  }
}

function validateFlatManifest(manifest, expectedNames, label) {
  if (manifest === null || Array.isArray(manifest) || typeof manifest !== "object") {
    throw new Error(`${label} must be a JSON object`);
  }
  const names = Object.keys(manifest);
  assertExactFileSet(names, expectedNames, label);
  return names.map((name) => ({
    name: assertSafeRelativePath(name, label),
    sha256: assertSha256(manifest[name], `${label} hash for ${name}`),
  }));
}

async function verifyFlatManifest(manifestPath, rootPath, expectedNames, label) {
  const manifest = await fetchChecked(staticUrl(manifestPath), "json");
  const entries = validateFlatManifest(manifest, expectedNames, label);
  const verified = new Map();

  await Promise.all(
    entries.map(async (entry) => {
      const relativePath = rootPath ? `${rootPath}/${entry.name}` : entry.name;
      const bytes = await fetchChecked(staticUrl(relativePath), "arrayBuffer");
      const actualHash = await sha256(bytes);
      if (actualHash !== entry.sha256) {
        throw new Error(`${label} SHA-256 mismatch: ${entry.name}`);
      }
      verified.set(entry.name, bytes);
    }),
  );
  return verified;
}

function setLiveProofStatus(kind, message) {
  elements.liveProofStatus.className = "evidence-status";
  elements.liveProofStatus.classList.add(`is-${kind}`);
  elements.liveProofStatus.textContent = message;
}

async function verifyPublishedLiveEvidence(config) {
  if (!config || config.status === "pending") {
    setLiveProofStatus("pending", "Live DataHub OSS evidence: pending");
    return { status: "pending", fileCount: 0 };
  }
  if (config.status !== "published") {
    throw new Error("Live evidence status must be pending or published");
  }
  if (replayFixture.liveVerified !== true) {
    throw new Error("Published live evidence must explicitly set liveVerified=true");
  }
  if (!Array.isArray(config.expectedFiles) || config.expectedFiles.length === 0) {
    throw new Error("Published live evidence requires a non-empty canonical file list");
  }

  const manifestPath = assertSafeRelativePath(
    config.manifestPath,
    "Live evidence manifest",
  );
  if (!manifestPath.endsWith("/manifest.json")) {
    throw new Error("Live evidence manifest must end in /manifest.json");
  }
  const expectedFiles = config.expectedFiles.map((path) =>
    assertSafeRelativePath(path, "Live evidence file list"),
  );
  assertExactFileSet(expectedFiles, expectedFiles, "Live evidence file list");

  const manifest = await fetchChecked(staticUrl(manifestPath), "json");
  if (
    manifest === null ||
    Array.isArray(manifest) ||
    typeof manifest !== "object" ||
    manifest.schema_version !== 1 ||
    manifest.manifest_self_hash_excluded !== true
  ) {
    throw new Error("Live evidence manifest schema is invalid");
  }
  if (manifest.migration_id !== config.expectedMigrationId) {
    throw new Error("Live evidence belongs to another migration");
  }
  assertSha256(manifest.aggregate_sha256, "Live evidence aggregate hash");
  if (!Array.isArray(manifest.files)) {
    throw new Error("Live evidence manifest files must be an array");
  }

  const descriptors = manifest.files.map((entry) => {
    if (entry === null || Array.isArray(entry) || typeof entry !== "object") {
      throw new Error("Live evidence manifest contains an invalid file entry");
    }
    const path = assertSafeRelativePath(entry.path, "Live evidence manifest");
    const fileHash = assertSha256(
      entry.sha256,
      `Live evidence hash for ${path}`,
    );
    if (!Number.isSafeInteger(entry.size_bytes) || entry.size_bytes < 0) {
      throw new Error(`Live evidence size is invalid: ${path}`);
    }
    return { path, sha256: fileHash, size_bytes: entry.size_bytes };
  });

  const descriptorPaths = descriptors.map(({ path }) => path);
  assertExactFileSet(descriptorPaths, expectedFiles, "Live evidence manifest");
  const sortedPaths = [...descriptorPaths].sort();
  if (descriptorPaths.some((path, index) => path !== sortedPaths[index])) {
    throw new Error("Live evidence manifest file entries must be sorted by path");
  }

  const bundleRoot = manifestPath.slice(0, -"/manifest.json".length);
  await Promise.all(
    descriptors.map(async (entry) => {
      const bytes = await fetchChecked(
        staticUrl(`${bundleRoot}/${entry.path}`),
        "arrayBuffer",
      );
      if (bytes.byteLength !== entry.size_bytes) {
        throw new Error(`Live evidence size mismatch: ${entry.path}`);
      }
      if ((await sha256(bytes)) !== entry.sha256) {
        throw new Error(`Live evidence SHA-256 mismatch: ${entry.path}`);
      }
    }),
  );

  const aggregateBytes = new TextEncoder().encode(JSON.stringify(descriptors));
  if ((await sha256(aggregateBytes)) !== manifest.aggregate_sha256) {
    throw new Error("Live evidence aggregate SHA-256 mismatch");
  }

  setLiveProofStatus(
    "good",
    `Live DataHub OSS evidence: ${descriptors.length} files verified`,
  );
  return { status: "published", fileCount: descriptors.length };
}

async function verifyEvidence() {
  elements.verifyEvidence.disabled = true;
  elements.evidenceStatus.className = "evidence-status";
  elements.evidenceStatus.textContent = "Verifying replay and legacy evidence…";

  try {
    await verifyFlatManifest(
      replayManifestPath,
      "",
      canonicalReplayFiles,
      "Replay manifest",
    );
    await verifyFlatManifest(
      `${runPath}/manifest.json`,
      runPath,
      canonicalLegacyEvidenceFiles,
      "Legacy evidence manifest",
    );
    const liveResult = await verifyPublishedLiveEvidence(replayFixture.liveEvidence);

    elements.evidenceStatus.classList.add("is-good");
    elements.evidenceStatus.textContent =
      liveResult.status === "published"
        ? `Replay fixture, all 8 legacy artifacts, and ${liveResult.fileCount} live artifacts verified by SHA-256`
        : "Replay fixture + all 8 legacy artifacts verified; live OSS proof pending";
  } catch (error) {
    elements.evidenceStatus.classList.add("is-bad");
    elements.evidenceStatus.textContent = `Verification failed: ${error.message}`;
  } finally {
    elements.verifyEvidence.disabled = false;
  }
}

async function loadLegacyDiff() {
  try {
    elements.legacyDiff.textContent = await fetchChecked(`${runPath}/patch.diff`, "text");
  } catch (error) {
    elements.legacyDiff.textContent = `Captured patch unavailable: ${error.message}`;
  }
}

async function loadReplayFixture() {
  try {
    const verified = await verifyFlatManifest(
      replayManifestPath,
      "",
      canonicalReplayFiles,
      "Replay manifest",
    );
    const replayBytes = verified.get("lineagetx-replay.json");
    const fixture = JSON.parse(new TextDecoder().decode(replayBytes));
    if (fixture.migration && fixture.timing) {
      replayFixture = fixture;
      document.documentElement.dataset.replaySource = "fixture";
      if (fixture.liveEvidence?.status === "pending") {
        setLiveProofStatus("pending", "Live DataHub OSS evidence: pending");
      }
    }
  } catch (_error) {
    document.documentElement.dataset.replaySource = "embedded-fallback";
    setLiveProofStatus("bad", "Replay fixture integrity check failed; embedded fallback active");
  }
}

elements.start.addEventListener("click", startReplay);
elements.reset.addEventListener("click", resetReplay);
elements.abort.addEventListener("click", abortReplay);
elements.approve.addEventListener("click", approveReplay);
elements.verifyEvidence.addEventListener("click", verifyEvidence);

resetReplay();
loadReplayFixture();
loadLegacyDiff();
