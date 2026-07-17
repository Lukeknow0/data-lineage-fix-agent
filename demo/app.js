const runPath = "evidence/20260716T091507467904Z";
const diffNode = document.querySelector("#diff");
const statusNode = document.querySelector("#status");
const verifyButton = document.querySelector("#verify");

async function fetchChecked(path, responseType) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response[responseType]();
}

async function loadDiff() {
  try {
    diffNode.textContent = await fetchChecked(`${runPath}/patch.diff`, "text");
  } catch (error) {
    diffNode.textContent = `Captured patch unavailable: ${error.message}`;
    diffNode.classList.add("bad");
  }
}

function hexDigest(buffer) {
  return Array.from(new Uint8Array(buffer), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

async function sha256(buffer) {
  return hexDigest(await crypto.subtle.digest("SHA-256", buffer));
}

async function verifyEvidence() {
  verifyButton.disabled = true;
  statusNode.className = "";
  statusNode.textContent = "Verifying 8 evidence files…";

  try {
    const manifest = await fetchChecked(`${runPath}/manifest.json`, "json");
    const entries = Object.entries(manifest);
    const results = await Promise.all(
      entries.map(async ([name, expectedHash]) => {
        const bytes = await fetchChecked(`${runPath}/${name}`, "arrayBuffer");
        return { name, matches: (await sha256(bytes)) === expectedHash };
      }),
    );
    const mismatches = results.filter(({ matches }) => !matches);

    if (mismatches.length) {
      statusNode.className = "bad";
      statusNode.textContent = `✕ Evidence mismatch: ${mismatches
        .map(({ name }) => name)
        .join(", ")}`;
      return;
    }

    statusNode.className = "good";
    statusNode.textContent = `✓ All ${entries.length} evidence files verified`;
  } catch (error) {
    statusNode.className = "bad";
    statusNode.textContent = `✕ Verification failed: ${error.message}`;
  } finally {
    verifyButton.disabled = false;
  }
}

verifyButton.addEventListener("click", verifyEvidence);
loadDiff();
