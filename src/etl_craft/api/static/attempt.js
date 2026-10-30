// One attempt's log: read it once, keep the last part on screen, and while the attempt is live
// fetch only the bytes after what was read, from the same API route the CLI uses.
const log = document.querySelector("#log");
const follow = document.querySelector("#log-follow");
const state = document.querySelector("#log-state");
const KEEP = 256 * 1024;
const LIVE = new Set(["QUEUED", "CLAIMED", "RUNNING"]);
const decoder = new TextDecoder();
let offset = 0;
let text = "";
let trimmed = false;

function show() {
  if (text.length > KEEP) {
    text = text.slice(text.length - KEEP);
    trimmed = true;
  }
  log.textContent = (trimmed ? "… earlier lines are in the download …\n" : "") + text;
  if (follow.checked) log.scrollTop = log.scrollHeight;
}

// ponytail: the first read streams the whole log to keep its tail; add a tail-size parameter to
// the logs read if attempt logs grow past a few hundred megabytes.
async function read() {
  const response = await fetch(`${log.dataset.logUrl}?offset=${offset}`, { headers: { Accept: "text/plain" } });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    state.textContent = body.detail || body.message || `Log unavailable (HTTP ${response.status}).`;
    if (offset === 0) log.textContent = "";
    return false;
  }
  const reader = response.body.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    offset += value.byteLength;
    text += decoder.decode(value, { stream: true });
  }
  if (offset === 0 && !text) log.textContent = "The log is empty.";
  else show();
  return true;
}

async function poll() {
  const response = await fetch(log.dataset.attemptUrl, { headers: { Accept: "application/json" } });
  const attempt = response.ok ? await response.json() : null;
  await read();
  if (attempt && !LIVE.has(attempt.status)) {
    state.textContent = `Attempt ended ${attempt.status}; reload for its final details.`;
    return;
  }
  setTimeout(poll, 2000);
}

read().then((ok) => {
  if (ok && log.dataset.live === "yes") setTimeout(poll, 2000);
});
follow.addEventListener("change", show);
