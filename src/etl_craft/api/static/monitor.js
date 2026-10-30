// Actions on the selected scope: preview the exact scope and effect, then post the same
// request the CLI sends. Receipt is not completion: the result shows the recorded status.
const dialog = document.querySelector("#control-dialog");
const form = document.querySelector("#control-form");
const csrf = () => document.querySelector('meta[name="csrf-token"]')?.content || "";
const DEFINITION = new Set(["PAUSE", "RESUME"]);
let pending = null;
let opener = null;

function field(name, value) {
  dialog.querySelector(`[data-field="${name}"]`).textContent = value;
}

function selected() {
  return [...document.querySelectorAll("[data-select]:checked")].map((box) => box.dataset.select);
}

function open(button, kind) {
  opener = button;
  const action = kind === "run" ? "RUN" : button.dataset.control || button.dataset.bulk;
  const codes = kind === "bulk" ? selected() : [];
  pending = { action, kind, codes, key: crypto.randomUUID() };
  field("action", action.replaceAll("_", " ").toLowerCase());
  field("scope", kind === "bulk" ? `${codes.length} selected pipelines: ${codes.join(", ")}` : button.dataset.scope);
  field("effect", button.dataset.effect);
  field("preserves", button.dataset.preserves === "no" ? "Not kept: this creates new work" : "Kept");
  form.reset();
  dialog.querySelector(".resume-only").hidden = !(action === "RESUME" && button.dataset.scheduled === "yes");
  dialog.querySelector(".run-only").hidden = kind !== "run";
  dialog.querySelector("#control-error").hidden = true;
  dialog.showModal();
}

function control(pipeline, suffix = "") {
  const request = { pipeline_code: pipeline, action: pending.action, reason: form.reason.value };
  request.request_key = pending.key + suffix;
  if (!DEFINITION.has(pending.action) && dialog.dataset.run) request.run_id = Number(dialog.dataset.run);
  if (dialog.dataset.task) request.task_code = dialog.dataset.task;
  if (!dialog.querySelector(".resume-only").hidden) request.then = form.then.value;
  return request;
}

function request() {
  if (pending.kind === "run") {
    const body = { pipeline_code: dialog.dataset.pipeline, reason: form.reason.value || null };
    if (form.run_date.value) body.run_date = form.run_date.value;
    return { url: dialog.dataset.runsUrl, body };
  }
  if (pending.kind === "bulk") {
    const requests = pending.codes.map((code, index) => control(code, `-${index}`));
    return { url: `${dialog.dataset.url}/bulk`, body: { requests } };
  }
  return { url: dialog.dataset.url, body: control(dialog.dataset.pipeline) };
}

function lines(result) {
  if (result.outcomes) {
    return result.outcomes.map((o) => `${o.pipeline_code}: ${o.error ? "refused: " + o.error : "requested"}`);
  }
  const ids = [result.control_id && `Control ${result.control_id}`, result.pipeline_run_id && `Run ${result.pipeline_run_id}`];
  return [[result.message || "Request recorded.", ...ids.filter(Boolean)].join(" ")];
}

function announce(result) {
  const section = document.createElement("section");
  section.className = "notice";
  section.setAttribute("role", "status");
  const heading = document.createElement("h2");
  heading.textContent = `${pending.action.replaceAll("_", " ")} ${result.status || result.outcome || "REQUESTED"}`;
  const list = document.createElement("ul");
  for (const line of lines(result)) {
    const item = document.createElement("li");
    item.textContent = line;
    list.append(item);
  }
  const reload = document.createElement("a");
  reload.href = location.pathname + location.search;
  reload.textContent = "Reload to see the current state";
  section.append(heading, list, reload);
  document.querySelector("#result").replaceChildren(section);
}

async function submit(event) {
  event.preventDefault();
  const error = dialog.querySelector("#control-error");
  const confirm = dialog.querySelector("#control-confirm");
  const { url, body } = request();
  confirm.disabled = true;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json", "X-CSRF-Token": csrf() },
      body: JSON.stringify(body),
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
      error.textContent = result.detail || result.message || `Request refused (HTTP ${response.status}).`;
      error.hidden = false;
      error.focus();
      return;
    }
    dialog.close();
    announce(result);
  } finally {
    confirm.disabled = false;
  }
}

function refreshSelection() {
  const codes = selected();
  document.querySelector("#selected-count").textContent = codes.length
    ? `${codes.length} pipeline${codes.length === 1 ? "" : "s"} selected`
    : "No pipelines selected";
  document.querySelectorAll("[data-bulk]").forEach((button) => { button.disabled = codes.length === 0; });
  const all = document.querySelector("#select-all");
  const boxes = document.querySelectorAll("[data-select]");
  all.checked = codes.length > 0 && codes.length === boxes.length;
  all.indeterminate = codes.length > 0 && codes.length < boxes.length;
}

function csvCell(text) {
  const value = text.replace(/\s+/g, " ").trim();
  const safe = /^[=+\-@\t\r]/.test(value) ? `'${value}` : value;
  return /[",\n]/.test(safe) ? `"${safe.replaceAll('"', '""')}"` : safe;
}

function exportCsv(button) {
  const table = document.getElementById(button.dataset.csv);
  const rows = [...table.querySelectorAll("tr")].map((row) =>
    [...row.querySelectorAll("th, td")]
      .filter((cell) => !cell.querySelector("input[type=checkbox]"))
      .map((cell) => csvCell(cell.textContent))
      .join(","),
  );
  const link = document.createElement("a");
  link.href = URL.createObjectURL(new Blob([rows.join("\r\n") + "\r\n"], { type: "text/csv" }));
  link.download = button.dataset.csvName;
  link.click();
  URL.revokeObjectURL(link.href);
}

// Listeners sit on the document so content replaced by live updates keeps working.
document.addEventListener("click", (event) => {
  const button = event.target.closest("button");
  if (!button || !dialog) return;
  if (button.matches("[data-control]")) open(button, "control");
  else if (button.matches("[data-bulk]")) open(button, "bulk");
  else if (button.matches("[data-run-now]")) open(button, "run");
});
document.addEventListener("click", (event) => {
  const button = event.target.closest("[data-csv]");
  if (button) exportCsv(button);
});
document.addEventListener("change", (event) => {
  if (event.target.matches("#select-all")) {
    document.querySelectorAll("[data-select]").forEach((box) => { box.checked = event.target.checked; });
  }
  if (event.target.matches("#select-all, [data-select]")) refreshSelection();
});
if (dialog) {
  form.addEventListener("submit", submit);
  dialog.querySelector("#control-cancel").addEventListener("click", () => dialog.close());
  dialog.addEventListener("close", () => {
    if (opener?.isConnected) opener.focus();
    if (stale) refresh();
  });
}
if (document.querySelector("#select-all")) refreshSelection();

// Live updates: the server re-reads this page's documents and says when they changed. Fresh
// content replaces the page's live region unless someone is working in it; then a notice offers
// the refresh instead, so a selection or an open dialog never changes under the user.
const region = document.querySelector("#live[data-live-url]");
const updated = document.querySelector("#live-updated");
const notice = document.querySelector("#live-notice");
const refreshButton = document.querySelector("#live-refresh");
let checkedAt = Date.now();
let stale = false;

function busy() {
  return dialog?.open || region.contains(document.activeElement) && document.activeElement !== region;
}

async function refresh() {
  const kept = new Set(selected());
  const response = await fetch(location.href, { headers: { Accept: "text/html" } });
  if (!response.ok) {
    notice.textContent = `Could not refresh (HTTP ${response.status}). Reload the page.`;
    return;
  }
  const fresh = new DOMParser().parseFromString(await response.text(), "text/html").querySelector("#live");
  if (!fresh) return;
  region.replaceChildren(...fresh.childNodes);
  region.querySelectorAll("[data-select]").forEach((box) => { box.checked = kept.has(box.dataset.select); });
  if (document.querySelector("#select-all")) refreshSelection();
  stale = false;
  refreshButton.hidden = true;
  notice.textContent = "";
}

function changed() {
  if (busy()) {
    stale = true;
    refreshButton.hidden = false;
    notice.textContent = "This page has changed since it loaded.";
  } else {
    refresh();
  }
}

if (region && typeof EventSource === "function") {
  const source = new EventSource(region.dataset.liveUrl);
  const mark = (event) => {
    checkedAt = Date.parse(event.data) || Date.now();
    if (notice.textContent.startsWith("Live updates lost")) notice.textContent = "";
  };
  source.addEventListener("checked", mark);
  source.addEventListener("changed", (event) => { mark(event); changed(); });
  source.addEventListener("ended", (event) => {
    source.close();
    notice.textContent = `Live updates stopped: ${event.data}`;
  });
  source.addEventListener("error", () => {
    if (source.readyState !== EventSource.CLOSED) notice.textContent = "Live updates lost, reconnecting…";
  });
  refreshButton.addEventListener("click", refresh);
  const tick = () => { updated.textContent = `Updated ${Math.max(0, Math.round((Date.now() - checkedAt) / 1000))} s ago.`; };
  tick();
  setInterval(tick, 1000);
}
